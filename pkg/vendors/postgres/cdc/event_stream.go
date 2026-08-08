// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"context"
	"iter"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gsoultan/gpool/pkg/gpool/cdc"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// statusTimeout bounds a single standby status update.
const statusTimeout = 5 * time.Second

// pgEventStream is a live logical replication stream implementing cdc.EventStream.
//
// One goroutine owns the connection for the stream's whole life: it reads WAL,
// decodes it, and sends every standby status update. Nothing else touches the
// connection, because pgconn.PgConn is not safe for concurrent use and its internal
// busy check is a debug assertion rather than a lock.
//
// Decoding is decoupled from consumption by a buffered channel so a slow consumer
// cannot stall the keepalives. The reader keeps confirming its position even while
// blocked handing an event over, which is what stops the server from dropping the
// connection on wal_sender_timeout.
type pgEventStream struct {
	conn     *pgconn.PgConn
	ctx      context.Context
	cancel   context.CancelFunc
	events   chan pendingEvent
	interval time.Duration

	// received is the highest WAL position read off the wire.
	received atomic.Uint64
	// lastPushed is the position of the most recent event handed to the channel.
	lastPushed atomic.Uint64
	// flushed is the position the consumer has finished processing. It is what the
	// server records as confirmed_flush_lsn, and therefore what releases WAL.
	flushed atomic.Uint64

	mu  sync.Mutex
	err error

	iterating atomic.Bool
	closeOnce sync.Once
	closeErr  error
	onClose   func()
	done      chan struct{}
}

var _ cdc.EventStream = (*pgEventStream)(nil)

// newEventStream starts the reader goroutine for an already-started replication
// connection. The stream takes ownership of conn.
//
// PostgreSQL retains WAL behind an unconfirmed position, so a consumer that
// stops draining this stream grows the primary's disk rather than losing data —
// the opposite of the failure mode on a source with an expiring log.
func newEventStream(parent context.Context, conn *pgconn.PgConn, config Config, onClose func()) *pgEventStream {
	// The stream outlives the call that created it, so it is detached from the
	// caller's context: cancelling Subscribe's context must not tear down a stream
	// that is already handed over. Close is the way to stop it.
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))

	s := &pgEventStream{
		conn:     conn,
		ctx:      ctx,
		cancel:   cancel,
		events:   make(chan pendingEvent, config.Buffer),
		interval: config.StandbyInterval,
		onClose:  onClose,
		done:     make(chan struct{}),
	}

	go s.run()
	return s
}

// All iterates the stream, confirming each event's position only after the loop
// body has returned. Delivery is therefore at-least-once.
func (s *pgEventStream) All() iter.Seq[cdc.Event] {
	return func(yield func(cdc.Event) bool) {
		// A second concurrent iteration would silently split the stream between two
		// consumers, each seeing an arbitrary subset.
		if !s.iterating.CompareAndSwap(false, true) {
			return
		}
		defer s.iterating.Store(false)
		defer func() { _ = s.Close() }()

		for pending := range s.events {
			if !yield(pending.event) {
				return
			}
			advance(&s.flushed, pending.lsn)
		}
	}
}

// Err returns the error that terminated the stream, if any.
func (s *pgEventStream) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Close stops the stream and releases its connection. It is idempotent.
//
// It cancels the reader and waits for it to exit before closing the connection, so
// the connection is never touched from two goroutines. The reader sends a final
// status update on its way out, so work the consumer already finished is not replayed.
func (s *pgEventStream) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		<-s.done

		closeCtx, cancel := context.WithTimeout(context.Background(), statusTimeout)
		s.closeErr = s.conn.Close(closeCtx)
		cancel()

		if s.onClose != nil {
			s.onClose()
		}
	})
	return s.closeErr
}

// run is the reader loop. It is the only goroutine that touches s.conn.
func (s *pgEventStream) run() {
	// Ordering matters: the events channel closes first so a waiting consumer wakes
	// up, then the final position is confirmed, then done unblocks Close so it can
	// close the connection knowing this goroutine is finished with it.
	defer close(s.done)
	defer s.sendFinalStatus()
	defer close(s.events)

	relations := make(map[uint32]*pglogrepl.RelationMessage)
	// pgoutput sends the commit time once, in the Begin that opens a transaction,
	// and every change in that transaction carries it.
	var committed time.Time
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if !s.sendStatus() {
				return
			}
		default:
		}

		msg, err := s.receive()
		if err != nil {
			if s.ctx.Err() != nil {
				return
			}
			if pgconn.Timeout(err) {
				continue
			}
			s.setErr(err)
			return
		}

		copyData, ok := msg.(*pgproto3.CopyData)
		if !ok || len(copyData.Data) == 0 {
			continue
		}

		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			if !s.handleKeepalive(copyData.Data[1:]) {
				return
			}
		case pglogrepl.XLogDataByteID:
			if !s.handleXLogData(copyData.Data[1:], relations, &committed, ticker) {
				return
			}
		}
	}
}

// receive reads one message, bounded so the loop returns to check the keepalive
// ticker even on a silent connection.
func (s *pgEventStream) receive() (pgproto3.BackendMessage, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s.interval)
	defer cancel()
	return s.conn.ReceiveMessage(ctx)
}

// handleKeepalive processes a server keepalive. It reports whether to keep running.
func (s *pgEventStream) handleKeepalive(data []byte) bool {
	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)
	if err != nil {
		s.setErr(err)
		return false
	}

	serverEnd := uint64(pkm.ServerWALEnd)
	advance(&s.received, serverEnd)
	s.catchUp(serverEnd)

	if pkm.ReplyRequested {
		return s.sendStatus()
	}
	return true
}

// handleXLogData decodes one WAL record and, when it yields an event, delivers it.
// It reports whether to keep running.
func (s *pgEventStream) handleXLogData(data []byte, relations map[uint32]*pglogrepl.RelationMessage, committed *time.Time, ticker *time.Ticker) bool {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		s.setErr(err)
		return false
	}

	end := uint64(xld.WALStart) + uint64(len(xld.WALData))
	advance(&s.received, end)

	logical, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		s.setErr(err)
		return false
	}

	switch m := logical.(type) {
	case *pglogrepl.BeginMessage:
		*committed = m.CommitTime
		return true

	case *pglogrepl.RelationMessage:
		// Relation metadata arrives once per table per session and is required to
		// name the columns of every later row for that relation.
		relations[m.RelationID] = m
		return true

	case *pglogrepl.InsertMessage:
		rel, ok := relations[m.RelationID]
		if !ok {
			return true
		}
		return s.emit(pendingEvent{event: decodeInsert(rel, m, end, *committed), lsn: end}, ticker)

	case *pglogrepl.UpdateMessage:
		rel, ok := relations[m.RelationID]
		if !ok {
			return true
		}
		return s.emit(pendingEvent{event: decodeUpdate(rel, m, end, *committed), lsn: end}, ticker)

	case *pglogrepl.DeleteMessage:
		rel, ok := relations[m.RelationID]
		if !ok {
			return true
		}
		return s.emit(pendingEvent{event: decodeDelete(rel, m, end, *committed), lsn: end}, ticker)

	default:
		// COMMIT, ORIGIN, TYPE, TRUNCATE and friends carry no row change.
		// Their positions are confirmed by catchUp once the pipeline drains.
		return true
	}
}

// emit hands an event to the consumer, continuing to confirm the stream's position
// while it waits. It reports whether to keep running.
func (s *pgEventStream) emit(pending pendingEvent, ticker *time.Ticker) bool {
	// Published before the send so catchUp, which runs in this same goroutine
	// between sends, can never see an empty channel and conclude the pipeline is
	// drained while an event is in flight.
	s.lastPushed.Store(pending.lsn)

	for {
		select {
		case s.events <- pending:
			return true
		case <-ticker.C:
			if !s.sendStatus() {
				return false
			}
		case <-s.ctx.Done():
			return false
		}
	}
}

// catchUp confirms the server's WAL end once the pipeline is fully drained.
//
// Without it the confirmed position only ever advances on a row change, so a
// publication that goes quiet pins every WAL segment written since the last event.
// On a busy database with a quiet publication that fills the primary's disk.
func (s *pgEventStream) catchUp(serverEnd uint64) {
	if len(s.events) != 0 {
		return
	}
	pushed := s.lastPushed.Load()
	if pushed != s.flushed.Load() || serverEnd <= pushed {
		return
	}

	s.flushed.Store(serverEnd)
	s.lastPushed.Store(serverEnd)
}

// sendStatus confirms the stream's position to the server. It reports whether to
// keep running.
func (s *pgEventStream) sendStatus() bool {
	ctx, cancel := context.WithTimeout(s.ctx, statusTimeout)
	defer cancel()

	if err := s.writeStatus(ctx); err != nil {
		if s.ctx.Err() == nil {
			s.setErr(err)
		}
		return false
	}
	return true
}

// sendFinalStatus makes a best-effort confirmation as the reader exits, so work the
// consumer completed is not replayed on the next connection. It runs in the reader
// goroutine, after the loop, so it still has exclusive use of the connection.
func (s *pgEventStream) sendFinalStatus() {
	ctx, cancel := context.WithTimeout(context.Background(), statusTimeout)
	defer cancel()
	_ = s.writeStatus(ctx)
}

func (s *pgEventStream) writeStatus(ctx context.Context) error {
	received := s.received.Load()
	// Never confirm past what was actually received, whatever the counters say.
	flushed := min(s.flushed.Load(), received)

	return pglogrepl.SendStandbyStatusUpdate(ctx, s.conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: pglogrepl.LSN(received),
		WALFlushPosition: pglogrepl.LSN(flushed),
		WALApplyPosition: pglogrepl.LSN(flushed),
	})
}

// setErr records the first error to terminate the stream.
func (s *pgEventStream) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
	}
}

// advance raises dst to value if value is higher, keeping positions monotonic.
func advance(dst *atomic.Uint64, value uint64) {
	for {
		current := dst.Load()
		if value <= current || dst.CompareAndSwap(current, value) {
			return
		}
	}
}
