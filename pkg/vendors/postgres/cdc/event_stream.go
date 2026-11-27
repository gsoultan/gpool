// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"context"
	"errors"
	"iter"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gsoultan/caduceus/replication"
	"github.com/gsoultan/caduceus/wire"
	"github.com/gsoultan/gpool/pkg/gpool/cdc"
)

// statusTimeout bounds a single standby status update.
const statusTimeout = 5 * time.Second

// pgEventStream is a live logical replication stream implementing cdc.EventStream.
//
// One goroutine owns the connection for the stream's whole life: it reads WAL,
// decodes it, and sends every standby status update. Nothing else touches the
// connection, because a connection is a single stream of bytes in each direction
// and has no lock of its own.
//
// Decoding is decoupled from consumption by a buffered channel so a slow consumer
// cannot stall the keepalives. The reader keeps confirming its position even while
// blocked handing an event over, which is what stops the server from dropping the
// connection on wal_sender_timeout.
type pgEventStream struct {
	conn     *wire.Conn
	ctx      context.Context
	cancel   context.CancelFunc
	events   chan pendingEvent
	interval time.Duration

	// received is the highest WAL position read off the wire.
	received atomic.Uint64
	// flushed is the position the consumer has finished processing. It is what the
	// server records as confirmed_flush_lsn, and therefore what releases WAL.
	flushed atomic.Uint64

	// pushed and acked count events rather than compare positions. pushed is
	// how many the reader has handed to the channel; acked is how many loop
	// bodies have returned. Equal counts are the only reliable proof that the
	// consumer holds nothing: positions cannot say it, because pgoutput sends
	// transactions in commit order while each change keeps the LSN of its own
	// record, so interleaved transactions arrive with positions going backwards.
	pushed atomic.Uint64
	acked  atomic.Uint64

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
func newEventStream(parent context.Context, conn *wire.Conn, config Config, onClose func()) *pgEventStream {
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
			// The position first, then the count: catchUp reads the count and
			// relies on every acknowledged position already being in flushed.
			advance(&s.flushed, pending.lsn)
			s.acked.Add(1)
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

	// Close interrupts a blocked read by moving the read deadline into the past.
	// Only the read side: the final status update still has to be written.
	netConn := s.conn.NetConn()
	stop := context.AfterFunc(s.ctx, func() { _ = netConn.SetReadDeadline(time.Unix(1, 0)) })
	defer stop()

	// The decoder remembers the Relation messages that name each table's
	// columns, and the Begin whose commit LSN and time every change carries.
	decoder := replication.NewDecoder()
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

		data, err := s.receive()
		if err != nil {
			if s.ctx.Err() != nil {
				return
			}
			if wire.IsTimeout(err) {
				continue
			}
			s.setErr(err)
			return
		}
		if len(data) == 0 {
			continue
		}

		switch data[0] {
		case replication.PrimaryKeepalive:
			if !s.handleKeepalive(data) {
				return
			}
		case replication.XLogData:
			if !s.handleXLogData(data, decoder, ticker) {
				return
			}
		}
	}
}

// errStreamEnded is a server that ended the copy, which it does only when the
// walsender is shutting down.
var errStreamEnded = errors.New("gpool/postgres/cdc: the server ended the replication stream")

// receive reads the next CopyData payload, bounded so the loop returns to check
// the keepalive ticker even on a silent connection. A read that times out
// leaves the connection intact. The payload aliases the connection's buffer
// until the next read.
func (s *pgEventStream) receive() ([]byte, error) {
	if err := s.conn.NetConn().SetReadDeadline(time.Now().Add(s.interval)); err != nil {
		return nil, err
	}
	if s.ctx.Err() != nil {
		// Close may have moved the deadline into the past just before it was
		// moved forward again above.
		return nil, s.ctx.Err()
	}
	kind, body, err := s.conn.Receive()
	if err != nil {
		return nil, err
	}
	switch kind {
	case wire.MsgCopyData:
		return body, nil
	case wire.MsgErrorResponse:
		return nil, wire.ParseError(body)
	case wire.MsgCopyDone:
		return nil, errStreamEnded
	}
	return nil, nil
}

// handleKeepalive processes a server keepalive. It reports whether to keep running.
func (s *pgEventStream) handleKeepalive(data []byte) bool {
	keepalive, err := replication.ParseKeepalive(data)
	if err != nil {
		s.setErr(err)
		return false
	}
	serverEnd := uint64(keepalive.ServerEnd)

	advance(&s.received, serverEnd)
	s.catchUp(serverEnd)

	if keepalive.ReplyRequested {
		return s.sendStatus()
	}
	return true
}

// handleXLogData decodes one WAL record and, when it yields an event, delivers it.
// It reports whether to keep running.
func (s *pgEventStream) handleXLogData(data []byte, decoder *replication.Decoder, ticker *time.Ticker) bool {
	record, err := replication.ParseXLogData(data)
	if err != nil {
		s.setErr(err)
		return false
	}
	end := uint64(record.End())
	advance(&s.received, end)

	pending, ok, err := decodeRecord(record.Data, decoder, end)
	if err != nil {
		s.setErr(err)
		return false
	}
	if !ok {
		return true
	}
	return s.emit(pending, ticker)
}

// decodeRecord decodes one pgoutput message. It reports whether the message was
// a row change, which is the only kind that becomes an event; Begin and
// Relation update the decoder's state, and COMMIT, ORIGIN, TYPE, TRUNCATE and
// friends carry no row change. Their positions are confirmed by catchUp once
// the pipeline drains.
func decodeRecord(record []byte, decoder *replication.Decoder, end uint64) (pendingEvent, bool, error) {
	change, ok, err := decoder.Decode(record)
	if err != nil || !ok {
		return pendingEvent{}, false, err
	}
	return pendingEvent{event: toEvent(change, end), lsn: end}, true, nil
}

// toEvent is a decoded change as a cdc.Event. Position is the end of the
// change's own record; Transaction is its transaction's commit LSN, the same
// for every change in it.
func toEvent(change replication.Change, end uint64) cdc.Event {
	return cdc.Event{
		Op:          toOp(change.Op),
		Schema:      change.Relation.Namespace,
		Table:       change.Relation.Name,
		Before:      change.Before,
		After:       change.After,
		Position:    position(end),
		Timestamp:   change.Transaction.Committed,
		Transaction: position(uint64(change.Transaction.FinalLSN)),
	}
}

func toOp(op replication.Op) cdc.Op {
	switch op {
	case replication.Insert:
		return cdc.OpInsert
	case replication.Update:
		return cdc.OpUpdate
	}
	return cdc.OpDelete
}

// emit hands an event to the consumer, continuing to confirm the stream's position
// while it waits. It reports whether to keep running.
func (s *pgEventStream) emit(pending pendingEvent, ticker *time.Ticker) bool {
	// Counted before the send so catchUp, which runs in this same goroutine
	// between sends, can never see an empty channel and conclude the pipeline is
	// drained while an event is in flight.
	s.pushed.Add(1)

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
//
// "Drained" is decided by counting, never by comparing positions. An earlier
// version required the last pushed position to equal the confirmed one, which
// stopped holding for good once two transactions interleaved — the later commit
// carries the lower record LSNs — and the slot was pinned again. Comparing with
// a maximum instead would be worse: with a lower-positioned event still inside
// the consumer's loop body the two would match, and confirming the server's end
// would skip that event's transaction on restart.
func (s *pgEventStream) catchUp(serverEnd uint64) {
	if len(s.events) != 0 {
		return
	}
	if s.acked.Load() != s.pushed.Load() {
		return
	}
	advance(&s.flushed, serverEnd)
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

	deadline := time.Now().Add(statusTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := s.conn.NetConn().SetWriteDeadline(deadline); err != nil {
		return err
	}
	return s.conn.Send(replication.AppendStandbyStatusUpdate(s.conn.Buffer(), replication.StandbyStatusUpdate{
		WALWritePosition: replication.LSN(received),
		WALFlushPosition: replication.LSN(flushed),
		WALApplyPosition: replication.LSN(flushed),
	}))
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
