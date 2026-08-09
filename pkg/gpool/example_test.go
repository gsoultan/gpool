// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package gpool_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/gsoultan/gpool/pkg/gpool"
	"github.com/gsoultan/gpool/pkg/gpool/cdc"
	postgrescdc "github.com/gsoultan/gpool/pkg/vendors/postgres/cdc"
	postgrespool "github.com/gsoultan/gpool/pkg/vendors/postgres/pool"
)

// These compile but do not run: every one needs a database, and an example that
// silently skips teaches nothing. They exist so pkg.go.dev shows working code
// rather than prose, and so the README's snippets cannot drift away from an API
// that still compiles.

// Importing the vendor package is what makes its name resolvable. Nothing else
// registers it, which is the same arrangement database/sql uses.
func ExampleNewPool() {
	pool, err := gpool.NewPool(gpool.Postgres, postgrespool.Config{
		ConnString: "postgres://user:pass@localhost:5432/app",
		MaxConns:   25,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	var total int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM users").Scan(&total); err != nil {
		log.Fatal(err)
	}
	fmt.Println(total)
}

// A connection error may already be fixed by the time you see it: the pool
// retires the dead connection that produced it and dials a replacement, so one
// retry is the difference between a failed request and a slow one.
func ExamplePool_Acquire_retryOnce() {
	pool, err := gpool.NewPool(gpool.Postgres, postgrespool.Config{
		ConnString: "postgres://user:pass@localhost:5432/app",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	ctx := context.Background()
	var name string
	for attempt := range 2 {
		err = pool.QueryRow(ctx, "SELECT name FROM users WHERE id = $1", 1).Scan(&name)
		if err == nil || attempt == 1 {
			break
		}
	}
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(name)
}

// Releasing is idempotent, so deferring it alongside an explicit Commit is safe
// — which is the shape most transaction code actually takes.
func ExampleConn_Begin() {
	pool, err := gpool.NewPool(gpool.Postgres, postgrespool.Config{
		ConnString: "postgres://user:pass@localhost:5432/app",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, "UPDATE accounts SET balance = balance - $1 WHERE id = $2", 100, 1); err != nil {
		log.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		log.Fatal(err)
	}
}

// Capabilities not every engine has are reached by type assertion rather than
// living on Pool, so adding a vendor never means implementing something it
// cannot do. A failed assertion means "this engine does not offer it", not an
// error.
func ExampleResizable() {
	pool, err := gpool.NewPool(gpool.Postgres, postgrespool.Config{
		ConnString:    "postgres://user:pass@localhost:5432/app",
		MaxConns:      10,
		MaxConnsLimit: 100,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	if resizable, ok := pool.(gpool.Resizable); ok {
		// Never blocks: growing takes effect at once, and shrinking is applied as
		// checked-out connections come back rather than by waiting for them.
		if err := resizable.SetMaxConns(50); err != nil {
			log.Fatal(err)
		}
		// After a credential rotation or a failover, what is pooled may be stale.
		fmt.Println(resizable.EvictIdle(), "idle connections discarded")
	}
}

// Occupancy alone cannot tell a pool that is busy from one that is too small.
// EmptyAcquireCount against AcquireCount is the pressure signal, and
// WaitingAcquires answers "is it short right now" rather than "has it ever been".
func ExamplePool_Stat() {
	pool, err := gpool.NewPool(gpool.Postgres, postgrespool.Config{
		ConnString: "postgres://user:pass@localhost:5432/app",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	stat := pool.Stat()
	fmt.Printf("%d/%d in use, %d waiting, %d acquisitions found the pool empty\n",
		stat.ActiveConnections(), stat.MaxConnections(),
		stat.WaitingAcquires(), stat.EmptyAcquireCount())
}

// Record Event.Position as you go and hand it back to resume. What Subscribe
// alone does without one differs by vendor: PostgreSQL resumes from the slot and
// loses nothing, while MySQL and SQL Server start at the end of their logs.
func ExampleNewSubscriber() {
	subscriber, err := gpool.NewSubscriber(gpool.Postgres, postgrescdc.Config{
		ConnString:        "postgres://user:pass@localhost:5432/app",
		SlotName:          "gpool_slot",
		PublicationName:   "gpool_pub",
		Tables:            []string{"public.users"},
		CreateSlot:        true,
		CreatePublication: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer subscriber.Close()

	stream, err := subscriber.Subscribe(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer stream.Close()

	for event := range stream.All() {
		fmt.Printf("%s %s.%s at %s\n", event.Op, event.Schema, event.Table, event.Timestamp)
	}
	if err := stream.Err(); err != nil {
		log.Fatal(err)
	}
}

// Resuming starts at or before the change the position came from, never after
// it, so a restart may repeat work but never skips it. Changes committed
// together share a Transaction, which is what lets a batch be applied atomically.
func Example_resumingFromACheckpoint() {
	subscriber, err := gpool.NewSubscriber(gpool.Postgres, postgrescdc.Config{
		ConnString:      "postgres://user:pass@localhost:5432/app",
		SlotName:        "gpool_slot",
		PublicationName: "gpool_pub",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer subscriber.Close()

	checkpoint := load() // whatever was durably recorded last run

	stream, err := subscriber.SubscribeFrom(context.Background(), checkpoint)
	if err != nil {
		// PostgreSQL refuses a position the slot has already moved past, rather
		// than silently starting from wherever it can.
		if errors.Is(err, postgrescdc.ErrPositionBehindSlot) {
			log.Fatal("checkpoint is older than the slot retains; changes have been lost")
		}
		log.Fatal(err)
	}
	defer stream.Close()

	var batch []cdc.Event
	for event := range stream.All() {
		// The transaction changed, so everything held is complete.
		if len(batch) > 0 && event.Transaction != batch[0].Transaction {
			apply(batch)
			save(batch[len(batch)-1].Position)
			batch = batch[:0]
		}
		batch = append(batch, event)
	}
}

// Slots and publications are PostgreSQL's model, so they are an optional
// capability rather than part of Subscriber — MySQL has no equivalent at all.
func Example_slotAdministration() {
	subscriber, err := gpool.NewSubscriber(gpool.Postgres, postgrescdc.Config{
		ConnString:      "postgres://user:pass@localhost:5432/app",
		SlotName:        "gpool_slot",
		PublicationName: "gpool_pub",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer subscriber.Close()

	slots, ok := subscriber.(cdc.ReplicationManager)
	if !ok {
		fmt.Println("this engine has no slots or publications")
		return
	}
	// Dropping a slot discards the position it was holding, which is data loss,
	// so nothing here does it on your behalf.
	if err := slots.CreateSlot(context.Background(), "gpool_slot"); err != nil {
		log.Fatal(err)
	}
}

// Several databases means several pools, registered by name and sharing nothing:
// separate capacity is what stops one saturated backend starving the rest.
func ExampleEngine() {
	orders, err := gpool.NewPool(gpool.Postgres, postgrespool.Config{
		ConnString: "postgres://user:pass@orders-db:5432/orders",
		MaxConns:   25,
	})
	if err != nil {
		log.Fatal(err)
	}
	billing, err := gpool.NewPool(gpool.Postgres, postgrespool.Config{
		ConnString:      "postgres://user:pass@billing-db:5432/billing",
		MaxConns:        10,
		MaxConnLifetime: 30 * time.Minute,
	})
	if err != nil {
		log.Fatal(err)
	}

	engine := gpool.NewEngine(orders, nil)
	engine.AddPool("billing", billing)
	defer engine.Close() // closes every pool and subscriber, joining errors

	// Pool with no argument resolves the default, so single-database callers are
	// unaffected by any of this.
	fmt.Println(engine.Pool("billing").Stat().MaxConnections())
}

func load() cdc.Position { return cdc.NoPosition }
func save(cdc.Position)  {}
func apply([]cdc.Event)  {}

// Occupancy and Acquisition say how full the pool is and how hard callers are
// competing for it. Neither says why connections are being replaced, and the two
// reasons want opposite responses: a pool that is too small should be grown, and
// one whose connections keep dying should not be.
func ExampleLifecycle() {
	pool, err := gpool.NewPool(gpool.Postgres, postgrespool.Config{
		ConnString: "postgres://user:pass@localhost:5432/app",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	stat := pool.Stat()
	lifecycle, ok := stat.(gpool.Lifecycle)
	if !ok {
		return // this engine does not account for its connections
	}

	// Rising together, the pool is short. Rising apart, the database is losing
	// connections and a larger pool would only dial more of them.
	fmt.Printf("%d waited for a connection, %d connections died, %d retired on schedule\n",
		stat.EmptyAcquireCount(), lifecycle.UnhealthyConnections(), lifecycle.ExpiredConnections())
}
