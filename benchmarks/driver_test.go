// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package benchmarks

import (
	"context"
	"testing"
	"time"

	"github.com/gsoultan/gpool/pkg/gpool"
	postgrespool "github.com/gsoultan/gpool/pkg/vendors/postgres/pool"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The driver comparison: gpool's own protocol implementation against pgx, with
// the pools on both sides given the same number of connections. "SELECT 1"
// measures the pool and barely touches a driver, so these workloads bind
// parameters and decode rows, which is where a driver spends its time.
//
// Serial runs use one connection and one caller, so the number is the driver's
// cost per round trip with no queueing in it. Parallel runs use pgxpool's
// default size on both sides.

const rowsSQL = `SELECT g, 'customer-' || g, now(), g * 1.5 FROM generate_series(1, $1::int4) AS g`

type queryer interface {
	queryRow(ctx context.Context, n int64) (int64, error)
	rows(ctx context.Context, n int32) (int, error)
}

type gpoolQueryer struct{ pool gpool.Pool }

func (q gpoolQueryer) queryRow(ctx context.Context, n int64) (int64, error) {
	var got int64
	err := q.pool.QueryRow(ctx, "SELECT $1::int8 + 1", n).Scan(&got)
	return got, err
}

func (q gpoolQueryer) rows(ctx context.Context, n int32) (int, error) {
	rows, err := q.pool.Query(ctx, rowsSQL, n)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	count := 0
	var (
		id     int64
		name   string
		at     time.Time
		amount float64
	)
	for rows.Next() {
		if err := rows.Scan(&id, &name, &at, &amount); err != nil {
			return 0, err
		}
		count++
	}
	return count, rows.Err()
}

type pgxQueryer struct{ pool *pgxpool.Pool }

func (q pgxQueryer) queryRow(ctx context.Context, n int64) (int64, error) {
	var got int64
	err := q.pool.QueryRow(ctx, "SELECT $1::int8 + 1", n).Scan(&got)
	return got, err
}

func (q pgxQueryer) rows(ctx context.Context, n int32) (int, error) {
	rows, err := q.pool.Query(ctx, rowsSQL, n)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	count := 0
	var (
		id     int64
		name   string
		at     time.Time
		amount float64
	)
	for rows.Next() {
		if err := rows.Scan(&id, &name, &at, &amount); err != nil {
			return 0, err
		}
		count++
	}
	return count, rows.Err()
}

func driverPools(b *testing.B, conns int32) map[string]queryer {
	gp := newGpool(b, postgrespool.Config{MaxConns: conns, MinConns: conns})
	config, err := pgxpool.ParseConfig(requireDatabase(b))
	if err != nil {
		b.Fatal(err)
	}
	config.MaxConns, config.MinConns = conns, conns
	pp, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(pp.Close)
	return map[string]queryer{"gpool": gpoolQueryer{gp}, "pgx": pgxQueryer{pp}}
}

func BenchmarkDriverQueryRowSerial(b *testing.B) {
	for _, name := range []string{"gpool", "pgx"} {
		b.Run(name, func(b *testing.B) {
			q := driverPools(b, 1)[name]
			ctx := context.Background()
			b.ReportAllocs()
			for i := int64(0); b.Loop(); i++ {
				if got, err := q.queryRow(ctx, i); err != nil || got != i+1 {
					b.Fatal(got, err)
				}
			}
		})
	}
}

func BenchmarkDriverRows100Serial(b *testing.B) {
	for _, name := range []string{"gpool", "pgx"} {
		b.Run(name, func(b *testing.B) {
			q := driverPools(b, 1)[name]
			ctx := context.Background()
			b.ReportAllocs()
			for b.Loop() {
				if n, err := q.rows(ctx, 100); err != nil || n != 100 {
					b.Fatal(n, err)
				}
			}
		})
	}
}

func BenchmarkDriverQueryRowParallel(b *testing.B) {
	for _, name := range []string{"gpool", "pgx"} {
		b.Run(name, func(b *testing.B) {
			q := driverPools(b, pgxpoolDefaultMaxConns())[name]
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				ctx := context.Background()
				for i := int64(0); pb.Next(); i++ {
					if got, err := q.queryRow(ctx, i); err != nil || got != i+1 {
						b.Error(got, err)
						return
					}
				}
			})
		})
	}
}

func BenchmarkDriverRows100Parallel(b *testing.B) {
	for _, name := range []string{"gpool", "pgx"} {
		b.Run(name, func(b *testing.B) {
			q := driverPools(b, pgxpoolDefaultMaxConns())[name]
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				ctx := context.Background()
				for pb.Next() {
					if n, err := q.rows(ctx, 100); err != nil || n != 100 {
						b.Error(n, err)
						return
					}
				}
			})
		})
	}
}

// BenchmarkDriverQueryRowContended runs 16 callers per CPU over 32 connections
// on each side, the shape of a busy service rather than a microbenchmark.
func BenchmarkDriverQueryRowContended(b *testing.B) {
	for _, name := range []string{"gpool", "pgx"} {
		b.Run(name, func(b *testing.B) {
			q := driverPools(b, 32)[name]
			b.ReportAllocs()
			b.SetParallelism(16)
			b.RunParallel(func(pb *testing.PB) {
				ctx := context.Background()
				for i := int64(0); pb.Next(); i++ {
					if got, err := q.queryRow(ctx, i); err != nil || got != i+1 {
						b.Error(got, err)
						return
					}
				}
			})
		})
	}
}
