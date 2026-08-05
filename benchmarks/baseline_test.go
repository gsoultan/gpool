// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package benchmarks

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func BenchmarkPgxPool(b *testing.B) {
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		b.Skip("DATABASE_URL not set")
	}

	pool, err := pgxpool.New(context.Background(), connStr)
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var val int
			err := pool.QueryRow(context.Background(), "SELECT 1").Scan(&val)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkStdlib(b *testing.B) {
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		b.Skip("DATABASE_URL not set")
	}

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var val int
			err := db.QueryRow("SELECT 1").Scan(&val)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPgxPoolStress(b *testing.B) {
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		b.Skip("DATABASE_URL not set")
	}

	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		b.Fatal(err)
	}
	config.MaxConns = 100

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close()

	b.SetParallelism(100) // Increase parallelism to stress the pool
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var val int
			err := pool.QueryRow(context.Background(), "SELECT 1").Scan(&val)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkStdlibStress(b *testing.B) {
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		b.Skip("DATABASE_URL not set")
	}

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		b.Fatal(err)
	}
	db.SetMaxOpenConns(100)
	defer db.Close()

	b.SetParallelism(100)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var val int
			err := db.QueryRow("SELECT 1").Scan(&val)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPgBouncer(b *testing.B) {
	connStr := os.Getenv("PGBOUNCER_URL")
	if connStr == "" {
		b.Skip("PGBOUNCER_URL not set")
	}

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var val int
			err := db.QueryRow("SELECT 1").Scan(&val)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
