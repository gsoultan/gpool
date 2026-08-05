// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"errors"
	"testing"
)

// The canonical Go transaction idiom defers a Rollback and commits on the happy
// path, so the rollback always runs. It used to dereference a nil driver handle and
// panic, and re-pool an already-pooled transaction object.
func TestTxCommitWithDeferredRollback(t *testing.T) {
	t.Parallel()

	driver := &fakeTx{}
	tx := newTx(driver)

	err := func() error {
		defer func() { _ = tx.Rollback(t.Context()) }()
		return tx.Commit(t.Context())
	}()
	if err != nil {
		t.Fatalf("Commit() = %v", err)
	}

	if got := driver.commits.Load(); got != 1 {
		t.Errorf("driver Commit called %d times, want 1", got)
	}
	if got := driver.rollbacks.Load(); got != 0 {
		t.Errorf("driver Rollback called %d times after a commit, want 0", got)
	}
}

func TestTxRollbackWithDeferredRollback(t *testing.T) {
	t.Parallel()

	driver := &fakeTx{}
	tx := newTx(driver)

	func() {
		defer func() { _ = tx.Rollback(t.Context()) }()
		if err := tx.Rollback(t.Context()); err != nil {
			t.Errorf("Rollback() = %v", err)
		}
	}()

	if got := driver.rollbacks.Load(); got != 1 {
		t.Fatalf("driver Rollback called %d times, want 1", got)
	}
}

func TestTxSettlesExactlyOnce(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		call          func(*pgTx) error
		wantCommits   int32
		wantRollbacks int32
	}{
		{
			name: "commit then commit",
			call: func(tx *pgTx) error {
				_ = tx.Commit(t.Context())
				return tx.Commit(t.Context())
			},
			wantCommits: 1,
		},
		{
			name: "rollback then rollback",
			call: func(tx *pgTx) error {
				_ = tx.Rollback(t.Context())
				return tx.Rollback(t.Context())
			},
			wantRollbacks: 1,
		},
		{
			name: "rollback then commit",
			call: func(tx *pgTx) error {
				_ = tx.Rollback(t.Context())
				return tx.Commit(t.Context())
			},
			wantRollbacks: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			driver := &fakeTx{}
			tx := newTx(driver)

			if err := tt.call(tx); !errors.Is(err, ErrTxClosed) {
				t.Fatalf("second settle = %v, want ErrTxClosed", err)
			}
			if got := driver.commits.Load(); got != tt.wantCommits {
				t.Errorf("driver Commit called %d times, want %d", got, tt.wantCommits)
			}
			if got := driver.rollbacks.Load(); got != tt.wantRollbacks {
				t.Errorf("driver Rollback called %d times, want %d", got, tt.wantRollbacks)
			}
		})
	}
}

func TestTxRefusesUseAfterSettle(t *testing.T) {
	t.Parallel()

	tx := newTx(&fakeTx{})
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("Commit() = %v", err)
	}

	if _, err := tx.Exec(t.Context(), "SELECT 1"); !errors.Is(err, ErrTxClosed) {
		t.Errorf("Exec() = %v, want ErrTxClosed", err)
	}
	if _, err := tx.Query(t.Context(), "SELECT 1"); !errors.Is(err, ErrTxClosed) {
		t.Errorf("Query() = %v, want ErrTxClosed", err)
	}
	if err := tx.QueryRow(t.Context(), "SELECT 1").Scan(); !errors.Is(err, ErrTxClosed) {
		t.Errorf("QueryRow().Scan() = %v, want ErrTxClosed", err)
	}
}
