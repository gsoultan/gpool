// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Close used to nil the driver handle and then dereference it on the next call,
// so a second Close panicked and re-pooled an already-pooled object.
func TestRowsCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	driver := &fakeRows{remaining: 2}
	rows := newRows(driver, nil)

	rows.Close()
	rows.Close()
	rows.Close()

	if got := driver.closes.Load(); got != 1 {
		t.Fatalf("driver Close called %d times, want 1", got)
	}
}

// The natural idiom pairs a deferred Close with a range over All, and All closes
// the rows itself when iteration ends. Both paths must land on one driver close.
func TestRowsDeferredCloseAlongsideAll(t *testing.T) {
	t.Parallel()

	driver := &fakeRows{remaining: 3}
	rows := newRows(driver, nil)

	seen := 0
	func() {
		defer rows.Close()
		for range rows.All() {
			seen++
		}
	}()

	if seen != 3 {
		t.Errorf("iterated %d rows, want 3", seen)
	}
	if got := driver.closes.Load(); got != 1 {
		t.Fatalf("driver Close called %d times, want 1", got)
	}
}

func TestRowsAllClosesOnEarlyBreak(t *testing.T) {
	t.Parallel()

	driver := &fakeRows{remaining: 10}
	rows := newRows(driver, nil)

	seen := 0
	for range rows.All() {
		seen++
		if seen == 2 {
			break
		}
	}

	if seen != 2 {
		t.Errorf("iterated %d rows, want 2", seen)
	}
	if got := driver.closes.Load(); got != 1 {
		t.Fatalf("driver Close called %d times, want 1", got)
	}
}

func TestRowsRefuseUseAfterClose(t *testing.T) {
	t.Parallel()

	driver := &fakeRows{remaining: 5}
	rows := newRows(driver, nil)
	rows.Close()

	if rows.Next() {
		t.Error("Next() should report false once the rows are closed")
	}
	if err := rows.Scan(); !errors.Is(err, ErrRowsClosed) {
		t.Errorf("Scan() = %v, want ErrRowsClosed", err)
	}
}

func TestRowsFieldDescriptionsAreTranslated(t *testing.T) {
	t.Parallel()

	driver := &fakeRows{
		fields: []pgconn.FieldDescription{
			{Name: "id", DataTypeOID: 23, DataTypeSize: 4},
			{Name: "email", DataTypeOID: 25, DataTypeSize: -1},
		},
	}

	got := newRows(driver, nil).FieldDescriptions()
	if len(got) != 2 {
		t.Fatalf("got %d fields, want 2", len(got))
	}
	if got[0].Name != "id" || got[0].DataTypeOID != 23 {
		t.Errorf("field 0 = %+v", got[0])
	}
	if got[1].Name != "email" || got[1].DataTypeSize != -1 {
		t.Errorf("field 1 = %+v", got[1])
	}
}

// Scan settles the row, and so does Release. Either alone is enough and both
// together must not settle it twice, which is what returned a connection to the
// pool while its result was still being read.
func TestRowSettlesExactlyOnce(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		call func(*pgRow)
	}{
		{name: "scan then release", call: func(r *pgRow) { _ = r.Scan(); r.Release() }},
		{name: "release then scan", call: func(r *pgRow) { r.Release(); _ = r.Scan() }},
		{name: "release twice", call: func(r *pgRow) { r.Release(); r.Release() }},
		{name: "scan twice", call: func(r *pgRow) { _ = r.Scan(); _ = r.Scan() }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			driver := &fakeRows{remaining: 1}
			row := newRow(driver, nil)
			tt.call(row)

			if !row.done.Load() {
				t.Fatal("row should be settled")
			}
			if got := driver.closes.Load(); got < 1 {
				t.Fatalf("the result set was never closed")
			}
		})
	}
}

// Releasing without scanning must still close the query. Handing a connection back
// to the pool with a result set still open leaves it stuck mid-query, and the next
// caller gets "conn busy".
func TestRowReleaseWithoutScanClosesTheResultSet(t *testing.T) {
	t.Parallel()

	driver := &fakeRows{remaining: 1}
	newRow(driver, nil).Release()

	if got := driver.closes.Load(); got != 1 {
		t.Fatalf("driver Close called %d times, want 1", got)
	}
	if got := driver.scans.Load(); got != 0 {
		t.Fatalf("driver Scan called %d times, want 0", got)
	}
}

func TestRowScanReportsNoRows(t *testing.T) {
	t.Parallel()

	row := newRow(&fakeRows{remaining: 0}, nil)

	if err := row.Scan(); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("Scan() on an empty result = %v, want pgx.ErrNoRows", err)
	}
}

func TestRowScanAfterReleaseIsRefused(t *testing.T) {
	t.Parallel()

	driver := &fakeRows{remaining: 1}
	row := newRow(driver, nil)
	row.Release()

	if err := row.Scan(); !errors.Is(err, ErrRowsClosed) {
		t.Fatalf("Scan() = %v, want ErrRowsClosed", err)
	}
	if got := driver.scans.Load(); got != 0 {
		t.Fatalf("driver Scan called %d times after Release, want 0", got)
	}
}

func TestErrorRowDefersTheError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("acquire failed")
	row := errorRow{err: sentinel}

	if err := row.Scan(); !errors.Is(err, sentinel) {
		t.Fatalf("Scan() = %v, want the deferred error", err)
	}
	row.Release()
}

func TestResultReportsRowsAffected(t *testing.T) {
	t.Parallel()

	result := pgResult{tag: pgconn.NewCommandTag("UPDATE 3")}

	if got := result.RowsAffected(); got != 3 {
		t.Errorf("RowsAffected() = %d, want 3", got)
	}
	if got := result.String(); got != "UPDATE 3" {
		t.Errorf("String() = %q, want %q", got, "UPDATE 3")
	}
	// Release is a no-op on an immutable value; calling it repeatedly is harmless.
	result.Release()
	result.Release()
}
