// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package cdc

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestQuoteIdentifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "plain", input: "users", want: `"users"`},
		{name: "mixed case is preserved", input: "Users", want: `"Users"`},
		{name: "embedded quote is doubled", input: `we"ird`, want: `"we""ird"`},
		{name: "injection attempt is neutralised", input: `x"; DROP TABLE y; --`, want: `"x""; DROP TABLE y; --"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := quoteIdentifier(tt.input); got != tt.want {
				t.Fatalf("quoteIdentifier(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// Quoting "public.users" as a single identifier names a table literally called
// "public.users" rather than users in schema public, so the parts must be split.
func TestQuoteQualifiedName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "unqualified", input: "users", want: `"users"`},
		{name: "schema qualified", input: "public.users", want: `"public"."users"`},
		{name: "empty schema falls back", input: ".users", want: `".users"`},
		{name: "empty table falls back", input: "public.", want: `"public."`},
		{name: "extra dots stay in the table part", input: "public.a.b", want: `"public"."a.b"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := quoteQualifiedName(tt.input); got != tt.want {
				t.Fatalf("quoteQualifiedName(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestSplitQualifiedName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      string
		wantSchema string
		wantTable  string
	}{
		{input: "users", wantSchema: "", wantTable: "users"},
		{input: "public.users", wantSchema: "public", wantTable: "users"},
		{input: ".users", wantSchema: "", wantTable: ".users"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			schema, table := splitQualifiedName(tt.input)
			if schema != tt.wantSchema || table != tt.wantTable {
				t.Fatalf("splitQualifiedName(%q) = (%q, %q), want (%q, %q)",
					tt.input, schema, table, tt.wantSchema, tt.wantTable)
			}
		})
	}
}

// Plugin arguments are interpolated into START_REPLICATION, so a quote in a
// publication name must not be able to close the literal.
func TestQuoteLiteral(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  string
	}{
		{input: "pub", want: `'pub'`},
		{input: "it's", want: `'it''s'`},
		{input: `a', injected '`, want: `'a'', injected '''`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			if got := quoteLiteral(tt.input); got != tt.want {
				t.Fatalf("quoteLiteral(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// Classifying catalog errors by SQLSTATE keeps behaviour correct on a server whose
// lc_messages is not English, where matching on message text silently fails.
func TestErrorClassificationUsesSQLState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		err           error
		wantDuplicate bool
		wantUndefined bool
	}{
		{
			name:          "duplicate object",
			err:           &pgconn.PgError{Code: codeDuplicateObject, Message: "publication already exists"},
			wantDuplicate: true,
		},
		{
			name:          "duplicate table",
			err:           &pgconn.PgError{Code: codeDuplicateTable},
			wantDuplicate: true,
		},
		{
			name:          "undefined object",
			err:           &pgconn.PgError{Code: codeUndefinedObject},
			wantUndefined: true,
		},
		{
			name: "wrapped error is still classified",
			err: fmt.Errorf("creating publication: %w",
				&pgconn.PgError{Code: codeDuplicateObject}),
			wantDuplicate: true,
		},
		{
			name: "localised message without a matching code",
			err:  &pgconn.PgError{Code: "42501", Message: "permiso denegado"},
		},
		{
			name: "plain error",
			err:  errors.New("already exists"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := isDuplicateObject(tt.err); got != tt.wantDuplicate {
				t.Errorf("isDuplicateObject() = %v, want %v", got, tt.wantDuplicate)
			}
			if got := isUndefinedObject(tt.err); got != tt.wantUndefined {
				t.Errorf("isUndefinedObject() = %v, want %v", got, tt.wantUndefined)
			}
		})
	}
}

func TestSetOperations(t *testing.T) {
	t.Parallel()

	t.Run("dedupe drops repeats and blanks, preserving order", func(t *testing.T) {
		t.Parallel()

		got := dedupe([]string{"b", "a", "b", "", "c", "a"})
		if want := []string{"b", "a", "c"}; !slices.Equal(got, want) {
			t.Fatalf("dedupe() = %v, want %v", got, want)
		}
	})

	t.Run("difference", func(t *testing.T) {
		t.Parallel()

		got := difference([]string{"a", "b", "c"}, []string{"b"})
		if want := []string{"a", "c"}; !slices.Equal(got, want) {
			t.Fatalf("difference() = %v, want %v", got, want)
		}
	})

	t.Run("intersection", func(t *testing.T) {
		t.Parallel()

		got := intersection([]string{"a", "b", "c"}, []string{"c", "a", "z"})
		if want := []string{"a", "c"}; !slices.Equal(got, want) {
			t.Fatalf("intersection() = %v, want %v", got, want)
		}
	})

	t.Run("empty inputs", func(t *testing.T) {
		t.Parallel()

		if got := dedupe(nil); got != nil {
			t.Errorf("dedupe(nil) = %v, want nil", got)
		}
		if got := difference(nil, []string{"a"}); got != nil {
			t.Errorf("difference(nil, ...) = %v, want nil", got)
		}
		if got := intersection([]string{"a"}, nil); got != nil {
			t.Errorf("intersection(..., nil) = %v, want nil", got)
		}
	})
}

func TestStatementBuildersQuoteTables(t *testing.T) {
	t.Parallel()

	tables := []string{"public.users", "orders"}

	tests := []struct {
		name string
		got  string
		want string
	}{
		{
			name: "create",
			got:  createPublicationSQL("my pub", tables),
			want: `CREATE PUBLICATION "my pub" FOR TABLE "public"."users", "orders"`,
		},
		{
			name: "set",
			got:  setPublicationTablesSQL("pub", tables),
			want: `ALTER PUBLICATION "pub" SET TABLE "public"."users", "orders"`,
		},
		{
			name: "add",
			got:  addPublicationTablesSQL("pub", tables),
			want: `ALTER PUBLICATION "pub" ADD TABLE "public"."users", "orders"`,
		},
		{
			name: "drop tables",
			got:  dropPublicationTablesSQL("pub", tables),
			want: `ALTER PUBLICATION "pub" DROP TABLE "public"."users", "orders"`,
		},
		{
			name: "drop publication is conditional",
			got:  dropPublicationSQL("pub"),
			want: `DROP PUBLICATION IF EXISTS "pub"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.got != tt.want {
				t.Fatalf("got  %s\nwant %s", tt.got, tt.want)
			}
		})
	}
}
