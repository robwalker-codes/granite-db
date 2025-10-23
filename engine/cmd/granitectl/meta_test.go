package main

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/example/granite-db/engine/internal/api"
)

func TestParseMetaArgs(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		args       []string
		wantJSON   bool
		wantDB     string
		wantErr    bool
		usageError bool
	}{{
		name:     "with json flag",
		args:     []string{"--json", "demo.gdb"},
		wantJSON: true,
		wantDB:   "demo.gdb",
	}, {
		name:     "with shorthand json",
		args:     []string{"-json", "demo.gdb"},
		wantJSON: true,
		wantDB:   "demo.gdb",
	}, {
		name:   "without flags",
		args:   []string{"demo.gdb"},
		wantDB: "demo.gdb",
	}, {
		name:       "missing database",
		args:       []string{"--json"},
		wantErr:    true,
		usageError: true,
	}, {
		name:    "unknown option",
		args:    []string{"--bogus", "demo.gdb"},
		wantErr: true,
	}, {
		name:    "duplicate database",
		args:    []string{"demo.gdb", "extra.gdb"},
		wantErr: true,
	}}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			jsonOut, dbPath, err := parseMetaArgs(tc.args)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error")
				}
				if tc.usageError && err != errMetaUsage {
					t.Fatalf("expected usage error, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if jsonOut != tc.wantJSON {
				t.Fatalf("json flag mismatch: got %v want %v", jsonOut, tc.wantJSON)
			}
			if dbPath != tc.wantDB {
				t.Fatalf("database path mismatch: got %q want %q", dbPath, tc.wantDB)
			}
		})
	}
}

func TestLoadDatabaseMeta(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "meta.gdb")
	if err := api.Create(path); err != nil {
		t.Fatalf("create: %v", err)
	}

	db, err := api.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.Execute("CREATE TABLE customers(id INT PRIMARY KEY, name VARCHAR(50))"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Execute("CREATE TABLE orders(id INT PRIMARY KEY, customer_id INT REFERENCES customers(id))"); err != nil {
		t.Fatalf("create child table: %v", err)
	}
	if _, err := db.Execute("CREATE INDEX idx_orders_customer ON orders(customer_id)"); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	meta, err := api.LoadDatabaseMeta(path)
	if err != nil {
		t.Fatalf("LoadDatabaseMeta: %v", err)
	}
	if meta.Database == "" {
		t.Fatalf("expected database path to be populated")
	}
	if len(meta.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(meta.Tables))
	}

	data, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	payload := string(data)
	if !containsAll(payload, []string{"customers", "orders", "idx_orders_customer"}) {
		t.Fatalf("metadata missing expected entries: %s", payload)
	}
}

func containsAll(haystack string, needles []string) bool {
	for _, needle := range needles {
		if !strings.Contains(haystack, needle) {
			return false
		}
	}
	return true
}
