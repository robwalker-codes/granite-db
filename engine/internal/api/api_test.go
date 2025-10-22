package api_test

import (
	"path/filepath"
	"testing"

	"github.com/example/granite-db/engine/internal/api"
)

func TestEndToEndWorkflow(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "demo.gdb")
	if err := api.Create(path); err != nil {
		t.Fatalf("create: %v", err)
	}
	db, err := api.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.Execute("CREATE TABLE people(id INT NOT NULL, name VARCHAR(50), PRIMARY KEY(id))"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Execute("INSERT INTO people(id, name) VALUES (1, 'Ada')"); err != nil {
		t.Fatalf("insert 1: %v", err)
	}
	if _, err := db.Execute("INSERT INTO people(id, name) VALUES (2, 'Grace')"); err != nil {
		t.Fatalf("insert 2: %v", err)
	}

	res, err := db.Execute("SELECT * FROM people")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Rows))
	}
	if res.Rows[0][0] != "1" || res.Rows[0][1] != "Ada" {
		t.Fatalf("unexpected first row: %v", res.Rows[0])
	}
	if res.Rows[1][0] != "2" || res.Rows[1][1] != "Grace" {
		t.Fatalf("unexpected second row: %v", res.Rows[1])
	}
}
