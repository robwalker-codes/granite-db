package exec_test

import (
	"path/filepath"
	"testing"

	"github.com/example/granite-db/engine/internal/catalog"
	engineexec "github.com/example/granite-db/engine/internal/exec"
	"github.com/example/granite-db/engine/internal/sql/parser"
	"github.com/example/granite-db/engine/internal/storage"
)

func TestExecutorSelectExpressions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "expr.gdb")
	if err := storage.New(path); err != nil {
		t.Fatalf("storage new: %v", err)
	}
	mgr, err := storage.Open(path)
	if err != nil {
		t.Fatalf("storage open: %v", err)
	}
	defer mgr.Close()

	cat, err := catalog.Load(mgr)
	if err != nil {
		t.Fatalf("catalog load: %v", err)
	}

	executor := engineexec.New(cat, mgr)

	mustExec(t, executor, "CREATE TABLE people(id INT NOT NULL, name VARCHAR(50), nick VARCHAR(50))")
	mustExec(t, executor, "INSERT INTO people(id, name, nick) VALUES (1, 'Ada', NULL)")
	mustExec(t, executor, "INSERT INTO people(id, name, nick) VALUES (2, 'Grace', 'G')")

	stmt, err := parser.Parse("SELECT id+1 AS next, UPPER(name) AS uname, COALESCE(nick,name) AS display FROM people ORDER BY id")
	if err != nil {
		t.Fatalf("parse select: %v", err)
	}
	res, err := executor.Execute(stmt)
	if err != nil {
		t.Fatalf("execute select: %v", err)
	}
	if want := []string{"next", "uname", "display"}; !equalStrings(res.Columns, want) {
		t.Fatalf("unexpected columns: %v", res.Columns)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Rows))
	}
	if got := res.Rows[0]; got[0] != "2" || got[1] != "ADA" || got[2] != "Ada" {
		t.Fatalf("unexpected first row: %v", got)
	}
	if got := res.Rows[1]; got[0] != "3" || got[1] != "GRACE" || got[2] != "G" {
		t.Fatalf("unexpected second row: %v", got)
	}

	stmt2, err := parser.Parse("SELECT 1+2*3 AS a, (1+2)*3 AS b")
	if err != nil {
		t.Fatalf("parse arithmetic select: %v", err)
	}
	res2, err := executor.Execute(stmt2)
	if err != nil {
		t.Fatalf("execute arithmetic select: %v", err)
	}
	if len(res2.Rows) != 1 {
		t.Fatalf("expected 1 row for arithmetic query, got %d", len(res2.Rows))
	}
	if row := res2.Rows[0]; row[0] != "7" || row[1] != "9" {
		t.Fatalf("unexpected arithmetic row: %v", row)
	}

	stmt3, err := parser.Parse("SELECT UPPER(id) FROM people")
	if err != nil {
		t.Fatalf("parse invalid select: %v", err)
	}
	if _, err := executor.Execute(stmt3); err == nil {
		t.Fatalf("expected error for invalid function usage")
	}
}

func mustExec(t *testing.T, executor *engineexec.Executor, sql string) {
	t.Helper()
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	if _, err := executor.Execute(stmt); err != nil {
		t.Fatalf("execute %q: %v", sql, err)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
