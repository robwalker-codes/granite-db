package exec_test

import (
        "path/filepath"
        "sort"
        "testing"

        "github.com/example/granite-db/engine/internal/catalog"
        engineexec "github.com/example/granite-db/engine/internal/exec"
        "github.com/example/granite-db/engine/internal/sql/parser"
        "github.com/example/granite-db/engine/internal/storage"
        "github.com/example/granite-db/engine/internal/storage/indexmgr"
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

        idx := indexmgr.New(mgr.Path())
        defer idx.Close()
        executor := engineexec.New(cat, mgr, idx)

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

func TestExecutorJoins(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "join.gdb")
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
        idx := indexmgr.New(mgr.Path())
        defer idx.Close()
        executor := engineexec.New(cat, mgr, idx)

	mustExec(t, executor, "CREATE TABLE customers(id INT NOT NULL, name VARCHAR(50), PRIMARY KEY(id))")
	mustExec(t, executor, "CREATE TABLE orders(id INT NOT NULL, customer_id INT, total INT, PRIMARY KEY(id))")
	mustExec(t, executor, "INSERT INTO customers(id, name) VALUES (1,'Ada'),(2,'Grace'),(3,'Lin')")
	mustExec(t, executor, "INSERT INTO orders(id, customer_id, total) VALUES (100,1,4250),(101,1,725),(200,2,9999)")

	inner := execQuery(t, executor, "SELECT c.name, o.total FROM customers c JOIN orders o ON c.id=o.customer_id ORDER BY o.id")
	expectedInner := [][]string{{"Ada", "4250"}, {"Ada", "725"}, {"Grace", "9999"}}
	if !equalRows(inner.Rows, expectedInner) {
		t.Fatalf("unexpected INNER JOIN rows: %v", inner.Rows)
	}

	left := execQuery(t, executor, "SELECT c.id, c.name, o.id AS order_id FROM customers c LEFT JOIN orders o ON c.id=o.customer_id")
	expectedLeft := [][]string{{"1", "Ada", "100"}, {"1", "Ada", "101"}, {"2", "Grace", "200"}, {"3", "Lin", "NULL"}}
	if !equalRows(sortedRows(left.Rows, func(a, b []string) bool {
		if a[0] == b[0] {
			return a[2] < b[2]
		}
		return a[0] < b[0]
	}), expectedLeft) {
		t.Fatalf("unexpected LEFT JOIN rows: %v", left.Rows)
	}

	filtered := execQuery(t, executor, "SELECT c.name, o.total FROM customers c LEFT JOIN orders o ON c.id=o.customer_id WHERE o.total > 1000 ORDER BY c.name")
	expectedFiltered := [][]string{{"Ada", "4250"}, {"Grace", "9999"}}
	if !equalRows(filtered.Rows, expectedFiltered) {
		t.Fatalf("unexpected filtered rows: %v", filtered.Rows)
	}

	nonEqui := execQuery(t, executor, "SELECT c.id, o.id FROM customers c JOIN orders o ON c.id < o.customer_id")
	expectedNonEqui := [][]string{{"1", "200"}}
	if !equalRows(sortedRows(nonEqui.Rows, func(a, b []string) bool {
		if a[0] == b[0] {
			return a[1] < b[1]
		}
		return a[0] < b[0]
	}), expectedNonEqui) {
		t.Fatalf("unexpected non-equi rows: %v", nonEqui.Rows)
	}
}

func TestExecutorDecimalInsertSelect(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "decimal.gdb")
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
        idx := indexmgr.New(mgr.Path())
        defer idx.Close()
        executor := engineexec.New(cat, mgr, idx)

	mustExec(t, executor, "CREATE TABLE accounts(id INT, balance DECIMAL(10,2) NOT NULL, PRIMARY KEY(id))")
	mustExec(t, executor, "INSERT INTO accounts(id, balance) VALUES (1, 12.34)")
	mustExec(t, executor, "INSERT INTO accounts(id, balance) VALUES (2, '56.00')")

	stmt, err := parser.Parse("SELECT balance FROM accounts ORDER BY id")
	if err != nil {
		t.Fatalf("parse select: %v", err)
	}
	res, err := executor.Execute(stmt)
	if err != nil {
		t.Fatalf("execute select: %v", err)
	}
	expected := [][]string{{"12.34"}, {"56.00"}}
	if !equalRows(res.Rows, expected) {
		t.Fatalf("unexpected balances: %v", res.Rows)
	}

	stmt2, err := parser.Parse("INSERT INTO accounts(id, balance) VALUES (3, 123456789.12)")
	if err != nil {
		t.Fatalf("parse overflow insert: %v", err)
	}
	if _, err := executor.Execute(stmt2); err == nil {
		t.Fatalf("expected precision error for large DECIMAL insert")
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

func execQuery(t *testing.T, executor *engineexec.Executor, sql string) *engineexec.Result {
	t.Helper()
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	res, err := executor.Execute(stmt)
	if err != nil {
		t.Fatalf("execute %q: %v", sql, err)
	}
	return res
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

func equalRows(a, b [][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !equalStrings(a[i], b[i]) {
			return false
		}
	}
	return true
}

func sortedRows(rows [][]string, less func(a, b []string) bool) [][]string {
	clone := make([][]string, len(rows))
	for i, row := range rows {
		copied := make([]string, len(row))
		copy(copied, row)
		clone[i] = copied
	}
	sort.Slice(clone, func(i, j int) bool {
		return less(clone[i], clone[j])
	})
	return clone
}

func TestExecutorIndexScan(t *testing.T) {
        dir := t.TempDir()
        path := filepath.Join(dir, "index.gdb")
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
        idx := indexmgr.New(mgr.Path())
        defer idx.Close()
        executor := engineexec.New(cat, mgr, idx)

        mustExec(t, executor, "CREATE TABLE orders(id INT PRIMARY KEY, total INT)")
        mustExec(t, executor, "INSERT INTO orders VALUES (1, 4250), (2, 725), (3, 9999), (4, 1200)")
        mustExec(t, executor, "CREATE INDEX idx_total ON orders(total)")

        res := execQuery(t, executor, "SELECT id FROM orders WHERE total > 5000 ORDER BY id")
        expected := [][]string{{"3"}}
        if !equalRows(res.Rows, expected) {
                t.Fatalf("unexpected rows: %v", res.Rows)
        }

        planStmt, err := parser.Parse("SELECT id FROM orders WHERE total > 5000")
        if err != nil {
                t.Fatalf("parse explain: %v", err)
        }
        plan, err := executor.Explain(planStmt)
        if err != nil {
                t.Fatalf("explain: %v", err)
        }
        if !containsIndexScan(plan.Root) {
                t.Fatalf("expected plan to include IndexScan, got %v", plan.Root)
        }
}

func TestExecutorUniqueIndex(t *testing.T) {
        dir := t.TempDir()
        path := filepath.Join(dir, "unique.gdb")
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
        idx := indexmgr.New(mgr.Path())
        defer idx.Close()
        executor := engineexec.New(cat, mgr, idx)

        mustExec(t, executor, "CREATE TABLE customers(id INT PRIMARY KEY, name VARCHAR(50))")
        mustExec(t, executor, "INSERT INTO customers VALUES (1,'Ada'),(2,'Grace')")
        mustExec(t, executor, "CREATE UNIQUE INDEX idx_customer_name ON customers(name)")

        dupStmt, err := parser.Parse("INSERT INTO customers VALUES (3,'Ada')")
        if err != nil {
                t.Fatalf("parse duplicate: %v", err)
        }
        if _, err := executor.Execute(dupStmt); err == nil {
                t.Fatalf("expected unique violation error")
        }
}

func containsIndexScan(node *engineexec.PlanNode) bool {
        if node == nil {
                return false
        }
        if node.Name == "IndexScan" {
                return true
        }
        for _, child := range node.Children {
                if containsIndexScan(child) {
                        return true
                }
        }
        return false
}
