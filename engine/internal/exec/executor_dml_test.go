package exec_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/example/granite-db/engine/internal/catalog"
	engineexec "github.com/example/granite-db/engine/internal/exec"
	"github.com/example/granite-db/engine/internal/sql/parser"
	"github.com/example/granite-db/engine/internal/storage"
	"github.com/example/granite-db/engine/internal/storage/indexmgr"
)

func newDMLExecutor(t *testing.T) (*engineexec.Executor, func()) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "dml.gdb")
	if err := storage.New(path); err != nil {
		t.Fatalf("storage new: %v", err)
	}
	mgr, err := storage.Open(path)
	if err != nil {
		t.Fatalf("storage open: %v", err)
	}
	cat, err := catalog.Load(mgr)
	if err != nil {
		t.Fatalf("catalog load: %v", err)
	}
	idx := indexmgr.New(mgr.Path())
	executor := engineexec.New(cat, mgr, idx)
	cleanup := func() {
		idx.Close()
		mgr.Close()
	}
	return executor, cleanup
}

func execExpectError(t *testing.T, executor *engineexec.Executor, sql string) error {
	t.Helper()
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	if _, err := executor.Execute(stmt); err == nil {
		t.Fatalf("expected error executing %q", sql)
	} else {
		return err
	}
	return nil
}

func TestExecutorForeignKeyEnforcementSingleColumn(t *testing.T) {
	executor, cleanup := newDMLExecutor(t)
	defer cleanup()

	mustExec(t, executor, "CREATE TABLE customers(id INT NOT NULL, name VARCHAR(50), PRIMARY KEY(id))")
	mustExec(t, executor, `CREATE TABLE orders(
                id INT NOT NULL,
                customer_id INT,
                total DECIMAL(10,2),
                PRIMARY KEY(id),
                CONSTRAINT fk_orders_customer FOREIGN KEY(customer_id)
                        REFERENCES customers(id)
                        ON DELETE RESTRICT ON UPDATE RESTRICT
        )`)

	mustExec(t, executor, "INSERT INTO customers(id, name) VALUES (1,'Ada'),(2,'Grace')")
	mustExec(t, executor, "INSERT INTO orders(id, customer_id, total) VALUES (100,1,42.50)")
	mustExec(t, executor, "INSERT INTO orders(id, customer_id, total) VALUES (101,2,7.50)")

	if err := execExpectError(t, executor, "INSERT INTO orders(id, customer_id, total) VALUES (101,3,10.00)"); !strings.Contains(err.Error(), "no parent row") {
		t.Fatalf("expected missing parent error, got %v", err)
	}

	if err := execExpectError(t, executor, "DELETE FROM customers WHERE id=1"); !strings.Contains(err.Error(), "referenced by \"orders\"") {
		t.Fatalf("expected referencing child error, got %v", err)
	}

	mustExec(t, executor, "INSERT INTO orders(id, customer_id, total) VALUES (102,NULL,5.00)")

	if err := execExpectError(t, executor, "UPDATE orders SET customer_id=99 WHERE id=100"); !strings.Contains(err.Error(), "no parent row") {
		t.Fatalf("expected update parent check error, got %v", err)
	}

	if err := execExpectError(t, executor, "UPDATE customers SET id=9 WHERE id=2"); !strings.Contains(err.Error(), "referenced by \"orders\"") {
		t.Fatalf("expected parent update restriction, got %v", err)
	}

	mustExec(t, executor, "UPDATE orders SET customer_id=NULL WHERE id=100")
	mustExec(t, executor, "UPDATE orders SET customer_id=NULL WHERE id=101")
	mustExec(t, executor, "DELETE FROM customers WHERE id=2")
}

func TestExecutorForeignKeyCompositeKeys(t *testing.T) {
	executor, cleanup := newDMLExecutor(t)
	defer cleanup()

	mustExec(t, executor, "CREATE TABLE categories(code INT NOT NULL, region INT NOT NULL, name VARCHAR(20))")
	mustExec(t, executor, "CREATE UNIQUE INDEX idx_categories_code_region ON categories(code, region)")
	mustExec(t, executor, `CREATE TABLE items(
                id INT PRIMARY KEY,
                category_code INT,
                category_region INT,
                CONSTRAINT fk_items_category FOREIGN KEY(category_code, category_region)
                        REFERENCES categories(code, region)
                        ON DELETE RESTRICT ON UPDATE RESTRICT
        )`)

	mustExec(t, executor, "INSERT INTO categories(code, region, name) VALUES (1,10,'One'),(1,20,'Other')")
	mustExec(t, executor, "INSERT INTO items(id, category_code, category_region) VALUES (1,1,10)")

	if err := execExpectError(t, executor, "INSERT INTO items(id, category_code, category_region) VALUES (2,1,99)"); !strings.Contains(err.Error(), "no parent row") {
		t.Fatalf("expected composite missing parent error, got %v", err)
	}

	if err := execExpectError(t, executor, "DELETE FROM categories WHERE code=1 AND region=10"); !strings.Contains(err.Error(), "referenced by \"items\"") {
		t.Fatalf("expected composite delete restriction, got %v", err)
	}

	if err := execExpectError(t, executor, "UPDATE categories SET code=5 WHERE code=1 AND region=10"); !strings.Contains(err.Error(), "referenced by \"items\"") {
		t.Fatalf("expected composite update restriction, got %v", err)
	}

	mustExec(t, executor, "UPDATE items SET category_code=NULL, category_region=NULL WHERE id=1")
	mustExec(t, executor, "DELETE FROM categories WHERE code=1 AND region=10")
}

func TestExecutorForeignKeyIndexAssistedLookup(t *testing.T) {
	executor, cleanup := newDMLExecutor(t)
	defer cleanup()

	mustExec(t, executor, "CREATE TABLE parents(id INT PRIMARY KEY, name VARCHAR(40))")
	mustExec(t, executor, `CREATE TABLE dependents(
                id INT PRIMARY KEY,
                parent_id INT,
                CONSTRAINT fk_dependents_parent FOREIGN KEY(parent_id)
                        REFERENCES parents(id)
                        ON DELETE RESTRICT ON UPDATE RESTRICT
        )`)
	mustExec(t, executor, "CREATE INDEX idx_dependents_parent ON dependents(parent_id)")

	mustExec(t, executor, "INSERT INTO parents(id, name) VALUES (1,'Ada')")
	mustExec(t, executor, "INSERT INTO dependents(id, parent_id) VALUES (10,1)")

	if err := execExpectError(t, executor, "DELETE FROM parents WHERE id=1"); !strings.Contains(err.Error(), "referenced by \"dependents\"") {
		t.Fatalf("expected delete restriction with index, got %v", err)
	}

	mustExec(t, executor, "DELETE FROM dependents WHERE id=10")
	mustExec(t, executor, "DELETE FROM parents WHERE id=1")
}
