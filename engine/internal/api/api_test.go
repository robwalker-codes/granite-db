package api_test

import (
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/example/granite-db/engine/internal/api"
	engineexec "github.com/example/granite-db/engine/internal/exec"
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

func TestCatalogPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persist.gdb")
	if err := api.Create(path); err != nil {
		t.Fatalf("create: %v", err)
	}
	db, err := api.Open(path)
	if err != nil {
		t.Fatalf("open initial: %v", err)
	}
	if _, err := db.Execute("CREATE TABLE people(id INT, name VARCHAR(50))"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close initial: %v", err)
	}

	reopened, err := api.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	tables, err := reopened.Tables()
	if err != nil {
		t.Fatalf("tables: %v", err)
	}
	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(tables))
	}
	if tables[0].Name != "people" {
		t.Fatalf("unexpected table name: %s", tables[0].Name)
	}
	if len(tables[0].Columns) != 2 {
		t.Fatalf("unexpected column count: %d", len(tables[0].Columns))
	}
}

func TestExplicitTransactionCommitAndRollback(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "txn.gdb")
	if err := api.Create(path); err != nil {
		t.Fatalf("create: %v", err)
	}
	db, err := api.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mustExec(t, db, "CREATE TABLE items(id INT PRIMARY KEY, value INT)")
	mustExec(t, db, "INSERT INTO items VALUES (1, 10)")

	mustExec(t, db, "BEGIN")
	mustExec(t, db, "UPDATE items SET value = 20 WHERE id = 1")
	mustExec(t, db, "COMMIT")

	res := mustQuery(t, db, "SELECT value FROM items WHERE id = 1")
	if len(res.Rows) != 1 || res.Rows[0][0] != "20" {
		t.Fatalf("expected committed value 20, got %v", res.Rows)
	}

	mustExec(t, db, "BEGIN")
	mustExec(t, db, "UPDATE items SET value = 99 WHERE id = 1")
	mustExec(t, db, "ROLLBACK")

	res2 := mustQuery(t, db, "SELECT value FROM items WHERE id = 1")
	if len(res2.Rows) != 1 || res2.Rows[0][0] != "20" {
		t.Fatalf("expected rollback to preserve value 20, got %v", res2.Rows)
	}

	if _, err := db.Execute("COMMIT"); err == nil {
		t.Fatalf("expected error when committing without active transaction")
	}
	if _, err := db.Execute("ROLLBACK"); err == nil {
		t.Fatalf("expected error when rolling back without active transaction")
	}
}

func TestReadCommittedPreventsDirtyRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "isolation.gdb")
	if err := api.Create(path); err != nil {
		t.Fatalf("create: %v", err)
	}
	db, err := api.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mustExec(t, db, "CREATE TABLE totals(id INT PRIMARY KEY, amount INT)")
	mustExec(t, db, "INSERT INTO totals VALUES (1, 10)")

	mustExec(t, db, "BEGIN")
	mustExec(t, db, "UPDATE totals SET amount = 20 WHERE id = 1")

	done := make(chan struct{})
	var result string
	var selectErr error
	go func() {
		defer close(done)
		res, err := db.Execute("SELECT amount FROM totals WHERE id = 1")
		selectErr = err
		if err == nil && len(res.Rows) == 1 {
			result = res.Rows[0][0]
		}
	}()

	select {
	case <-done:
		t.Fatalf("select returned before commit, dirty read possible")
	case <-time.After(150 * time.Millisecond):
	}

	mustExec(t, db, "COMMIT")
	<-done
	if selectErr != nil {
		t.Fatalf("select failed: %v", selectErr)
	}
	if result != "20" {
		t.Fatalf("expected select to observe committed value 20, got %s", result)
	}
}

func TestWriteContentionTimeout(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "locks.gdb")
	if err := api.Create(path); err != nil {
		t.Fatalf("create: %v", err)
	}
	db, err := api.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mustExec(t, db, "CREATE TABLE contested(id INT PRIMARY KEY, value INT)")
	mustExec(t, db, "INSERT INTO contested VALUES (1, 5)")

	mustExec(t, db, "BEGIN")
	mustExec(t, db, "UPDATE contested SET value = 10 WHERE id = 1")

	var wg sync.WaitGroup
	wg.Add(1)
	var errMsg string
	go func() {
		defer wg.Done()
		_, err := db.Execute("UPDATE contested SET value = 15 WHERE id = 1")
		if err == nil {
			errMsg = "expected timeout"
		} else {
			errMsg = err.Error()
		}
	}()

	wg.Wait()
	if errMsg == "expected timeout" {
		t.Fatalf("second update unexpectedly succeeded")
	}
	if errMsg == "" || !containsTimeout(errMsg) {
		t.Fatalf("expected lock timeout error, got %q", errMsg)
	}

	mustExec(t, db, "ROLLBACK")
}

func mustExec(t *testing.T, db *api.Database, sql string) {
	t.Helper()
	if _, err := db.Execute(sql); err != nil {
		t.Fatalf("execute %q: %v", sql, err)
	}
}

func mustQuery(t *testing.T, db *api.Database, sql string) *engineexec.Result {
	t.Helper()
	res, err := db.Execute(sql)
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	return res
}

func containsTimeout(msg string) bool {
	return strings.Contains(msg, "lock timeout")
}
