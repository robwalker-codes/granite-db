package catalog_test

import (
	"path/filepath"
	"testing"

	"github.com/example/granite-db/engine/internal/catalog"
	"github.com/example/granite-db/engine/internal/storage"
)

func TestCatalogCreateAndListTables(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.gdb")
	if err := storage.New(path); err != nil {
		t.Fatalf("create db: %v", err)
	}
	mgr, err := storage.Open(path)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer mgr.Close()

	cat, err := catalog.Load(mgr)
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}

	cols := []catalog.Column{
		{Name: "id", Type: catalog.ColumnTypeInt, NotNull: true},
		{Name: "name", Type: catalog.ColumnTypeVarChar, Length: 32},
	}
	table, err := cat.CreateTable("people", cols, "id")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	if table.RootPage == 0 {
		t.Fatalf("expected root page allocated")
	}

	tables := cat.ListTables()
	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(tables))
	}
	got := tables[0]
	if got.Name != "people" {
		t.Fatalf("expected table name people, got %s", got.Name)
	}
	if len(got.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(got.Columns))
	}
	if !got.Columns[0].PrimaryKey {
		t.Fatalf("expected id to be primary key")
	}
	if got.Columns[1].PrimaryKey {
		t.Fatalf("name should not be primary key")
	}
}

func TestCatalogPersistDecimalMetadata(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.gdb")
	if err := storage.New(path); err != nil {
		t.Fatalf("create db: %v", err)
	}
	mgr, err := storage.Open(path)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	cat, err := catalog.Load(mgr)
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	cols := []catalog.Column{
		{Name: "id", Type: catalog.ColumnTypeInt},
		{Name: "balance", Type: catalog.ColumnTypeDecimal, Precision: 18, Scale: 4},
	}
	if _, err := cat.CreateTable("accounts", cols, ""); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("close manager: %v", err)
	}
	mgr, err = storage.Open(path)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer mgr.Close()
	cat, err = catalog.Load(mgr)
	if err != nil {
		t.Fatalf("reload catalog: %v", err)
	}
	table, ok := cat.GetTable("accounts")
	if !ok {
		t.Fatalf("expected accounts table present")
	}
	if len(table.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(table.Columns))
	}
	balance := table.Columns[1]
	if balance.Type != catalog.ColumnTypeDecimal {
		t.Fatalf("expected DECIMAL type, got %v", balance.Type)
	}
	if balance.Precision != 18 || balance.Scale != 4 {
		t.Fatalf("unexpected precision/scale: %d/%d", balance.Precision, balance.Scale)
	}
}
