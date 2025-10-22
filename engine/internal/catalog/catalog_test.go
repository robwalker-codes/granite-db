package catalog_test

import (
	"path/filepath"
	"strings"
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
	table, err := cat.CreateTable("people", cols, "id", nil)
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
	if _, err := cat.CreateTable("accounts", cols, "", nil); err != nil {
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

func TestCatalogPersistIndexes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "idx.gdb")
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
	cols := []catalog.Column{{Name: "id", Type: catalog.ColumnTypeInt, NotNull: true}, {Name: "name", Type: catalog.ColumnTypeVarChar, Length: 32}}
	if _, err := cat.CreateTable("people", cols, "id", nil); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := cat.CreateIndex("people", "idx_people_name", []string{"name"}, true); err != nil {
		t.Fatalf("create index: %v", err)
	}
	mgr.Close()

	mgr, err = storage.Open(path)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer mgr.Close()
	cat, err = catalog.Load(mgr)
	if err != nil {
		t.Fatalf("reload catalog: %v", err)
	}
	table, ok := cat.GetTable("people")
	if !ok {
		t.Fatalf("expected people table present")
	}
	if len(table.Indexes) != 1 {
		t.Fatalf("expected 1 index, got %d", len(table.Indexes))
	}
	idx, ok := table.Indexes["idx_people_name"]
	if !ok {
		for name := range table.Indexes {
			if strings.EqualFold(name, "idx_people_name") {
				idx = table.Indexes[name]
				ok = true
				break
			}
		}
	}
	if !ok {
		t.Fatalf("expected idx_people_name present")
	}
	if !idx.IsUnique {
		t.Fatalf("expected unique index")
	}
	if len(idx.Columns) != 1 || idx.Columns[0] != "name" {
		t.Fatalf("unexpected columns: %+v", idx.Columns)
	}
}

func TestCatalogPersistForeignKeys(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fk.gdb")
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
	parentCols := []catalog.Column{{Name: "id", Type: catalog.ColumnTypeInt, NotNull: true}}
	if _, err := cat.CreateTable("parents", parentCols, "id", nil); err != nil {
		t.Fatalf("create parents: %v", err)
	}
	childCols := []catalog.Column{{Name: "id", Type: catalog.ColumnTypeInt, NotNull: true}, {Name: "parent_id", Type: catalog.ColumnTypeInt}}
	fk := &catalog.ForeignKey{
		Name:          "fk_children_parent",
		ChildColumns:  []string{"parent_id"},
		ParentTable:   "parents",
		ParentColumns: []string{"id"},
		OnDelete:      catalog.ForeignKeyActionRestrict,
		OnUpdate:      catalog.ForeignKeyActionRestrict,
		Deferrable:    false,
		Valid:         true,
	}
	if _, err := cat.CreateTable("children", childCols, "id", []*catalog.ForeignKey{fk}); err != nil {
		t.Fatalf("create children: %v", err)
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
	table, ok := cat.GetTable("children")
	if !ok {
		t.Fatalf("expected children table present")
	}
	if len(table.ForeignKeys) != 1 {
		t.Fatalf("expected 1 foreign key, got %d", len(table.ForeignKeys))
	}
	var restored *catalog.ForeignKey
	for _, candidate := range table.ForeignKeys {
		if strings.EqualFold(candidate.Name, "fk_children_parent") {
			restored = candidate
			break
		}
	}
	if restored == nil {
		t.Fatalf("foreign key fk_children_parent not found")
	}
	if len(restored.ChildColumns) != 1 || restored.ChildColumns[0] != "parent_id" {
		t.Fatalf("unexpected child columns: %+v", restored.ChildColumns)
	}
	if restored.ParentTable != "parents" {
		t.Fatalf("expected parent table parents, got %s", restored.ParentTable)
	}
	if len(restored.ParentColumns) != 1 || restored.ParentColumns[0] != "id" {
		t.Fatalf("unexpected parent columns: %+v", restored.ParentColumns)
	}
	if !restored.Valid {
		t.Fatalf("expected foreign key to be marked valid")
	}
}
