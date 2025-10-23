package api

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"

	"github.com/example/granite-db/engine/internal/catalog"
)

// DatabaseMeta summarises the schema structure for tooling integration.
type DatabaseMeta struct {
	Database string      `json:"database"`
	Tables   []TableMeta `json:"tables"`
}

// TableMeta captures table-level metadata.
type TableMeta struct {
	Name        string           `json:"name"`
	RowCount    int64            `json:"rowCount"`
	Columns     []ColumnMeta     `json:"columns"`
	Indexes     []IndexMeta      `json:"indexes"`
	ForeignKeys []ForeignKeyMeta `json:"foreignKeys"`
}

// ColumnMeta describes a column definition.
type ColumnMeta struct {
	Name         string  `json:"name"`
	Type         string  `json:"type"`
	NotNull      bool    `json:"notNull"`
	DefaultValue *string `json:"default"`
	IsPrimaryKey bool    `json:"isPrimaryKey"`
}

// IndexMeta outlines an index entry.
type IndexMeta struct {
	Name    string   `json:"name"`
	Unique  bool     `json:"unique"`
	Columns []string `json:"columns"`
	Type    string   `json:"type"`
}

// ForeignKeyMeta lists referential constraints.
type ForeignKeyMeta struct {
	Name        string   `json:"name"`
	FromColumns []string `json:"fromColumns"`
	ToTable     string   `json:"toTable"`
	ToColumns   []string `json:"toColumns"`
	OnDelete    string   `json:"onDelete"`
	OnUpdate    string   `json:"onUpdate"`
}

// LoadDatabaseMeta opens the database at dbPath and extracts catalogue metadata.
func LoadDatabaseMeta(dbPath string) (DatabaseMeta, error) {
	db, err := Open(dbPath)
	if err != nil {
		return DatabaseMeta{}, err
	}
	defer db.Close()

	meta, err := db.DatabaseMeta()
	if err != nil {
		return DatabaseMeta{}, err
	}
	if meta.Database == "" {
		meta.Database = dbPath
	}
	return meta, nil
}

// DatabaseMeta gathers schema information for an open database handle.
func (db *Database) DatabaseMeta() (DatabaseMeta, error) {
	if db.catalog == nil {
		return DatabaseMeta{}, fmt.Errorf("api: database not open")
	}
	tables := db.catalog.ListTables()
	meta := DatabaseMeta{
		Tables: make([]TableMeta, len(tables)),
	}
	if db.storage != nil {
		meta.Database = db.storage.Path()
	}
	for i, table := range tables {
		meta.Tables[i] = buildTableMeta(table)
	}
	return meta, nil
}

// MetadataJSON returns the schema metadata encoded as JSON.
func (db *Database) MetadataJSON() ([]byte, error) {
	meta, err := db.DatabaseMeta()
	if err != nil {
		return nil, err
	}
	return json.Marshal(meta)
}

func buildTableMeta(table *catalog.Table) TableMeta {
	columns := make([]ColumnMeta, len(table.Columns))
	for i, col := range table.Columns {
		columns[i] = ColumnMeta{
			Name:         col.Name,
			Type:         formatColumnType(col),
			NotNull:      col.NotNull,
			DefaultValue: nil,
			IsPrimaryKey: col.PrimaryKey,
		}
	}

	indexes := make([]IndexMeta, 0, len(table.Indexes))
	if len(table.Indexes) > 0 {
		names := make([]string, 0, len(table.Indexes))
		for key := range table.Indexes {
			names = append(names, key)
		}
		sort.Strings(names)
		for _, key := range names {
			idx := table.Indexes[key]
			cols := make([]string, len(idx.Columns))
			copy(cols, idx.Columns)
			indexes = append(indexes, IndexMeta{
				Name:    idx.Name,
				Unique:  idx.IsUnique,
				Columns: cols,
				Type:    "BTREE",
			})
		}
	}

	foreignKeys := make([]ForeignKeyMeta, 0, len(table.ForeignKeys))
	if len(table.ForeignKeys) > 0 {
		names := make([]string, 0, len(table.ForeignKeys))
		for key := range table.ForeignKeys {
			names = append(names, key)
		}
		sort.Strings(names)
		for _, key := range names {
			fk := table.ForeignKeys[key]
			child := make([]string, len(fk.ChildColumns))
			copy(child, fk.ChildColumns)
			parent := make([]string, len(fk.ParentColumns))
			copy(parent, fk.ParentColumns)
			foreignKeys = append(foreignKeys, ForeignKeyMeta{
				Name:        fk.Name,
				FromColumns: child,
				ToTable:     fk.ParentTable,
				ToColumns:   parent,
				OnDelete:    actionName(fk.OnDelete),
				OnUpdate:    actionName(fk.OnUpdate),
			})
		}
	}

	rowCount := int64(-1)
	if table.RowCount <= math.MaxInt64 {
		rowCount = int64(table.RowCount)
	}

	return TableMeta{
		Name:        table.Name,
		RowCount:    rowCount,
		Columns:     columns,
		Indexes:     indexes,
		ForeignKeys: foreignKeys,
	}
}

func actionName(action catalog.ForeignKeyAction) string {
	switch action {
	case catalog.ForeignKeyActionRestrict:
		return "RESTRICT"
	case catalog.ForeignKeyActionNoAction:
		return "NO ACTION"
	default:
		return "UNKNOWN"
	}
}
