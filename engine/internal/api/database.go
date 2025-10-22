package api

import (
	"fmt"

	"github.com/example/granite-db/engine/internal/catalog"
	"github.com/example/granite-db/engine/internal/exec"
	"github.com/example/granite-db/engine/internal/sql/parser"
	"github.com/example/granite-db/engine/internal/storage"
)

// Database provides a public façade over the GraniteDB engine.
type Database struct {
	storage  *storage.Manager
	catalog  *catalog.Catalog
	executor *exec.Executor
}

// Create initialises a new GraniteDB database file at the given path.
func Create(path string) error {
	return storage.New(path)
}

// Open loads an existing database and prepares it for SQL execution.
func Open(path string) (*Database, error) {
	mgr, err := storage.Open(path)
	if err != nil {
		return nil, err
	}
	cat, err := catalog.Load(mgr)
	if err != nil {
		mgr.Close()
		return nil, err
	}
	return &Database{storage: mgr, catalog: cat, executor: exec.New(cat, mgr)}, nil
}

// Close flushes data and releases resources.
func (db *Database) Close() error {
	if db.storage == nil {
		return nil
	}
	err := db.storage.Close()
	db.storage = nil
	return err
}

// Execute parses and executes the provided SQL statement string.
func (db *Database) Execute(sql string) (*exec.Result, error) {
	stmt, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}
	res, err := db.executor.Execute(stmt)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Explain parses the SQL string and returns the executor's plan representation.
func (db *Database) Explain(sql string) (*exec.Plan, error) {
	stmt, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}
	plan, err := db.executor.Explain(stmt)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

// Tables returns copies of table metadata for inspection.
func (db *Database) Tables() ([]*catalog.Table, error) {
	if db.catalog == nil {
		return nil, fmt.Errorf("api: database not open")
	}
	return db.catalog.ListTables(), nil
}
