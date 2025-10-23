package api

import (
	"encoding/json"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/example/granite-db/engine/internal/catalog"
	"github.com/example/granite-db/engine/internal/exec"
	"github.com/example/granite-db/engine/internal/sql/parser"
	"github.com/example/granite-db/engine/internal/storage"
	"github.com/example/granite-db/engine/internal/storage/indexmgr"
	"github.com/example/granite-db/engine/internal/txn"
	"github.com/example/granite-db/engine/internal/wal"
)

// Database provides a public façade over the GraniteDB engine.
type Database struct {
	storage  *storage.Manager
	catalog  *catalog.Catalog
	executor *exec.Executor
	indexes  *indexmgr.Manager
	locks    *txn.LockManager
	txns     *txn.Manager
	wal      *wal.Manager
	mu       sync.Mutex
	sessions map[int64]*txn.Transaction
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
	log, err := wal.Open(path)
	if err != nil {
		mgr.Close()
		return nil, err
	}
	if err := recoverDatabase(mgr, log); err != nil {
		log.Close()
		mgr.Close()
		return nil, err
	}
	cat, err := catalog.Load(mgr)
	if err != nil {
		log.Close()
		mgr.Close()
		return nil, err
	}
	idx := indexmgr.New(mgr.Path())
	locks := txn.NewLockManager(0)
	txns := txn.NewManager(locks, log)
	return &Database{
		storage:  mgr,
		catalog:  cat,
		executor: exec.New(cat, mgr, idx, locks, log),
		indexes:  idx,
		locks:    locks,
		txns:     txns,
		wal:      log,
		sessions: make(map[int64]*txn.Transaction),
	}, nil
}

// Close flushes data and releases resources.
func (db *Database) Close() error {
	if db.storage == nil {
		return nil
	}
	if db.indexes != nil {
		_ = db.indexes.Close()
	}
	if db.wal != nil {
		_ = db.wal.Close()
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
	session := currentSessionID()
	switch stmt.(type) {
	case *parser.BeginStmt:
		return db.begin(session)
	case *parser.CommitStmt:
		return db.commit(session)
	case *parser.RollbackStmt:
		return db.rollback(session)
	default:
		return db.executeStatement(session, stmt)
	}
}

// ExecuteJSON runs the supplied SQL text and serialises the outcome for tooling.
func (db *Database) ExecuteJSON(sql string) ([]byte, error) {
	start := time.Now()
	res, err := db.Execute(sql)
	if err != nil {
		return nil, err
	}
	payload := struct {
		Columns      []string   `json:"columns"`
		Rows         [][]string `json:"rows"`
		DurationMs   int64      `json:"durationMs"`
		RowsAffected *int       `json:"rowsAffected,omitempty"`
		Message      string     `json:"message,omitempty"`
	}{
		Columns:    append([]string(nil), res.Columns...),
		Rows:       cloneRows(res.Rows),
		DurationMs: time.Since(start).Milliseconds(),
		Message:    res.Message,
	}
	if res.RowsAffected > 0 {
		value := res.RowsAffected
		payload.RowsAffected = &value
	}
	if payload.Message == "" {
		payload.Message = "Query executed"
	}
	return json.Marshal(payload)
}

func (db *Database) begin(session int64) (*exec.Result, error) {
	if db.txns == nil {
		return nil, fmt.Errorf("api: transaction support unavailable")
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, ok := db.sessions[session]; ok {
		return nil, fmt.Errorf("api: transaction already active")
	}
	tx := db.txns.Begin()
	db.sessions[session] = tx
	return &exec.Result{Message: "Transaction started"}, nil
}

func (db *Database) commit(session int64) (*exec.Result, error) {
	if db.txns == nil {
		return nil, fmt.Errorf("api: transaction support unavailable")
	}
	tx := db.sessionTxn(session)
	if tx == nil {
		return nil, fmt.Errorf("api: no active transaction")
	}
	if err := db.txns.Commit(tx.ID()); err != nil {
		return nil, err
	}
	db.clearSession(session)
	return &exec.Result{Message: "Transaction committed"}, nil
}

func (db *Database) rollback(session int64) (*exec.Result, error) {
	if db.txns == nil {
		return nil, fmt.Errorf("api: transaction support unavailable")
	}
	tx := db.sessionTxn(session)
	if tx == nil {
		return nil, fmt.Errorf("api: no active transaction")
	}
	if err := db.txns.Rollback(tx.ID()); err != nil {
		return nil, err
	}
	db.clearSession(session)
	return &exec.Result{Message: "Transaction rolled back"}, nil
}

func (db *Database) executeStatement(session int64, stmt parser.Statement) (*exec.Result, error) {
	if db.executor == nil {
		return nil, fmt.Errorf("api: database not open")
	}
	var (
		tx         *txn.Transaction
		autocommit bool
	)
	if existing := db.sessionTxn(session); existing != nil {
		tx = existing
	} else {
		if db.txns == nil {
			return nil, fmt.Errorf("api: transaction support unavailable")
		}
		tx = db.txns.Begin()
		tx.SetAutocommit(true)
		autocommit = true
	}
	res, err := db.executor.Execute(tx, stmt)
	if err != nil {
		if autocommit {
			if rbErr := db.txns.Rollback(tx.ID()); rbErr != nil {
				return nil, fmt.Errorf("api: rollback failed after error: %v (original: %w)", rbErr, err)
			}
		}
		return nil, err
	}
	if autocommit {
		if err := db.txns.Commit(tx.ID()); err != nil {
			return nil, err
		}
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

// ExplainJSON serialises the physical plan for the provided SQL statement.
func (db *Database) ExplainJSON(sql string) ([]byte, error) {
	stmt, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}
	plan, err := db.executor.Explain(stmt)
	if err != nil {
		return nil, err
	}
	physical, err := db.executor.PhysicalPlan(stmt)
	if err != nil {
		return nil, err
	}
	payload := struct {
		Version  int                    `json:"version"`
		Physical *exec.PhysicalPlanNode `json:"physical,omitempty"`
		Text     string                 `json:"text"`
	}{
		Version:  1,
		Physical: physical,
		Text:     plan.Text(),
	}
	return json.Marshal(payload)
}

// Tables returns copies of table metadata for inspection.
func (db *Database) Tables() ([]*catalog.Table, error) {
	if db.catalog == nil {
		return nil, fmt.Errorf("api: database not open")
	}
	return db.catalog.ListTables(), nil
}

func (db *Database) sessionTxn(session int64) *txn.Transaction {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.sessions[session]
}

func (db *Database) clearSession(session int64) {
	db.mu.Lock()
	delete(db.sessions, session)
	db.mu.Unlock()
}

func currentSessionID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	if n <= 0 {
		return 0
	}
	fields := strings.Fields(string(buf[:n]))
	if len(fields) < 2 {
		return 0
	}
	id, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return id
}

func formatColumnType(col catalog.Column) string {
	switch col.Type {
	case catalog.ColumnTypeInt:
		return "INT"
	case catalog.ColumnTypeBigInt:
		return "BIGINT"
	case catalog.ColumnTypeVarChar:
		return fmt.Sprintf("VARCHAR(%d)", col.Length)
	case catalog.ColumnTypeBoolean:
		return "BOOLEAN"
	case catalog.ColumnTypeDate:
		return "DATE"
	case catalog.ColumnTypeTimestamp:
		return "TIMESTAMP"
	case catalog.ColumnTypeDecimal:
		return fmt.Sprintf("DECIMAL(%d,%d)", col.Precision, col.Scale)
	default:
		return "UNKNOWN"
	}
}

func cloneRows(rows [][]string) [][]string {
	if len(rows) == 0 {
		return nil
	}
	out := make([][]string, len(rows))
	for i, row := range rows {
		copyRow := make([]string, len(row))
		copy(copyRow, row)
		out[i] = copyRow
	}
	return out
}
