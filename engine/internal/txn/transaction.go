package txn

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// ID uniquely identifies an active transaction.
type ID uint64

// State represents the lifecycle state of a transaction.
type State int

const (
	// StateActive indicates the transaction is currently running.
	StateActive State = iota
	// StateCommitted indicates the transaction has been committed.
	StateCommitted
	// StateRolledBack indicates the transaction has been rolled back.
	StateRolledBack
)

// HeldLock records a granted lock for inspection and diagnostics.
type HeldLock struct {
	Resource Resource
	Mode     LockMode
}

// WriteOperation captures metadata for a modification performed within a transaction.
type WriteOperation struct {
	Table string
	Kind  string
}

// Transaction represents a unit of work executed against the database.
type Transaction struct {
	mu         sync.Mutex
	id         ID
	state      State
	startTime  time.Time
	startLSN   uint64
	locks      []HeldLock
	writes     []WriteOperation
	rollback   []func() error
	autocommit bool
}

func newTransaction(id ID) *Transaction {
	return &Transaction{
		id:        id,
		state:     StateActive,
		startTime: time.Now(),
	}
}

// ID returns the identifier of the transaction.
func (tx *Transaction) ID() ID {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.id
}

// State returns the current lifecycle state of the transaction.
func (tx *Transaction) State() State {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.state
}

func (tx *Transaction) setState(state State) {
	tx.mu.Lock()
	tx.state = state
	tx.mu.Unlock()
}

// StartTime returns the timestamp when the transaction began.
func (tx *Transaction) StartTime() time.Time {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.startTime
}

// StartLSN returns the log sequence number recorded at begin.
func (tx *Transaction) StartLSN() uint64 {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.startLSN
}

// SetStartLSN records the log sequence number for the transaction begin marker.
func (tx *Transaction) SetStartLSN(lsn uint64) {
	tx.mu.Lock()
	tx.startLSN = lsn
	tx.mu.Unlock()
}

func (tx *Transaction) recordLock(res Resource, mode LockMode) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	for i := range tx.locks {
		if tx.locks[i].Resource == res {
			if mode > tx.locks[i].Mode {
				tx.locks[i].Mode = mode
			}
			return
		}
	}
	tx.locks = append(tx.locks, HeldLock{Resource: res, Mode: mode})
}

func (tx *Transaction) clearLocks() {
	tx.mu.Lock()
	tx.locks = nil
	tx.mu.Unlock()
}

// Locks returns a snapshot of locks held by the transaction.
func (tx *Transaction) Locks() []HeldLock {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if len(tx.locks) == 0 {
		return nil
	}
	out := make([]HeldLock, len(tx.locks))
	copy(out, tx.locks)
	return out
}

// RecordWrite appends metadata describing a data modification performed by the transaction.
func (tx *Transaction) RecordWrite(op WriteOperation) {
	tx.mu.Lock()
	tx.writes = append(tx.writes, op)
	tx.mu.Unlock()
}

// Writes returns a copy of the write operations recorded for the transaction.
func (tx *Transaction) Writes() []WriteOperation {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if len(tx.writes) == 0 {
		return nil
	}
	out := make([]WriteOperation, len(tx.writes))
	copy(out, tx.writes)
	return out
}

// SetAutocommit marks whether the transaction was created for an autocommit statement.
func (tx *Transaction) SetAutocommit(value bool) {
	tx.mu.Lock()
	tx.autocommit = value
	tx.mu.Unlock()
}

// Autocommit reports whether the transaction originated from an autocommit statement.
func (tx *Transaction) Autocommit() bool {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.autocommit
}

// RegisterRollback registers an action to execute if the transaction rolls back.
func (tx *Transaction) RegisterRollback(action func() error) {
	if action == nil {
		return
	}
	tx.mu.Lock()
	tx.rollback = append(tx.rollback, action)
	tx.mu.Unlock()
}

func (tx *Transaction) runRollback() error {
	tx.mu.Lock()
	actions := make([]func() error, len(tx.rollback))
	copy(actions, tx.rollback)
	tx.rollback = nil
	tx.mu.Unlock()

	if len(actions) == 0 {
		return nil
	}
	var errs []string
	for i := len(actions) - 1; i >= 0; i-- {
		if err := actions[i](); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("txn: rollback encountered errors: %s", strings.Join(errs, "; "))
	}
	return nil
}

func (tx *Transaction) discardRollback() {
	tx.mu.Lock()
	tx.rollback = nil
	tx.mu.Unlock()
}
