package txn

import (
	"errors"
	"sync"
)

// ErrNotActive indicates the provided transaction identifier is not currently active.
var ErrNotActive = errors.New("txn: transaction not active")

// Manager coordinates transaction lifecycles.
type Manager struct {
	mu      sync.Mutex
	nextID  ID
	active  map[ID]*Transaction
	lockMgr *LockManager
}

// NewManager constructs a Manager using the provided lock manager.
func NewManager(lockMgr *LockManager) *Manager {
	return &Manager{
		nextID:  1,
		active:  make(map[ID]*Transaction),
		lockMgr: lockMgr,
	}
}

// Begin starts a new transaction and returns it.
func (m *Manager) Begin() *Transaction {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := m.nextID
	m.nextID++
	tx := newTransaction(id)
	m.active[id] = tx
	return tx
}

// Commit finalises the transaction, releasing any held locks.
func (m *Manager) Commit(id ID) error {
	tx, err := m.remove(id)
	if err != nil {
		return err
	}
	tx.setState(StateCommitted)
	tx.discardRollback()
	if m.lockMgr != nil {
		m.lockMgr.ReleaseAll(id)
	}
	tx.clearLocks()
	return nil
}

// Rollback aborts the transaction and releases its locks.
func (m *Manager) Rollback(id ID) error {
	tx, err := m.remove(id)
	if err != nil {
		return err
	}
	rollbackErr := tx.runRollback()
	tx.setState(StateRolledBack)
	if m.lockMgr != nil {
		m.lockMgr.ReleaseAll(id)
	}
	tx.clearLocks()
	return rollbackErr
}

// Lookup returns the active transaction for the given identifier.
func (m *Manager) Lookup(id ID) (*Transaction, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, ok := m.active[id]
	return tx, ok
}

func (m *Manager) remove(id ID) (*Transaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, ok := m.active[id]
	if !ok {
		return nil, ErrNotActive
	}
	delete(m.active, id)
	return tx, nil
}
