package txn

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// LockMode represents the kind of lock requested on a resource.
type LockMode int

const (
	// LockModeShared allows concurrent readers.
	LockModeShared LockMode = iota
	// LockModeExclusive provides exclusive access to the resource.
	LockModeExclusive
)

// ResourceKind classifies a lockable resource.
type ResourceKind int

const (
	// ResourceTable identifies a table-level lock.
	ResourceTable ResourceKind = iota
	// ResourceRow identifies a single row/key lock.
	ResourceRow
)

// Resource describes a lockable object within the database.
type Resource struct {
	Kind  ResourceKind
	Table string
	Key   string
}

func (r Resource) normalised() Resource {
	r.Table = strings.ToLower(r.Table)
	return r
}

func (r Resource) key() string {
	return fmt.Sprintf("%d|%s|%s", r.Kind, r.Table, r.Key)
}

func (r Resource) String() string {
	switch r.Kind {
	case ResourceTable:
		return fmt.Sprintf("table %s", r.Table)
	case ResourceRow:
		if r.Key != "" {
			return fmt.Sprintf("row %s[%s]", r.Table, r.Key)
		}
		return fmt.Sprintf("row %s", r.Table)
	default:
		return r.Table
	}
}

// TableResource constructs a table-level lock resource.
func TableResource(name string) Resource {
	return Resource{Kind: ResourceTable, Table: strings.ToLower(name)}
}

// RowResource constructs a row-level lock resource.
func RowResource(table, key string) Resource {
	return Resource{Kind: ResourceRow, Table: strings.ToLower(table), Key: key}
}

// LockTimeoutError indicates a lock request timed out.
type LockTimeoutError struct {
	Resource Resource
}

func (e *LockTimeoutError) Error() string {
	return fmt.Sprintf("lock timeout on %s", e.Resource)
}

// LockManager coordinates locking for transactions.
type LockManager struct {
	mu      sync.Mutex
	locks   map[string]*lockState
	held    map[ID]map[string]Resource
	timeout time.Duration
}

type lockState struct {
	holders map[ID]*lockHolder
}

type lockHolder struct {
	mode  LockMode
	count int
}

// ErrTxnRequired indicates Acquire was invoked without a transaction context.
var ErrTxnRequired = errors.New("txn: lock requires active transaction")

const defaultLockTimeout = 2 * time.Second

// NewLockManager creates a lock manager using the provided timeout.
func NewLockManager(timeout time.Duration) *LockManager {
	if timeout <= 0 {
		timeout = defaultLockTimeout
	}
	return &LockManager{
		locks:   make(map[string]*lockState),
		held:    make(map[ID]map[string]Resource),
		timeout: timeout,
	}
}

// Acquire requests the specified lock for the transaction, blocking until it is granted or the timeout expires.
func (lm *LockManager) Acquire(tx *Transaction, res Resource, mode LockMode) error {
	if tx == nil {
		return ErrTxnRequired
	}
	resource := res.normalised()
	key := resource.key()
	deadline := time.Now().Add(lm.timeout)
	for {
		if lm.tryAcquire(tx, key, resource, mode) {
			tx.recordLock(resource, mode)
			return nil
		}
		if time.Now().After(deadline) {
			return &LockTimeoutError{Resource: resource}
		}
		time.Sleep(lm.backoff(deadline))
	}
}

func (lm *LockManager) tryAcquire(tx *Transaction, key string, res Resource, mode LockMode) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	state, ok := lm.locks[key]
	if !ok {
		state = &lockState{holders: make(map[ID]*lockHolder)}
		lm.locks[key] = state
	}
	holder, exists := state.holders[tx.id]
	if !exists {
		switch mode {
		case LockModeShared:
			if lm.hasConflictingExclusive(state, tx.id) {
				return false
			}
			state.holders[tx.id] = &lockHolder{mode: LockModeShared, count: 1}
		case LockModeExclusive:
			if len(state.holders) > 0 {
				return false
			}
			state.holders[tx.id] = &lockHolder{mode: LockModeExclusive, count: 1}
		}
		lm.trackHeld(tx.id, key, res)
		return true
	}
	if holder.mode == LockModeExclusive {
		holder.count++
		return true
	}
	if mode == LockModeShared {
		holder.count++
		return true
	}
	if mode == LockModeExclusive {
		if len(state.holders) == 1 {
			holder.mode = LockModeExclusive
			holder.count++
			lm.trackHeld(tx.id, key, res)
			return true
		}
		return false
	}
	return false
}

func (lm *LockManager) hasConflictingExclusive(state *lockState, requester ID) bool {
	for id, holder := range state.holders {
		if id == requester {
			continue
		}
		if holder.mode == LockModeExclusive {
			return true
		}
	}
	return false
}

func (lm *LockManager) trackHeld(id ID, key string, res Resource) {
	resources, ok := lm.held[id]
	if !ok {
		resources = make(map[string]Resource)
		lm.held[id] = resources
	}
	resources[key] = res
}

func (lm *LockManager) backoff(deadline time.Time) time.Duration {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return 0
	}
	slice := remaining / 10
	if slice < 5*time.Millisecond {
		return 5 * time.Millisecond
	}
	if slice > 50*time.Millisecond {
		return 50 * time.Millisecond
	}
	return slice
}

// ReleaseAll frees all locks held by the specified transaction.
func (lm *LockManager) ReleaseAll(id ID) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	resources := lm.held[id]
	for key := range resources {
		state := lm.locks[key]
		if state == nil {
			continue
		}
		delete(state.holders, id)
		if len(state.holders) == 0 {
			delete(lm.locks, key)
		}
	}
	delete(lm.held, id)
}
