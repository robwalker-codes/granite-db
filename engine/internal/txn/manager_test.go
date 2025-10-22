package txn_test

import (
	"testing"

	"github.com/example/granite-db/engine/internal/txn"
)

func TestManagerLifecycle(t *testing.T) {
	locks := txn.NewLockManager(0)
	mgr := txn.NewManager(locks)

	tx := mgr.Begin()
	if tx.ID() == 0 {
		t.Fatalf("expected transaction ID to be non-zero")
	}
	if tx.State() != txn.StateActive {
		t.Fatalf("expected transaction to be active")
	}
	if err := mgr.Commit(tx.ID()); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if tx.State() != txn.StateCommitted {
		t.Fatalf("expected committed state, got %v", tx.State())
	}
	if err := mgr.Commit(tx.ID()); err != txn.ErrNotActive {
		t.Fatalf("expected ErrNotActive on double commit, got %v", err)
	}

	tx2 := mgr.Begin()
	if err := mgr.Rollback(tx2.ID()); err != nil {
		t.Fatalf("rollback failed: %v", err)
	}
	if tx2.State() != txn.StateRolledBack {
		t.Fatalf("expected rolled back state, got %v", tx2.State())
	}
	if err := mgr.Rollback(tx2.ID()); err != txn.ErrNotActive {
		t.Fatalf("expected ErrNotActive on double rollback, got %v", err)
	}
}
