package txn_test

import (
	"testing"
	"time"

	"github.com/example/granite-db/engine/internal/txn"
)

func TestLockManagerSharedCompatibility(t *testing.T) {
	locks := txn.NewLockManager(0)
	mgr := txn.NewManager(locks, nil)

	tx1 := mgr.Begin()
	tx2 := mgr.Begin()

	if err := locks.Acquire(tx1, txn.TableResource("orders"), txn.LockModeShared); err != nil {
		t.Fatalf("tx1 acquire shared: %v", err)
	}
	if err := locks.Acquire(tx2, txn.TableResource("orders"), txn.LockModeShared); err != nil {
		t.Fatalf("tx2 acquire shared: %v", err)
	}

	if err := mgr.Commit(tx1.ID()); err != nil {
		t.Fatalf("commit tx1: %v", err)
	}
	if err := mgr.Commit(tx2.ID()); err != nil {
		t.Fatalf("commit tx2: %v", err)
	}
}

func TestLockManagerExclusiveTimeout(t *testing.T) {
	locks := txn.NewLockManager(50 * time.Millisecond)
	mgr := txn.NewManager(locks, nil)

	tx1 := mgr.Begin()
	tx2 := mgr.Begin()

	if err := locks.Acquire(tx1, txn.RowResource("orders", "1"), txn.LockModeExclusive); err != nil {
		t.Fatalf("tx1 acquire exclusive: %v", err)
	}
	start := time.Now()
	err := locks.Acquire(tx2, txn.RowResource("orders", "1"), txn.LockModeExclusive)
	if err == nil {
		t.Fatalf("expected timeout acquiring conflicting lock")
	}
	if _, ok := err.(*txn.LockTimeoutError); !ok {
		t.Fatalf("expected LockTimeoutError, got %T", err)
	}
	if time.Since(start) < 50*time.Millisecond {
		t.Fatalf("expected acquisition to wait before timing out")
	}

	if err := mgr.Commit(tx1.ID()); err != nil {
		t.Fatalf("commit tx1: %v", err)
	}
	if err := mgr.Rollback(tx2.ID()); err != nil {
		t.Fatalf("rollback tx2: %v", err)
	}
}
