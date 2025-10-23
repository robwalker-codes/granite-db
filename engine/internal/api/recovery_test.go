package api

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/example/granite-db/engine/internal/storage"
	"github.com/example/granite-db/engine/internal/wal"
)

func TestRecoveryRedoAppliesCommittedRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ordering.gdb")
	if err := storage.New(path); err != nil {
		t.Fatalf("create storage: %v", err)
	}
	mgr, err := storage.Open(path)
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	pageID, buf, err := mgr.AllocatePage()
	if err != nil {
		t.Fatalf("allocate page: %v", err)
	}
	if err := mgr.WritePage(pageID, buf); err != nil {
		t.Fatalf("write blank page: %v", err)
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("close storage: %v", err)
	}

	log, err := wal.Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	payload := bytes.Repeat([]byte{0xAB}, storage.PageSize)
	lsn, err := log.Append(1, 0, wal.RecordInsert, uint32(pageID), payload)
	if err != nil {
		t.Fatalf("append insert: %v", err)
	}
	if err := log.Sync(); err != nil {
		t.Fatalf("sync insert: %v", err)
	}
	if _, err := log.Append(1, lsn, wal.RecordCommit, 0, nil); err != nil {
		t.Fatalf("append commit: %v", err)
	}
	if err := log.Sync(); err != nil {
		t.Fatalf("sync commit: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close database: %v", err)
	}

	mgr2, err := storage.Open(path)
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	page, err := mgr2.ReadPage(pageID)
	if err != nil {
		t.Fatalf("read page: %v", err)
	}
	if !bytes.Equal(page, payload) {
		t.Fatalf("expected payload to be restored, got mismatch")
	}
	_ = mgr2.Close()
}

func TestRecoveryIgnoresAbortedTransactions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "abort.gdb")
	if err := storage.New(path); err != nil {
		t.Fatalf("create storage: %v", err)
	}
	mgr, err := storage.Open(path)
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	page1, buf1, err := mgr.AllocatePage()
	if err != nil {
		t.Fatalf("alloc page1: %v", err)
	}
	if err := mgr.WritePage(page1, buf1); err != nil {
		t.Fatalf("write blank page1: %v", err)
	}
	page2, buf2, err := mgr.AllocatePage()
	if err != nil {
		t.Fatalf("alloc page2: %v", err)
	}
	if err := mgr.WritePage(page2, buf2); err != nil {
		t.Fatalf("write blank page2: %v", err)
	}
	blank := make([]byte, storage.PageSize)
	_ = mgr.Close()

	log, err := wal.Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	committed := bytes.Repeat([]byte{0xCD}, storage.PageSize)
	lsn1, err := log.Append(100, 0, wal.RecordInsert, uint32(page1), committed)
	if err != nil {
		t.Fatalf("append committed: %v", err)
	}
	if err := log.Sync(); err != nil {
		t.Fatalf("sync committed: %v", err)
	}
	if _, err := log.Append(100, lsn1, wal.RecordCommit, 0, nil); err != nil {
		t.Fatalf("append commit: %v", err)
	}
	if err := log.Sync(); err != nil {
		t.Fatalf("sync commit: %v", err)
	}

	abortedPayload := bytes.Repeat([]byte{0xEF}, storage.PageSize)
	lsn2, err := log.Append(200, 0, wal.RecordInsert, uint32(page2), abortedPayload)
	if err != nil {
		t.Fatalf("append abort payload: %v", err)
	}
	if err := log.Sync(); err != nil {
		t.Fatalf("sync abort payload: %v", err)
	}
	if _, err := log.Append(200, lsn2, wal.RecordAbort, 0, nil); err != nil {
		t.Fatalf("append abort: %v", err)
	}
	if err := log.Sync(); err != nil {
		t.Fatalf("sync abort: %v", err)
	}
	_ = log.Close()

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	_ = db.Close()

	mgr2, err := storage.Open(path)
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	pageData1, err := mgr2.ReadPage(page1)
	if err != nil {
		t.Fatalf("read page1: %v", err)
	}
	if !bytes.Equal(pageData1, committed) {
		t.Fatalf("committed page not restored")
	}
	pageData2, err := mgr2.ReadPage(page2)
	if err != nil {
		t.Fatalf("read page2: %v", err)
	}
	if !bytes.Equal(pageData2, blank) {
		t.Fatalf("aborted transaction should not modify page")
	}
	_ = mgr2.Close()
}

func TestRecoveryIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "idempotent.gdb")
	if err := storage.New(path); err != nil {
		t.Fatalf("create storage: %v", err)
	}
	mgr, err := storage.Open(path)
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	pageID, buf, err := mgr.AllocatePage()
	if err != nil {
		t.Fatalf("allocate page: %v", err)
	}
	if err := mgr.WritePage(pageID, buf); err != nil {
		t.Fatalf("write blank page: %v", err)
	}
	_ = mgr.Close()

	log, err := wal.Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	payload := bytes.Repeat([]byte{0x11}, storage.PageSize)
	lsn, err := log.Append(1, 0, wal.RecordInsert, uint32(pageID), payload)
	if err != nil {
		t.Fatalf("append payload: %v", err)
	}
	if err := log.Sync(); err != nil {
		t.Fatalf("sync payload: %v", err)
	}
	if _, err := log.Append(1, lsn, wal.RecordCommit, 0, nil); err != nil {
		t.Fatalf("append commit: %v", err)
	}
	if err := log.Sync(); err != nil {
		t.Fatalf("sync commit: %v", err)
	}
	_ = log.Close()

	for i := 0; i < 2; i++ {
		db, err := Open(path)
		if err != nil {
			t.Fatalf("open database iteration %d: %v", i, err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close database iteration %d: %v", i, err)
		}
	}

	mgr2, err := storage.Open(path)
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	page, err := mgr2.ReadPage(pageID)
	if err != nil {
		t.Fatalf("read page: %v", err)
	}
	if !bytes.Equal(page, payload) {
		t.Fatalf("payload mismatch after repeated recovery")
	}
	_ = mgr2.Close()
}

func TestRecoverySkipsUncommitted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "uncommitted.gdb")
	if err := storage.New(path); err != nil {
		t.Fatalf("create storage: %v", err)
	}
	mgr, err := storage.Open(path)
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	pageID, buf, err := mgr.AllocatePage()
	if err != nil {
		t.Fatalf("allocate page: %v", err)
	}
	if err := mgr.WritePage(pageID, buf); err != nil {
		t.Fatalf("write blank page: %v", err)
	}
	blank := make([]byte, storage.PageSize)
	_ = mgr.Close()

	log, err := wal.Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	payload := bytes.Repeat([]byte{0x77}, storage.PageSize)
	if _, err := log.Append(10, 0, wal.RecordInsert, uint32(pageID), payload); err != nil {
		t.Fatalf("append payload: %v", err)
	}
	if err := log.Sync(); err != nil {
		t.Fatalf("sync payload: %v", err)
	}
	_ = log.Close()

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	_ = db.Close()

	mgr2, err := storage.Open(path)
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	page, err := mgr2.ReadPage(pageID)
	if err != nil {
		t.Fatalf("read page: %v", err)
	}
	if !bytes.Equal(page, blank) {
		t.Fatalf("uncommitted record should be ignored")
	}
	_ = mgr2.Close()
}

func TestRecoverySkipsCorruptTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupt.gdb")
	if err := storage.New(path); err != nil {
		t.Fatalf("create storage: %v", err)
	}
	mgr, err := storage.Open(path)
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	page1, buf1, err := mgr.AllocatePage()
	if err != nil {
		t.Fatalf("alloc page1: %v", err)
	}
	if err := mgr.WritePage(page1, buf1); err != nil {
		t.Fatalf("write blank page1: %v", err)
	}
	page2, buf2, err := mgr.AllocatePage()
	if err != nil {
		t.Fatalf("alloc page2: %v", err)
	}
	if err := mgr.WritePage(page2, buf2); err != nil {
		t.Fatalf("write blank page2: %v", err)
	}
	blank := make([]byte, storage.PageSize)
	_ = mgr.Close()

	log, err := wal.Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	payload1 := bytes.Repeat([]byte{0x33}, storage.PageSize)
	lsn1, err := log.Append(1, 0, wal.RecordInsert, uint32(page1), payload1)
	if err != nil {
		t.Fatalf("append txn1: %v", err)
	}
	if err := log.Sync(); err != nil {
		t.Fatalf("sync txn1: %v", err)
	}
	if _, err := log.Append(1, lsn1, wal.RecordCommit, 0, nil); err != nil {
		t.Fatalf("commit txn1: %v", err)
	}
	if err := log.Sync(); err != nil {
		t.Fatalf("sync commit1: %v", err)
	}

	payload2 := bytes.Repeat([]byte{0x44}, storage.PageSize)
	lsn2, err := log.Append(2, 0, wal.RecordInsert, uint32(page2), payload2)
	if err != nil {
		t.Fatalf("append txn2: %v", err)
	}
	if err := log.Sync(); err != nil {
		t.Fatalf("sync txn2: %v", err)
	}
	if _, err := log.Append(2, lsn2, wal.RecordCommit, 0, nil); err != nil {
		t.Fatalf("commit txn2: %v", err)
	}
	if err := log.Sync(); err != nil {
		t.Fatalf("sync commit2: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	walPath := path + ".wal"
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat wal: %v", err)
	}
	f, err := os.OpenFile(walPath, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open wal for corruption: %v", err)
	}
	defer f.Close()
	if _, err := f.WriteAt([]byte{0}, info.Size()-1); err != nil {
		t.Fatalf("corrupt wal: %v", err)
	}

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	_ = db.Close()

	mgr2, err := storage.Open(path)
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	pageData1, err := mgr2.ReadPage(page1)
	if err != nil {
		t.Fatalf("read page1: %v", err)
	}
	if !bytes.Equal(pageData1, payload1) {
		t.Fatalf("first transaction should be redone")
	}
	pageData2, err := mgr2.ReadPage(page2)
	if err != nil {
		t.Fatalf("read page2: %v", err)
	}
	if !bytes.Equal(pageData2, blank) {
		t.Fatalf("corrupt tail should prevent second transaction from applying")
	}
	_ = mgr2.Close()
}
