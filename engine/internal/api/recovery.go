package api

import (
	"fmt"

	"github.com/example/granite-db/engine/internal/storage"
	"github.com/example/granite-db/engine/internal/wal"
)

func recoverDatabase(mgr *storage.Manager, log *wal.Manager) error {
	if log == nil {
		return nil
	}
	records, err := log.Scan()
	if err != nil {
		return err
	}
	committed := make(map[uint64]bool)
	aborted := make(map[uint64]bool)
	for _, rec := range records {
		switch rec.Type {
		case wal.RecordCommit:
			committed[rec.TxnID] = true
		case wal.RecordAbort:
			aborted[rec.TxnID] = true
		}
	}
	for _, rec := range records {
		switch rec.Type {
		case wal.RecordInsert, wal.RecordUpdate, wal.RecordDelete, wal.RecordPageMeta:
			if rec.TxnID == 0 {
				continue
			}
			if !committed[rec.TxnID] || aborted[rec.TxnID] {
				continue
			}
			if len(rec.Payload) != storage.PageSize {
				return fmt.Errorf("api: invalid WAL payload length for page %d", rec.PageID)
			}
			page := make([]byte, len(rec.Payload))
			copy(page, rec.Payload)
			if err := mgr.WritePage(storage.PageID(rec.PageID), page); err != nil {
				return err
			}
		}
	}
	return nil
}
