package storage

import (
	"strings"
	"testing"
)

func TestHeapPageInsertAndRecords(t *testing.T) {
	buf := make([]byte, PageSize)
	if err := InitialiseHeapPage(buf); err != nil {
		t.Fatalf("init page: %v", err)
	}
	page, err := LoadHeapPage(1, buf)
	if err != nil {
		t.Fatalf("load page: %v", err)
	}

	records := [][]byte{
		[]byte("short record"),
		[]byte(strings.Repeat("x", 200)),
	}

	for _, rec := range records {
		if _, err := page.Insert(rec); err != nil {
			t.Fatalf("insert record: %v", err)
		}
	}

	var idx int
	if err := page.Records(func(record []byte) error {
		if string(record) != string(records[idx]) {
			t.Fatalf("record mismatch: got %q want %q", string(record), string(records[idx]))
		}
		idx++
		return nil
	}); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if idx != len(records) {
		t.Fatalf("expected %d records, read %d", len(records), idx)
	}
}
