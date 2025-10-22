package storage

// RowID uniquely identifies a record stored within a heap file. It combines
// the physical page identifier with the slot offset inside that page. The pair
// is stable for the lifetime of the row unless compaction is introduced.
type RowID struct {
        Page PageID
        Slot uint16
}
