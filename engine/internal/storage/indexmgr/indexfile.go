package indexmgr

import (
        "bytes"
        "encoding/binary"
        "fmt"
        "io"
        "os"
        "sort"
        "sync"

        "github.com/example/granite-db/engine/internal/storage"
)

const (
        indexMagic   = "GRNIDX01"
        indexVersion = uint16(1)
)

// Entry represents a single key → row pointer association.
type Entry struct {
        Key []byte
        Row storage.RowID
}

// IndexFile keeps an in-memory sorted copy of the index entries and persists
// them to a dedicated on-disk file.
type IndexFile struct {
        path    string
        mu      sync.Mutex
        entries []Entry
}

func newIndexFile(path string) *IndexFile {
        return &IndexFile{path: path, entries: make([]Entry, 0)}
}

func (f *IndexFile) load() error {
        file, err := os.Open(f.path)
        if os.IsNotExist(err) {
                f.entries = make([]Entry, 0)
                return nil
        }
        if err != nil {
                return err
        }
        defer file.Close()

        header := make([]byte, len(indexMagic)+2)
        if _, err := io.ReadFull(file, header); err != nil {
                return err
        }
        if string(header[:len(indexMagic)]) != indexMagic {
                return fmt.Errorf("indexmgr: invalid index file header")
        }
        version := binary.LittleEndian.Uint16(header[len(indexMagic):])
        if version != indexVersion {
                return fmt.Errorf("indexmgr: unsupported index file version %d", version)
        }
        var count uint32
        if err := binary.Read(file, binary.LittleEndian, &count); err != nil {
                return err
        }
        entries := make([]Entry, count)
        for i := uint32(0); i < count; i++ {
                var keyLen uint32
                if err := binary.Read(file, binary.LittleEndian, &keyLen); err != nil {
                        return err
                }
                key := make([]byte, keyLen)
                if _, err := io.ReadFull(file, key); err != nil {
                        return err
                }
                var page uint32
                if err := binary.Read(file, binary.LittleEndian, &page); err != nil {
                        return err
                }
                var slot uint16
                if err := binary.Read(file, binary.LittleEndian, &slot); err != nil {
                        return err
                }
                entries[i] = Entry{Key: key, Row: storage.RowID{Page: storage.PageID(page), Slot: slot}}
        }
        f.entries = entries
        return nil
}

func (f *IndexFile) persist() error {
        f.mu.Lock()
        defer f.mu.Unlock()
        return f.persistLocked()
}

func (f *IndexFile) persistLocked() error {
        tmpPath := f.path + ".tmp"
        file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
        if err != nil {
                return err
        }
        defer file.Close()

        header := make([]byte, len(indexMagic)+2)
        copy(header, []byte(indexMagic))
        binary.LittleEndian.PutUint16(header[len(indexMagic):], indexVersion)
        if _, err := file.Write(header); err != nil {
                return err
        }
        if err := binary.Write(file, binary.LittleEndian, uint32(len(f.entries))); err != nil {
                return err
        }
        for _, entry := range f.entries {
                if err := binary.Write(file, binary.LittleEndian, uint32(len(entry.Key))); err != nil {
                        return err
                }
                if _, err := file.Write(entry.Key); err != nil {
                        return err
                }
                if err := binary.Write(file, binary.LittleEndian, uint32(entry.Row.Page)); err != nil {
                        return err
                }
                if err := binary.Write(file, binary.LittleEndian, entry.Row.Slot); err != nil {
                        return err
                }
        }
        if err := file.Close(); err != nil {
                        return err
        }
        return os.Rename(tmpPath, f.path)
}

// Rebuild replaces the entire index contents with the supplied entries.
func (f *IndexFile) Rebuild(entries []Entry, unique bool) error {
        f.mu.Lock()
        defer f.mu.Unlock()

        sorted := make([]Entry, len(entries))
        copy(sorted, entries)
        sort.Slice(sorted, func(i, j int) bool {
                if cmp := bytes.Compare(sorted[i].Key, sorted[j].Key); cmp != 0 {
                        return cmp < 0
                }
                if sorted[i].Row.Page != sorted[j].Row.Page {
                        return sorted[i].Row.Page < sorted[j].Row.Page
                }
                return sorted[i].Row.Slot < sorted[j].Row.Slot
        })
        if unique {
                for i := 1; i < len(sorted); i++ {
                        if bytes.Equal(sorted[i-1].Key, sorted[i].Key) {
                                return fmt.Errorf("indexmgr: duplicate key detected during build")
                        }
                }
        }
        f.entries = sorted
        return f.persistLocked()
}

// Insert adds a new key → row mapping to the index.
func (f *IndexFile) Insert(key []byte, row storage.RowID, unique bool) error {
        f.mu.Lock()
        defer f.mu.Unlock()

        idx := f.lowerBound(key)
        if unique {
                for i := idx; i < len(f.entries); i++ {
                        if cmp := bytes.Compare(f.entries[i].Key, key); cmp != 0 {
                                break
                        }
                        return fmt.Errorf("indexmgr: duplicate key")
                }
        }
        entry := Entry{Key: cloneBytes(key), Row: row}
        f.entries = append(f.entries, Entry{})
        copy(f.entries[idx+1:], f.entries[idx:])
        f.entries[idx] = entry
        return f.persistLocked()
}

// Delete removes the provided key/row pair if present.
func (f *IndexFile) Delete(key []byte, row storage.RowID) error {
        f.mu.Lock()
        defer f.mu.Unlock()

        idx := f.lowerBound(key)
        for idx < len(f.entries) && bytes.Compare(f.entries[idx].Key, key) == 0 {
                        if f.entries[idx].Row == row {
                                f.entries = append(f.entries[:idx], f.entries[idx+1:]...)
                                return f.persistLocked()
                        }
                        idx++
        }
        return nil
}

// SeekExact retrieves all row identifiers matching the precise key.
func (f *IndexFile) SeekExact(key []byte) []storage.RowID {
        f.mu.Lock()
        defer f.mu.Unlock()

        idx := f.lowerBound(key)
        results := make([]storage.RowID, 0)
        for idx < len(f.entries) && bytes.Compare(f.entries[idx].Key, key) == 0 {
                results = append(results, f.entries[idx].Row)
                idx++
        }
        return results
}

// SeekPrefix returns the row identifiers whose keys start with the given prefix.
func (f *IndexFile) SeekPrefix(prefix []byte) []storage.RowID {
        f.mu.Lock()
        defer f.mu.Unlock()

        idx := f.lowerBound(prefix)
        results := make([]storage.RowID, 0)
        for idx < len(f.entries) && hasPrefix(f.entries[idx].Key, prefix) {
                results = append(results, f.entries[idx].Row)
                idx++
        }
        return results
}

// Range collects row identifiers within the provided prefix-constrained range.
func (f *IndexFile) Range(prefix []byte, lower []byte, includeLower bool, upper []byte, includeUpper bool) []storage.RowID {
        f.mu.Lock()
        defer f.mu.Unlock()

        startKey := append(cloneBytes(prefix), encodeComponentLength(lower)...)
        idx := f.lowerBound(startKey)
        results := make([]storage.RowID, 0)
        prefixLen := len(prefix)
        for idx < len(f.entries) {
                key := f.entries[idx].Key
                if !hasPrefix(key, prefix) {
                        break
                }
                component := key[prefixLen:]
                if !componentInLowerBound(component, lower, includeLower) {
                        idx++
                        continue
                }
                if !componentInUpperBound(component, upper, includeUpper) {
                        break
                }
                results = append(results, f.entries[idx].Row)
                idx++
        }
        return results
}

func (f *IndexFile) lowerBound(key []byte) int {
        return sort.Search(len(f.entries), func(i int) bool {
                return bytes.Compare(f.entries[i].Key, key) >= 0
        })
}

func cloneBytes(src []byte) []byte {
        if len(src) == 0 {
                return nil
        }
        dst := make([]byte, len(src))
        copy(dst, src)
        return dst
}

func hasPrefix(key, prefix []byte) bool {
        return len(key) >= len(prefix) && bytes.Equal(key[:len(prefix)], prefix)
}

func encodeComponentLength(component []byte) []byte {
        if component == nil {
                return nil
        }
        buf := make([]byte, 2+len(component))
        binary.BigEndian.PutUint16(buf[:2], uint16(len(component)))
        copy(buf[2:], component)
        return buf
}

func componentInLowerBound(component []byte, lower []byte, include bool) bool {
        if lower == nil {
                return true
        }
        if len(component) < 2 {
                return false
        }
        length := int(binary.BigEndian.Uint16(component[:2]))
        if len(component) < 2+length {
                return false
        }
        cmp := bytes.Compare(component[2:2+length], lower)
        if include {
                return cmp >= 0
        }
        return cmp > 0
}

func componentInUpperBound(component []byte, upper []byte, include bool) bool {
        if upper == nil {
                return true
        }
        if len(component) < 2 {
                return false
        }
        length := int(binary.BigEndian.Uint16(component[:2]))
        if len(component) < 2+length {
                return false
        }
        cmp := bytes.Compare(component[2:2+length], upper)
        if include {
                return cmp <= 0
        }
        return cmp < 0
}
