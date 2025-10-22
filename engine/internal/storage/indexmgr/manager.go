package indexmgr

import (
        "fmt"
        "os"
        "path/filepath"
        "strings"
        "sync"
)

// Manager coordinates access to per-index storage files. It keeps the files in
// memory for the lifetime of the database connection to amortise decoding
// overhead.
type Manager struct {
        basePath string

        mu      sync.Mutex
        handles map[string]*IndexFile
}

// New constructs an index manager rooted at the provided database file path.
func New(basePath string) *Manager {
        return &Manager{
                basePath: basePath,
                handles:  make(map[string]*IndexFile),
        }
}

// Close flushes any pending index data. The current implementation persists on
// every mutation, so Close simply releases references.
func (m *Manager) Close() error {
        m.mu.Lock()
        defer m.mu.Unlock()
        m.handles = make(map[string]*IndexFile)
        return nil
}

// Create initialises a brand new index file. The file must not already exist
// on disk.
func (m *Manager) Create(table, name string) (*IndexFile, error) {
        key := m.makeKey(table, name)
        path := m.indexPath(table, name)

        m.mu.Lock()
        defer m.mu.Unlock()

        if _, err := os.Stat(path); err == nil {
                return nil, fmt.Errorf("indexmgr: index %s already exists", name)
        }
        handle := newIndexFile(path)
        if err := handle.persist(); err != nil {
                return nil, err
        }
        m.handles[key] = handle
        return handle, nil
}

// Open loads an existing index file from disk.
func (m *Manager) Open(table, name string) (*IndexFile, error) {
        key := m.makeKey(table, name)

        m.mu.Lock()
        defer m.mu.Unlock()

        if handle, ok := m.handles[key]; ok {
                return handle, nil
        }
        handle := newIndexFile(m.indexPath(table, name))
        if err := handle.load(); err != nil {
                return nil, err
        }
        m.handles[key] = handle
        return handle, nil
}

// Drop removes the on-disk file for the index and evicts any cached handle.
func (m *Manager) Drop(table, name string) error {
        key := m.makeKey(table, name)
        path := m.indexPath(table, name)

        m.mu.Lock()
        defer m.mu.Unlock()

        delete(m.handles, key)
        if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
                return err
        }
        return nil
}

func (m *Manager) makeKey(table, name string) string {
        return strings.ToLower(table) + ":" + strings.ToLower(name)
}

func (m *Manager) indexPath(table, name string) string {
        base := m.basePath
        dir := filepath.Dir(base)
        file := filepath.Base(base)
        safeTable := strings.ReplaceAll(strings.ToLower(table), " ", "_")
        safeName := strings.ReplaceAll(strings.ToLower(name), " ", "_")
        return filepath.Join(dir, fmt.Sprintf("%s.%s_%s.idx", file, safeTable, safeName))
}
