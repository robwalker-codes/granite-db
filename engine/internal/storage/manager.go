package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

const (
	// PageSize defines the fixed page size for GraniteDB heap files.
	PageSize = 4096

	headerMagic   = "GRANITED"
	headerVersion = uint16(1)

	freeListNil = uint32(0xFFFFFFFF)
)

var (
	errInvalidHeader = errors.New("storage: invalid database header")
	errShortPage     = errors.New("storage: page buffer too small")
)

// PageID represents the position of a page within the database file.
// Page numbering starts at 0, where page 0 is reserved for the database header.
type PageID uint32

type databaseHeader struct {
	Magic        [8]byte
	Version      uint16
	_padding     uint16
	PageCount    uint32
	FreeListHead uint32
	CatalogSize  uint32
}

const headerSize = 8 + 2 + 2 + 4 + 4 + 4

// Manager coordinates access to the on-disk database file and handles page
// allocation, deallocation and catalog persistence.
type Manager struct {
	mu     sync.Mutex
	file   *os.File
	header databaseHeader
}

// New creates a brand-new GraniteDB database file.
func New(path string) error {
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("storage: database %s already exists", path)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	header := databaseHeader{}
	copy(header.Magic[:], headerMagic)
	header.Version = headerVersion
	header.PageCount = 1 // header page only
	header.FreeListHead = freeListNil
	header.CatalogSize = 0

	buf := make([]byte, PageSize)
	writeHeader(buf, &header)
	if _, err := f.Write(buf); err != nil {
		return err
	}
	return nil
}

// Open loads an existing database file.
func Open(path string) (*Manager, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	m := &Manager{file: f}
	if err := m.loadHeader(); err != nil {
		f.Close()
		return nil, err
	}
	return m, nil
}

func (m *Manager) loadHeader() error {
	buf := make([]byte, PageSize)
	if _, err := io.ReadFull(m.file, buf); err != nil {
		return err
	}
	header, err := readHeader(buf)
	if err != nil {
		return err
	}
	m.header = *header
	return nil
}

// Close flushes header information and closes the backing file.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.file == nil {
		return nil
	}
	if err := m.flushHeaderLocked(nil); err != nil {
		return err
	}
	err := m.file.Close()
	m.file = nil
	return err
}

// CatalogData returns a copy of the persisted catalog payload from page 0.
func (m *Manager) CatalogData() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	buf := make([]byte, PageSize)
	if _, err := m.file.ReadAt(buf, 0); err != nil {
		return nil, err
	}
	header, err := readHeader(buf)
	if err != nil {
		return nil, err
	}
	if header.CatalogSize == 0 {
		return nil, nil
	}
	if int(header.CatalogSize) > PageSize-headerSize {
		return nil, fmt.Errorf("storage: catalog too large")
	}
	data := make([]byte, header.CatalogSize)
	copy(data, buf[headerSize:headerSize+int(header.CatalogSize)])
	return data, nil
}

// UpdateCatalog persists catalog bytes to page 0.
func (m *Manager) UpdateCatalog(payload []byte) error {
	if len(payload) > PageSize-headerSize {
		return fmt.Errorf("storage: catalog payload exceeds header page capacity")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	buf := make([]byte, PageSize)
	writeHeader(buf, &m.header)
	copy(buf[headerSize:], payload)
	m.header.CatalogSize = uint32(len(payload))
	writeHeader(buf, &m.header)
	if _, err := m.file.WriteAt(buf, 0); err != nil {
		return err
	}
	return nil
}

// ReadPage retrieves the raw bytes for the given page id.
func (m *Manager) ReadPage(id PageID) ([]byte, error) {
	if id >= PageID(m.header.PageCount) {
		return nil, fmt.Errorf("storage: page %d out of bounds", id)
	}
	buf := make([]byte, PageSize)
	offset := int64(id) * PageSize
	if _, err := m.file.ReadAt(buf, offset); err != nil {
		return nil, err
	}
	return buf, nil
}

// WritePage writes a full page back to disk.
func (m *Manager) WritePage(id PageID, data []byte) error {
	if len(data) != PageSize {
		return errShortPage
	}
	if id >= PageID(m.header.PageCount) {
		return fmt.Errorf("storage: page %d out of bounds", id)
	}
	offset := int64(id) * PageSize
	_, err := m.file.WriteAt(data, offset)
	return err
}

// AllocatePage returns a zeroed page suitable for writing records.
func (m *Manager) AllocatePage() (PageID, []byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var id PageID
	var buf []byte

	if m.header.FreeListHead != freeListNil {
		id = PageID(m.header.FreeListHead)
		buf = make([]byte, PageSize)
		offset := int64(id) * PageSize
		if _, err := m.file.ReadAt(buf, offset); err != nil {
			return 0, nil, err
		}
		// The first 4 bytes of the recycled page store the next pointer.
		m.header.FreeListHead = binary.LittleEndian.Uint32(buf[:4])
		buf = make([]byte, PageSize)
	} else {
		id = PageID(m.header.PageCount)
		buf = make([]byte, PageSize)
		offset := int64(id) * PageSize
		if _, err := m.file.WriteAt(buf, offset); err != nil {
			return 0, nil, err
		}
		m.header.PageCount++
	}

	if err := m.flushHeaderLocked(nil); err != nil {
		return 0, nil, err
	}
	return id, buf, nil
}

// FreePage adds the specified page to the freelist for reuse.
func (m *Manager) FreePage(id PageID) error {
	if id == 0 {
		return fmt.Errorf("storage: cannot free header page")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	buf := make([]byte, PageSize)
	binary.LittleEndian.PutUint32(buf[:4], m.header.FreeListHead)
	offset := int64(id) * PageSize
	if _, err := m.file.WriteAt(buf, offset); err != nil {
		return err
	}
	m.header.FreeListHead = uint32(id)
	return m.flushHeaderLocked(nil)
}

func (m *Manager) flushHeaderLocked(catalog []byte) error {
	buf := make([]byte, PageSize)
	writeHeader(buf, &m.header)
	if catalog != nil {
		copy(buf[headerSize:], catalog)
		m.header.CatalogSize = uint32(len(catalog))
		writeHeader(buf, &m.header)
	}
	_, err := m.file.WriteAt(buf, 0)
	return err
}

func readHeader(buf []byte) (*databaseHeader, error) {
	if len(buf) < headerSize {
		return nil, errShortPage
	}
	h := &databaseHeader{}
	copy(h.Magic[:], buf[:8])
	if string(h.Magic[:]) != headerMagic {
		return nil, errInvalidHeader
	}
	h.Version = binary.LittleEndian.Uint16(buf[8:10])
	if h.Version != headerVersion {
		return nil, fmt.Errorf("storage: unsupported header version %d", h.Version)
	}
	h.PageCount = binary.LittleEndian.Uint32(buf[12:16])
	h.FreeListHead = binary.LittleEndian.Uint32(buf[16:20])
	h.CatalogSize = binary.LittleEndian.Uint32(buf[20:24])
	return h, nil
}

func writeHeader(buf []byte, h *databaseHeader) {
	copy(buf[:8], []byte(headerMagic))
	binary.LittleEndian.PutUint16(buf[8:10], h.Version)
	binary.LittleEndian.PutUint32(buf[12:16], h.PageCount)
	binary.LittleEndian.PutUint32(buf[16:20], h.FreeListHead)
	binary.LittleEndian.PutUint32(buf[20:24], h.CatalogSize)
}
