package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// RecordType identifies the kind of WAL record stored on disk.
type RecordType uint8

const (
	// RecordInsert captures a physical heap page image after an INSERT.
	RecordInsert RecordType = 1 + iota
	// RecordUpdate captures a physical heap page image after an UPDATE.
	RecordUpdate
	// RecordDelete captures a physical heap page image after a DELETE.
	RecordDelete
	// RecordPageMeta records structural changes to a page (catalog, links etc.).
	RecordPageMeta
	// RecordCommit marks a committed transaction.
	RecordCommit
	// RecordAbort marks an aborted transaction.
	RecordAbort
)

// Record exposes the parsed representation of a WAL entry.
type Record struct {
	LSN     uint64
	TxnID   uint64
	PrevLSN uint64
	Type    RecordType
	PageID  uint32
	Payload []byte
}

const (
	recordHeaderSize = 8 + 8 + 8 + 1 + 3 + 4 + 4 // LSN, TxnID, PrevLSN, Type+pad, PageID, PayloadLen
	lengthFieldSize  = 4
	checksumSize     = 4
)

// Manager coordinates access to the WAL file.
type Manager struct {
	mu              sync.Mutex
	file            *os.File
	path            string
	lastLSN         uint64
	walBytesWritten uint64
}

// Open initialises a WAL manager anchored to the supplied database path.
func Open(dbPath string) (*Manager, error) {
	walPath := dbPath + ".wal"
	if err := os.MkdirAll(filepath.Dir(walPath), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	m := &Manager{file: file, path: walPath}
	if err := m.bootstrap(); err != nil {
		file.Close()
		return nil, err
	}
	return m, nil
}

func (m *Manager) bootstrap() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, err := m.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	var (
		recordsSize uint64
		buf         [lengthFieldSize]byte
	)

	for {
		if _, err := io.ReadFull(m.file, buf[:]); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return err
		}
		length := binary.LittleEndian.Uint32(buf[:])
		if length < recordHeaderSize+checksumSize {
			break
		}
		recBuf := make([]byte, length)
		if _, err := io.ReadFull(m.file, recBuf); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return err
		}
		storedChecksum := binary.LittleEndian.Uint32(recBuf[length-checksumSize:])
		computed := crc32.ChecksumIEEE(recBuf[:length-checksumSize])
		if storedChecksum != computed {
			break
		}
		if length < recordHeaderSize+checksumSize {
			break
		}
		lsn := binary.LittleEndian.Uint64(recBuf[0:8])
		if lsn > m.lastLSN {
			m.lastLSN = lsn
		}
		recordsSize += uint64(lengthFieldSize) + uint64(length)
	}

	if err := m.file.Truncate(int64(recordsSize)); err != nil {
		return err
	}
	if _, err := m.file.Seek(int64(recordsSize), io.SeekStart); err != nil {
		return err
	}
	m.walBytesWritten = recordsSize
	return nil
}

// Close flushes the WAL file and releases the handle.
func (m *Manager) Close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.file == nil {
		return nil
	}
	err := m.file.Close()
	m.file = nil
	return err
}

// Append writes a record to the WAL, returning the assigned LSN.
func (m *Manager) Append(txnID, prevLSN uint64, typ RecordType, pageID uint32, payload []byte) (uint64, error) {
	if m == nil {
		return 0, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	lsn := m.lastLSN + 1
	payloadLen := len(payload)
	length := recordHeaderSize + payloadLen + checksumSize
	buf := make([]byte, lengthFieldSize+length)
	binary.LittleEndian.PutUint32(buf[:lengthFieldSize], uint32(length))
	pos := lengthFieldSize
	binary.LittleEndian.PutUint64(buf[pos:pos+8], lsn)
	pos += 8
	binary.LittleEndian.PutUint64(buf[pos:pos+8], txnID)
	pos += 8
	binary.LittleEndian.PutUint64(buf[pos:pos+8], prevLSN)
	pos += 8
	buf[pos] = byte(typ)
	pos++
	for i := 0; i < 3; i++ {
		buf[pos] = 0
		pos++
	}
	binary.LittleEndian.PutUint32(buf[pos:pos+4], pageID)
	pos += 4
	binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(payloadLen))
	pos += 4
	copy(buf[pos:pos+payloadLen], payload)
	pos += payloadLen
	checksum := crc32.ChecksumIEEE(buf[lengthFieldSize : lengthFieldSize+recordHeaderSize+payloadLen])
	binary.LittleEndian.PutUint32(buf[pos:pos+checksumSize], checksum)
	if _, err := m.file.Write(buf); err != nil {
		return 0, err
	}
	m.lastLSN = lsn
	m.walBytesWritten += uint64(len(buf))
	return lsn, nil
}

// Sync forces the WAL contents to durable storage.
func (m *Manager) Sync() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.file.Sync()
}

// LastLSN returns the last assigned log sequence number.
func (m *Manager) LastLSN() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastLSN
}

// BytesWritten returns the number of bytes flushed to the WAL.
func (m *Manager) BytesWritten() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.walBytesWritten
}

// Scan reads the WAL from the beginning and returns valid records.
func (m *Manager) Scan() ([]Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, err := m.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	records := make([]Record, 0)
	var buf [lengthFieldSize]byte

	for {
		if _, err := io.ReadFull(m.file, buf[:]); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, err
		}
		length := binary.LittleEndian.Uint32(buf[:])
		if length < recordHeaderSize+checksumSize {
			break
		}
		recBuf := make([]byte, length)
		if _, err := io.ReadFull(m.file, recBuf); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, err
		}
		storedChecksum := binary.LittleEndian.Uint32(recBuf[length-checksumSize:])
		computed := crc32.ChecksumIEEE(recBuf[:length-checksumSize])
		if storedChecksum != computed {
			break
		}
		record := decodeRecord(recBuf[:length-checksumSize])
		records = append(records, record)
	}

	_, err := m.file.Seek(0, io.SeekEnd)
	return records, err
}

func decodeRecord(buf []byte) Record {
	pos := 0
	lsn := binary.LittleEndian.Uint64(buf[pos : pos+8])
	pos += 8
	txnID := binary.LittleEndian.Uint64(buf[pos : pos+8])
	pos += 8
	prevLSN := binary.LittleEndian.Uint64(buf[pos : pos+8])
	pos += 8
	typ := RecordType(buf[pos])
	pos += 4 // skip type + padding
	pageID := binary.LittleEndian.Uint32(buf[pos : pos+4])
	pos += 4
	payloadLen := binary.LittleEndian.Uint32(buf[pos : pos+4])
	pos += 4
	payload := make([]byte, int(payloadLen))
	copy(payload, buf[pos:pos+int(payloadLen)])
	return Record{
		LSN:     lsn,
		TxnID:   txnID,
		PrevLSN: prevLSN,
		Type:    typ,
		PageID:  pageID,
		Payload: payload,
	}
}
