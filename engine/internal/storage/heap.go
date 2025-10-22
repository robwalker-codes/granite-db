package storage

import (
	"encoding/binary"
	"fmt"
)

const (
	heapHeaderSize = 16
	slotSize       = 4
)

// InitialiseHeapPage prepares a blank heap page for row storage.
func InitialiseHeapPage(page []byte) error {
	if len(page) != PageSize {
		return errShortPage
	}
	for i := range page {
		page[i] = 0
	}
	binary.LittleEndian.PutUint32(page[0:4], 0)              // next page id
	binary.LittleEndian.PutUint16(page[4:6], 0)              // slot count
	binary.LittleEndian.PutUint16(page[6:8], heapHeaderSize) // free start
	binary.LittleEndian.PutUint16(page[8:10], PageSize)      // free end
	binary.LittleEndian.PutUint32(page[12:16], 0)            // reserved/row count
	return nil
}

// HeapPageHeader exposes metadata for a heap page.
type HeapPageHeader struct {
	NextPage  PageID
	SlotCount uint16
	FreeStart uint16
	FreeEnd   uint16
}

func readHeapHeader(page []byte) HeapPageHeader {
	return HeapPageHeader{
		NextPage:  PageID(binary.LittleEndian.Uint32(page[0:4])),
		SlotCount: binary.LittleEndian.Uint16(page[4:6]),
		FreeStart: binary.LittleEndian.Uint16(page[6:8]),
		FreeEnd:   binary.LittleEndian.Uint16(page[8:10]),
	}
}

func writeHeapHeader(page []byte, header HeapPageHeader) {
	binary.LittleEndian.PutUint32(page[0:4], uint32(header.NextPage))
	binary.LittleEndian.PutUint16(page[4:6], header.SlotCount)
	binary.LittleEndian.PutUint16(page[6:8], header.FreeStart)
	binary.LittleEndian.PutUint16(page[8:10], header.FreeEnd)
}

func heapFreeSpace(header HeapPageHeader) int {
	return int(header.FreeEnd) - int(header.FreeStart)
}

// HeapPage manages row insertion and iteration for a single page.
type HeapPage struct {
	id   PageID
	data []byte
	hdr  HeapPageHeader
}

// LoadHeapPage constructs a heap page from the supplied buffer.
func LoadHeapPage(id PageID, buf []byte) (*HeapPage, error) {
	if len(buf) != PageSize {
		return nil, errShortPage
	}
	return &HeapPage{id: id, data: buf, hdr: readHeapHeader(buf)}, nil
}

// FreeSpace returns the number of bytes available for a new record.
func (p *HeapPage) FreeSpace() int {
	return heapFreeSpace(p.hdr)
}

// NextPage returns the linked next page id.
func (p *HeapPage) NextPage() PageID {
	return p.hdr.NextPage
}

// SetNextPage updates the link to the next page.
func (p *HeapPage) SetNextPage(id PageID) {
	p.hdr.NextPage = id
	writeHeapHeader(p.data, p.hdr)
}

// Insert appends a new record to the page if space permits.
func (p *HeapPage) Insert(record []byte) (uint16, error) {
	required := len(record) + slotSize
	if required > p.FreeSpace() {
		return 0, fmt.Errorf("storage: insufficient free space in page %d", p.id)
	}
	offset := int(p.hdr.FreeStart)
	copy(p.data[offset:], record)
	p.hdr.FreeStart += uint16(len(record))

	slotPos := int(p.hdr.FreeEnd) - slotSize
	binary.LittleEndian.PutUint16(p.data[slotPos:slotPos+2], uint16(offset))
	binary.LittleEndian.PutUint16(p.data[slotPos+2:slotPos+4], uint16(len(record)))

	p.hdr.SlotCount++
	p.hdr.FreeEnd = uint16(slotPos)
	writeHeapHeader(p.data, p.hdr)
	return p.hdr.SlotCount - 1, nil
}

// Records iterates over stored rows, invoking fn for each entry.
func (p *HeapPage) Records(fn func(slot uint16, record []byte) error) error {
	for i := uint16(0); i < p.hdr.SlotCount; i++ {
		slotPos := int(p.hdr.FreeEnd) + int(p.hdr.SlotCount-1-i)*slotSize
		length := binary.LittleEndian.Uint16(p.data[slotPos+2 : slotPos+4])
		if length == 0 {
			continue
		}
		offset := binary.LittleEndian.Uint16(p.data[slotPos : slotPos+2])
		rec := p.data[offset : offset+length]
		if err := fn(i, rec); err != nil {
			return err
		}
	}
	return nil
}

// Data exposes the underlying buffer for persistence.
func (p *HeapPage) Data() []byte {
	return p.data
}

// Record retrieves the raw bytes stored at the provided slot position.
func (p *HeapPage) Record(slot uint16) ([]byte, error) {
	if slot >= p.hdr.SlotCount {
		return nil, fmt.Errorf("storage: slot %d out of bounds", slot)
	}
	slotPos := int(p.hdr.FreeEnd) + int(p.hdr.SlotCount-1-slot)*slotSize
	length := binary.LittleEndian.Uint16(p.data[slotPos+2 : slotPos+4])
	if length == 0 {
		return nil, fmt.Errorf("storage: slot %d is empty", slot)
	}
	offset := binary.LittleEndian.Uint16(p.data[slotPos : slotPos+2])
	if int(offset)+int(length) > len(p.data) {
		return nil, fmt.Errorf("storage: corrupt slot %d", slot)
	}
	return p.data[offset : offset+length], nil
}
