package storage

import "fmt"

// HeapFile coordinates heap pages for a table.
type HeapFile struct {
	manager *Manager
	root    PageID
}

// NewHeapFile creates a heap file from the provided root page id.
func NewHeapFile(mgr *Manager, root PageID) *HeapFile {
	return &HeapFile{manager: mgr, root: root}
}

// Root returns the first page of the heap file.
func (hf *HeapFile) Root() PageID {
	return hf.root
}

// Insert writes the record to the first page with sufficient space.
func (hf *HeapFile) Insert(record []byte) error {
	if hf.root == 0 {
		return fmt.Errorf("storage: heap file has no root page")
	}

	currentID := hf.root
	for {
		pageBuf, err := hf.manager.ReadPage(currentID)
		if err != nil {
			return err
		}
		page, err := LoadHeapPage(currentID, pageBuf)
		if err != nil {
			return err
		}
		if page.FreeSpace() >= len(record)+slotSize {
			if _, err := page.Insert(record); err != nil {
				return err
			}
			return hf.manager.WritePage(currentID, page.Data())
		}
		if page.NextPage() == 0 {
			newID, newBuf, err := hf.manager.AllocatePage()
			if err != nil {
				return err
			}
			if err := InitialiseHeapPage(newBuf); err != nil {
				return err
			}
			if err := hf.manager.WritePage(newID, newBuf); err != nil {
				return err
			}
			page.SetNextPage(newID)
			if err := hf.manager.WritePage(currentID, page.Data()); err != nil {
				return err
			}
			currentID = newID
			continue
		}
		currentID = page.NextPage()
	}
}

// Scan iterates through every record in the heap file in order.
func (hf *HeapFile) Scan(fn func(record []byte) error) error {
	if hf.root == 0 {
		return nil
	}
	currentID := hf.root
	for currentID != 0 {
		pageBuf, err := hf.manager.ReadPage(currentID)
		if err != nil {
			return err
		}
		page, err := LoadHeapPage(currentID, pageBuf)
		if err != nil {
			return err
		}
		if err := page.Records(fn); err != nil {
			return err
		}
		currentID = page.NextPage()
	}
	return nil
}

// Pages returns all page ids used by the heap file.
func (hf *HeapFile) Pages() ([]PageID, error) {
	pages := []PageID{}
	currentID := hf.root
	for currentID != 0 {
		pages = append(pages, currentID)
		pageBuf, err := hf.manager.ReadPage(currentID)
		if err != nil {
			return nil, err
		}
		page, err := LoadHeapPage(currentID, pageBuf)
		if err != nil {
			return nil, err
		}
		currentID = page.NextPage()
	}
	return pages, nil
}
