package catalog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	"github.com/example/granite-db/engine/internal/storage"
)

// ColumnType enumerates supported GraniteDB column kinds.
type ColumnType uint8

const (
	ColumnTypeInt ColumnType = iota
	ColumnTypeBigInt
	ColumnTypeVarChar
	ColumnTypeBoolean
	ColumnTypeDate
	ColumnTypeTimestamp
)

// Column describes a table column.
type Column struct {
	Name       string
	Type       ColumnType
	Length     int
	NotNull    bool
	PrimaryKey bool
}

// Table captures metadata for a user table.
type Table struct {
	Name     string
	Columns  []Column
	RootPage storage.PageID
	RowCount uint64
}

// Catalog holds definitions of all tables within the database.
type Catalog struct {
	storage *storage.Manager
	tables  map[string]*Table
}

// Load constructs a catalog by reading the system metadata from storage.
func Load(mgr *storage.Manager) (*Catalog, error) {
	payload, err := mgr.CatalogData()
	if err != nil {
		return nil, err
	}
	cat := &Catalog{
		storage: mgr,
		tables:  make(map[string]*Table),
	}
	if len(payload) == 0 {
		return cat, nil
	}

	reader := bytes.NewReader(payload)
	var tableCount uint16
	if err := binary.Read(reader, binary.LittleEndian, &tableCount); err != nil {
		return nil, err
	}
	for i := uint16(0); i < tableCount; i++ {
		name, err := readString(reader)
		if err != nil {
			return nil, err
		}
		var rootPage uint32
		if err := binary.Read(reader, binary.LittleEndian, &rootPage); err != nil {
			return nil, err
		}
		var rowCount uint64
		if err := binary.Read(reader, binary.LittleEndian, &rowCount); err != nil {
			return nil, err
		}
		var columnCount uint16
		if err := binary.Read(reader, binary.LittleEndian, &columnCount); err != nil {
			return nil, err
		}
		var primaryIndex int16
		if err := binary.Read(reader, binary.LittleEndian, &primaryIndex); err != nil {
			return nil, err
		}
		cols := make([]Column, columnCount)
		for c := uint16(0); c < columnCount; c++ {
			colName, err := readString(reader)
			if err != nil {
				return nil, err
			}
			var typeCode uint8
			if err := binary.Read(reader, binary.LittleEndian, &typeCode); err != nil {
				return nil, err
			}
			var length uint16
			if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
				return nil, err
			}
			var notNull uint8
			if err := binary.Read(reader, binary.LittleEndian, &notNull); err != nil {
				return nil, err
			}
			cols[c] = Column{
				Name:    colName,
				Type:    ColumnType(typeCode),
				Length:  int(length),
				NotNull: notNull == 1,
			}
		}
		if primaryIndex >= 0 && int(primaryIndex) < len(cols) {
			cols[primaryIndex].PrimaryKey = true
		}
		table := &Table{
			Name:     name,
			Columns:  cols,
			RootPage: storage.PageID(rootPage),
			RowCount: rowCount,
		}
		cat.tables[strings.ToLower(name)] = table
	}
	return cat, nil
}

func readString(r *bytes.Reader) (string, error) {
	var length uint16
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return "", err
	}
	data := make([]byte, length)
	if _, err := r.Read(data); err != nil {
		return "", err
	}
	return string(data), nil
}

func writeString(buf *bytes.Buffer, value string) error {
	if len(value) > 0xFFFF {
		return fmt.Errorf("catalog: string too long")
	}
	if err := binary.Write(buf, binary.LittleEndian, uint16(len(value))); err != nil {
		return err
	}
	_, err := buf.WriteString(value)
	return err
}

func (c *Catalog) persist() error {
	buf := &bytes.Buffer{}
	tableCount := uint16(len(c.tables))
	if err := binary.Write(buf, binary.LittleEndian, tableCount); err != nil {
		return err
	}
	names := make([]string, 0, len(c.tables))
	for name := range c.tables {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, lower := range names {
		table := c.tables[lower]
		if err := writeString(buf, table.Name); err != nil {
			return err
		}
		if err := binary.Write(buf, binary.LittleEndian, uint32(table.RootPage)); err != nil {
			return err
		}
		if err := binary.Write(buf, binary.LittleEndian, table.RowCount); err != nil {
			return err
		}
		if err := binary.Write(buf, binary.LittleEndian, uint16(len(table.Columns))); err != nil {
			return err
		}
		var primaryIndex int16 = -1
		for idx, col := range table.Columns {
			if col.PrimaryKey {
				primaryIndex = int16(idx)
				break
			}
		}
		if err := binary.Write(buf, binary.LittleEndian, primaryIndex); err != nil {
			return err
		}
		for _, col := range table.Columns {
			if err := writeString(buf, col.Name); err != nil {
				return err
			}
			if err := binary.Write(buf, binary.LittleEndian, uint8(col.Type)); err != nil {
				return err
			}
			if err := binary.Write(buf, binary.LittleEndian, uint16(col.Length)); err != nil {
				return err
			}
			var notNull uint8
			if col.NotNull {
				notNull = 1
			}
			if err := binary.Write(buf, binary.LittleEndian, notNull); err != nil {
				return err
			}
		}
	}
	return c.storage.UpdateCatalog(buf.Bytes())
}

// CreateTable registers a new table and allocates its first heap page.
func (c *Catalog) CreateTable(name string, columns []Column, primaryKey string) (*Table, error) {
	if name == "" {
		return nil, fmt.Errorf("catalog: table name required")
	}
	lower := strings.ToLower(name)
	if _, ok := c.tables[lower]; ok {
		return nil, fmt.Errorf("catalog: table %s already exists", name)
	}
	cols := make([]Column, len(columns))
	copy(cols, columns)
	for i := range cols {
		if cols[i].Type == ColumnTypeVarChar && cols[i].Length <= 0 {
			return nil, fmt.Errorf("catalog: VARCHAR length must be positive")
		}
	}
	if primaryKey != "" {
		matched := false
		for i := range cols {
			if strings.EqualFold(cols[i].Name, primaryKey) {
				cols[i].PrimaryKey = true
				matched = true
				break
			}
		}
		if !matched {
			return nil, fmt.Errorf("catalog: primary key column %s not found", primaryKey)
		}
	}
	rootID, buf, err := c.storage.AllocatePage()
	if err != nil {
		return nil, err
	}
	if err := storage.InitialiseHeapPage(buf); err != nil {
		return nil, err
	}
	if err := c.storage.WritePage(rootID, buf); err != nil {
		return nil, err
	}
	table := &Table{
		Name:     name,
		Columns:  cols,
		RootPage: rootID,
		RowCount: 0,
	}
	c.tables[lower] = table
	if err := c.persist(); err != nil {
		delete(c.tables, lower)
		return nil, err
	}
	return table, nil
}

// DropTable removes a table definition and frees its pages.
func (c *Catalog) DropTable(name string) error {
	lower := strings.ToLower(name)
	table, ok := c.tables[lower]
	if !ok {
		return fmt.Errorf("catalog: table %s does not exist", name)
	}
	heap := storage.NewHeapFile(c.storage, table.RootPage)
	pages, err := heap.Pages()
	if err != nil {
		return err
	}
	for _, id := range pages {
		if err := c.storage.FreePage(id); err != nil {
			return err
		}
	}
	delete(c.tables, lower)
	return c.persist()
}

// GetTable retrieves the table metadata if present.
func (c *Catalog) GetTable(name string) (*Table, bool) {
	table, ok := c.tables[strings.ToLower(name)]
	return table, ok
}

// ListTables returns table metadata snapshots in name order.
func (c *Catalog) ListTables() []*Table {
	names := make([]string, 0, len(c.tables))
	for name := range c.tables {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]*Table, 0, len(names))
	for _, lower := range names {
		table := c.tables[lower]
		copyCols := make([]Column, len(table.Columns))
		copy(copyCols, table.Columns)
		result = append(result, &Table{
			Name:     table.Name,
			Columns:  copyCols,
			RootPage: table.RootPage,
			RowCount: table.RowCount,
		})
	}
	return result
}

// IncrementRowCount increases the stored row count for the table.
func (c *Catalog) IncrementRowCount(name string) error {
	table, ok := c.tables[strings.ToLower(name)]
	if !ok {
		return fmt.Errorf("catalog: table %s not found", name)
	}
	table.RowCount++
	return c.persist()
}

// SetRowCount sets the exact row count (used by tests).
func (c *Catalog) SetRowCount(name string, count uint64) error {
	table, ok := c.tables[strings.ToLower(name)]
	if !ok {
		return fmt.Errorf("catalog: table %s not found", name)
	}
	table.RowCount = count
	return c.persist()
}
