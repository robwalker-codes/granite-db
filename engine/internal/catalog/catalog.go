package catalog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
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
	ColumnTypeDecimal
)

// Column describes a table column.
type Column struct {
	Name       string
	Type       ColumnType
	Length     int
	Precision  int
	Scale      int
	NotNull    bool
	PrimaryKey bool
}

const maxColumnLength = 0xFFFF

const (
	indexSectionMarker      uint16 = 0xFFFF
	foreignKeySectionMarker uint16 = 0xFFFE
)

func encodeColumnMetadata(col Column) (uint16, error) {
	switch col.Type {
	case ColumnTypeVarChar:
		if col.Length <= 0 {
			return 0, fmt.Errorf("catalog: VARCHAR length must be positive")
		}
		if col.Length > maxColumnLength {
			return 0, fmt.Errorf("catalog: VARCHAR length exceeds limit")
		}
		return uint16(col.Length), nil
	case ColumnTypeDecimal:
		if col.Precision <= 0 {
			return 0, fmt.Errorf("catalog: DECIMAL precision must be positive")
		}
		if col.Scale < 0 {
			return 0, fmt.Errorf("catalog: DECIMAL scale must be non-negative")
		}
		if col.Scale > col.Precision {
			return 0, fmt.Errorf("catalog: DECIMAL scale cannot exceed precision")
		}
		if col.Precision > 255 {
			return 0, fmt.Errorf("catalog: DECIMAL precision exceeds supported limit")
		}
		if col.Scale > 255 {
			return 0, fmt.Errorf("catalog: DECIMAL scale exceeds supported limit")
		}
		return uint16((col.Precision << 8) | col.Scale), nil
	default:
		if col.Length < 0 {
			return 0, fmt.Errorf("catalog: negative length for column %s", col.Name)
		}
		if col.Length > maxColumnLength {
			return 0, fmt.Errorf("catalog: metadata for column %s exceeds limit", col.Name)
		}
		return uint16(col.Length), nil
	}
}

func decodeColumnMetadata(colType ColumnType, raw uint16) (length int, precision int, scale int, err error) {
	switch colType {
	case ColumnTypeVarChar:
		return int(raw), 0, 0, nil
	case ColumnTypeDecimal:
		precision = int(raw >> 8)
		scale = int(raw & 0xFF)
		if precision == 0 && scale == 0 {
			return 0, 0, 0, fmt.Errorf("catalog: invalid DECIMAL metadata")
		}
		if scale > precision {
			return 0, 0, 0, fmt.Errorf("catalog: DECIMAL scale exceeds precision")
		}
		return 0, precision, scale, nil
	default:
		return int(raw), 0, 0, nil
	}
}

// ForeignKeyAction identifies supported referential behaviours.
type ForeignKeyAction uint8

const (
	ForeignKeyActionRestrict ForeignKeyAction = iota
	ForeignKeyActionNoAction
)

// ForeignKey describes a child-to-parent relationship between tables.
type ForeignKey struct {
	Name          string
	ChildColumns  []string
	ParentTable   string
	ParentColumns []string
	OnDelete      ForeignKeyAction
	OnUpdate      ForeignKeyAction
	Deferrable    bool
	Valid         bool
}

// Table captures metadata for a user table.
type Table struct {
	Name        string
	Columns     []Column
	RootPage    storage.PageID
	RowCount    uint64
	Indexes     map[string]*Index
	ForeignKeys map[string]*ForeignKey
}

// Index describes a secondary index definition.
type Index struct {
	Name     string
	Columns  []string
	IsUnique bool
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
			col := Column{
				Name:    colName,
				Type:    ColumnType(typeCode),
				NotNull: notNull == 1,
			}
			decodedLength, precision, scale, err := decodeColumnMetadata(col.Type, length)
			if err != nil {
				return nil, err
			}
			col.Length = decodedLength
			col.Precision = precision
			col.Scale = scale
			cols[c] = col
		}
		if primaryIndex >= 0 && int(primaryIndex) < len(cols) {
			cols[primaryIndex].PrimaryKey = true
		}
		table := &Table{
			Name:        name,
			Columns:     cols,
			RootPage:    storage.PageID(rootPage),
			RowCount:    rowCount,
			Indexes:     make(map[string]*Index),
			ForeignKeys: make(map[string]*ForeignKey),
		}
		if err := readIndexMetadata(reader, table); err != nil {
			return nil, err
		}
		if err := readForeignKeyMetadata(reader, table); err != nil {
			return nil, err
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

func readIndexMetadata(r *bytes.Reader, table *Table) error {
	pos, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	var marker uint16
	if err := binary.Read(r, binary.LittleEndian, &marker); err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}
	if marker != indexSectionMarker {
		if _, err := r.Seek(pos, io.SeekStart); err != nil {
			return err
		}
		return nil
	}
	var count uint16
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return err
	}
	if table.Indexes == nil {
		table.Indexes = make(map[string]*Index)
	}
	for i := uint16(0); i < count; i++ {
		name, err := readString(r)
		if err != nil {
			return err
		}
		var unique uint8
		if err := binary.Read(r, binary.LittleEndian, &unique); err != nil {
			return err
		}
		var colCount uint16
		if err := binary.Read(r, binary.LittleEndian, &colCount); err != nil {
			return err
		}
		cols := make([]string, colCount)
		for j := uint16(0); j < colCount; j++ {
			colName, err := readString(r)
			if err != nil {
				return err
			}
			cols[j] = colName
		}
		table.Indexes[strings.ToLower(name)] = &Index{Name: name, Columns: cols, IsUnique: unique == 1}
	}
	return nil
}

func readForeignKeyMetadata(r *bytes.Reader, table *Table) error {
	pos, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	var marker uint16
	if err := binary.Read(r, binary.LittleEndian, &marker); err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}
	if marker != foreignKeySectionMarker {
		if _, err := r.Seek(pos, io.SeekStart); err != nil {
			return err
		}
		return nil
	}
	var count uint16
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return err
	}
	if table.ForeignKeys == nil {
		table.ForeignKeys = make(map[string]*ForeignKey)
	}
	for i := uint16(0); i < count; i++ {
		name, err := readString(r)
		if err != nil {
			return err
		}
		parentTable, err := readString(r)
		if err != nil {
			return err
		}
		var childCount uint16
		if err := binary.Read(r, binary.LittleEndian, &childCount); err != nil {
			return err
		}
		childCols := make([]string, childCount)
		for j := uint16(0); j < childCount; j++ {
			colName, err := readString(r)
			if err != nil {
				return err
			}
			childCols[j] = colName
		}
		var parentCount uint16
		if err := binary.Read(r, binary.LittleEndian, &parentCount); err != nil {
			return err
		}
		parentCols := make([]string, parentCount)
		for j := uint16(0); j < parentCount; j++ {
			colName, err := readString(r)
			if err != nil {
				return err
			}
			parentCols[j] = colName
		}
		var onDelete uint8
		if err := binary.Read(r, binary.LittleEndian, &onDelete); err != nil {
			return err
		}
		var onUpdate uint8
		if err := binary.Read(r, binary.LittleEndian, &onUpdate); err != nil {
			return err
		}
		var deferrable uint8
		if err := binary.Read(r, binary.LittleEndian, &deferrable); err != nil {
			return err
		}
		var valid uint8
		if err := binary.Read(r, binary.LittleEndian, &valid); err != nil {
			return err
		}
		table.ForeignKeys[strings.ToLower(name)] = &ForeignKey{
			Name:          name,
			ChildColumns:  childCols,
			ParentTable:   parentTable,
			ParentColumns: parentCols,
			OnDelete:      ForeignKeyAction(onDelete),
			OnUpdate:      ForeignKeyAction(onUpdate),
			Deferrable:    deferrable == 1,
			Valid:         valid == 1,
		}
	}
	return nil
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

func writeIndexMetadata(buf *bytes.Buffer, table *Table) error {
	if err := binary.Write(buf, binary.LittleEndian, indexSectionMarker); err != nil {
		return err
	}
	count := uint16(0)
	if table.Indexes != nil {
		count = uint16(len(table.Indexes))
	}
	if err := binary.Write(buf, binary.LittleEndian, count); err != nil {
		return err
	}
	if count == 0 {
		return nil
	}
	names := make([]string, 0, len(table.Indexes))
	for _, idx := range table.Indexes {
		names = append(names, idx.Name)
	}
	sort.Strings(names)
	for _, name := range names {
		idx := table.Indexes[strings.ToLower(name)]
		if idx == nil {
			continue
		}
		if err := writeString(buf, idx.Name); err != nil {
			return err
		}
		var unique uint8
		if idx.IsUnique {
			unique = 1
		}
		if err := binary.Write(buf, binary.LittleEndian, unique); err != nil {
			return err
		}
		if err := binary.Write(buf, binary.LittleEndian, uint16(len(idx.Columns))); err != nil {
			return err
		}
		for _, col := range idx.Columns {
			if err := writeString(buf, col); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeForeignKeyMetadata(buf *bytes.Buffer, table *Table) error {
	if err := binary.Write(buf, binary.LittleEndian, foreignKeySectionMarker); err != nil {
		return err
	}
	count := uint16(0)
	if table.ForeignKeys != nil {
		count = uint16(len(table.ForeignKeys))
	}
	if err := binary.Write(buf, binary.LittleEndian, count); err != nil {
		return err
	}
	if count == 0 {
		return nil
	}
	names := make([]string, 0, len(table.ForeignKeys))
	for _, fk := range table.ForeignKeys {
		names = append(names, fk.Name)
	}
	sort.Strings(names)
	for _, name := range names {
		fk := table.ForeignKeys[strings.ToLower(name)]
		if fk == nil {
			continue
		}
		if err := writeString(buf, fk.Name); err != nil {
			return err
		}
		if err := writeString(buf, fk.ParentTable); err != nil {
			return err
		}
		if err := binary.Write(buf, binary.LittleEndian, uint16(len(fk.ChildColumns))); err != nil {
			return err
		}
		for _, col := range fk.ChildColumns {
			if err := writeString(buf, col); err != nil {
				return err
			}
		}
		if err := binary.Write(buf, binary.LittleEndian, uint16(len(fk.ParentColumns))); err != nil {
			return err
		}
		for _, col := range fk.ParentColumns {
			if err := writeString(buf, col); err != nil {
				return err
			}
		}
		if err := binary.Write(buf, binary.LittleEndian, uint8(fk.OnDelete)); err != nil {
			return err
		}
		if err := binary.Write(buf, binary.LittleEndian, uint8(fk.OnUpdate)); err != nil {
			return err
		}
		var def uint8
		if fk.Deferrable {
			def = 1
		}
		if err := binary.Write(buf, binary.LittleEndian, def); err != nil {
			return err
		}
		var valid uint8
		if fk.Valid {
			valid = 1
		}
		if err := binary.Write(buf, binary.LittleEndian, valid); err != nil {
			return err
		}
	}
	return nil
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
			meta, err := encodeColumnMetadata(col)
			if err != nil {
				return err
			}
			if err := binary.Write(buf, binary.LittleEndian, meta); err != nil {
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
		if err := writeIndexMetadata(buf, table); err != nil {
			return err
		}
		if err := writeForeignKeyMetadata(buf, table); err != nil {
			return err
		}
	}
	return c.storage.UpdateCatalog(buf.Bytes())
}

// CreateTable registers a new table and allocates its first heap page.
func (c *Catalog) CreateTable(name string, columns []Column, primaryKey string, foreignKeys []*ForeignKey) (*Table, error) {
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
		if _, err := encodeColumnMetadata(cols[i]); err != nil {
			return nil, err
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
		Name:        name,
		Columns:     cols,
		RootPage:    rootID,
		RowCount:    0,
		Indexes:     make(map[string]*Index),
		ForeignKeys: make(map[string]*ForeignKey),
	}
	for _, fk := range foreignKeys {
		if fk == nil {
			continue
		}
		childCols := make([]string, len(fk.ChildColumns))
		copy(childCols, fk.ChildColumns)
		parentCols := make([]string, len(fk.ParentColumns))
		copy(parentCols, fk.ParentColumns)
		table.ForeignKeys[strings.ToLower(fk.Name)] = &ForeignKey{
			Name:          fk.Name,
			ChildColumns:  childCols,
			ParentTable:   fk.ParentTable,
			ParentColumns: parentCols,
			OnDelete:      fk.OnDelete,
			OnUpdate:      fk.OnUpdate,
			Deferrable:    fk.Deferrable,
			Valid:         fk.Valid,
		}
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
	for key, other := range c.tables {
		if key == lower {
			continue
		}
		for _, fk := range other.ForeignKeys {
			if strings.EqualFold(fk.ParentTable, table.Name) {
				return fmt.Errorf("catalog: table %s is referenced by foreign key %s on table %s", table.Name, fk.Name, other.Name)
			}
		}
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
		copyIdx := make(map[string]*Index, len(table.Indexes))
		if len(table.Indexes) > 0 {
			for key, idx := range table.Indexes {
				cols := make([]string, len(idx.Columns))
				copy(cols, idx.Columns)
				copyIdx[key] = &Index{Name: idx.Name, Columns: cols, IsUnique: idx.IsUnique}
			}
		}
		copyFks := make(map[string]*ForeignKey, len(table.ForeignKeys))
		if len(table.ForeignKeys) > 0 {
			for key, fk := range table.ForeignKeys {
				childCols := make([]string, len(fk.ChildColumns))
				copy(childCols, fk.ChildColumns)
				parentCols := make([]string, len(fk.ParentColumns))
				copy(parentCols, fk.ParentColumns)
				copyFks[key] = &ForeignKey{
					Name:          fk.Name,
					ChildColumns:  childCols,
					ParentTable:   fk.ParentTable,
					ParentColumns: parentCols,
					OnDelete:      fk.OnDelete,
					OnUpdate:      fk.OnUpdate,
					Deferrable:    fk.Deferrable,
					Valid:         fk.Valid,
				}
			}
		}
		result = append(result, &Table{
			Name:        table.Name,
			Columns:     copyCols,
			RootPage:    table.RootPage,
			RowCount:    table.RowCount,
			Indexes:     copyIdx,
			ForeignKeys: copyFks,
		})
	}
	return result
}

// CreateIndex registers a new index definition on an existing table.
func (c *Catalog) CreateIndex(tableName, indexName string, columns []string, unique bool) (*Index, error) {
	table, ok := c.tables[strings.ToLower(tableName)]
	if !ok {
		return nil, fmt.Errorf("catalog: table %s not found", tableName)
	}
	if table.Indexes == nil {
		table.Indexes = make(map[string]*Index)
	}
	lower := strings.ToLower(indexName)
	if _, exists := table.Indexes[lower]; exists {
		return nil, fmt.Errorf("catalog: index %s already exists on table %s", indexName, tableName)
	}
	resolved := make([]string, len(columns))
	for i, name := range columns {
		found := false
		for _, col := range table.Columns {
			if strings.EqualFold(col.Name, name) {
				resolved[i] = col.Name
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("catalog: column %s not found in table %s", name, tableName)
		}
	}
	idx := &Index{Name: indexName, Columns: resolved, IsUnique: unique}
	table.Indexes[lower] = idx
	if err := c.persist(); err != nil {
		delete(table.Indexes, lower)
		return nil, err
	}
	return idx, nil
}

// DropIndex removes an index definition from the catalog.
func (c *Catalog) DropIndex(tableName, indexName string) error {
	table, ok := c.tables[strings.ToLower(tableName)]
	if !ok {
		return fmt.Errorf("catalog: table %s not found", tableName)
	}
	lower := strings.ToLower(indexName)
	if _, exists := table.Indexes[lower]; !exists {
		return fmt.Errorf("catalog: index %s not found on table %s", indexName, tableName)
	}
	delete(table.Indexes, lower)
	return c.persist()
}

// FindIndex locates an index by name across all tables.
func (c *Catalog) FindIndex(name string) (*Table, *Index, bool) {
	lower := strings.ToLower(name)
	for _, table := range c.tables {
		if idx, ok := table.Indexes[lower]; ok {
			return table, idx, true
		}
	}
	return nil, nil, false
}

// TableIndexes returns copies of the index definitions for the specified table.
func (c *Catalog) TableIndexes(tableName string) []*Index {
	table, ok := c.tables[strings.ToLower(tableName)]
	if !ok || len(table.Indexes) == 0 {
		return nil
	}
	names := make([]string, 0, len(table.Indexes))
	for _, idx := range table.Indexes {
		names = append(names, idx.Name)
	}
	sort.Strings(names)
	result := make([]*Index, 0, len(names))
	for _, name := range names {
		idx := table.Indexes[strings.ToLower(name)]
		if idx == nil {
			continue
		}
		cols := make([]string, len(idx.Columns))
		copy(cols, idx.Columns)
		result = append(result, &Index{Name: idx.Name, Columns: cols, IsUnique: idx.IsUnique})
	}
	return result
}

// TableForeignKeys returns copies of the foreign key definitions for the specified table.
func (c *Catalog) TableForeignKeys(tableName string) []*ForeignKey {
	table, ok := c.tables[strings.ToLower(tableName)]
	if !ok || len(table.ForeignKeys) == 0 {
		return nil
	}
	names := make([]string, 0, len(table.ForeignKeys))
	for _, fk := range table.ForeignKeys {
		names = append(names, fk.Name)
	}
	sort.Strings(names)
	result := make([]*ForeignKey, 0, len(names))
	for _, name := range names {
		fk := table.ForeignKeys[strings.ToLower(name)]
		if fk == nil {
			continue
		}
		childCols := make([]string, len(fk.ChildColumns))
		copy(childCols, fk.ChildColumns)
		parentCols := make([]string, len(fk.ParentColumns))
		copy(parentCols, fk.ParentColumns)
		result = append(result, &ForeignKey{
			Name:          fk.Name,
			ChildColumns:  childCols,
			ParentTable:   fk.ParentTable,
			ParentColumns: parentCols,
			OnDelete:      fk.OnDelete,
			OnUpdate:      fk.OnUpdate,
			Deferrable:    fk.Deferrable,
			Valid:         fk.Valid,
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

// DecrementRowCount decreases the stored row count for the table.
func (c *Catalog) DecrementRowCount(name string) error {
	table, ok := c.tables[strings.ToLower(name)]
	if !ok {
		return fmt.Errorf("catalog: table %s not found", name)
	}
	if table.RowCount > 0 {
		table.RowCount--
	}
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
