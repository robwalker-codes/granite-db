package parser

// Statement represents a parsed SQL statement.
type Statement interface {
	stmt()
}

// DataType identifies allowed column types in GraniteDB.
type DataType int

const (
	DataTypeInt DataType = iota
	DataTypeBigInt
	DataTypeVarChar
	DataTypeBoolean
	DataTypeDate
	DataTypeTimestamp
)

// ColumnDef models a column definition in CREATE TABLE.
type ColumnDef struct {
	Name    string
	Type    DataType
	Length  int
	NotNull bool
}

// CreateTableStmt represents a CREATE TABLE statement.
type CreateTableStmt struct {
	Name       string
	Columns    []ColumnDef
	PrimaryKey string
}

func (*CreateTableStmt) stmt() {}

// DropTableStmt represents DROP TABLE.
type DropTableStmt struct {
	Name string
}

func (*DropTableStmt) stmt() {}

// InsertStmt represents INSERT INTO.
type InsertStmt struct {
	Table   string
	Columns []string
	Values  []Literal
}

func (*InsertStmt) stmt() {}

// SelectStmt models SELECT * FROM table.
type SelectStmt struct {
	Table string
}

func (*SelectStmt) stmt() {}

// LiteralKind identifies literal types.
type LiteralKind int

const (
	LiteralNumber LiteralKind = iota
	LiteralString
	LiteralBoolean
)

// Literal captures a literal value.
type Literal struct {
	Kind  LiteralKind
	Value string
}
