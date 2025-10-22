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

// SelectStmt models SELECT * FROM table with optional clauses.
type SelectStmt struct {
	Table   string
	Where   Expression
	OrderBy *OrderByClause
	Limit   *LimitClause
}

func (*SelectStmt) stmt() {}

// LiteralKind identifies literal types.
type LiteralKind int

const (
	LiteralNumber LiteralKind = iota
	LiteralString
	LiteralBoolean
	LiteralNull
)

// Literal captures a literal value.
type Literal struct {
	Kind  LiteralKind
	Value string
}

// Expression represents a scalar boolean expression.
type Expression interface {
	expr()
}

// ColumnRef references a column within the current row.
type ColumnRef struct {
	Name string
}

func (*ColumnRef) expr() {}

// LiteralExpr wraps a literal value.
type LiteralExpr struct {
	Literal Literal
}

func (*LiteralExpr) expr() {}

// ComparisonOp enumerates comparison operators.
type ComparisonOp string

const (
	ComparisonEqual        ComparisonOp = "="
	ComparisonNotEqual     ComparisonOp = "<>"
	ComparisonLess         ComparisonOp = "<"
	ComparisonLessEqual    ComparisonOp = "<="
	ComparisonGreater      ComparisonOp = ">"
	ComparisonGreaterEqual ComparisonOp = ">="
)

// ComparisonExpr compares two expressions.
type ComparisonExpr struct {
	Left  Expression
	Right Expression
	Op    ComparisonOp
}

func (*ComparisonExpr) expr() {}

// BooleanOp enumerates logical operators.
type BooleanOp string

const (
	BooleanAnd BooleanOp = "AND"
	BooleanOr  BooleanOp = "OR"
)

// BooleanExpr combines two expressions with AND/OR.
type BooleanExpr struct {
	Left  Expression
	Right Expression
	Op    BooleanOp
}

func (*BooleanExpr) expr() {}

// NotExpr negates the result of its operand.
type NotExpr struct {
	Expr Expression
}

func (*NotExpr) expr() {}

// IsNullExpr tests whether the operand is NULL, optionally negated.
type IsNullExpr struct {
	Expr    Expression
	Negated bool
}

func (*IsNullExpr) expr() {}

// OrderByClause describes an ORDER BY specification.
type OrderByClause struct {
	Column string
	Desc   bool
}

// LimitClause captures LIMIT/OFFSET information.
type LimitClause struct {
	Limit  int
	Offset int
}
