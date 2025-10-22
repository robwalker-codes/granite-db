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
	DataTypeDecimal
	DataTypeVarChar
	DataTypeBoolean
	DataTypeDate
	DataTypeTimestamp
)

// ColumnDef models a column definition in CREATE TABLE.
type ColumnDef struct {
	Name       string
	Type       DataType
	Length     int
	Precision  int
	Scale      int
	NotNull    bool
	PrimaryKey bool
}

// ForeignKeyAction enumerates supported referential actions.
type ForeignKeyAction int

const (
	ForeignKeyActionRestrict ForeignKeyAction = iota
	ForeignKeyActionNoAction
)

// ForeignKeyDef captures a table foreign key definition.
type ForeignKeyDef struct {
	Name            string
	Columns         []string
	ReferencedTable string
	ReferencedCols  []string
	OnDelete        ForeignKeyAction
	OnUpdate        ForeignKeyAction
	Deferrable      bool
}

// CreateTableStmt represents a CREATE TABLE statement.
type CreateTableStmt struct {
	Name        string
	Columns     []ColumnDef
	PrimaryKey  string
	ForeignKeys []ForeignKeyDef
}

func (*CreateTableStmt) stmt() {}

// DropTableStmt represents DROP TABLE.
type DropTableStmt struct {
	Name string
}

func (*DropTableStmt) stmt() {}

// CreateIndexStmt models CREATE INDEX statements.
type CreateIndexStmt struct {
	Name    string
	Table   string
	Columns []string
	Unique  bool
}

func (*CreateIndexStmt) stmt() {}

// DropIndexStmt represents DROP INDEX operations.
type DropIndexStmt struct {
	Name string
}

func (*DropIndexStmt) stmt() {}

// UpdateAssignment describes a column assignment within an UPDATE statement.
type UpdateAssignment struct {
	Column string
	Expr   Expression
}

// UpdateStmt models UPDATE table statements.
type UpdateStmt struct {
	Table       string
	Assignments []UpdateAssignment
	Where       Expression
}

func (*UpdateStmt) stmt() {}

// DeleteStmt models DELETE FROM statements.
type DeleteStmt struct {
	Table string
	Where Expression
}

func (*DeleteStmt) stmt() {}

// InsertStmt represents INSERT INTO.
type InsertStmt struct {
	Table   string
	Columns []string
	Rows    [][]Literal
}

func (*InsertStmt) stmt() {}

// SelectStmt models SELECT queries with optional clauses.
type SelectStmt struct {
	From    TableExpr
	Items   []SelectItem
	Where   Expression
	GroupBy []Expression
	Having  Expression
	OrderBy []*OrderByExpr
	Limit   *LimitClause
}

func (*SelectStmt) stmt() {}

// BeginStmt represents the start of an explicit transaction.
type BeginStmt struct{}

func (*BeginStmt) stmt() {}

// CommitStmt signals a request to make the current transaction durable.
type CommitStmt struct{}

func (*CommitStmt) stmt() {}

// RollbackStmt aborts the current transaction, discarding its changes.
type RollbackStmt struct{}

func (*RollbackStmt) stmt() {}

// SelectItem marks an entry in the SELECT projection list.
type SelectItem interface {
	selectItem()
}

// SelectExprItem describes an expression projection with an optional alias.
type SelectExprItem struct {
	Expr  Expression
	Alias string
}

func (*SelectExprItem) selectItem() {}

// SelectStarItem represents a SELECT * entry.
type SelectStarItem struct{}

func (*SelectStarItem) selectItem() {}

// TableExpr represents a table expression in the FROM clause.
type TableExpr interface {
	tableExpr()
}

// TableName identifies a base table optionally referenced with an alias.
type TableName struct {
	Name  string
	Alias string
}

func (*TableName) tableExpr() {}

// JoinType enumerates supported join kinds.
type JoinType int

const (
	JoinTypeInner JoinType = iota
	JoinTypeLeft
)

// JoinExpr models a binary join between two table expressions.
type JoinExpr struct {
	Left      TableExpr
	Right     TableExpr
	Type      JoinType
	Condition Expression
}

func (*JoinExpr) tableExpr() {}

// LiteralKind identifies literal types.
type LiteralKind int

const (
	LiteralNumber LiteralKind = iota
	LiteralBigInt
	LiteralDecimal
	LiteralString
	LiteralBoolean
	LiteralNull
	LiteralDate
	LiteralTimestamp
)

// Literal captures a literal value.
type Literal struct {
	Kind  LiteralKind
	Value string
}

// Expression represents a scalar SQL expression.
type Expression interface {
	expr()
}

// ColumnRef references a column within the current row.
type ColumnRef struct {
	Table string
	Name  string
}

func (*ColumnRef) expr() {}

// LiteralExpr wraps a literal value.
type LiteralExpr struct {
	Literal Literal
}

func (*LiteralExpr) expr() {}

// UnaryOp identifies unary operators.
type UnaryOp string

const (
	UnaryPlus  UnaryOp = "+"
	UnaryMinus UnaryOp = "-"
	UnaryNot   UnaryOp = "NOT"
)

// UnaryExpr represents a unary operator application.
type UnaryExpr struct {
	Op   UnaryOp
	Expr Expression
}

func (*UnaryExpr) expr() {}

// BinaryOp enumerates binary operators.
type BinaryOp string

const (
	BinaryAdd          BinaryOp = "+"
	BinarySubtract     BinaryOp = "-"
	BinaryMultiply     BinaryOp = "*"
	BinaryDivide       BinaryOp = "/"
	BinaryModulo       BinaryOp = "%"
	BinaryEqual        BinaryOp = "="
	BinaryNotEqual     BinaryOp = "<>"
	BinaryLess         BinaryOp = "<"
	BinaryLessEqual    BinaryOp = "<="
	BinaryGreater      BinaryOp = ">"
	BinaryGreaterEqual BinaryOp = ">="
	BinaryAnd          BinaryOp = "AND"
	BinaryOr           BinaryOp = "OR"
)

// BinaryExpr combines two operands with a binary operator.
type BinaryExpr struct {
	Left  Expression
	Right Expression
	Op    BinaryOp
}

func (*BinaryExpr) expr() {}

// FunctionCallExpr captures function invocations.
type FunctionCallExpr struct {
	Name     string
	Args     []Expression
	Distinct bool
}

func (*FunctionCallExpr) expr() {}

// StarExpr represents the * token in contexts such as COUNT(*).
type StarExpr struct{}

func (*StarExpr) expr() {}

// IsNullExpr tests whether the operand is NULL, optionally negated.
type IsNullExpr struct {
	Expr    Expression
	Negated bool
}

func (*IsNullExpr) expr() {}

// OrderByExpr describes an ORDER BY specification.
type OrderByExpr struct {
	Expr Expression
	Desc bool
}

// LimitClause captures LIMIT/OFFSET information.
type LimitClause struct {
	Limit  int
	Offset int
}
