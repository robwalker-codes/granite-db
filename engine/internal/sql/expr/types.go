package expr

import "github.com/example/granite-db/engine/internal/catalog"

// TypeKind enumerates the logical scalar types supported by the expression
// layer. It intentionally mirrors the catalog's column types with the
// addition of a dedicated NULL marker used during inference.
type TypeKind int

const (
	TypeUnknown TypeKind = iota
	TypeNull
	TypeInt
	TypeBigInt
	TypeDecimal
	TypeVarChar
	TypeBoolean
	TypeDate
	TypeTimestamp
)

// Type describes the logical type and nullability of an expression.
type Type struct {
	Kind      TypeKind
	Nullable  bool
	Precision int
	Scale     int
	Length    int
}

// WithNullability produces a copy of the type with the provided nullability.
func (t Type) WithNullability(nullable bool) Type {
	t.Nullable = nullable
	return t
}

// IsNumeric reports whether the type is one of the numeric kinds.
func (t Type) IsNumeric() bool {
	switch t.Kind {
	case TypeInt, TypeBigInt, TypeDecimal:
		return true
	default:
		return false
	}
}

// IsString reports whether the type represents textual data.
func (t Type) IsString() bool {
	return t.Kind == TypeVarChar
}

// IsTemporal reports whether the type represents a date or timestamp.
func (t Type) IsTemporal() bool {
	switch t.Kind {
	case TypeDate, TypeTimestamp:
		return true
	default:
		return false
	}
}

// FromColumn maps a catalog column into an expression type.
func FromColumn(col catalog.Column) Type {
	switch col.Type {
	case catalog.ColumnTypeInt:
		return Type{Kind: TypeInt, Nullable: !col.NotNull}
	case catalog.ColumnTypeBigInt:
		return Type{Kind: TypeBigInt, Nullable: !col.NotNull}
	case catalog.ColumnTypeVarChar:
		return Type{Kind: TypeVarChar, Nullable: !col.NotNull, Length: col.Length}
	case catalog.ColumnTypeBoolean:
		return Type{Kind: TypeBoolean, Nullable: !col.NotNull}
	case catalog.ColumnTypeDate:
		return Type{Kind: TypeDate, Nullable: !col.NotNull}
	case catalog.ColumnTypeTimestamp:
		return Type{Kind: TypeTimestamp, Nullable: !col.NotNull}
	default:
		return Type{Kind: TypeUnknown, Nullable: true}
	}
}

// NullType returns the canonical NULL type used during inference.
func NullType() Type {
	return Type{Kind: TypeNull, Nullable: true}
}

// BooleanType returns the BOOLEAN type with specified nullability.
func BooleanType(nullable bool) Type {
	return Type{Kind: TypeBoolean, Nullable: nullable}
}

// IntType returns the INT type with specified nullability.
func IntType(nullable bool) Type {
	return Type{Kind: TypeInt, Nullable: nullable}
}

// BigIntType returns the BIGINT type with specified nullability.
func BigIntType(nullable bool) Type {
	return Type{Kind: TypeBigInt, Nullable: nullable}
}

// DecimalType constructs a DECIMAL type.
func DecimalType(nullable bool, precision, scale int) Type {
	return Type{Kind: TypeDecimal, Nullable: nullable, Precision: precision, Scale: scale}
}

// VarCharType constructs a VARCHAR type definition.
func VarCharType(nullable bool, length int) Type {
	return Type{Kind: TypeVarChar, Nullable: nullable, Length: length}
}

// DateType constructs a DATE type definition.
func DateType(nullable bool) Type {
	return Type{Kind: TypeDate, Nullable: nullable}
}

// TimestampType constructs a TIMESTAMP type definition.
func TimestampType(nullable bool) Type {
	return Type{Kind: TypeTimestamp, Nullable: nullable}
}

// TypedExpr represents an expression with an inferred result type.
type TypedExpr interface {
	ResultType() Type
}

// ColumnRef references a column within the current row buffer.
type ColumnRef struct {
	Index  int
	Column catalog.Column
	typ    Type
}

// NewColumnRef constructs a typed column reference.
func NewColumnRef(idx int, col catalog.Column) *ColumnRef {
	return &ColumnRef{Index: idx, Column: col, typ: FromColumn(col)}
}

// ResultType implements TypedExpr.
func (c *ColumnRef) ResultType() Type {
	return c.typ
}

// Literal represents a constant expression.
type Literal struct {
	Value interface{}
	typ   Type
}

// NewLiteral constructs a literal expression.
func NewLiteral(value interface{}, typ Type) *Literal {
	return &Literal{Value: value, typ: typ}
}

// ResultType implements TypedExpr.
func (l *Literal) ResultType() Type {
	return l.typ
}

// UnaryOp enumerates unary operators supported by the evaluator.
type UnaryOp int

const (
	UnaryOpPlus UnaryOp = iota
	UnaryOpMinus
	UnaryOpNot
)

// UnaryExpr models a unary expression.
type UnaryExpr struct {
	Op   UnaryOp
	Expr TypedExpr
	typ  Type
}

// NewUnary constructs a typed unary expression.
func NewUnary(op UnaryOp, expr TypedExpr, typ Type) *UnaryExpr {
	return &UnaryExpr{Op: op, Expr: expr, typ: typ}
}

// ResultType implements TypedExpr.
func (u *UnaryExpr) ResultType() Type {
	return u.typ
}

// BinaryOp enumerates binary operators.
type BinaryOp int

const (
	BinaryOpAdd BinaryOp = iota
	BinaryOpSubtract
	BinaryOpMultiply
	BinaryOpDivide
	BinaryOpModulo
	BinaryOpEqual
	BinaryOpNotEqual
	BinaryOpLess
	BinaryOpLessEqual
	BinaryOpGreater
	BinaryOpGreaterEqual
	BinaryOpAnd
	BinaryOpOr
)

// BinaryExpr describes a binary expression.
type BinaryExpr struct {
	Left  TypedExpr
	Right TypedExpr
	Op    BinaryOp
	typ   Type
}

// NewBinary constructs a typed binary expression.
func NewBinary(left, right TypedExpr, op BinaryOp, typ Type) *BinaryExpr {
	return &BinaryExpr{Left: left, Right: right, Op: op, typ: typ}
}

// ResultType implements TypedExpr.
func (b *BinaryExpr) ResultType() Type {
	return b.typ
}

// FunctionExpr captures simple scalar function invocations.
type FunctionExpr struct {
	Name string
	Args []TypedExpr
	typ  Type
}

// NewFunction constructs a typed function call.
func NewFunction(name string, args []TypedExpr, typ Type) *FunctionExpr {
	return &FunctionExpr{Name: name, Args: args, typ: typ}
}

// ResultType implements TypedExpr.
func (f *FunctionExpr) ResultType() Type {
	return f.typ
}

// CoalesceExpr models the COALESCE(a, b) builtin.
type CoalesceExpr struct {
	Left  TypedExpr
	Right TypedExpr
	typ   Type
}

// NewCoalesce constructs a typed COALESCE expression.
func NewCoalesce(left, right TypedExpr, typ Type) *CoalesceExpr {
	return &CoalesceExpr{Left: left, Right: right, typ: typ}
}

// ResultType implements TypedExpr.
func (c *CoalesceExpr) ResultType() Type {
	return c.typ
}

// IsNullExpr tests whether the operand yields NULL.
type IsNullExpr struct {
	Expr    TypedExpr
	Negated bool
}

// ResultType implements TypedExpr.
func (i *IsNullExpr) ResultType() Type {
	return BooleanType(false)
}
