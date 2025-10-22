package validator

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/shopspring/decimal"

	"github.com/example/granite-db/engine/internal/catalog"
	"github.com/example/granite-db/engine/internal/sql/expr"
	"github.com/example/granite-db/engine/internal/sql/parser"
)

// OutputColumn describes a projection in the SELECT list.
type OutputColumn struct {
	Name string
	Expr expr.TypedExpr
	Type expr.Type
}

// Ordering captures ORDER BY information following validation.
type Ordering struct {
	ColumnIndex int
	Desc        bool
}

// ValidatedSelect is the semantic representation of a SELECT statement.
type ValidatedSelect struct {
	Table   *catalog.Table
	Outputs []OutputColumn
	Filter  expr.TypedExpr
	OrderBy *Ordering
	Limit   *parser.LimitClause
}

// ValidateSelect analyses the parsed statement against the provided table
// metadata and returns a typed representation suitable for planning.
func ValidateSelect(table *catalog.Table, stmt *parser.SelectStmt) (*ValidatedSelect, error) {
	if stmt.HasTable && table == nil {
		return nil, fmt.Errorf("validator: table metadata is required")
	}
	validator := &selectValidator{
		table:   table,
		columns: make(map[string]int),
	}
	if table != nil {
		validator.columns = make(map[string]int, len(table.Columns))
		for idx, col := range table.Columns {
			validator.columns[strings.ToLower(col.Name)] = idx
		}
	}

	outputs, err := validator.buildOutputs(stmt.Items)
	if err != nil {
		return nil, err
	}

	var filter expr.TypedExpr
	if stmt.Where != nil {
		filter, err = validator.buildExpression(stmt.Where, "WHERE clause")
		if err != nil {
			return nil, err
		}
		if filter.ResultType().Kind != expr.TypeBoolean {
			return nil, fmt.Errorf("validator: WHERE clause must evaluate to BOOLEAN")
		}
	}

	var orderBy *Ordering
	if stmt.OrderBy != nil {
		if table == nil {
			return nil, fmt.Errorf("validator: ORDER BY requires a FROM table")
		}
		idx, ok := validator.columns[strings.ToLower(stmt.OrderBy.Column)]
		if !ok {
			return nil, fmt.Errorf("validator: unknown column %q in ORDER BY", stmt.OrderBy.Column)
		}
		orderBy = &Ordering{ColumnIndex: idx, Desc: stmt.OrderBy.Desc}
	}

	return &ValidatedSelect{
		Table:   table,
		Outputs: outputs,
		Filter:  filter,
		OrderBy: orderBy,
		Limit:   stmt.Limit,
	}, nil
}

type selectValidator struct {
	table   *catalog.Table
	columns map[string]int
}

func (v *selectValidator) buildOutputs(items []parser.SelectItem) ([]OutputColumn, error) {
	if len(items) == 0 {
		return nil, fmt.Errorf("validator: SELECT list cannot be empty")
	}
	if len(items) == 1 {
		if _, ok := items[0].(*parser.SelectStarItem); ok {
			if v.table == nil {
				return nil, fmt.Errorf("validator: SELECT * requires a FROM table")
			}
			cols := make([]OutputColumn, len(v.table.Columns))
			for i, col := range v.table.Columns {
				expression := expr.NewColumnRef(i, col)
				cols[i] = OutputColumn{Name: col.Name, Expr: expression, Type: expression.ResultType()}
			}
			return cols, nil
		}
	}

	outputs := make([]OutputColumn, 0, len(items))
	for _, item := range items {
		exprItem, ok := item.(*parser.SelectExprItem)
		if !ok {
			return nil, fmt.Errorf("validator: SELECT * cannot be combined with expressions (yet)")
		}
		typed, err := v.buildExpression(exprItem.Expr, "SELECT list")
		if err != nil {
			return nil, err
		}
		name := exprItem.Alias
		if name == "" {
			name = parser.FormatExpression(exprItem.Expr)
		}
		outputs = append(outputs, OutputColumn{Name: name, Expr: typed, Type: typed.ResultType()})
	}
	return outputs, nil
}

func (v *selectValidator) buildExpression(node parser.Expression, context string) (expr.TypedExpr, error) {
	switch e := node.(type) {
	case *parser.ColumnRef:
		return v.buildColumnRef(e, context)
	case *parser.LiteralExpr:
		return v.buildLiteral(e.Literal)
	case *parser.UnaryExpr:
		return v.buildUnary(e, context)
	case *parser.BinaryExpr:
		return v.buildBinary(e, context)
	case *parser.FunctionCallExpr:
		return v.buildFunction(e, context)
	case *parser.IsNullExpr:
		operand, err := v.buildExpression(e.Expr, context)
		if err != nil {
			return nil, err
		}
		return &expr.IsNullExpr{Expr: operand, Negated: e.Negated}, nil
	default:
		return nil, fmt.Errorf("validator: unsupported expression %T", node)
	}
}

func (v *selectValidator) buildColumnRef(ref *parser.ColumnRef, context string) (expr.TypedExpr, error) {
	if v.table == nil {
		if ref.Table != "" {
			return nil, fmt.Errorf("validator: unknown table %q referenced in %s", ref.Table, context)
		}
		return nil, fmt.Errorf("validator: unknown column %q in %s", ref.Name, context)
	}
	if ref.Table != "" && !strings.EqualFold(ref.Table, v.table.Name) {
		return nil, fmt.Errorf("validator: unknown table %q referenced in %s", ref.Table, context)
	}
	idx, ok := v.columns[strings.ToLower(ref.Name)]
	if !ok {
		return nil, fmt.Errorf("validator: unknown column %q in %s", ref.Name, context)
	}
	return expr.NewColumnRef(idx, v.table.Columns[idx]), nil
}

func (v *selectValidator) buildLiteral(lit parser.Literal) (expr.TypedExpr, error) {
	switch lit.Kind {
	case parser.LiteralNull:
		return expr.NewLiteral(nil, expr.NullType()), nil
	case parser.LiteralString:
		typ := expr.VarCharType(false, utf8.RuneCountInString(lit.Value))
		return expr.NewLiteral(lit.Value, typ), nil
	case parser.LiteralBoolean:
		value := strings.EqualFold(lit.Value, "TRUE")
		return expr.NewLiteral(value, expr.BooleanType(false)), nil
	case parser.LiteralNumber:
		return buildIntegerLiteral(lit.Value)
	case parser.LiteralDecimal:
		return buildDecimalLiteral(lit.Value)
	case parser.LiteralDate:
		parsed, err := time.Parse("2006-01-02", lit.Value)
		if err != nil {
			return nil, fmt.Errorf("validator: invalid DATE literal %q", lit.Value)
		}
		return expr.NewLiteral(parsed, expr.DateType(false)), nil
	case parser.LiteralTimestamp:
		layouts := []string{time.RFC3339, "2006-01-02 15:04:05"}
		var parsed time.Time
		var err error
		for _, layout := range layouts {
			parsed, err = time.Parse(layout, lit.Value)
			if err == nil {
				return expr.NewLiteral(parsed, expr.TimestampType(false)), nil
			}
		}
		return nil, fmt.Errorf("validator: invalid TIMESTAMP literal %q", lit.Value)
	default:
		return nil, fmt.Errorf("validator: unsupported literal kind %d", lit.Kind)
	}
}

func buildIntegerLiteral(raw string) (expr.TypedExpr, error) {
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("validator: invalid integer literal %q", raw)
	}
	if value >= math.MinInt32 && value <= math.MaxInt32 {
		return expr.NewLiteral(int32(value), expr.IntType(false)), nil
	}
	return expr.NewLiteral(value, expr.BigIntType(false)), nil
}

func buildDecimalLiteral(raw string) (expr.TypedExpr, error) {
	dec, err := decimal.NewFromString(raw)
	if err != nil {
		return nil, fmt.Errorf("validator: invalid DECIMAL literal %q", raw)
	}
	precision, scale := decimalMetadata(raw)
	return expr.NewLiteral(dec, expr.DecimalType(false, precision, scale)), nil
}

func (v *selectValidator) buildUnary(node *parser.UnaryExpr, context string) (expr.TypedExpr, error) {
	operand, err := v.buildExpression(node.Expr, context)
	if err != nil {
		return nil, err
	}
	typ := operand.ResultType()
	switch node.Op {
	case parser.UnaryPlus:
		if !typ.IsNumeric() && typ.Kind != expr.TypeNull {
			return nil, fmt.Errorf("validator: unary + requires numeric operand")
		}
		return expr.NewUnary(expr.UnaryOpPlus, operand, typ), nil
	case parser.UnaryMinus:
		if !typ.IsNumeric() && typ.Kind != expr.TypeNull {
			return nil, fmt.Errorf("validator: unary - requires numeric operand")
		}
		return expr.NewUnary(expr.UnaryOpMinus, operand, typ), nil
	case parser.UnaryNot:
		if typ.Kind != expr.TypeBoolean && typ.Kind != expr.TypeNull {
			return nil, fmt.Errorf("validator: NOT expects BOOLEAN operand")
		}
		return expr.NewUnary(expr.UnaryOpNot, operand, expr.BooleanType(true)), nil
	default:
		return nil, fmt.Errorf("validator: unsupported unary operator %s", node.Op)
	}
}

func (v *selectValidator) buildBinary(node *parser.BinaryExpr, context string) (expr.TypedExpr, error) {
	left, err := v.buildExpression(node.Left, context)
	if err != nil {
		return nil, err
	}
	right, err := v.buildExpression(node.Right, context)
	if err != nil {
		return nil, err
	}
	leftType := left.ResultType()
	rightType := right.ResultType()
	switch node.Op {
	case parser.BinaryAdd, parser.BinarySubtract, parser.BinaryMultiply, parser.BinaryDivide, parser.BinaryModulo:
		resultType, err := promoteNumeric(leftType, rightType)
		if err != nil {
			return nil, err
		}
		if node.Op == parser.BinaryDivide {
			if resultType.Kind != expr.TypeDecimal {
				scale := maxScale(leftType, rightType) + 6
				precision := scale + 18
				resultType = expr.DecimalType(resultType.Nullable, precision, scale)
			}
		}
		if node.Op == parser.BinaryModulo && resultType.Kind == expr.TypeDecimal {
			return nil, fmt.Errorf("validator: modulo is only supported for integral types")
		}
		return expr.NewBinary(left, right, mapArithmeticOp(node.Op), resultType), nil
	case parser.BinaryEqual, parser.BinaryNotEqual, parser.BinaryLess, parser.BinaryLessEqual, parser.BinaryGreater, parser.BinaryGreaterEqual:
		if err := ensureComparable(leftType, rightType); err != nil {
			return nil, err
		}
		nullable := leftType.Nullable || rightType.Nullable || leftType.Kind == expr.TypeNull || rightType.Kind == expr.TypeNull
		return expr.NewBinary(left, right, mapComparisonOp(node.Op), expr.BooleanType(nullable)), nil
	case parser.BinaryAnd, parser.BinaryOr:
		if err := ensureBoolean(leftType); err != nil {
			return nil, err
		}
		if err := ensureBoolean(rightType); err != nil {
			return nil, err
		}
		op := mapBooleanOp(node.Op)
		return expr.NewBinary(left, right, op, expr.BooleanType(true)), nil
	default:
		return nil, fmt.Errorf("validator: unsupported binary operator %s", node.Op)
	}
}

func (v *selectValidator) buildFunction(fn *parser.FunctionCallExpr, context string) (expr.TypedExpr, error) {
	name := strings.ToUpper(fn.Name)
	switch name {
	case "LOWER", "UPPER":
		if len(fn.Args) != 1 {
			return nil, fmt.Errorf("validator: %s expects exactly 1 argument", name)
		}
		arg, err := v.buildExpression(fn.Args[0], context)
		if err != nil {
			return nil, err
		}
		argType := arg.ResultType()
		if !argType.IsString() && argType.Kind != expr.TypeNull {
			return nil, fmt.Errorf("validator: function %s expects VARCHAR argument but received %s", name, describeType(argType))
		}
		return expr.NewFunction(name, []expr.TypedExpr{arg}, expr.VarCharType(argType.Nullable || argType.Kind == expr.TypeNull, argType.Length)), nil
	case "LENGTH":
		if len(fn.Args) != 1 {
			return nil, fmt.Errorf("validator: LENGTH expects exactly 1 argument")
		}
		arg, err := v.buildExpression(fn.Args[0], context)
		if err != nil {
			return nil, err
		}
		argType := arg.ResultType()
		if !argType.IsString() && argType.Kind != expr.TypeNull {
			return nil, fmt.Errorf("validator: function LENGTH expects VARCHAR argument but received %s", describeType(argType))
		}
		return expr.NewFunction(name, []expr.TypedExpr{arg}, expr.IntType(argType.Nullable || argType.Kind == expr.TypeNull)), nil
	case "COALESCE":
		if len(fn.Args) != 2 {
			return nil, fmt.Errorf("validator: COALESCE expects exactly 2 arguments")
		}
		left, err := v.buildExpression(fn.Args[0], context)
		if err != nil {
			return nil, err
		}
		right, err := v.buildExpression(fn.Args[1], context)
		if err != nil {
			return nil, err
		}
		resultType, err := coalesceType(left.ResultType(), right.ResultType())
		if err != nil {
			return nil, err
		}
		return expr.NewCoalesce(left, right, resultType), nil
	default:
		return nil, fmt.Errorf("validator: unknown function %s", fn.Name)
	}
}

func promoteNumeric(left, right expr.Type) (expr.Type, error) {
	if left.Kind == expr.TypeNull {
		return right.WithNullability(true), nil
	}
	if right.Kind == expr.TypeNull {
		return left.WithNullability(true), nil
	}
	if !left.IsNumeric() || !right.IsNumeric() {
		return expr.Type{}, fmt.Errorf("validator: numeric operator requires numeric operands")
	}
	nullable := left.Nullable || right.Nullable
	if left.Kind == expr.TypeDecimal || right.Kind == expr.TypeDecimal {
		precision := max(left.Precision, right.Precision)
		scale := max(left.Scale, right.Scale)
		return expr.DecimalType(nullable, precision, scale), nil
	}
	if left.Kind == expr.TypeBigInt || right.Kind == expr.TypeBigInt {
		return expr.BigIntType(nullable), nil
	}
	return expr.IntType(nullable), nil
}

func ensureComparable(left, right expr.Type) error {
	if left.Kind == expr.TypeNull || right.Kind == expr.TypeNull {
		return nil
	}
	if left.IsNumeric() && right.IsNumeric() {
		return nil
	}
	if left.IsString() && right.IsString() {
		return nil
	}
	if left.IsTemporal() && right.IsTemporal() {
		return nil
	}
	if left.Kind == expr.TypeBoolean && right.Kind == expr.TypeBoolean {
		return nil
	}
	return fmt.Errorf("validator: incompatible comparison between %s and %s", describeType(left), describeType(right))
}

func ensureBoolean(t expr.Type) error {
	if t.Kind == expr.TypeBoolean || t.Kind == expr.TypeNull {
		return nil
	}
	return fmt.Errorf("validator: expected BOOLEAN expression but found %s", describeType(t))
}

func coalesceType(left, right expr.Type) (expr.Type, error) {
	if left.Kind == expr.TypeNull {
		return right.WithNullability(right.Nullable), nil
	}
	if right.Kind == expr.TypeNull {
		return left.WithNullability(left.Nullable), nil
	}
	if left.Kind != right.Kind {
		return expr.Type{}, fmt.Errorf("validator: COALESCE arguments must share the same type (found %s and %s)", describeType(left), describeType(right))
	}
	nullable := left.Nullable && right.Nullable
	switch left.Kind {
	case expr.TypeVarChar:
		length := max(left.Length, right.Length)
		return expr.VarCharType(nullable, length), nil
	case expr.TypeDecimal:
		precision := max(left.Precision, right.Precision)
		scale := max(left.Scale, right.Scale)
		return expr.DecimalType(nullable, precision, scale), nil
	case expr.TypeInt:
		return expr.IntType(nullable), nil
	case expr.TypeBigInt:
		return expr.BigIntType(nullable), nil
	case expr.TypeBoolean:
		return expr.BooleanType(nullable), nil
	case expr.TypeDate:
		return expr.DateType(nullable), nil
	case expr.TypeTimestamp:
		return expr.TimestampType(nullable), nil
	default:
		return expr.Type{}, fmt.Errorf("validator: unsupported COALESCE type %s", describeType(left))
	}
}

func mapArithmeticOp(op parser.BinaryOp) expr.BinaryOp {
	switch op {
	case parser.BinaryAdd:
		return expr.BinaryOpAdd
	case parser.BinarySubtract:
		return expr.BinaryOpSubtract
	case parser.BinaryMultiply:
		return expr.BinaryOpMultiply
	case parser.BinaryDivide:
		return expr.BinaryOpDivide
	case parser.BinaryModulo:
		return expr.BinaryOpModulo
	default:
		return expr.BinaryOpAdd
	}
}

func mapComparisonOp(op parser.BinaryOp) expr.BinaryOp {
	switch op {
	case parser.BinaryEqual:
		return expr.BinaryOpEqual
	case parser.BinaryNotEqual:
		return expr.BinaryOpNotEqual
	case parser.BinaryLess:
		return expr.BinaryOpLess
	case parser.BinaryLessEqual:
		return expr.BinaryOpLessEqual
	case parser.BinaryGreater:
		return expr.BinaryOpGreater
	case parser.BinaryGreaterEqual:
		return expr.BinaryOpGreaterEqual
	default:
		return expr.BinaryOpEqual
	}
}

func mapBooleanOp(op parser.BinaryOp) expr.BinaryOp {
	if op == parser.BinaryAnd {
		return expr.BinaryOpAnd
	}
	return expr.BinaryOpOr
}

func decimalMetadata(raw string) (int, int) {
	clean := strings.ReplaceAll(raw, "_", "")
	parts := strings.SplitN(clean, ".", 2)
	precision := len(strings.ReplaceAll(clean, ".", ""))
	scale := 0
	if len(parts) == 2 {
		scale = len(parts[1])
	}
	return precision, scale
}

func describeType(t expr.Type) string {
	switch t.Kind {
	case expr.TypeNull:
		return "NULL"
	case expr.TypeInt:
		return "INT"
	case expr.TypeBigInt:
		return "BIGINT"
	case expr.TypeDecimal:
		return "DECIMAL"
	case expr.TypeVarChar:
		return "VARCHAR"
	case expr.TypeBoolean:
		return "BOOLEAN"
	case expr.TypeDate:
		return "DATE"
	case expr.TypeTimestamp:
		return "TIMESTAMP"
	default:
		return "UNKNOWN"
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func maxScale(left, right expr.Type) int {
	return max(left.Scale, right.Scale)
}
