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
	Sources  []*TableSource
	Joins    []*JoinClause
	Bindings []ColumnBinding
	Outputs  []OutputColumn
	Filter   expr.TypedExpr
	OrderBy  *Ordering
	Limit    *parser.LimitClause
}

// TableSource captures metadata about a table referenced in the FROM clause.
type TableSource struct {
	Name        string
	Alias       string
	Table       *catalog.Table
	ColumnStart int
	ColumnCount int
}

// ColumnBinding describes a column available to expressions during validation.
type ColumnBinding struct {
	Index      int
	TableAlias string
	Column     catalog.Column
}

// JoinType enumerates supported join kinds at validation time.
type JoinType int

const (
	JoinTypeInner JoinType = iota
	JoinTypeLeft
)

// EquiCondition summarises a single equality predicate suitable for hash joins.
type EquiCondition struct {
	LeftColumn  int
	RightColumn int
	LeftOffset  int
	RightOffset int
}

// JoinClause describes a validated join between the accumulated left side and a new right source.
type JoinClause struct {
	Type           JoinType
	Condition      expr.TypedExpr
	EquiConditions []EquiCondition
	Residuals      []expr.TypedExpr
	Right          *TableSource
}

// ValidateSelect analyses the parsed statement against the provided table
// metadata and returns a typed representation suitable for planning.
func ValidateSelect(cat *catalog.Catalog, stmt *parser.SelectStmt) (*ValidatedSelect, error) {
	validator := newSelectValidator(cat)

	joins, err := validator.buildFrom(stmt.From)
	if err != nil {
		return nil, err
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

	orderBy, err := validator.buildOrdering(stmt.OrderBy)
	if err != nil {
		return nil, err
	}

	return &ValidatedSelect{
		Sources:  validator.sources(),
		Joins:    joins,
		Bindings: validator.bindings(),
		Outputs:  outputs,
		Filter:   filter,
		OrderBy:  orderBy,
		Limit:    stmt.Limit,
	}, nil
}

type selectValidator struct {
	catalog *catalog.Catalog
	scope   *validationScope
}

func newSelectValidator(cat *catalog.Catalog) *selectValidator {
	return &selectValidator{
		catalog: cat,
		scope:   newValidationScope(),
	}
}

func (v *selectValidator) addTable(name, alias string) (*TableSource, error) {
	if v.catalog == nil {
		return nil, fmt.Errorf("validator: catalog metadata is required for table resolution")
	}
	table, ok := v.catalog.GetTable(name)
	if !ok {
		return nil, fmt.Errorf("validator: table %q not found", name)
	}
	return v.scope.addTable(table, alias)
}

func (v *selectValidator) sources() []*TableSource {
	out := make([]*TableSource, len(v.scope.sources))
	copy(out, v.scope.sources)
	return out
}

func (v *selectValidator) bindings() []ColumnBinding {
	return v.scope.bindings()
}

func (v *selectValidator) resolveColumn(table, name, context string) (*columnBinding, error) {
	if len(v.scope.columns) == 0 {
		if table != "" {
			return nil, fmt.Errorf("validator: unknown table alias %q referenced in %s", table, context)
		}
		return nil, fmt.Errorf("validator: unknown column %q in %s", name, context)
	}
	if table != "" {
		return v.scope.resolveByAlias(table, name, context)
	}
	return v.scope.resolveByName(name, context)
}

type validationScope struct {
	sources         []*TableSource
	aliasIndex      map[string]*TableSource
	columns         []*columnBinding
	columnsByName   map[string][]*columnBinding
	columnsBySource map[*TableSource]map[string]*columnBinding
}

type columnBinding struct {
	binding   ColumnBinding
	source    *TableSource
	nameLower string
}

func newValidationScope() *validationScope {
	return &validationScope{
		aliasIndex:      make(map[string]*TableSource),
		columnsByName:   make(map[string][]*columnBinding),
		columnsBySource: make(map[*TableSource]map[string]*columnBinding),
	}
}

func (s *validationScope) addTable(table *catalog.Table, alias string) (*TableSource, error) {
	if alias == "" {
		alias = table.Name
	}
	key := strings.ToLower(alias)
	if _, exists := s.aliasIndex[key]; exists {
		return nil, fmt.Errorf("validator: duplicate table alias %q", alias)
	}
	source := &TableSource{
		Name:        table.Name,
		Alias:       alias,
		Table:       table,
		ColumnStart: len(s.columns),
		ColumnCount: len(table.Columns),
	}
	columnMap := make(map[string]*columnBinding, len(table.Columns))
	for _, col := range table.Columns {
		binding := &columnBinding{
			binding: ColumnBinding{
				Index:      len(s.columns),
				TableAlias: alias,
				Column:     col,
			},
			source:    source,
			nameLower: strings.ToLower(col.Name),
		}
		s.columns = append(s.columns, binding)
		s.columnsByName[binding.nameLower] = append(s.columnsByName[binding.nameLower], binding)
		columnMap[binding.nameLower] = binding
	}
	s.sources = append(s.sources, source)
	s.aliasIndex[key] = source
	s.columnsBySource[source] = columnMap
	return source, nil
}

func (s *validationScope) resolveByAlias(alias, name, context string) (*columnBinding, error) {
	source, ok := s.aliasIndex[strings.ToLower(alias)]
	if !ok {
		return nil, fmt.Errorf("validator: unknown table alias %q referenced in %s", alias, context)
	}
	column, ok := s.columnsBySource[source][strings.ToLower(name)]
	if !ok {
		return nil, fmt.Errorf("validator: unknown column %q on %s in %s", name, source.Alias, context)
	}
	return column, nil
}

func (s *validationScope) resolveByName(name, context string) (*columnBinding, error) {
	matches := s.columnsByName[strings.ToLower(name)]
	if len(matches) == 0 {
		return nil, fmt.Errorf("validator: unknown column %q in %s", name, context)
	}
	if len(matches) > 1 {
		candidates := make([]string, len(matches))
		for i, match := range matches {
			candidates[i] = fmt.Sprintf("%s.%s", match.binding.TableAlias, match.binding.Column.Name)
		}
		return nil, fmt.Errorf("validator: ambiguous column %q (candidates: %s)", name, strings.Join(candidates, ", "))
	}
	return matches[0], nil
}

func (s *validationScope) bindings() []ColumnBinding {
	bindings := make([]ColumnBinding, len(s.columns))
	for i, binding := range s.columns {
		bindings[i] = binding.binding
	}
	return bindings
}

func (v *selectValidator) buildFrom(node parser.TableExpr) ([]*JoinClause, error) {
	if node == nil {
		return nil, nil
	}
	switch t := node.(type) {
	case *parser.TableName:
		if _, err := v.addTable(t.Name, t.Alias); err != nil {
			return nil, err
		}
		return nil, nil
	case *parser.JoinExpr:
		joins, err := v.buildFrom(t.Left)
		if err != nil {
			return nil, err
		}
		rightTable, ok := t.Right.(*parser.TableName)
		if !ok {
			return nil, fmt.Errorf("validator: unsupported join operand %T", t.Right)
		}
		right, err := v.addTable(rightTable.Name, rightTable.Alias)
		if err != nil {
			return nil, err
		}
		condition, err := v.buildExpression(t.Condition, "JOIN condition")
		if err != nil {
			return nil, err
		}
		if condition.ResultType().Kind != expr.TypeBoolean {
			return nil, fmt.Errorf("validator: JOIN condition must evaluate to BOOLEAN")
		}
		equi, residuals := v.analyseJoinCondition(condition, right)
		join := &JoinClause{
			Type:           mapJoinType(t.Type),
			Condition:      condition,
			EquiConditions: equi,
			Residuals:      residuals,
			Right:          right,
		}
		return append(joins, join), nil
	default:
		return nil, fmt.Errorf("validator: unsupported table expression %T", node)
	}
}

func mapJoinType(joinType parser.JoinType) JoinType {
	switch joinType {
	case parser.JoinTypeLeft:
		return JoinTypeLeft
	default:
		return JoinTypeInner
	}
}

func (v *selectValidator) analyseJoinCondition(condition expr.TypedExpr, right *TableSource) ([]EquiCondition, []expr.TypedExpr) {
	conjuncts := splitConjuncts(condition)
	equi := make([]EquiCondition, 0, len(conjuncts))
	residuals := make([]expr.TypedExpr, 0)
	for _, conjunct := range conjuncts {
		if eq, ok := v.extractEqui(conjunct, right); ok {
			equi = append(equi, eq)
			continue
		}
		residuals = append(residuals, conjunct)
	}
	return equi, residuals
}

func splitConjuncts(node expr.TypedExpr) []expr.TypedExpr {
	binary, ok := node.(*expr.BinaryExpr)
	if ok && binary.Op == expr.BinaryOpAnd {
		left := splitConjuncts(binary.Left)
		right := splitConjuncts(binary.Right)
		return append(left, right...)
	}
	return []expr.TypedExpr{node}
}

func (v *selectValidator) extractEqui(node expr.TypedExpr, right *TableSource) (EquiCondition, bool) {
	binary, ok := node.(*expr.BinaryExpr)
	if !ok || binary.Op != expr.BinaryOpEqual {
		return EquiCondition{}, false
	}
	leftRef, leftOK := binary.Left.(*expr.ColumnRef)
	rightRef, rightOK := binary.Right.(*expr.ColumnRef)
	if !leftOK || !rightOK {
		return EquiCondition{}, false
	}
	rightStart := right.ColumnStart
	rightEnd := rightStart + right.ColumnCount
	leftIsRight := leftRef.Index >= rightStart && leftRef.Index < rightEnd
	rightIsRight := rightRef.Index >= rightStart && rightRef.Index < rightEnd
	if leftIsRight == rightIsRight {
		return EquiCondition{}, false
	}
	eq := EquiCondition{}
	if leftIsRight {
		eq.LeftColumn = rightRef.Index
		eq.RightColumn = leftRef.Index
	} else {
		eq.LeftColumn = leftRef.Index
		eq.RightColumn = rightRef.Index
	}
	eq.LeftOffset = eq.LeftColumn
	eq.RightOffset = eq.RightColumn - rightStart
	if eq.RightOffset < 0 || eq.RightOffset >= right.ColumnCount {
		return EquiCondition{}, false
	}
	return eq, true
}

func (v *selectValidator) buildOrdering(clause *parser.OrderByClause) (*Ordering, error) {
	if clause == nil {
		return nil, nil
	}
	if len(v.scope.columns) == 0 {
		return nil, fmt.Errorf("validator: ORDER BY requires a FROM table")
	}
	qualifier := ""
	column := clause.Column
	if parts := strings.SplitN(column, ".", 2); len(parts) == 2 {
		qualifier = parts[0]
		column = parts[1]
	}
	binding, err := v.resolveColumn(qualifier, column, "ORDER BY")
	if err != nil {
		return nil, err
	}
	return &Ordering{ColumnIndex: binding.binding.Index, Desc: clause.Desc}, nil
}

func (v *selectValidator) buildOutputs(items []parser.SelectItem) ([]OutputColumn, error) {
	if len(items) == 0 {
		return nil, fmt.Errorf("validator: SELECT list cannot be empty")
	}
	if len(items) == 1 {
		if _, ok := items[0].(*parser.SelectStarItem); ok {
			if len(v.scope.sources) == 0 {
				return nil, fmt.Errorf("validator: SELECT * requires a FROM table")
			}
			cols := make([]OutputColumn, len(v.scope.columns))
			includeAlias := len(v.scope.sources) > 1
			for i, binding := range v.scope.columns {
				expression := expr.NewColumnRef(binding.binding.Index, binding.binding.Column)
				name := binding.binding.Column.Name
				if includeAlias {
					name = binding.binding.TableAlias + "." + name
				}
				cols[i] = OutputColumn{Name: name, Expr: expression, Type: expression.ResultType()}
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
	binding, err := v.resolveColumn(ref.Table, ref.Name, context)
	if err != nil {
		return nil, err
	}
	return expr.NewColumnRef(binding.binding.Index, binding.binding.Column), nil
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
