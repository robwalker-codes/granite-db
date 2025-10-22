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

// ValidatedSelect is the semantic representation of a SELECT statement.
type ValidatedSelect struct {
	Sources    []*TableSource
	Joins      []*JoinClause
	Bindings   []ColumnBinding
	Outputs    []OutputColumn
	Filter     expr.TypedExpr
	Groupings  []Grouping
	Aggregates []AggregateDefinition
	Having     expr.TypedExpr
	OrderBy    []OrderingTerm
	Limit      *parser.LimitClause
}

// OrderingTerm captures a single ORDER BY expression.
type OrderingTerm struct {
	Expr expr.TypedExpr
	Desc bool
	Text string
}

// Grouping records a GROUP BY expression for downstream planning.
type Grouping struct {
	Expr expr.TypedExpr
	Text string
}

// AggregateFunction identifies supported aggregate kinds.
type AggregateFunction int

const (
	AggregateCountStar AggregateFunction = iota
	AggregateCount
	AggregateSum
	AggregateAvg
	AggregateMin
	AggregateMax
)

// AggregateDefinition describes an aggregate to compute during execution.
type AggregateDefinition struct {
	Func       AggregateFunction
	Name       string
	Arg        expr.TypedExpr
	ResultType expr.Type
	InputType  expr.Type
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

	groupingInfo, err := validator.buildGroupings(stmt.GroupBy)
	if err != nil {
		return nil, err
	}

	aggregated := len(groupingInfo.expressions) > 0 || containsAggregates(stmt.Items, stmt.Having, stmt.OrderBy)

	var (
		outputs    []OutputColumn
		aggregates []AggregateDefinition
		having     expr.TypedExpr
		orderBy    []OrderingTerm
	)

	if aggregated {
		builder := newAggregateBuilder(validator, groupingInfo)
		outputs, err = validator.buildAggregatedOutputs(stmt.Items, builder)
		if err != nil {
			return nil, err
		}
		if stmt.Having != nil {
			having, err = builder.buildExpression(stmt.Having, "HAVING clause")
			if err != nil {
				return nil, err
			}
			if having.ResultType().Kind != expr.TypeBoolean && having.ResultType().Kind != expr.TypeNull {
				return nil, fmt.Errorf("validator: HAVING clause must evaluate to BOOLEAN")
			}
		}
		orderBy, err = validator.buildAggregatedOrdering(stmt.OrderBy, builder, outputs)
		if err != nil {
			return nil, err
		}
		aggregates = builder.definitions()
	} else {
		if stmt.Having != nil {
			return nil, fmt.Errorf("validator: HAVING requires aggregates or GROUP BY")
		}
		outputs, err = validator.buildScalarOutputs(stmt.Items)
		if err != nil {
			return nil, err
		}
		orderBy, err = validator.buildScalarOrdering(stmt.OrderBy, outputs)
		if err != nil {
			return nil, err
		}
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

	return &ValidatedSelect{
		Sources:    validator.sources(),
		Joins:      joins,
		Bindings:   validator.bindings(),
		Outputs:    outputs,
		Filter:     filter,
		Groupings:  groupingInfo.toGroupings(),
		Aggregates: aggregates,
		Having:     having,
		OrderBy:    orderBy,
		Limit:      stmt.Limit,
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

func containsAggregates(items []parser.SelectItem, having parser.Expression, order []*parser.OrderByExpr) bool {
	for _, item := range items {
		if exprItem, ok := item.(*parser.SelectExprItem); ok {
			if expressionContainsAggregate(exprItem.Expr) {
				return true
			}
		}
	}
	if having != nil && expressionContainsAggregate(having) {
		return true
	}
	for _, clause := range order {
		if clause != nil && expressionContainsAggregate(clause.Expr) {
			return true
		}
	}
	return false
}

func expressionContainsAggregate(node parser.Expression) bool {
	switch e := node.(type) {
	case *parser.FunctionCallExpr:
		name := strings.ToUpper(e.Name)
		if isAggregateFunction(name) {
			return true
		}
		for _, arg := range e.Args {
			if expressionContainsAggregate(arg) {
				return true
			}
		}
		return false
	case *parser.UnaryExpr:
		return expressionContainsAggregate(e.Expr)
	case *parser.BinaryExpr:
		if expressionContainsAggregate(e.Left) {
			return true
		}
		return expressionContainsAggregate(e.Right)
	case *parser.IsNullExpr:
		return expressionContainsAggregate(e.Expr)
	default:
		return false
	}
}

type groupingInfo struct {
	expressions []expr.TypedExpr
	texts       []string
	indexByText map[string]int
	columnIndex map[int]int
}

func (g *groupingInfo) toGroupings() []Grouping {
	if g == nil || len(g.expressions) == 0 {
		return nil
	}
	result := make([]Grouping, len(g.expressions))
	for i, exp := range g.expressions {
		result[i] = Grouping{Expr: exp, Text: g.texts[i]}
	}
	return result
}

func (g *groupingInfo) lookupExpression(node parser.Expression) (int, bool) {
	if g == nil {
		return 0, false
	}
	if g.indexByText == nil {
		return 0, false
	}
	text := parser.FormatExpression(node)
	idx, ok := g.indexByText[text]
	return idx, ok
}

func (g *groupingInfo) groupIndexForColumn(idx int) (int, bool) {
	if g == nil {
		return 0, false
	}
	value, ok := g.columnIndex[idx]
	return value, ok
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

func (v *selectValidator) buildGroupings(nodes []parser.Expression) (*groupingInfo, error) {
	info := &groupingInfo{
		expressions: make([]expr.TypedExpr, 0, len(nodes)),
		texts:       make([]string, 0, len(nodes)),
		indexByText: make(map[string]int),
		columnIndex: make(map[int]int),
	}
	for i, node := range nodes {
		typed, err := v.buildExpression(node, "GROUP BY")
		if err != nil {
			return nil, err
		}
		info.expressions = append(info.expressions, typed)
		text := parser.FormatExpression(node)
		info.texts = append(info.texts, text)
		info.indexByText[text] = i
		if column, ok := typed.(*expr.ColumnRef); ok {
			info.columnIndex[column.Index] = i
		}
	}
	return info, nil
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

func (v *selectValidator) buildScalarOutputs(items []parser.SelectItem) ([]OutputColumn, error) {
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

func (v *selectValidator) buildAggregatedOutputs(items []parser.SelectItem, builder *aggregateBuilder) ([]OutputColumn, error) {
	if len(items) == 0 {
		return nil, fmt.Errorf("validator: SELECT list cannot be empty")
	}
	if len(items) == 1 {
		if _, ok := items[0].(*parser.SelectStarItem); ok {
			return nil, fmt.Errorf("validator: SELECT * is not supported with aggregates")
		}
	}

	outputs := make([]OutputColumn, 0, len(items))
	for _, item := range items {
		exprItem, ok := item.(*parser.SelectExprItem)
		if !ok {
			return nil, fmt.Errorf("validator: SELECT * cannot be combined with expressions (yet)")
		}
		typed, err := builder.buildExpression(exprItem.Expr, "SELECT list")
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

func (v *selectValidator) buildScalarOrdering(clauses []*parser.OrderByExpr, outputs []OutputColumn) ([]OrderingTerm, error) {
	if len(clauses) == 0 {
		return nil, nil
	}
	aliasMap := make(map[string]expr.TypedExpr, len(outputs))
	for _, out := range outputs {
		aliasMap[strings.ToLower(out.Name)] = out.Expr
	}
	terms := make([]OrderingTerm, 0, len(clauses))
	for _, clause := range clauses {
		text := parser.FormatExpression(clause.Expr)
		var termExpr expr.TypedExpr
		if col, ok := clause.Expr.(*parser.ColumnRef); ok && col.Table == "" {
			if aliasExpr, exists := aliasMap[strings.ToLower(col.Name)]; exists {
				termExpr = aliasExpr
			}
		}
		if termExpr == nil {
			var err error
			termExpr, err = v.buildExpression(clause.Expr, "ORDER BY")
			if err != nil {
				return nil, err
			}
		}
		terms = append(terms, OrderingTerm{Expr: termExpr, Desc: clause.Desc, Text: text})
	}
	return terms, nil
}

func (v *selectValidator) buildAggregatedOrdering(clauses []*parser.OrderByExpr, builder *aggregateBuilder, outputs []OutputColumn) ([]OrderingTerm, error) {
	if len(clauses) == 0 {
		return nil, nil
	}
	aliasMap := make(map[string]expr.TypedExpr, len(outputs))
	for _, out := range outputs {
		aliasMap[strings.ToLower(out.Name)] = out.Expr
	}
	terms := make([]OrderingTerm, 0, len(clauses))
	for _, clause := range clauses {
		text := parser.FormatExpression(clause.Expr)
		var termExpr expr.TypedExpr
		if col, ok := clause.Expr.(*parser.ColumnRef); ok && col.Table == "" {
			if aliasExpr, exists := aliasMap[strings.ToLower(col.Name)]; exists {
				termExpr = aliasExpr
			}
		}
		if termExpr == nil {
			var err error
			termExpr, err = builder.buildExpression(clause.Expr, "ORDER BY")
			if err != nil {
				return nil, err
			}
		}
		terms = append(terms, OrderingTerm{Expr: termExpr, Desc: clause.Desc, Text: text})
	}
	return terms, nil
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

type aggregateBuilder struct {
	validator      *selectValidator
	grouping       *groupingInfo
	aggregates     []AggregateDefinition
	aggregateIndex map[string]int
}

func newAggregateBuilder(v *selectValidator, grouping *groupingInfo) *aggregateBuilder {
	return &aggregateBuilder{
		validator:      v,
		grouping:       grouping,
		aggregateIndex: make(map[string]int),
	}
}

func (b *aggregateBuilder) definitions() []AggregateDefinition {
	defs := make([]AggregateDefinition, len(b.aggregates))
	copy(defs, b.aggregates)
	return defs
}

func (b *aggregateBuilder) buildExpression(node parser.Expression, context string) (expr.TypedExpr, error) {
	if idx, ok := b.grouping.lookupExpression(node); ok {
		typ := b.grouping.expressions[idx].ResultType()
		return expr.NewGroupRef(idx, typ), nil
	}
	switch e := node.(type) {
	case *parser.ColumnRef:
		return b.buildColumnRef(e, context)
	case *parser.LiteralExpr:
		return b.validator.buildLiteral(e.Literal)
	case *parser.UnaryExpr:
		operand, err := b.buildExpression(e.Expr, context)
		if err != nil {
			return nil, err
		}
		return b.validator.makeUnary(e.Op, operand)
	case *parser.BinaryExpr:
		left, err := b.buildExpression(e.Left, context)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpression(e.Right, context)
		if err != nil {
			return nil, err
		}
		return b.validator.makeBinary(e.Op, left, right)
	case *parser.FunctionCallExpr:
		name := strings.ToUpper(e.Name)
		if isAggregateFunction(name) {
			return b.registerAggregate(name, e, context)
		}
		if e.Distinct {
			return nil, fmt.Errorf("validator: DISTINCT is only supported in aggregate functions")
		}
		args := make([]expr.TypedExpr, len(e.Args))
		for i, arg := range e.Args {
			typed, err := b.buildExpression(arg, context)
			if err != nil {
				return nil, err
			}
			args[i] = typed
		}
		return b.validator.makeScalarFunction(name, args, context)
	case *parser.IsNullExpr:
		operand, err := b.buildExpression(e.Expr, context)
		if err != nil {
			return nil, err
		}
		return &expr.IsNullExpr{Expr: operand, Negated: e.Negated}, nil
	default:
		return nil, fmt.Errorf("validator: unsupported expression %T", node)
	}
}

func (b *aggregateBuilder) buildColumnRef(ref *parser.ColumnRef, context string) (expr.TypedExpr, error) {
	binding, err := b.validator.resolveColumn(ref.Table, ref.Name, context)
	if err != nil {
		return nil, err
	}
	idx, ok := b.grouping.groupIndexForColumn(binding.binding.Index)
	if !ok {
		return nil, fmt.Errorf("validator: column/expression not grouped: %s", parser.FormatExpression(ref))
	}
	typ := b.grouping.expressions[idx].ResultType()
	return expr.NewGroupRef(idx, typ), nil
}

func (b *aggregateBuilder) registerAggregate(name string, fn *parser.FunctionCallExpr, context string) (expr.TypedExpr, error) {
	if fn.Distinct {
		return nil, fmt.Errorf("validator: DISTINCT is not supported in aggregate functions")
	}
	keyArg := ""
	var arg expr.TypedExpr
	var err error
	if len(fn.Args) == 0 {
		return nil, fmt.Errorf("validator: %s expects an argument", name)
	}
	if len(fn.Args) > 1 {
		return nil, fmt.Errorf("validator: %s expects exactly 1 argument", name)
	}
	if _, isStar := fn.Args[0].(*parser.StarExpr); isStar {
		keyArg = "*"
		if name != "COUNT" {
			return nil, fmt.Errorf("validator: %s does not support * argument", name)
		}
	} else {
		arg, err = b.validator.buildExpression(fn.Args[0], context)
		if err != nil {
			return nil, err
		}
		keyArg = parser.FormatExpression(fn.Args[0])
	}

	signature := name + ":" + keyArg
	if idx, exists := b.aggregateIndex[signature]; exists {
		typ := b.aggregates[idx].ResultType
		offset := len(b.grouping.expressions)
		return expr.NewAggregateRef(offset+idx, typ), nil
	}

	def, err := b.createAggregateDefinition(name, fn, arg)
	if err != nil {
		return nil, err
	}

	idx := len(b.aggregates)
	b.aggregateIndex[signature] = idx
	b.aggregates = append(b.aggregates, def)
	offset := len(b.grouping.expressions)
	return expr.NewAggregateRef(offset+idx, def.ResultType), nil
}

func (b *aggregateBuilder) createAggregateDefinition(name string, call *parser.FunctionCallExpr, arg expr.TypedExpr) (AggregateDefinition, error) {
	upper := strings.ToUpper(name)
	definition := AggregateDefinition{Name: parser.FormatExpression(call)}
	switch upper {
	case "COUNT":
		if len(call.Args) == 1 {
			if _, isStar := call.Args[0].(*parser.StarExpr); isStar {
				definition.Func = AggregateCountStar
				definition.ResultType = expr.BigIntType(false)
				return definition, nil
			}
		}
		definition.Func = AggregateCount
		if arg != nil {
			definition.Arg = arg
			definition.InputType = arg.ResultType()
		}
		definition.ResultType = expr.BigIntType(false)
		return definition, nil
	case "SUM":
		if arg == nil {
			return AggregateDefinition{}, fmt.Errorf("validator: SUM expects an argument")
		}
		result, err := deriveSumType(arg.ResultType())
		if err != nil {
			return AggregateDefinition{}, err
		}
		definition.Func = AggregateSum
		definition.Arg = arg
		definition.InputType = arg.ResultType()
		definition.ResultType = result
		return definition, nil
	case "AVG":
		if arg == nil {
			return AggregateDefinition{}, fmt.Errorf("validator: AVG expects an argument")
		}
		result, err := deriveAvgType(arg.ResultType())
		if err != nil {
			return AggregateDefinition{}, err
		}
		definition.Func = AggregateAvg
		definition.Arg = arg
		definition.InputType = arg.ResultType()
		definition.ResultType = result
		return definition, nil
	case "MIN":
		if arg == nil {
			return AggregateDefinition{}, fmt.Errorf("validator: MIN expects an argument")
		}
		result, err := deriveMinMaxType(arg.ResultType(), "MIN")
		if err != nil {
			return AggregateDefinition{}, err
		}
		definition.Func = AggregateMin
		definition.Arg = arg
		definition.InputType = arg.ResultType()
		definition.ResultType = result
		return definition, nil
	case "MAX":
		if arg == nil {
			return AggregateDefinition{}, fmt.Errorf("validator: MAX expects an argument")
		}
		result, err := deriveMinMaxType(arg.ResultType(), "MAX")
		if err != nil {
			return AggregateDefinition{}, err
		}
		definition.Func = AggregateMax
		definition.Arg = arg
		definition.InputType = arg.ResultType()
		definition.ResultType = result
		return definition, nil
	default:
		return AggregateDefinition{}, fmt.Errorf("validator: unknown aggregate function %s", name)
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
	return v.makeUnary(node.Op, operand)
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
	return v.makeBinary(node.Op, left, right)
}

func (v *selectValidator) buildFunction(fn *parser.FunctionCallExpr, context string) (expr.TypedExpr, error) {
	name := strings.ToUpper(fn.Name)
	if isAggregateFunction(name) {
		return nil, fmt.Errorf("validator: aggregate function %s is not allowed in %s", name, context)
	}
	if fn.Distinct {
		return nil, fmt.Errorf("validator: DISTINCT is only supported in aggregate functions")
	}
	args := make([]expr.TypedExpr, len(fn.Args))
	for i, arg := range fn.Args {
		typed, err := v.buildExpression(arg, context)
		if err != nil {
			return nil, err
		}
		args[i] = typed
	}
	return v.makeScalarFunction(name, args, context)
}

func (v *selectValidator) makeUnary(op parser.UnaryOp, operand expr.TypedExpr) (expr.TypedExpr, error) {
	typ := operand.ResultType()
	switch op {
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
		return nil, fmt.Errorf("validator: unsupported unary operator %s", op)
	}
}

func (v *selectValidator) makeBinary(op parser.BinaryOp, left, right expr.TypedExpr) (expr.TypedExpr, error) {
	leftType := left.ResultType()
	rightType := right.ResultType()
	switch op {
	case parser.BinaryAdd, parser.BinarySubtract, parser.BinaryMultiply, parser.BinaryDivide, parser.BinaryModulo:
		resultType, err := promoteNumeric(leftType, rightType)
		if err != nil {
			return nil, err
		}
		if op == parser.BinaryDivide {
			if resultType.Kind != expr.TypeDecimal {
				scale := maxScale(leftType, rightType) + 6
				precision := scale + 18
				resultType = expr.DecimalType(resultType.Nullable, precision, scale)
			}
		}
		if op == parser.BinaryModulo && resultType.Kind == expr.TypeDecimal {
			return nil, fmt.Errorf("validator: modulo is only supported for integral types")
		}
		return expr.NewBinary(left, right, mapArithmeticOp(op), resultType), nil
	case parser.BinaryEqual, parser.BinaryNotEqual, parser.BinaryLess, parser.BinaryLessEqual, parser.BinaryGreater, parser.BinaryGreaterEqual:
		if err := ensureComparable(leftType, rightType); err != nil {
			return nil, err
		}
		nullable := leftType.Nullable || rightType.Nullable || leftType.Kind == expr.TypeNull || rightType.Kind == expr.TypeNull
		return expr.NewBinary(left, right, mapComparisonOp(op), expr.BooleanType(nullable)), nil
	case parser.BinaryAnd, parser.BinaryOr:
		if err := ensureBoolean(leftType); err != nil {
			return nil, err
		}
		if err := ensureBoolean(rightType); err != nil {
			return nil, err
		}
		mapped := mapBooleanOp(op)
		return expr.NewBinary(left, right, mapped, expr.BooleanType(true)), nil
	default:
		return nil, fmt.Errorf("validator: unsupported binary operator %s", op)
	}
}

func (v *selectValidator) makeScalarFunction(name string, args []expr.TypedExpr, context string) (expr.TypedExpr, error) {
	switch name {
	case "LOWER", "UPPER":
		if len(args) != 1 {
			return nil, fmt.Errorf("validator: %s expects exactly 1 argument", name)
		}
		argType := args[0].ResultType()
		if !argType.IsString() && argType.Kind != expr.TypeNull {
			return nil, fmt.Errorf("validator: function %s expects VARCHAR argument but received %s", name, describeType(argType))
		}
		return expr.NewFunction(name, args, expr.VarCharType(argType.Nullable || argType.Kind == expr.TypeNull, argType.Length)), nil
	case "LENGTH":
		if len(args) != 1 {
			return nil, fmt.Errorf("validator: LENGTH expects exactly 1 argument")
		}
		argType := args[0].ResultType()
		if !argType.IsString() && argType.Kind != expr.TypeNull {
			return nil, fmt.Errorf("validator: function LENGTH expects VARCHAR argument but received %s", describeType(argType))
		}
		return expr.NewFunction(name, args, expr.IntType(argType.Nullable || argType.Kind == expr.TypeNull)), nil
	case "COALESCE":
		if len(args) != 2 {
			return nil, fmt.Errorf("validator: COALESCE expects exactly 2 arguments")
		}
		resultType, err := coalesceType(args[0].ResultType(), args[1].ResultType())
		if err != nil {
			return nil, err
		}
		return expr.NewCoalesce(args[0], args[1], resultType), nil
	default:
		return nil, fmt.Errorf("validator: unknown function %s", name)
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

func deriveSumType(argType expr.Type) (expr.Type, error) {
	switch argType.Kind {
	case expr.TypeNull:
		return expr.DecimalType(true, 38, 0), nil
	case expr.TypeInt, expr.TypeBigInt:
		return expr.DecimalType(true, 38, 0), nil
	case expr.TypeDecimal:
		precision := argType.Precision
		if precision == 0 {
			precision = 18
		}
		return expr.DecimalType(true, precision+10, argType.Scale), nil
	default:
		return expr.Type{}, fmt.Errorf("validator: invalid aggregate argument type for SUM: %s", describeType(argType))
	}
}

func deriveAvgType(argType expr.Type) (expr.Type, error) {
	switch argType.Kind {
	case expr.TypeNull:
		return expr.DecimalType(true, 38, 6), nil
	case expr.TypeInt, expr.TypeBigInt:
		return expr.DecimalType(true, 38, 6), nil
	case expr.TypeDecimal:
		precision := argType.Precision
		if precision == 0 {
			precision = 18
		}
		return expr.DecimalType(true, precision+10, argType.Scale), nil
	default:
		return expr.Type{}, fmt.Errorf("validator: invalid aggregate argument type for AVG: %s", describeType(argType))
	}
}

func deriveMinMaxType(argType expr.Type, name string) (expr.Type, error) {
	switch argType.Kind {
	case expr.TypeNull:
		return expr.NullType(), nil
	case expr.TypeInt, expr.TypeBigInt, expr.TypeDecimal, expr.TypeVarChar, expr.TypeBoolean, expr.TypeDate, expr.TypeTimestamp:
		return argType.WithNullability(true), nil
	default:
		return expr.Type{}, fmt.Errorf("validator: invalid aggregate argument type for %s: %s", name, describeType(argType))
	}
}

func isAggregateFunction(name string) bool {
	switch name {
	case "COUNT", "SUM", "AVG", "MIN", "MAX":
		return true
	default:
		return false
	}
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
