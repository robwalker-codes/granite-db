package exec

import (
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"github.com/example/granite-db/engine/internal/catalog"
	"github.com/example/granite-db/engine/internal/sql/expr"
	"github.com/example/granite-db/engine/internal/sql/parser"
	"github.com/example/granite-db/engine/internal/sql/validator"
	"github.com/example/granite-db/engine/internal/storage"
)

// Result describes the outcome of executing a SQL statement.
type Result struct {
	Columns      []string
	Rows         [][]string
	RowsAffected int
	Message      string
}

// Executor evaluates parsed statements against the storage layer.
type Executor struct {
	catalog *catalog.Catalog
	storage *storage.Manager
}

// New creates an executor for the given catalog and storage manager.
func New(cat *catalog.Catalog, mgr *storage.Manager) *Executor {
	return &Executor{catalog: cat, storage: mgr}
}

// Execute runs the provided AST statement and returns a result summary.
func (e *Executor) Execute(stmt parser.Statement) (*Result, error) {
	switch s := stmt.(type) {
	case *parser.CreateTableStmt:
		return e.executeCreateTable(s)
	case *parser.DropTableStmt:
		return e.executeDropTable(s)
	case *parser.InsertStmt:
		return e.executeInsert(s)
	case *parser.SelectStmt:
		return e.executeSelect(s)
	default:
		return nil, fmt.Errorf("exec: unsupported statement type %T", stmt)
	}
}

// Explain builds a lightweight logical description of how the statement would
// execute. The implementation is deliberately simple but offers callers a
// stable JSON structure for tooling to consume.
func (e *Executor) Explain(stmt parser.Statement) (*Plan, error) {
	switch s := stmt.(type) {
	case *parser.CreateTableStmt:
		return newPlan("CreateTable", map[string]interface{}{"table": s.Name}), nil
	case *parser.DropTableStmt:
		return newPlan("DropTable", map[string]interface{}{"table": s.Name}), nil
	case *parser.InsertStmt:
		node := &PlanNode{
			Name:   "Insert",
			Detail: map[string]interface{}{"table": s.Table, "columns": s.Columns},
		}
		return &Plan{Root: node}, nil
	case *parser.SelectStmt:
		return e.explainSelect(s)
	default:
		return nil, fmt.Errorf("exec: unsupported statement type %T", stmt)
	}
}

func (e *Executor) executeCreateTable(stmt *parser.CreateTableStmt) (*Result, error) {
	if len(stmt.Columns) == 0 {
		return nil, fmt.Errorf("exec: CREATE TABLE requires at least one column")
	}
	cols := make([]catalog.Column, len(stmt.Columns))
	seen := map[string]struct{}{}
	for i, col := range stmt.Columns {
		if _, ok := seen[strings.ToLower(col.Name)]; ok {
			return nil, fmt.Errorf("exec: duplicate column %s", col.Name)
		}
		seen[strings.ToLower(col.Name)] = struct{}{}
		cols[i] = catalog.Column{
			Name:      col.Name,
			Type:      convertType(col.Type),
			Length:    col.Length,
			Precision: col.Precision,
			Scale:     col.Scale,
			NotNull:   col.NotNull,
		}
	}
	table, err := e.catalog.CreateTable(stmt.Name, cols, stmt.PrimaryKey)
	if err != nil {
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("Table %s created", table.Name)}, nil
}

func (e *Executor) executeDropTable(stmt *parser.DropTableStmt) (*Result, error) {
	if err := e.catalog.DropTable(stmt.Name); err != nil {
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("Table %s dropped", stmt.Name)}, nil
}

func (e *Executor) executeInsert(stmt *parser.InsertStmt) (*Result, error) {
	table, ok := e.catalog.GetTable(stmt.Table)
	if !ok {
		return nil, fmt.Errorf("exec: table %s not found", stmt.Table)
	}
	columnOrder := make([]int, len(table.Columns))
	if len(stmt.Columns) == 0 {
		for i := range table.Columns {
			columnOrder[i] = i
		}
	} else {
		provided := map[string]int{}
		for idx, name := range stmt.Columns {
			provided[strings.ToLower(name)] = idx
		}
		for i, col := range table.Columns {
			idx, ok := provided[strings.ToLower(col.Name)]
			if !ok {
				return nil, fmt.Errorf("exec: column %s missing from INSERT", col.Name)
			}
			columnOrder[i] = idx
		}
	}
	heap := storage.NewHeapFile(e.storage, table.RootPage)
	total := 0
	for _, row := range stmt.Rows {
		if len(row) != len(table.Columns) {
			return nil, fmt.Errorf("exec: column count %d does not match value count %d", len(table.Columns), len(row))
		}
		values := make([]interface{}, len(table.Columns))
		for i, col := range table.Columns {
			literal := row[columnOrder[i]]
			value, err := convertLiteral(literal, col)
			if err != nil {
				return nil, err
			}
			values[i] = value
		}
		encoded, err := EncodeRow(table.Columns, values)
		if err != nil {
			return nil, err
		}
		if err := heap.Insert(encoded); err != nil {
			return nil, err
		}
		if err := e.catalog.IncrementRowCount(table.Name); err != nil {
			return nil, err
		}
		total++
	}
	message := fmt.Sprintf("%d row(s) inserted", total)
	return &Result{RowsAffected: total, Message: message}, nil
}

func (e *Executor) executeSelect(stmt *parser.SelectStmt) (*Result, error) {
	validated, err := validator.ValidateSelect(e.catalog, stmt)
	if err != nil {
		return nil, err
	}

	evaluator := newValueEvaluator()
	rows, err := e.buildFromRows(validated, evaluator)
	if err != nil {
		return nil, err
	}

	rows, err = e.applyFilter(rows, validated.Filter, evaluator)
	if err != nil {
		return nil, err
	}

	rows, err = e.applyAggregation(rows, validated, evaluator)
	if err != nil {
		return nil, err
	}

	rows, err = e.applyHaving(rows, validated, evaluator)
	if err != nil {
		return nil, err
	}

	if err := e.applyOrdering(rows, validated.OrderBy, evaluator); err != nil {
		return nil, err
	}

	rows = applyLimit(rows, validated.Limit)

	projected, err := e.projectRows(rows, validated.Outputs, evaluator)
	if err != nil {
		return nil, err
	}

	columns := make([]string, len(validated.Outputs))
	for i, out := range validated.Outputs {
		columns[i] = out.Name
	}

	return &Result{Columns: columns, Rows: projected, RowsAffected: len(projected), Message: fmt.Sprintf("%d row(s)", len(projected))}, nil
}

func (e *Executor) buildFromRows(validated *validator.ValidatedSelect, evaluator *valueEvaluator) ([][]interface{}, error) {
	if len(validated.Sources) == 0 {
		return [][]interface{}{{nil}}, nil
	}

	leftRows, err := e.scanSourceRows(validated.Sources[0])
	if err != nil {
		return nil, err
	}

	for _, join := range validated.Joins {
		rightRows, err := e.scanSourceRows(join.Right)
		if err != nil {
			return nil, err
		}
		leftRows, err = e.executeJoin(leftRows, rightRows, join, evaluator)
		if err != nil {
			return nil, err
		}
	}

	return leftRows, nil
}

func (e *Executor) scanSourceRows(source *validator.TableSource) ([][]interface{}, error) {
	rows := make([][]interface{}, 0, source.Table.RowCount)
	heap := storage.NewHeapFile(e.storage, source.Table.RootPage)
	if err := heap.Scan(func(record []byte) error {
		values, err := DecodeRow(source.Table.Columns, record)
		if err != nil {
			return err
		}
		clone := make([]interface{}, len(values))
		copy(clone, values)
		rows = append(rows, clone)
		return nil
	}); err != nil {
		return nil, err
	}
	return rows, nil
}

func (e *Executor) executeJoin(leftRows, rightRows [][]interface{}, join *validator.JoinClause, evaluator *valueEvaluator) ([][]interface{}, error) {
	if len(join.EquiConditions) > 0 {
		return e.hashJoin(leftRows, rightRows, join, evaluator)
	}
	return e.nestedLoopJoin(leftRows, rightRows, join, evaluator)
}

// nestedLoopJoin performs a straightforward nested loop join.
//
//	+-------------------+
//	|   Left tuples     |
//	+-------------------+
//	      | for each
//	      v
//	+-------------------+
//	|  Right scan       |
//	+-------------------+
//	      | evaluate ON
//	      v
//	+-------------------+
//	|  Emit matches     |
//	+-------------------+
func (e *Executor) nestedLoopJoin(leftRows, rightRows [][]interface{}, join *validator.JoinClause, evaluator *valueEvaluator) ([][]interface{}, error) {
	result := make([][]interface{}, 0)
	rightWidth := join.Right.ColumnCount
	for _, left := range leftRows {
		matched := false
		if len(rightRows) == 0 {
			if join.Type == validator.JoinTypeLeft {
				result = append(result, appendNullRight(left, rightWidth))
			}
			continue
		}
		for _, right := range rightRows {
			combined := combineRows(left, right)
			ok, err := e.evaluateJoinCondition(combined, join.Condition, evaluator)
			if err != nil {
				return nil, err
			}
			if ok {
				result = append(result, combined)
				matched = true
			}
		}
		if join.Type == validator.JoinTypeLeft && !matched {
			result = append(result, appendNullRight(left, rightWidth))
		}
	}
	return result, nil
}

// hashJoin builds a hash table on the right input and probes it with the left side.
//
//	Build phase:
//	   right row --> hash --> bucket
//	Probe phase:
//	   left row  --> hash --> probe bucket --> residual filter --> emit
func (e *Executor) hashJoin(leftRows, rightRows [][]interface{}, join *validator.JoinClause, evaluator *valueEvaluator) ([][]interface{}, error) {
	hashTable := make(map[string][][]interface{}, len(rightRows))
	for _, row := range rightRows {
		key, ok := buildHashKey(row, join.EquiConditions, false)
		if !ok {
			continue
		}
		hashTable[key] = append(hashTable[key], row)
	}
	// TODO: implement fallback when the hash table grows beyond memory limits.

	result := make([][]interface{}, 0)
	rightWidth := join.Right.ColumnCount
	for _, left := range leftRows {
		key, ok := buildHashKey(left, join.EquiConditions, true)
		matched := false
		if ok {
			for _, right := range hashTable[key] {
				combined := combineRows(left, right)
				keep, err := e.joinResidualSatisfied(combined, join.Residuals, evaluator)
				if err != nil {
					return nil, err
				}
				if keep {
					result = append(result, combined)
					matched = true
				}
			}
		}
		if join.Type == validator.JoinTypeLeft && !matched {
			result = append(result, appendNullRight(left, rightWidth))
		}
	}
	return result, nil
}

func buildHashKey(row []interface{}, conditions []validator.EquiCondition, leftSide bool) (string, bool) {
	var builder strings.Builder
	for _, cond := range conditions {
		var value interface{}
		if leftSide {
			value = row[cond.LeftOffset]
		} else {
			value = row[cond.RightOffset]
		}
		if value == nil {
			return "", false
		}
		builder.WriteString(fmt.Sprintf("%T:%v|", value, value))
	}
	return builder.String(), true
}

func (e *Executor) joinResidualSatisfied(row []interface{}, residuals []expr.TypedExpr, evaluator *valueEvaluator) (bool, error) {
	if len(residuals) == 0 {
		return true, nil
	}
	evaluator.setRow(row)
	for _, residual := range residuals {
		value, err := evaluator.eval(residual)
		if err != nil {
			return false, err
		}
		truth, err := toTruthValue(value)
		if err != nil {
			return false, err
		}
		if truth != truthTrue {
			return false, nil
		}
	}
	return true, nil
}

func (e *Executor) evaluateJoinCondition(row []interface{}, condition expr.TypedExpr, evaluator *valueEvaluator) (bool, error) {
	if condition == nil {
		return true, nil
	}
	evaluator.setRow(row)
	value, err := evaluator.eval(condition)
	if err != nil {
		return false, err
	}
	truth, err := toTruthValue(value)
	if err != nil {
		return false, err
	}
	return truth == truthTrue, nil
}

func combineRows(left, right []interface{}) []interface{} {
	combined := make([]interface{}, len(left)+len(right))
	copy(combined, left)
	copy(combined[len(left):], right)
	return combined
}

func appendNullRight(left []interface{}, rightWidth int) []interface{} {
	combined := make([]interface{}, len(left)+rightWidth)
	copy(combined, left)
	for i := len(left); i < len(combined); i++ {
		combined[i] = nil
	}
	return combined
}

func (e *Executor) applyFilter(rows [][]interface{}, filter expr.TypedExpr, evaluator *valueEvaluator) ([][]interface{}, error) {
	if filter == nil {
		return rows, nil
	}
	filtered := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		evaluator.setRow(row)
		value, err := evaluator.eval(filter)
		if err != nil {
			return nil, err
		}
		truth, err := toTruthValue(value)
		if err != nil {
			return nil, err
		}
		if truth == truthTrue {
			filtered = append(filtered, row)
		}
	}
	return filtered, nil
}

func (e *Executor) applyAggregation(rows [][]interface{}, validated *validator.ValidatedSelect, evaluator *valueEvaluator) ([][]interface{}, error) {
	groupCount := len(validated.Groupings)
	aggCount := len(validated.Aggregates)
	if groupCount == 0 && aggCount == 0 {
		return rows, nil
	}

	type aggregateGroup struct {
		values []interface{}
		states []*aggregateAccumulator
	}

	makeGroup := func(values []interface{}) *aggregateGroup {
		states := make([]*aggregateAccumulator, aggCount)
		for i, def := range validated.Aggregates {
			states[i] = newAggregateAccumulator(def)
		}
		return &aggregateGroup{values: values, states: states}
	}

	groups := make(map[string]*aggregateGroup)
	order := make([]string, 0)
	var globalKey string
	if groupCount == 0 {
		globalKey = "__global__"
		groups[globalKey] = makeGroup(nil)
		order = append(order, globalKey)
	}

	for _, row := range rows {
		var key string
		var current *aggregateGroup
		if groupCount == 0 {
			key = globalKey
			current = groups[key]
		} else {
			evaluator.setRow(row)
			values := make([]interface{}, groupCount)
			var builder strings.Builder
			for idx, grouping := range validated.Groupings {
				if idx > 0 {
					builder.WriteString("|")
				}
				value, err := evaluator.eval(grouping.Expr)
				if err != nil {
					return nil, err
				}
				if value.isNull() {
					values[idx] = nil
					builder.WriteString("NULL")
					continue
				}
				values[idx] = value.data
				builder.WriteString(fmt.Sprintf("%T:%v", value.data, value.data))
			}
			key = builder.String()
			var exists bool
			current, exists = groups[key]
			if !exists {
				stored := make([]interface{}, len(values))
				copy(stored, values)
				current = makeGroup(stored)
				groups[key] = current
				order = append(order, key)
			}
		}

		if aggCount == 0 {
			continue
		}

		evaluator.setRow(row)
		for _, state := range current.states {
			if err := state.update(evaluator); err != nil {
				return nil, err
			}
		}
	}

	result := make([][]interface{}, 0, len(order))
	for _, key := range order {
		group := groups[key]
		row := make([]interface{}, groupCount+aggCount)
		if groupCount > 0 && len(group.values) > 0 {
			copy(row, group.values)
		}
		for idx, state := range group.states {
			value, err := state.finalise()
			if err != nil {
				return nil, err
			}
			row[groupCount+idx] = value
		}
		result = append(result, row)
	}
	return result, nil
}

func (e *Executor) applyHaving(rows [][]interface{}, validated *validator.ValidatedSelect, evaluator *valueEvaluator) ([][]interface{}, error) {
	if validated.Having == nil {
		return rows, nil
	}
	filtered := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		evaluator.setRow(row)
		value, err := evaluator.eval(validated.Having)
		if err != nil {
			return nil, err
		}
		truth, err := toTruthValue(value)
		if err != nil {
			return nil, err
		}
		if truth == truthTrue {
			filtered = append(filtered, row)
		}
	}
	return filtered, nil
}

func (e *Executor) applyOrdering(rows [][]interface{}, terms []validator.OrderingTerm, evaluator *valueEvaluator) error {
	if len(terms) == 0 {
		return nil
	}
	var sortErr error
	sort.SliceStable(rows, func(i, j int) bool {
		if sortErr != nil {
			return false
		}
		left := rows[i]
		right := rows[j]
		for _, term := range terms {
			evaluator.setRow(left)
			leftVal, err := evaluator.eval(term.Expr)
			if err != nil {
				sortErr = err
				return false
			}
			evaluator.setRow(right)
			rightVal, err := evaluator.eval(term.Expr)
			if err != nil {
				sortErr = err
				return false
			}
			if leftVal.isNull() && rightVal.isNull() {
				continue
			}
			if leftVal.isNull() {
				return false
			}
			if rightVal.isNull() {
				return true
			}
			cmp, err := compareNonNullValues(leftVal, rightVal)
			if err != nil {
				sortErr = err
				return false
			}
			if cmp == 0 {
				continue
			}
			if term.Desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
	if sortErr != nil {
		return sortErr
	}
	return nil
}

type aggregateAccumulator struct {
	def    validator.AggregateDefinition
	count  int64
	sum    decimal.Decimal
	sumSet bool
	min    typedValue
	minSet bool
	max    typedValue
	maxSet bool
}

func newAggregateAccumulator(def validator.AggregateDefinition) *aggregateAccumulator {
	return &aggregateAccumulator{def: def}
}

func (a *aggregateAccumulator) update(evaluator *valueEvaluator) error {
	switch a.def.Func {
	case validator.AggregateCountStar:
		a.count++
		return nil
	case validator.AggregateCount:
		value, err := evaluator.eval(a.def.Arg)
		if err != nil {
			return err
		}
		if !value.isNull() {
			a.count++
		}
		return nil
	case validator.AggregateSum:
		value, err := evaluator.eval(a.def.Arg)
		if err != nil {
			return err
		}
		if value.isNull() {
			return nil
		}
		dec, err := toDecimal(value)
		if err != nil {
			return err
		}
		if !a.sumSet {
			a.sum = dec
			a.sumSet = true
		} else {
			a.sum = a.sum.Add(dec)
		}
		return nil
	case validator.AggregateAvg:
		value, err := evaluator.eval(a.def.Arg)
		if err != nil {
			return err
		}
		if value.isNull() {
			return nil
		}
		dec, err := toDecimal(value)
		if err != nil {
			return err
		}
		if !a.sumSet {
			a.sum = dec
			a.sumSet = true
		} else {
			a.sum = a.sum.Add(dec)
		}
		a.count++
		return nil
	case validator.AggregateMin:
		value, err := evaluator.eval(a.def.Arg)
		if err != nil {
			return err
		}
		if value.isNull() {
			return nil
		}
		if !a.minSet {
			a.min = value
			a.minSet = true
			return nil
		}
		cmp, err := compareNonNullValues(value, a.min)
		if err != nil {
			return err
		}
		if cmp < 0 {
			a.min = value
		}
		return nil
	case validator.AggregateMax:
		value, err := evaluator.eval(a.def.Arg)
		if err != nil {
			return err
		}
		if value.isNull() {
			return nil
		}
		if !a.maxSet {
			a.max = value
			a.maxSet = true
			return nil
		}
		cmp, err := compareNonNullValues(value, a.max)
		if err != nil {
			return err
		}
		if cmp > 0 {
			a.max = value
		}
		return nil
	default:
		return fmt.Errorf("exec: unsupported aggregate function")
	}
}

func (a *aggregateAccumulator) finalise() (interface{}, error) {
	switch a.def.Func {
	case validator.AggregateCountStar, validator.AggregateCount:
		return a.count, nil
	case validator.AggregateSum:
		if !a.sumSet {
			return nil, nil
		}
		if a.def.ResultType.Kind == expr.TypeDecimal {
			return a.sum.Round(int32(a.def.ResultType.Scale)), nil
		}
		return a.sum, nil
	case validator.AggregateAvg:
		if !a.sumSet || a.count == 0 {
			return nil, nil
		}
		avg := a.sum.Div(decimal.NewFromInt(a.count))
		if a.def.ResultType.Kind == expr.TypeDecimal {
			avg = avg.Round(int32(a.def.ResultType.Scale))
		}
		return avg, nil
	case validator.AggregateMin:
		if !a.minSet {
			return nil, nil
		}
		return a.min.data, nil
	case validator.AggregateMax:
		if !a.maxSet {
			return nil, nil
		}
		return a.max.data, nil
	default:
		return nil, fmt.Errorf("exec: unsupported aggregate function")
	}
}

func compareNonNullValues(left, right typedValue) (int, error) {
	switch left.typ.Kind {
	case expr.TypeInt, expr.TypeBigInt:
		li, err := toInt64(left)
		if err != nil {
			return 0, err
		}
		ri, err := toInt64(right)
		if err != nil {
			return 0, err
		}
		switch {
		case li < ri:
			return -1, nil
		case li > ri:
			return 1, nil
		default:
			return 0, nil
		}
	case expr.TypeDecimal:
		ld, err := toDecimal(left)
		if err != nil {
			return 0, err
		}
		rd, err := toDecimal(right)
		if err != nil {
			return 0, err
		}
		return ld.Cmp(rd), nil
	case expr.TypeVarChar:
		ls := left.data.(string)
		rs := right.data.(string)
		return strings.Compare(ls, rs), nil
	case expr.TypeBoolean:
		lb := left.data.(bool)
		rb := right.data.(bool)
		switch {
		case lb == rb:
			return 0, nil
		case !lb && rb:
			return -1, nil
		default:
			return 1, nil
		}
	case expr.TypeDate, expr.TypeTimestamp:
		lt := left.data.(time.Time)
		rt := right.data.(time.Time)
		switch {
		case lt.Before(rt):
			return -1, nil
		case lt.After(rt):
			return 1, nil
		default:
			return 0, nil
		}
	default:
		return 0, fmt.Errorf("exec: unsupported comparison for type %v", left.typ.Kind)
	}
}

func applyLimit(rows [][]interface{}, clause *parser.LimitClause) [][]interface{} {
	if clause == nil {
		return rows
	}
	offset := clause.Offset
	if offset < 0 {
		offset = 0
	}
	if offset >= len(rows) {
		return [][]interface{}{}
	}
	rows = rows[offset:]
	if clause.Limit >= 0 && clause.Limit < len(rows) {
		rows = rows[:clause.Limit]
	}
	return rows
}

func (e *Executor) projectRows(rows [][]interface{}, outputs []validator.OutputColumn, evaluator *valueEvaluator) ([][]string, error) {
	projected := make([][]string, len(rows))
	for i, row := range rows {
		evaluator.setRow(row)
		display := make([]string, len(outputs))
		for j, out := range outputs {
			value, err := evaluator.eval(out.Expr)
			if err != nil {
				return nil, err
			}
			formatted, err := formatTypedValue(value)
			if err != nil {
				return nil, err
			}
			display[j] = formatted
		}
		projected[i] = display
	}
	return projected, nil
}

func (e *Executor) explainSelect(stmt *parser.SelectStmt) (*Plan, error) {
	validated, err := validator.ValidateSelect(e.catalog, stmt)
	if err != nil {
		return nil, err
	}

	columnNames := make([]string, len(validated.Outputs))
	for i, out := range validated.Outputs {
		columnNames[i] = out.Name
	}

	project := &PlanNode{Name: "Project", Detail: map[string]interface{}{"columns": columnNames}}
	current := project

	if validated.Limit != nil {
		limitNode := &PlanNode{Name: "Limit", Detail: map[string]interface{}{"limit": validated.Limit.Limit, "offset": validated.Limit.Offset}}
		current.Children = append(current.Children, limitNode)
		current = limitNode
	}
	if len(validated.OrderBy) > 0 {
		terms := make([]map[string]interface{}, len(validated.OrderBy))
		for i, term := range validated.OrderBy {
			terms[i] = map[string]interface{}{"expr": term.Text, "desc": term.Desc}
		}
		orderNode := &PlanNode{Name: "OrderBy", Detail: map[string]interface{}{"terms": terms}}
		current.Children = append(current.Children, orderNode)
		current = orderNode
	}
	if validated.Having != nil {
		havingNode := &PlanNode{Name: "Having"}
		current.Children = append(current.Children, havingNode)
		current = havingNode
	}
	if len(validated.Groupings) > 0 || len(validated.Aggregates) > 0 {
		detail := make(map[string]interface{})
		if len(validated.Groupings) > 0 {
			groups := make([]string, len(validated.Groupings))
			for i, grouping := range validated.Groupings {
				groups[i] = grouping.Text
			}
			detail["groups"] = groups
		}
		if len(validated.Aggregates) > 0 {
			aggs := make([]string, len(validated.Aggregates))
			for i, agg := range validated.Aggregates {
				aggs[i] = agg.Name
			}
			detail["aggregates"] = aggs
		}
		aggregateNode := &PlanNode{Name: "Aggregate"}
		if len(detail) > 0 {
			aggregateNode.Detail = detail
		}
		current.Children = append(current.Children, aggregateNode)
		current = aggregateNode
	}
	if validated.Filter != nil {
		filterNode := &PlanNode{Name: "Filter"}
		current.Children = append(current.Children, filterNode)
		current = filterNode
	}
	join := buildJoinPlan(validated)
	current.Children = append(current.Children, join)

	return &Plan{Root: project}, nil
}

func buildJoinPlan(validated *validator.ValidatedSelect) *PlanNode {
	if len(validated.Sources) == 0 {
		return &PlanNode{Name: "Const"}
	}
	left := planForSource(validated.Sources[0])
	for _, join := range validated.Joins {
		algorithm := "NestedLoopJoin"
		if len(join.EquiConditions) > 0 {
			algorithm = "HashJoin"
		}
		detail := map[string]interface{}{"type": joinTypeString(join.Type)}
		if len(join.EquiConditions) > 0 {
			keys := make([]string, len(join.EquiConditions))
			for i, cond := range join.EquiConditions {
				leftBinding := validated.Bindings[cond.LeftColumn]
				rightBinding := validated.Bindings[cond.RightColumn]
				keys[i] = fmt.Sprintf("%s.%s = %s.%s", leftBinding.TableAlias, leftBinding.Column.Name, rightBinding.TableAlias, rightBinding.Column.Name)
			}
			detail["keys"] = keys
		}
		if len(join.Residuals) > 0 {
			detail["residuals"] = len(join.Residuals)
		}
		joinNode := &PlanNode{Name: algorithm, Detail: detail}
		joinNode.Children = append(joinNode.Children, left)
		joinNode.Children = append(joinNode.Children, planForSource(join.Right))
		left = joinNode
	}
	return left
}

func planForSource(source *validator.TableSource) *PlanNode {
	detail := map[string]interface{}{"table": source.Table.Name}
	if !strings.EqualFold(source.Alias, source.Table.Name) {
		detail["alias"] = source.Alias
	}
	return &PlanNode{Name: "SeqScan", Detail: detail}
}

func joinTypeString(joinType validator.JoinType) string {
	switch joinType {
	case validator.JoinTypeLeft:
		return "LEFT"
	default:
		return "INNER"
	}
}

func convertType(dt parser.DataType) catalog.ColumnType {
	switch dt {
	case parser.DataTypeInt:
		return catalog.ColumnTypeInt
	case parser.DataTypeBigInt:
		return catalog.ColumnTypeBigInt
	case parser.DataTypeDecimal:
		return catalog.ColumnTypeDecimal
	case parser.DataTypeVarChar:
		return catalog.ColumnTypeVarChar
	case parser.DataTypeBoolean:
		return catalog.ColumnTypeBoolean
	case parser.DataTypeDate:
		return catalog.ColumnTypeDate
	case parser.DataTypeTimestamp:
		return catalog.ColumnTypeTimestamp
	default:
		return catalog.ColumnTypeVarChar
	}
}

func convertLiteral(lit parser.Literal, col catalog.Column) (interface{}, error) {
	if lit.Kind == parser.LiteralNull {
		if col.NotNull {
			return nil, fmt.Errorf("exec: column %s does not allow NULL", col.Name)
		}
		return nil, nil
	}
	switch col.Type {
	case catalog.ColumnTypeInt:
		if lit.Kind != parser.LiteralNumber {
			return nil, fmt.Errorf("exec: expected numeric literal for %s", col.Name)
		}
		value, err := strconv.Atoi(lit.Value)
		if err != nil {
			return nil, fmt.Errorf("exec: invalid INT literal %s", lit.Value)
		}
		return int32(value), nil
	case catalog.ColumnTypeBigInt:
		if lit.Kind != parser.LiteralNumber {
			return nil, fmt.Errorf("exec: expected numeric literal for %s", col.Name)
		}
		value, err := strconv.ParseInt(lit.Value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("exec: invalid BIGINT literal %s", lit.Value)
		}
		return value, nil
	case catalog.ColumnTypeBoolean:
		if lit.Kind == parser.LiteralBoolean {
			return strings.ToUpper(lit.Value) == "TRUE", nil
		}
		return nil, fmt.Errorf("exec: expected boolean literal for %s", col.Name)
	case catalog.ColumnTypeVarChar:
		if lit.Kind != parser.LiteralString {
			return nil, fmt.Errorf("exec: expected string literal for %s", col.Name)
		}
		return lit.Value, nil
	case catalog.ColumnTypeDate:
		if lit.Kind != parser.LiteralString {
			return nil, fmt.Errorf("exec: expected string literal for %s", col.Name)
		}
		t, err := time.Parse("2006-01-02", lit.Value)
		if err != nil {
			return nil, fmt.Errorf("exec: invalid DATE literal %s", lit.Value)
		}
		return t, nil
	case catalog.ColumnTypeTimestamp:
		if lit.Kind != parser.LiteralString {
			return nil, fmt.Errorf("exec: expected string literal for %s", col.Name)
		}
		layouts := []string{time.RFC3339, "2006-01-02 15:04:05"}
		var parsed time.Time
		var err error
		for _, layout := range layouts {
			parsed, err = time.Parse(layout, lit.Value)
			if err == nil {
				return parsed, nil
			}
		}
		return nil, fmt.Errorf("exec: invalid TIMESTAMP literal %s", lit.Value)
	case catalog.ColumnTypeDecimal:
		raw := ""
		switch lit.Kind {
		case parser.LiteralNumber, parser.LiteralDecimal, parser.LiteralString:
			raw = lit.Value
		default:
			return nil, fmt.Errorf("exec: expected decimal literal for %s", col.Name)
		}
		decValue, err := decimal.NewFromString(raw)
		if err != nil {
			return nil, fmt.Errorf("exec: invalid DECIMAL literal %s", raw)
		}
		normalised, err := validateDecimalValue(decValue, col)
		if err != nil {
			return nil, err
		}
		return normalised, nil
	default:
		return nil, fmt.Errorf("exec: unsupported column type for %s", col.Name)
	}
}

func formatTypedValue(value typedValue) (string, error) {
	if value.isNull() {
		return "NULL", nil
	}
	switch value.typ.Kind {
	case expr.TypeInt:
		switch v := value.data.(type) {
		case int32:
			return fmt.Sprintf("%d", v), nil
		case int64:
			return fmt.Sprintf("%d", v), nil
		default:
			return fmt.Sprintf("%v", value.data), nil
		}
	case expr.TypeBigInt:
		return fmt.Sprintf("%d", value.data.(int64)), nil
	case expr.TypeDecimal:
		dec, ok := value.data.(decimal.Decimal)
		if !ok {
			return fmt.Sprintf("%v", value.data), nil
		}
		if value.typ.Scale > 0 {
			return dec.StringFixed(int32(value.typ.Scale)), nil
		}
		return dec.String(), nil
	case expr.TypeVarChar:
		return value.data.(string), nil
	case expr.TypeBoolean:
		if value.data.(bool) {
			return "TRUE", nil
		}
		return "FALSE", nil
	case expr.TypeDate:
		return value.data.(time.Time).Format("2006-01-02"), nil
	case expr.TypeTimestamp:
		return value.data.(time.Time).Format(time.RFC3339), nil
	case expr.TypeNull:
		return "NULL", nil
	default:
		return fmt.Sprintf("%v", value.data), nil
	}
}

func compareColumn(column catalog.Column, left, right interface{}) int {
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return -1
	}
	if right == nil {
		return 1
	}
	switch column.Type {
	case catalog.ColumnTypeInt:
		l := left.(int32)
		r := right.(int32)
		switch {
		case l < r:
			return -1
		case l > r:
			return 1
		default:
			return 0
		}
	case catalog.ColumnTypeBigInt:
		l := left.(int64)
		r := right.(int64)
		switch {
		case l < r:
			return -1
		case l > r:
			return 1
		default:
			return 0
		}
	case catalog.ColumnTypeBoolean:
		l := 0
		if left.(bool) {
			l = 1
		}
		r := 0
		if right.(bool) {
			r = 1
		}
		switch {
		case l < r:
			return -1
		case l > r:
			return 1
		default:
			return 0
		}
	case catalog.ColumnTypeVarChar:
		return strings.Compare(left.(string), right.(string))
	case catalog.ColumnTypeDate, catalog.ColumnTypeTimestamp:
		lt := left.(time.Time)
		rt := right.(time.Time)
		switch {
		case lt.Before(rt):
			return -1
		case lt.After(rt):
			return 1
		default:
			return 0
		}
	case catalog.ColumnTypeDecimal:
		ld := left.(decimal.Decimal)
		rd := right.(decimal.Decimal)
		return ld.Cmp(rd)
	default:
		return 0
	}
}

func validateDecimalValue(value decimal.Decimal, col catalog.Column) (decimal.Decimal, error) {
	if col.Precision <= 0 {
		return decimal.Decimal{}, fmt.Errorf("exec: column %s has invalid DECIMAL definition", col.Name)
	}
	if col.Scale < 0 || col.Scale > col.Precision {
		return decimal.Decimal{}, fmt.Errorf("exec: column %s has invalid DECIMAL definition", col.Name)
	}
	actualScale := decimalScale(value)
	if actualScale > col.Scale {
		return decimal.Decimal{}, fmt.Errorf("exec: value %s exceeds DECIMAL(%d,%d) scale for column %s", value.String(), col.Precision, col.Scale, col.Name)
	}
	integerDigits := decimalIntegerDigits(value)
	maxInteger := col.Precision - col.Scale
	if integerDigits > maxInteger {
		return decimal.Decimal{}, fmt.Errorf("exec: value %s exceeds DECIMAL(%d,%d) precision for column %s", value.String(), col.Precision, col.Scale, col.Name)
	}
	digits := decimalDigitCount(value)
	if digits > col.Precision {
		return decimal.Decimal{}, fmt.Errorf("exec: value %s exceeds DECIMAL(%d,%d) precision for column %s", value.String(), col.Precision, col.Scale, col.Name)
	}
	return value, nil
}

func decimalScale(value decimal.Decimal) int {
	exp := value.Exponent()
	if exp >= 0 {
		return 0
	}
	return int(-exp)
}

func decimalDigitCount(value decimal.Decimal) int {
	coeff := value.Coefficient()
	if coeff.Sign() == 0 {
		return 0
	}
	abs := new(big.Int).Abs(coeff)
	return len(abs.String())
}

func decimalIntegerDigits(value decimal.Decimal) int {
	coeff := value.Coefficient()
	if coeff.Sign() == 0 {
		return 0
	}
	abs := new(big.Int).Abs(coeff)
	digits := len(abs.String())
	scale := decimalScale(value)
	if digits <= scale {
		return 0
	}
	return digits - scale
}
