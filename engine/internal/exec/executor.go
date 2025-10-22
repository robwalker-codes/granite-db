package exec

import (
	"fmt"
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
			Name:    col.Name,
			Type:    convertType(col.Type),
			Length:  col.Length,
			NotNull: col.NotNull,
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
	var table *catalog.Table
	if stmt.HasTable {
		var ok bool
		table, ok = e.catalog.GetTable(stmt.Table)
		if !ok {
			return nil, fmt.Errorf("exec: table %s not found", stmt.Table)
		}
	}
	validated, err := validator.ValidateSelect(table, stmt)
	if err != nil {
		return nil, err
	}

	baseRows := make([][]interface{}, 0)
	evaluator := newValueEvaluator()
	if table != nil {
		heap := storage.NewHeapFile(e.storage, table.RootPage)
		if err := heap.Scan(func(record []byte) error {
			values, err := DecodeRow(table.Columns, record)
			if err != nil {
				return err
			}
			evaluator.setRow(values)
			if validated.Filter != nil {
				filter, err := evaluator.eval(validated.Filter)
				if err != nil {
					return err
				}
				truth, err := toTruthValue(filter)
				if err != nil {
					return err
				}
				if truth != truthTrue {
					return nil
				}
			}
			clone := make([]interface{}, len(values))
			copy(clone, values)
			baseRows = append(baseRows, clone)
			return nil
		}); err != nil {
			return nil, err
		}
	} else {
		evaluator.setRow(nil)
		include := true
		if validated.Filter != nil {
			filter, err := evaluator.eval(validated.Filter)
			if err != nil {
				return nil, err
			}
			truth, err := toTruthValue(filter)
			if err != nil {
				return nil, err
			}
			include = truth == truthTrue
		}
		if include {
			baseRows = append(baseRows, nil)
		}
	}

	if validated.OrderBy != nil && table != nil {
		idx := validated.OrderBy.ColumnIndex
		column := table.Columns[idx]
		sort.SliceStable(baseRows, func(i, j int) bool {
			left := baseRows[i][idx]
			right := baseRows[j][idx]
			cmp := compareColumn(column, left, right)
			if validated.OrderBy.Desc {
				return cmp > 0
			}
			return cmp < 0
		})
	}

	if validated.Limit != nil {
		offset := validated.Limit.Offset
		if offset < 0 {
			offset = 0
		}
		if offset >= len(baseRows) {
			baseRows = [][]interface{}{}
		} else {
			baseRows = baseRows[offset:]
			if validated.Limit.Limit < len(baseRows) {
				baseRows = baseRows[:validated.Limit.Limit]
			}
		}
	}

	projector := newValueEvaluator()
	rows := make([][]string, len(baseRows))
	for i, values := range baseRows {
		projector.setRow(values)
		display := make([]string, len(validated.Outputs))
		for j, out := range validated.Outputs {
			value, err := projector.eval(out.Expr)
			if err != nil {
				return nil, err
			}
			formatted, err := formatTypedValue(value)
			if err != nil {
				return nil, err
			}
			display[j] = formatted
		}
		rows[i] = display
	}

	columns := make([]string, len(validated.Outputs))
	for i, out := range validated.Outputs {
		columns[i] = out.Name
	}

	return &Result{Columns: columns, Rows: rows, RowsAffected: len(rows), Message: fmt.Sprintf("%d row(s)", len(rows))}, nil
}

func (e *Executor) explainSelect(stmt *parser.SelectStmt) (*Plan, error) {
	var table *catalog.Table
	if stmt.HasTable {
		var ok bool
		table, ok = e.catalog.GetTable(stmt.Table)
		if !ok {
			return nil, fmt.Errorf("exec: table %s not found", stmt.Table)
		}
	}
	validated, err := validator.ValidateSelect(table, stmt)
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
	if validated.OrderBy != nil && table != nil {
		orderNode := &PlanNode{Name: "OrderBy", Detail: map[string]interface{}{"column": table.Columns[validated.OrderBy.ColumnIndex].Name, "desc": validated.OrderBy.Desc}}
		current.Children = append(current.Children, orderNode)
		current = orderNode
	}
	if validated.Filter != nil {
		filterNode := &PlanNode{Name: "Filter"}
		current.Children = append(current.Children, filterNode)
		current = filterNode
	}
	if table != nil {
		scan := &PlanNode{Name: "SeqScan", Detail: map[string]interface{}{"table": table.Name}}
		current.Children = append(current.Children, scan)
	} else {
		current.Children = append(current.Children, &PlanNode{Name: "Const"})
	}

	return &Plan{Root: project}, nil
}

func convertType(dt parser.DataType) catalog.ColumnType {
	switch dt {
	case parser.DataTypeInt:
		return catalog.ColumnTypeInt
	case parser.DataTypeBigInt:
		return catalog.ColumnTypeBigInt
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
	default:
		return 0
	}
}
