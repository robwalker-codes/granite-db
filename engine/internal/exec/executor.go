package exec

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/example/granite-db/engine/internal/catalog"
	"github.com/example/granite-db/engine/internal/sql/parser"
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
	values := make([]interface{}, len(table.Columns))
	for i, col := range table.Columns {
		literal := stmt.Values[columnOrder[i]]
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
	heap := storage.NewHeapFile(e.storage, table.RootPage)
	if err := heap.Insert(encoded); err != nil {
		return nil, err
	}
	if err := e.catalog.IncrementRowCount(table.Name); err != nil {
		return nil, err
	}
	return &Result{RowsAffected: 1, Message: "1 row inserted"}, nil
}

func (e *Executor) executeSelect(stmt *parser.SelectStmt) (*Result, error) {
	table, ok := e.catalog.GetTable(stmt.Table)
	if !ok {
		return nil, fmt.Errorf("exec: table %s not found", stmt.Table)
	}
	heap := storage.NewHeapFile(e.storage, table.RootPage)
	ctx := newEvalContext(table.Columns)
	valuesRows := make([][]interface{}, 0)
	if err := heap.Scan(func(record []byte) error {
		values, err := DecodeRow(table.Columns, record)
		if err != nil {
			return err
		}
		ctx.setValues(values)
		truth, err := ctx.eval(stmt.Where)
		if err != nil {
			return err
		}
		if truth != truthTrue {
			return nil
		}
		clone := make([]interface{}, len(values))
		copy(clone, values)
		valuesRows = append(valuesRows, clone)
		return nil
	}); err != nil {
		return nil, err
	}
	if stmt.OrderBy != nil {
		idx, ok := ctx.index[strings.ToLower(stmt.OrderBy.Column)]
		if !ok {
			return nil, fmt.Errorf("exec: unknown column %s in ORDER BY", stmt.OrderBy.Column)
		}
		column := table.Columns[idx]
		sort.SliceStable(valuesRows, func(i, j int) bool {
			left := valuesRows[i][idx]
			right := valuesRows[j][idx]
			cmp := orderCompare(column, left, right)
			if stmt.OrderBy.Desc {
				return cmp > 0
			}
			return cmp < 0
		})
	}
	if stmt.Limit != nil {
		offset := stmt.Limit.Offset
		if offset < 0 {
			offset = 0
		}
		if offset >= len(valuesRows) {
			valuesRows = [][]interface{}{}
		} else {
			valuesRows = valuesRows[offset:]
			if stmt.Limit.Limit < len(valuesRows) {
				valuesRows = valuesRows[:stmt.Limit.Limit]
			}
		}
	}
	rows := make([][]string, len(valuesRows))
	for i, values := range valuesRows {
		display := make([]string, len(values))
		for j, v := range values {
			display[j] = formatValue(table.Columns[j], v)
		}
		rows[i] = display
	}
	columns := make([]string, len(table.Columns))
	for i, col := range table.Columns {
		columns[i] = col.Name
	}
	return &Result{Columns: columns, Rows: rows, RowsAffected: len(rows), Message: fmt.Sprintf("%d row(s)", len(rows))}, nil
}

func (e *Executor) explainSelect(stmt *parser.SelectStmt) (*Plan, error) {
	table, ok := e.catalog.GetTable(stmt.Table)
	if !ok {
		return nil, fmt.Errorf("exec: table %s not found", stmt.Table)
	}
	detail := map[string]interface{}{"table": table.Name}
	root := &PlanNode{Name: "SeqScan", Detail: detail}
	if stmt.Where != nil {
		root.Children = append(root.Children, &PlanNode{Name: "Filter"})
	}
	if stmt.OrderBy != nil {
		root.Children = append(root.Children, &PlanNode{Name: "OrderBy", Detail: map[string]interface{}{"column": stmt.OrderBy.Column, "desc": stmt.OrderBy.Desc}})
	}
	if stmt.Limit != nil {
		root.Children = append(root.Children, &PlanNode{Name: "Limit", Detail: map[string]interface{}{"limit": stmt.Limit.Limit, "offset": stmt.Limit.Offset}})
	}
	return &Plan{Root: root}, nil
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

func formatValue(col catalog.Column, value interface{}) string {
	if value == nil {
		return "NULL"
	}
	switch col.Type {
	case catalog.ColumnTypeInt:
		return fmt.Sprintf("%d", value.(int32))
	case catalog.ColumnTypeBigInt:
		return fmt.Sprintf("%d", value.(int64))
	case catalog.ColumnTypeBoolean:
		if value.(bool) {
			return "TRUE"
		}
		return "FALSE"
	case catalog.ColumnTypeVarChar:
		return value.(string)
	case catalog.ColumnTypeDate:
		return value.(time.Time).Format("2006-01-02")
	case catalog.ColumnTypeTimestamp:
		return value.(time.Time).Format(time.RFC3339)
	default:
		return fmt.Sprintf("%v", value)
	}
}
