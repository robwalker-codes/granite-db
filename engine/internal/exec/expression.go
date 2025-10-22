package exec

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/example/granite-db/engine/internal/catalog"
	"github.com/example/granite-db/engine/internal/sql/parser"
)

type truthValue int

const (
	truthUnknown truthValue = iota
	truthFalse
	truthTrue
)

type evalContext struct {
	columns []catalog.Column
	values  []interface{}
	index   map[string]int
}

func newEvalContext(columns []catalog.Column) *evalContext {
	idx := make(map[string]int, len(columns))
	for i, col := range columns {
		idx[strings.ToLower(col.Name)] = i
	}
	return &evalContext{columns: columns, index: idx}
}

func (ctx *evalContext) setValues(values []interface{}) {
	ctx.values = values
}

func (ctx *evalContext) eval(expr parser.Expression) (truthValue, error) {
	if expr == nil {
		return truthTrue, nil
	}
	switch e := expr.(type) {
	case *parser.BooleanExpr:
		left, err := ctx.eval(e.Left)
		if err != nil {
			return truthUnknown, err
		}
		right, err := ctx.eval(e.Right)
		if err != nil {
			return truthUnknown, err
		}
		switch e.Op {
		case parser.BooleanAnd:
			return truthAnd(left, right), nil
		case parser.BooleanOr:
			return truthOr(left, right), nil
		default:
			return truthUnknown, fmt.Errorf("exec: unsupported boolean operator %s", e.Op)
		}
	case *parser.NotExpr:
		value, err := ctx.eval(e.Expr)
		if err != nil {
			return truthUnknown, err
		}
		return truthNot(value), nil
	case *parser.ComparisonExpr:
		return ctx.evalComparison(e)
	case *parser.IsNullExpr:
		datum, err := ctx.evalDatum(e.Expr)
		if err != nil {
			return truthUnknown, err
		}
		if datum.isNull {
			if e.Negated {
				return truthFalse, nil
			}
			return truthTrue, nil
		}
		if e.Negated {
			return truthTrue, nil
		}
		return truthFalse, nil
	default:
		// Allow bare column references to behave as boolean by checking non-null truthiness
		datum, err := ctx.evalDatum(expr)
		if err != nil {
			return truthUnknown, err
		}
		if datum.isNull {
			return truthUnknown, nil
		}
		switch v := datum.value.(type) {
		case bool:
			if v {
				return truthTrue, nil
			}
			return truthFalse, nil
		default:
			return truthUnknown, fmt.Errorf("exec: expression %T does not yield a boolean", expr)
		}
	}
}

type datum struct {
	value   interface{}
	column  *catalog.Column
	literal *parser.Literal
	isNull  bool
}

func (ctx *evalContext) evalDatum(expr parser.Expression) (datum, error) {
	switch e := expr.(type) {
	case *parser.ColumnRef:
		idx, ok := ctx.index[strings.ToLower(e.Name)]
		if !ok {
			return datum{}, fmt.Errorf("exec: unknown column %s", e.Name)
		}
		value := ctx.values[idx]
		return datum{value: value, column: &ctx.columns[idx], isNull: value == nil}, nil
	case *parser.LiteralExpr:
		if e.Literal.Kind == parser.LiteralNull {
			return datum{isNull: true, literal: &e.Literal}, nil
		}
		return datum{literal: &e.Literal}, nil
	default:
		return datum{}, fmt.Errorf("exec: unsupported expression in value context: %T", expr)
	}
}

func (ctx *evalContext) evalComparison(expr *parser.ComparisonExpr) (truthValue, error) {
	left, err := ctx.evalDatum(expr.Left)
	if err != nil {
		return truthUnknown, err
	}
	right, err := ctx.evalDatum(expr.Right)
	if err != nil {
		return truthUnknown, err
	}
	if left.isNull || right.isNull {
		return truthUnknown, nil
	}
	column := left.column
	if column == nil {
		column = right.column
	}
	if column == nil {
		return truthUnknown, fmt.Errorf("exec: comparison requires at least one column operand")
	}
	leftValue, err := normaliseDatum(left, column)
	if err != nil {
		return truthUnknown, err
	}
	rightValue, err := normaliseDatum(right, column)
	if err != nil {
		return truthUnknown, err
	}
	matched, err := compareValues(expr.Op, leftValue, rightValue, column.Type)
	if err != nil {
		return truthUnknown, err
	}
	if matched {
		return truthTrue, nil
	}
	return truthFalse, nil
}

func normaliseDatum(d datum, column *catalog.Column) (interface{}, error) {
	if d.column != nil {
		if d.column.Type != column.Type {
			return nil, fmt.Errorf("exec: incompatible column types %v and %v", d.column.Type, column.Type)
		}
		return d.value, nil
	}
	if d.literal == nil {
		return nil, fmt.Errorf("exec: expected literal value")
	}
	if d.literal.Kind == parser.LiteralNull {
		return nil, nil
	}
	return literalToType(*d.literal, column)
}

func literalToType(l parser.Literal, column *catalog.Column) (interface{}, error) {
	switch column.Type {
	case catalog.ColumnTypeInt:
		if l.Kind != parser.LiteralNumber {
			return nil, fmt.Errorf("exec: expected numeric literal for %s", column.Name)
		}
		value, err := strconv.Atoi(l.Value)
		if err != nil {
			return nil, fmt.Errorf("exec: invalid INT literal %s", l.Value)
		}
		return int32(value), nil
	case catalog.ColumnTypeBigInt:
		if l.Kind != parser.LiteralNumber {
			return nil, fmt.Errorf("exec: expected numeric literal for %s", column.Name)
		}
		value, err := strconv.ParseInt(l.Value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("exec: invalid BIGINT literal %s", l.Value)
		}
		return value, nil
	case catalog.ColumnTypeVarChar:
		if l.Kind != parser.LiteralString {
			return nil, fmt.Errorf("exec: expected string literal for %s", column.Name)
		}
		if column.Length > 0 && len(l.Value) > column.Length {
			return nil, fmt.Errorf("exec: literal exceeds length for %s", column.Name)
		}
		return l.Value, nil
	case catalog.ColumnTypeBoolean:
		if l.Kind != parser.LiteralBoolean {
			return nil, fmt.Errorf("exec: expected boolean literal for %s", column.Name)
		}
		return strings.ToUpper(l.Value) == "TRUE", nil
	case catalog.ColumnTypeDate:
		if l.Kind != parser.LiteralString {
			return nil, fmt.Errorf("exec: expected string literal for %s", column.Name)
		}
		t, err := time.Parse("2006-01-02", l.Value)
		if err != nil {
			return nil, fmt.Errorf("exec: invalid DATE literal %s", l.Value)
		}
		return t, nil
	case catalog.ColumnTypeTimestamp:
		if l.Kind != parser.LiteralString {
			return nil, fmt.Errorf("exec: expected string literal for %s", column.Name)
		}
		layouts := []string{time.RFC3339, "2006-01-02 15:04:05"}
		var parsed time.Time
		var err error
		for _, layout := range layouts {
			parsed, err = time.Parse(layout, l.Value)
			if err == nil {
				return parsed, nil
			}
		}
		return nil, fmt.Errorf("exec: invalid TIMESTAMP literal %s", l.Value)
	default:
		return nil, fmt.Errorf("exec: unsupported column type %d", column.Type)
	}
}

func compareValues(op parser.ComparisonOp, left, right interface{}, typ catalog.ColumnType) (bool, error) {
	switch typ {
	case catalog.ColumnTypeInt:
		l := left.(int32)
		r := right.(int32)
		return applyNumericComparison(op, int64(l), int64(r))
	case catalog.ColumnTypeBigInt:
		l := left.(int64)
		r := right.(int64)
		return applyNumericComparison(op, l, r)
	case catalog.ColumnTypeBoolean:
		l := left.(bool)
		r := right.(bool)
		switch op {
		case parser.ComparisonEqual:
			return l == r, nil
		case parser.ComparisonNotEqual:
			return l != r, nil
		default:
			return false, fmt.Errorf("exec: unsupported boolean comparison %s", op)
		}
	case catalog.ColumnTypeVarChar:
		l := left.(string)
		r := right.(string)
		return applyStringComparison(op, l, r)
	case catalog.ColumnTypeDate:
		l := left.(time.Time)
		r := right.(time.Time)
		return applyTimeComparison(op, l, r)
	case catalog.ColumnTypeTimestamp:
		l := left.(time.Time)
		r := right.(time.Time)
		return applyTimeComparison(op, l, r)
	default:
		return false, fmt.Errorf("exec: unsupported comparison for type %d", typ)
	}
}

func applyNumericComparison(op parser.ComparisonOp, left, right int64) (bool, error) {
	switch op {
	case parser.ComparisonEqual:
		return left == right, nil
	case parser.ComparisonNotEqual:
		return left != right, nil
	case parser.ComparisonLess:
		return left < right, nil
	case parser.ComparisonLessEqual:
		return left <= right, nil
	case parser.ComparisonGreater:
		return left > right, nil
	case parser.ComparisonGreaterEqual:
		return left >= right, nil
	default:
		return false, fmt.Errorf("exec: unknown comparison operator %s", op)
	}
}

func applyStringComparison(op parser.ComparisonOp, left, right string) (bool, error) {
	switch op {
	case parser.ComparisonEqual:
		return left == right, nil
	case parser.ComparisonNotEqual:
		return left != right, nil
	case parser.ComparisonLess:
		return left < right, nil
	case parser.ComparisonLessEqual:
		return left <= right, nil
	case parser.ComparisonGreater:
		return left > right, nil
	case parser.ComparisonGreaterEqual:
		return left >= right, nil
	default:
		return false, fmt.Errorf("exec: unknown comparison operator %s", op)
	}
}

func applyTimeComparison(op parser.ComparisonOp, left, right time.Time) (bool, error) {
	switch op {
	case parser.ComparisonEqual:
		return left.Equal(right), nil
	case parser.ComparisonNotEqual:
		return !left.Equal(right), nil
	case parser.ComparisonLess:
		return left.Before(right), nil
	case parser.ComparisonLessEqual:
		return left.Before(right) || left.Equal(right), nil
	case parser.ComparisonGreater:
		return left.After(right), nil
	case parser.ComparisonGreaterEqual:
		return left.After(right) || left.Equal(right), nil
	default:
		return false, fmt.Errorf("exec: unknown comparison operator %s", op)
	}
}

func truthNot(value truthValue) truthValue {
	switch value {
	case truthTrue:
		return truthFalse
	case truthFalse:
		return truthTrue
	default:
		return truthUnknown
	}
}

func truthAnd(left, right truthValue) truthValue {
	if left == truthFalse || right == truthFalse {
		return truthFalse
	}
	if left == truthTrue && right == truthTrue {
		return truthTrue
	}
	return truthUnknown
}

func truthOr(left, right truthValue) truthValue {
	if left == truthTrue || right == truthTrue {
		return truthTrue
	}
	if left == truthFalse && right == truthFalse {
		return truthFalse
	}
	return truthUnknown
}

func orderCompare(column catalog.Column, left, right interface{}) int {
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
		l := left.(string)
		r := right.(string)
		switch {
		case l < r:
			return -1
		case l > r:
			return 1
		default:
			return 0
		}
	case catalog.ColumnTypeDate, catalog.ColumnTypeTimestamp:
		l := left.(time.Time)
		r := right.(time.Time)
		switch {
		case l.Before(r):
			return -1
		case l.After(r):
			return 1
		default:
			return 0
		}
	default:
		return 0
	}
}
