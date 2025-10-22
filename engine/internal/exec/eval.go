package exec

import (
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/shopspring/decimal"

	"github.com/example/granite-db/engine/internal/sql/expr"
)

type truthValue int

const (
	truthUnknown truthValue = iota
	truthFalse
	truthTrue
)

type typedValue struct {
	data interface{}
	typ  expr.Type
	null bool
}

func (v typedValue) isNull() bool {
	return v.null
}

type valueEvaluator struct {
	row []interface{}
}

func newValueEvaluator() *valueEvaluator {
	return &valueEvaluator{}
}

func (e *valueEvaluator) setRow(row []interface{}) {
	e.row = row
}

func (e *valueEvaluator) eval(node expr.TypedExpr) (typedValue, error) {
	switch n := node.(type) {
	case *expr.ColumnRef:
		value := e.row[n.Index]
		if value == nil {
			return typedValue{typ: n.ResultType(), null: true}, nil
		}
		return typedValue{typ: n.ResultType(), data: value}, nil
	case *expr.Literal:
		if n.Value == nil {
			return typedValue{typ: n.ResultType(), null: true}, nil
		}
		return typedValue{typ: n.ResultType(), data: n.Value}, nil
	case *expr.UnaryExpr:
		return e.evalUnary(n)
	case *expr.BinaryExpr:
		return e.evalBinary(n)
	case *expr.FunctionExpr:
		return e.evalFunction(n)
	case *expr.CoalesceExpr:
		left, err := e.eval(n.Left)
		if err != nil {
			return typedValue{}, err
		}
		if !left.isNull() {
			return left, nil
		}
		return e.eval(n.Right)
	case *expr.IsNullExpr:
		inner, err := e.eval(n.Expr)
		if err != nil {
			return typedValue{}, err
		}
		result := !inner.isNull()
		if n.Negated {
			result = !result
		}
		return typedValue{typ: expr.BooleanType(false), data: result}, nil
	default:
		return typedValue{}, fmt.Errorf("exec: unsupported expression %T", node)
	}
}

func (e *valueEvaluator) evalUnary(unary *expr.UnaryExpr) (typedValue, error) {
	operand, err := e.eval(unary.Expr)
	if err != nil {
		return typedValue{}, err
	}
	if operand.isNull() {
		return typedValue{typ: unary.ResultType(), null: true}, nil
	}
	switch unary.Op {
	case expr.UnaryOpPlus:
		return typedValue{typ: unary.ResultType(), data: operand.data}, nil
	case expr.UnaryOpMinus:
		switch unary.ResultType().Kind {
		case expr.TypeInt:
			value, err := toInt64(operand)
			if err != nil {
				return typedValue{}, err
			}
			neg := -value
			if neg < math.MinInt32 || neg > math.MaxInt32 {
				return typedValue{}, fmt.Errorf("exec: INT overflow")
			}
			return typedValue{typ: unary.ResultType(), data: int32(neg)}, nil
		case expr.TypeBigInt:
			value, err := toInt64(operand)
			if err != nil {
				return typedValue{}, err
			}
			return typedValue{typ: unary.ResultType(), data: -value}, nil
		case expr.TypeDecimal:
			value, err := toDecimal(operand)
			if err != nil {
				return typedValue{}, err
			}
			return typedValue{typ: unary.ResultType(), data: value.Neg()}, nil
		default:
			return typedValue{}, fmt.Errorf("exec: unary - unsupported for %v", unary.ResultType().Kind)
		}
	case expr.UnaryOpNot:
		truth, err := toTruthValue(operand)
		if err != nil {
			return typedValue{}, err
		}
		switch truth {
		case truthTrue:
			return typedValue{typ: unary.ResultType(), data: false}, nil
		case truthFalse:
			return typedValue{typ: unary.ResultType(), data: true}, nil
		default:
			return typedValue{typ: unary.ResultType().WithNullability(true), null: true}, nil
		}
	default:
		return typedValue{}, fmt.Errorf("exec: unsupported unary operator %v", unary.Op)
	}
}

func (e *valueEvaluator) evalBinary(binary *expr.BinaryExpr) (typedValue, error) {
	left, err := e.eval(binary.Left)
	if err != nil {
		return typedValue{}, err
	}
	right, err := e.eval(binary.Right)
	if err != nil {
		return typedValue{}, err
	}
	resultType := binary.ResultType()
	switch binary.Op {
	case expr.BinaryOpAdd, expr.BinaryOpSubtract, expr.BinaryOpMultiply, expr.BinaryOpDivide, expr.BinaryOpModulo:
		if left.isNull() || right.isNull() {
			return typedValue{typ: resultType, null: true}, nil
		}
		return evalArithmetic(resultType, binary.Op, left, right)
	case expr.BinaryOpEqual, expr.BinaryOpNotEqual, expr.BinaryOpLess, expr.BinaryOpLessEqual, expr.BinaryOpGreater, expr.BinaryOpGreaterEqual:
		return evalComparison(resultType, binary.Op, left, right)
	case expr.BinaryOpAnd, expr.BinaryOpOr:
		return evalBoolean(resultType, binary.Op, left, right)
	default:
		return typedValue{}, fmt.Errorf("exec: unsupported binary operator %v", binary.Op)
	}
}

func (e *valueEvaluator) evalFunction(fn *expr.FunctionExpr) (typedValue, error) {
	name := strings.ToUpper(fn.Name)
	switch name {
	case "LOWER":
		arg, err := e.eval(fn.Args[0])
		if err != nil {
			return typedValue{}, err
		}
		if arg.isNull() {
			return typedValue{typ: fn.ResultType(), null: true}, nil
		}
		return typedValue{typ: fn.ResultType(), data: strings.ToLower(arg.data.(string))}, nil
	case "UPPER":
		arg, err := e.eval(fn.Args[0])
		if err != nil {
			return typedValue{}, err
		}
		if arg.isNull() {
			return typedValue{typ: fn.ResultType(), null: true}, nil
		}
		return typedValue{typ: fn.ResultType(), data: strings.ToUpper(arg.data.(string))}, nil
	case "LENGTH":
		arg, err := e.eval(fn.Args[0])
		if err != nil {
			return typedValue{}, err
		}
		if arg.isNull() {
			return typedValue{typ: fn.ResultType(), null: true}, nil
		}
		return typedValue{typ: fn.ResultType(), data: int32(utf8.RuneCountInString(arg.data.(string)))}, nil
	default:
		return typedValue{}, fmt.Errorf("exec: unsupported function %s", fn.Name)
	}
}

func evalArithmetic(resultType expr.Type, op expr.BinaryOp, left, right typedValue) (typedValue, error) {
	switch resultType.Kind {
	case expr.TypeInt:
		l, err := toInt64(left)
		if err != nil {
			return typedValue{}, err
		}
		r, err := toInt64(right)
		if err != nil {
			return typedValue{}, err
		}
		var res int64
		switch op {
		case expr.BinaryOpAdd:
			res = l + r
		case expr.BinaryOpSubtract:
			res = l - r
		case expr.BinaryOpMultiply:
			res = l * r
		case expr.BinaryOpModulo:
			if r == 0 {
				return typedValue{}, fmt.Errorf("exec: division by zero")
			}
			res = l % r
		default:
			return typedValue{}, fmt.Errorf("exec: unsupported INT operator %v", op)
		}
		if res < math.MinInt32 || res > math.MaxInt32 {
			return typedValue{}, fmt.Errorf("exec: INT overflow")
		}
		return typedValue{typ: resultType, data: int32(res)}, nil
	case expr.TypeBigInt:
		l, err := toInt64(left)
		if err != nil {
			return typedValue{}, err
		}
		r, err := toInt64(right)
		if err != nil {
			return typedValue{}, err
		}
		var res int64
		switch op {
		case expr.BinaryOpAdd:
			res = l + r
		case expr.BinaryOpSubtract:
			res = l - r
		case expr.BinaryOpMultiply:
			res = l * r
		case expr.BinaryOpModulo:
			if r == 0 {
				return typedValue{}, fmt.Errorf("exec: division by zero")
			}
			res = l % r
		default:
			return typedValue{}, fmt.Errorf("exec: unsupported BIGINT operator %v", op)
		}
		return typedValue{typ: resultType, data: res}, nil
	case expr.TypeDecimal:
		l, err := toDecimal(left)
		if err != nil {
			return typedValue{}, err
		}
		r, err := toDecimal(right)
		if err != nil {
			return typedValue{}, err
		}
		switch op {
		case expr.BinaryOpAdd:
			return typedValue{typ: resultType, data: l.Add(r)}, nil
		case expr.BinaryOpSubtract:
			return typedValue{typ: resultType, data: l.Sub(r)}, nil
		case expr.BinaryOpMultiply:
			return typedValue{typ: resultType, data: l.Mul(r)}, nil
		case expr.BinaryOpDivide:
			if r.IsZero() {
				return typedValue{}, fmt.Errorf("exec: division by zero")
			}
			return typedValue{typ: resultType, data: l.Div(r)}, nil
		default:
			return typedValue{}, fmt.Errorf("exec: unsupported DECIMAL operator %v", op)
		}
	default:
		return typedValue{}, fmt.Errorf("exec: arithmetic not implemented for %v", resultType.Kind)
	}
}

func evalComparison(resultType expr.Type, op expr.BinaryOp, left, right typedValue) (typedValue, error) {
	if left.isNull() || right.isNull() {
		return typedValue{typ: resultType.WithNullability(true), null: true}, nil
	}
	switch {
	case left.typ.IsNumeric() && right.typ.IsNumeric():
		l, err := toDecimal(left)
		if err != nil {
			return typedValue{}, err
		}
		r, err := toDecimal(right)
		if err != nil {
			return typedValue{}, err
		}
		cmp := l.Cmp(r)
		return booleanFromComparison(resultType, op, cmp)
	case left.typ.IsString() && right.typ.IsString():
		cmp := strings.Compare(left.data.(string), right.data.(string))
		return booleanFromComparison(resultType, op, cmp)
	case left.typ.IsTemporal() && right.typ.IsTemporal():
		cmp := 0
		lt := left.data.(time.Time)
		rt := right.data.(time.Time)
		if lt.Before(rt) {
			cmp = -1
		} else if lt.After(rt) {
			cmp = 1
		}
		return booleanFromComparison(resultType, op, cmp)
	case left.typ.Kind == expr.TypeBoolean && right.typ.Kind == expr.TypeBoolean:
		l := left.data.(bool)
		r := right.data.(bool)
		cmp := 0
		if l != r {
			if !l && r {
				cmp = -1
			} else {
				cmp = 1
			}
		}
		return booleanFromComparison(resultType, op, cmp)
	default:
		return typedValue{}, fmt.Errorf("exec: comparison not supported for %v and %v", left.typ.Kind, right.typ.Kind)
	}
}

func evalBoolean(resultType expr.Type, op expr.BinaryOp, left, right typedValue) (typedValue, error) {
	l, err := toTruthValue(left)
	if err != nil {
		return typedValue{}, err
	}
	r, err := toTruthValue(right)
	if err != nil {
		return typedValue{}, err
	}
	var result truthValue
	switch op {
	case expr.BinaryOpAnd:
		result = truthAnd(l, r)
	case expr.BinaryOpOr:
		result = truthOr(l, r)
	default:
		return typedValue{}, fmt.Errorf("exec: unknown boolean operator %v", op)
	}
	switch result {
	case truthTrue:
		return typedValue{typ: resultType, data: true}, nil
	case truthFalse:
		return typedValue{typ: resultType, data: false}, nil
	default:
		return typedValue{typ: resultType.WithNullability(true), null: true}, nil
	}
}

func booleanFromComparison(resultType expr.Type, op expr.BinaryOp, cmp int) (typedValue, error) {
	var value bool
	switch op {
	case expr.BinaryOpEqual:
		value = cmp == 0
	case expr.BinaryOpNotEqual:
		value = cmp != 0
	case expr.BinaryOpLess:
		value = cmp < 0
	case expr.BinaryOpLessEqual:
		value = cmp <= 0
	case expr.BinaryOpGreater:
		value = cmp > 0
	case expr.BinaryOpGreaterEqual:
		value = cmp >= 0
	default:
		return typedValue{}, fmt.Errorf("exec: unsupported comparison operator %v", op)
	}
	return typedValue{typ: resultType, data: value}, nil
}

func toTruthValue(value typedValue) (truthValue, error) {
	if value.isNull() {
		return truthUnknown, nil
	}
	b, ok := value.data.(bool)
	if !ok {
		return truthUnknown, fmt.Errorf("exec: cannot interpret %T as BOOLEAN", value.data)
	}
	if b {
		return truthTrue, nil
	}
	return truthFalse, nil
}

func toInt64(value typedValue) (int64, error) {
	switch v := value.data.(type) {
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	default:
		return 0, fmt.Errorf("exec: cannot interpret %T as integer", value.data)
	}
}

func toDecimal(value typedValue) (decimal.Decimal, error) {
	switch v := value.data.(type) {
	case decimal.Decimal:
		return v, nil
	case int32:
		return decimal.NewFromInt(int64(v)), nil
	case int64:
		return decimal.NewFromInt(v), nil
	default:
		return decimal.Decimal{}, fmt.Errorf("exec: cannot interpret %T as DECIMAL", value.data)
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
