package parser

import "strings"

// FormatExpression renders the AST expression into a deterministic SQL string
// used for derived column names and diagnostics.
func FormatExpression(expr Expression) string {
	return formatExpressionWithPrecedence(expr, lowestPrecedence)
}

func formatExpressionWithPrecedence(expr Expression, parent int) string {
	switch e := expr.(type) {
	case *ColumnRef:
		if e.Table != "" {
			return e.Table + "." + e.Name
		}
		return e.Name
	case *LiteralExpr:
		return formatLiteral(e.Literal)
	case *UnaryExpr:
		prec := precedenceForUnary(e.Op)
		inner := formatExpressionWithPrecedence(e.Expr, prec)
		switch e.Op {
		case UnaryNot:
			text := "NOT " + inner
			if prec < parent {
				return "(" + text + ")"
			}
			return text
		default:
			text := string(e.Op) + inner
			if prec < parent {
				return "(" + text + ")"
			}
			return text
		}
	case *BinaryExpr:
		prec := precedenceForBinary(e.Op)
		left := formatExpressionWithPrecedence(e.Left, prec)
		right := formatExpressionWithPrecedence(e.Right, prec+1)
		text := left + " " + string(e.Op) + " " + right
		if prec < parent {
			return "(" + text + ")"
		}
		return text
	case *FunctionCallExpr:
		parts := make([]string, len(e.Args))
		for i, arg := range e.Args {
			parts[i] = FormatExpression(arg)
		}
		return e.Name + "(" + strings.Join(parts, ", ") + ")"
	case *IsNullExpr:
		prec := comparisonPrecedence
		inner := formatExpressionWithPrecedence(e.Expr, prec)
		text := inner + " IS"
		if e.Negated {
			text += " NOT"
		}
		text += " NULL"
		if prec < parent {
			return "(" + text + ")"
		}
		return text
	default:
		return "<expr>"
	}
}

func formatLiteral(l Literal) string {
	switch l.Kind {
	case LiteralBoolean:
		return strings.ToUpper(l.Value)
	case LiteralNull:
		return "NULL"
	case LiteralString:
		escaped := strings.ReplaceAll(l.Value, "'", "''")
		return "'" + escaped + "'"
	default:
		return l.Value
	}
}

func precedenceForUnary(op UnaryOp) int {
	return prefixPrecedence
}

func precedenceForBinary(op BinaryOp) int {
	switch op {
	case BinaryAnd:
		return andPrecedence
	case BinaryOr:
		return orPrecedence
	case BinaryEqual, BinaryNotEqual, BinaryLess, BinaryLessEqual, BinaryGreater, BinaryGreaterEqual:
		return comparisonPrecedence
	case BinaryAdd, BinarySubtract:
		return additivePrecedence
	case BinaryMultiply, BinaryDivide, BinaryModulo:
		return multiplicativePrecedence
	default:
		return additivePrecedence
	}
}
