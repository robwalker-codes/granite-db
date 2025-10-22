package validator

import (
	"fmt"
	"strings"

	"github.com/example/granite-db/engine/internal/catalog"
	"github.com/example/granite-db/engine/internal/sql/expr"
	"github.com/example/granite-db/engine/internal/sql/parser"
)

// UpdateAssignment captures a typed assignment for UPDATE statements.
type UpdateAssignment struct {
	ColumnIndex int
	Column      catalog.Column
	Expr        expr.TypedExpr
}

// ValidatedUpdate provides the semantic representation of an UPDATE statement.
type ValidatedUpdate struct {
	Table       *catalog.Table
	Assignments []UpdateAssignment
	Where       expr.TypedExpr
}

// ValidatedDelete provides the semantic representation of a DELETE statement.
type ValidatedDelete struct {
	Table *catalog.Table
	Where expr.TypedExpr
}

// ValidateUpdate analyses the parsed UPDATE statement and produces a typed representation.
func ValidateUpdate(cat *catalog.Catalog, stmt *parser.UpdateStmt) (*ValidatedUpdate, error) {
	if stmt == nil {
		return nil, fmt.Errorf("validator: UPDATE statement required")
	}
	table, ok := cat.GetTable(stmt.Table)
	if !ok {
		return nil, fmt.Errorf("validator: table %q not found", stmt.Table)
	}
	if len(stmt.Assignments) == 0 {
		return nil, fmt.Errorf("validator: UPDATE requires at least one column assignment")
	}
	v := newSelectValidator(cat)
	if _, err := v.addTable(table.Name, ""); err != nil {
		return nil, err
	}
	assignments := make([]UpdateAssignment, 0, len(stmt.Assignments))
	seen := make(map[string]struct{})
	for _, assign := range stmt.Assignments {
		lower := strings.ToLower(assign.Column)
		if _, exists := seen[lower]; exists {
			return nil, fmt.Errorf("validator: column %q assigned multiple times", assign.Column)
		}
		seen[lower] = struct{}{}
		binding, err := v.resolveColumn("", assign.Column, "SET clause")
		if err != nil {
			return nil, err
		}
		typedExpr, err := v.buildExpression(assign.Expr, "SET clause")
		if err != nil {
			return nil, err
		}
		if err := ensureAssignmentCompatibility(binding.binding.Column, typedExpr); err != nil {
			return nil, err
		}
		assignments = append(assignments, UpdateAssignment{ColumnIndex: binding.binding.Index, Column: binding.binding.Column, Expr: typedExpr})
	}
	var where expr.TypedExpr
	if stmt.Where != nil {
		typed, err := v.buildExpression(stmt.Where, "WHERE clause")
		if err != nil {
			return nil, err
		}
		if typed.ResultType().Kind != expr.TypeBoolean {
			return nil, fmt.Errorf("validator: WHERE clause must evaluate to BOOLEAN")
		}
		where = typed
	}
	return &ValidatedUpdate{Table: table, Assignments: assignments, Where: where}, nil
}

// ValidateDelete analyses the parsed DELETE statement and produces a typed representation.
func ValidateDelete(cat *catalog.Catalog, stmt *parser.DeleteStmt) (*ValidatedDelete, error) {
	if stmt == nil {
		return nil, fmt.Errorf("validator: DELETE statement required")
	}
	table, ok := cat.GetTable(stmt.Table)
	if !ok {
		return nil, fmt.Errorf("validator: table %q not found", stmt.Table)
	}
	v := newSelectValidator(cat)
	if _, err := v.addTable(table.Name, ""); err != nil {
		return nil, err
	}
	var where expr.TypedExpr
	if stmt.Where != nil {
		typed, err := v.buildExpression(stmt.Where, "WHERE clause")
		if err != nil {
			return nil, err
		}
		if typed.ResultType().Kind != expr.TypeBoolean {
			return nil, fmt.Errorf("validator: WHERE clause must evaluate to BOOLEAN")
		}
		where = typed
	}
	return &ValidatedDelete{Table: table, Where: where}, nil
}

func ensureAssignmentCompatibility(column catalog.Column, expression expr.TypedExpr) error {
	exprType := expression.ResultType()
	colType := expr.FromColumn(column)
	if exprType.Kind == expr.TypeNull {
		if column.NotNull {
			return fmt.Errorf("validator: column %q does not allow NULL values", column.Name)
		}
		return nil
	}
	if compatibleNumericTypes(exprType, colType) {
		return nil
	}
	if exprType.Kind != colType.Kind {
		return fmt.Errorf("validator: cannot assign %s expression to column %q of type %s", formatExprType(exprType), column.Name, formatExprType(colType))
	}
	switch exprType.Kind {
	case expr.TypeVarChar:
		if colType.Length > 0 && exprType.Length > colType.Length {
			return fmt.Errorf("validator: value for column %q exceeds maximum length %d", column.Name, colType.Length)
		}
	case expr.TypeDecimal:
		if exprType.Precision > colType.Precision || exprType.Scale > colType.Scale {
			return fmt.Errorf("validator: value for column %q exceeds DECIMAL(%d,%d) bounds", column.Name, colType.Precision, colType.Scale)
		}
	}
	return nil
}

func compatibleNumericTypes(exprType, colType expr.Type) bool {
	switch colType.Kind {
	case expr.TypeBigInt:
		return exprType.Kind == expr.TypeBigInt || exprType.Kind == expr.TypeInt
	case expr.TypeDecimal:
		switch exprType.Kind {
		case expr.TypeDecimal:
			if exprType.Precision > colType.Precision || exprType.Scale > colType.Scale {
				return false
			}
			return true
		case expr.TypeBigInt, expr.TypeInt:
			return true
		}
	}
	return false
}

func formatExprType(t expr.Type) string {
	switch t.Kind {
	case expr.TypeInt:
		return "INT"
	case expr.TypeBigInt:
		return "BIGINT"
	case expr.TypeDecimal:
		return fmt.Sprintf("DECIMAL(%d,%d)", t.Precision, t.Scale)
	case expr.TypeVarChar:
		if t.Length > 0 {
			return fmt.Sprintf("VARCHAR(%d)", t.Length)
		}
		return "VARCHAR"
	case expr.TypeBoolean:
		return "BOOLEAN"
	case expr.TypeDate:
		return "DATE"
	case expr.TypeTimestamp:
		return "TIMESTAMP"
	case expr.TypeNull:
		return "NULL"
	default:
		return "UNKNOWN"
	}
}
