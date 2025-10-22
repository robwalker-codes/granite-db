package parser_test

import (
	"testing"

	"github.com/example/granite-db/engine/internal/sql/parser"
)

func TestSelectProjectionParsing(t *testing.T) {
	stmt, err := parser.Parse("SELECT id, name AS n, id + 1 AS next FROM people")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", stmt)
	}
	if len(selectStmt.Items) != 3 {
		t.Fatalf("expected 3 projection items, got %d", len(selectStmt.Items))
	}

	first := selectStmt.Items[0].(*parser.SelectExprItem)
	if first.Alias != "" {
		t.Fatalf("expected no alias for first column, got %q", first.Alias)
	}
	if col, ok := first.Expr.(*parser.ColumnRef); !ok || col.Name != "id" {
		t.Fatalf("expected column reference id, got %T", first.Expr)
	}

	second := selectStmt.Items[1].(*parser.SelectExprItem)
	if second.Alias != "n" {
		t.Fatalf("expected alias n, got %q", second.Alias)
	}
	if col, ok := second.Expr.(*parser.ColumnRef); !ok || col.Name != "name" {
		t.Fatalf("expected column reference name, got %T", second.Expr)
	}

	third := selectStmt.Items[2].(*parser.SelectExprItem)
	if third.Alias != "next" {
		t.Fatalf("expected alias next, got %q", third.Alias)
	}
	binary, ok := third.Expr.(*parser.BinaryExpr)
	if !ok || binary.Op != parser.BinaryAdd {
		t.Fatalf("expected binary addition, got %T with op %v", third.Expr, binary.Op)
	}
	if _, ok := binary.Left.(*parser.ColumnRef); !ok {
		t.Fatalf("expected column ref on left side of addition")
	}
	if lit, ok := binary.Right.(*parser.LiteralExpr); !ok || lit.Literal.Value != "1" {
		t.Fatalf("expected numeric literal 1 on right side")
	}
	tableRef, ok := selectStmt.From.(*parser.TableName)
	if !ok || tableRef.Name != "people" {
		t.Fatalf("expected FROM people, got %T", selectStmt.From)
	}
}

func TestSelectFunctionParsing(t *testing.T) {
	stmt, err := parser.Parse("SELECT UPPER(name), LENGTH(name) FROM people")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt := stmt.(*parser.SelectStmt)
	if len(selectStmt.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(selectStmt.Items))
	}
	first := selectStmt.Items[0].(*parser.SelectExprItem)
	call, ok := first.Expr.(*parser.FunctionCallExpr)
	if !ok || call.Name != "UPPER" {
		t.Fatalf("expected UPPER function, got %T/%s", first.Expr, call.Name)
	}
	if len(call.Args) != 1 {
		t.Fatalf("expected single argument to UPPER")
	}
	if _, ok := call.Args[0].(*parser.ColumnRef); !ok {
		t.Fatalf("expected column reference argument to UPPER")
	}
	if _, ok := selectStmt.From.(*parser.TableName); !ok {
		t.Fatalf("expected FROM table")
	}
}

func TestSelectCoalesceParsing(t *testing.T) {
	stmt, err := parser.Parse("SELECT COALESCE(nick, name) FROM people")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt := stmt.(*parser.SelectStmt)
	if len(selectStmt.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(selectStmt.Items))
	}
	item := selectStmt.Items[0].(*parser.SelectExprItem)
	call, ok := item.Expr.(*parser.FunctionCallExpr)
	if !ok || call.Name != "COALESCE" {
		t.Fatalf("expected COALESCE call, got %T/%s", item.Expr, call.Name)
	}
	if len(call.Args) != 2 {
		t.Fatalf("expected two arguments to COALESCE")
	}
	if _, ok := selectStmt.From.(*parser.TableName); !ok {
		t.Fatalf("expected FROM table")
	}
}

func TestExpressionPrecedence(t *testing.T) {
	stmt, err := parser.Parse("SELECT 1+2*3 AS a, (1+2)*3 AS b FROM dual")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt := stmt.(*parser.SelectStmt)
	if len(selectStmt.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(selectStmt.Items))
	}
	first := selectStmt.Items[0].(*parser.SelectExprItem)
	expr := first.Expr.(*parser.BinaryExpr)
	if expr.Op != parser.BinaryAdd {
		t.Fatalf("expected addition for first expression, got %v", expr.Op)
	}
	if _, ok := expr.Right.(*parser.BinaryExpr); !ok {
		t.Fatalf("expected multiplication on right-hand side of first expression")
	}

	second := selectStmt.Items[1].(*parser.SelectExprItem)
	mult := second.Expr.(*parser.BinaryExpr)
	if mult.Op != parser.BinaryMultiply {
		t.Fatalf("expected multiplication for second expression, got %v", mult.Op)
	}
	if add, ok := mult.Left.(*parser.BinaryExpr); !ok || add.Op != parser.BinaryAdd {
		t.Fatalf("expected parenthesised addition on left-hand side")
	}
	if tableRef, ok := selectStmt.From.(*parser.TableName); !ok || tableRef.Name != "dual" {
		t.Fatalf("expected FROM dual")
	}
}

func TestSelectStarMixedExpressionsNotAllowed(t *testing.T) {
	if _, err := parser.Parse("SELECT *, id FROM people"); err == nil {
		t.Fatalf("expected error when mixing * with expressions")
	}
}

func TestSelectWithoutFrom(t *testing.T) {
	stmt, err := parser.Parse("SELECT 1+2")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", stmt)
	}
	if selectStmt.From != nil {
		t.Fatalf("expected SELECT without FROM to have no table")
	}
	if len(selectStmt.Items) != 1 {
		t.Fatalf("expected single item, got %d", len(selectStmt.Items))
	}
}

func TestJoinParsing(t *testing.T) {
	stmt, err := parser.Parse("SELECT c.name, o.total FROM customers c INNER JOIN orders o ON c.id = o.customer_id")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt := stmt.(*parser.SelectStmt)
	join, ok := selectStmt.From.(*parser.JoinExpr)
	if !ok {
		t.Fatalf("expected join expression, got %T", selectStmt.From)
	}
	left, ok := join.Left.(*parser.TableName)
	if !ok || left.Name != "customers" || left.Alias != "c" {
		t.Fatalf("unexpected left table: %+v", join.Left)
	}
	right, ok := join.Right.(*parser.TableName)
	if !ok || right.Name != "orders" || right.Alias != "o" {
		t.Fatalf("unexpected right table: %+v", join.Right)
	}
	if join.Type != parser.JoinTypeInner {
		t.Fatalf("expected INNER join, got %v", join.Type)
	}
	cond, ok := join.Condition.(*parser.BinaryExpr)
	if !ok || cond.Op != parser.BinaryEqual {
		t.Fatalf("expected equality condition, got %T", join.Condition)
	}
}

func TestLeftJoinParsing(t *testing.T) {
	stmt, err := parser.Parse("SELECT c.name FROM customers c LEFT JOIN orders o ON c.id = o.customer_id")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt := stmt.(*parser.SelectStmt)
	join, ok := selectStmt.From.(*parser.JoinExpr)
	if !ok {
		t.Fatalf("expected join expression, got %T", selectStmt.From)
	}
	if join.Type != parser.JoinTypeLeft {
		t.Fatalf("expected LEFT join, got %v", join.Type)
	}
	if _, ok := join.Left.(*parser.TableName); !ok {
		t.Fatalf("expected left table in join")
	}
	if _, ok := join.Right.(*parser.TableName); !ok {
		t.Fatalf("expected right table in join")
	}
}

func TestJoinUsingNotSupported(t *testing.T) {
	if _, err := parser.Parse("SELECT * FROM a JOIN b USING(id)"); err == nil {
		t.Fatalf("expected USING to be rejected")
	}
}

func TestOrderByQualifiedColumn(t *testing.T) {
	stmt, err := parser.Parse("SELECT c.id FROM customers c ORDER BY c.id DESC")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt := stmt.(*parser.SelectStmt)
	if selectStmt.OrderBy == nil {
		t.Fatalf("expected ORDER BY clause")
	}
	if selectStmt.OrderBy.Column != "c.id" {
		t.Fatalf("expected qualified column, got %s", selectStmt.OrderBy.Column)
	}
	if !selectStmt.OrderBy.Desc {
		t.Fatalf("expected DESC ordering")
	}
}
