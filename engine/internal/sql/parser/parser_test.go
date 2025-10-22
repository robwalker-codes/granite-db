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

func TestCreateTableDecimalParsing(t *testing.T) {
	stmt, err := parser.Parse("CREATE TABLE accounts(id INT, balance DECIMAL(12,2) NOT NULL, note VARCHAR(20))")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	create, ok := stmt.(*parser.CreateTableStmt)
	if !ok {
		t.Fatalf("expected CreateTableStmt, got %T", stmt)
	}
	if len(create.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(create.Columns))
	}
	balance := create.Columns[1]
	if balance.Type != parser.DataTypeDecimal {
		t.Fatalf("expected DECIMAL type, got %v", balance.Type)
	}
	if balance.Precision != 12 || balance.Scale != 2 {
		t.Fatalf("unexpected precision/scale: %d/%d", balance.Precision, balance.Scale)
	}
	if !balance.NotNull {
		t.Fatalf("expected DECIMAL column to inherit NOT NULL")
	}
}

func TestCreateTableInlinePrimaryKey(t *testing.T) {
	stmt, err := parser.Parse("CREATE TABLE t(id INT PRIMARY KEY, name VARCHAR(10));")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	create := stmt.(*parser.CreateTableStmt)
	if create.PrimaryKey != "id" {
		t.Fatalf("expected primary key id, got %s", create.PrimaryKey)
	}
	if len(create.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(create.Columns))
	}
	if !create.Columns[0].NotNull {
		t.Fatalf("expected PRIMARY KEY column to be NOT NULL")
	}
}

func TestCreateTableForeignKeyParsing(t *testing.T) {
	sql := `CREATE TABLE order_items(
                id INT PRIMARY KEY,
                order_id INT REFERENCES orders(id) ON DELETE RESTRICT ON UPDATE NO ACTION,
                product_id INT,
                CONSTRAINT fk_items_product FOREIGN KEY(product_id)
                        REFERENCES products(id)
                        ON DELETE RESTRICT ON UPDATE RESTRICT
        );`
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("parse foreign keys: %v", err)
	}
	create := stmt.(*parser.CreateTableStmt)
	if len(create.ForeignKeys) != 2 {
		t.Fatalf("expected 2 foreign keys, got %d", len(create.ForeignKeys))
	}
	inline := create.ForeignKeys[0]
	if len(inline.Columns) != 1 || inline.Columns[0] != "order_id" {
		t.Fatalf("unexpected inline child columns: %+v", inline.Columns)
	}
	if inline.ReferencedTable != "orders" {
		t.Fatalf("expected inline referenced table orders, got %s", inline.ReferencedTable)
	}
	if len(inline.ReferencedCols) != 1 || inline.ReferencedCols[0] != "id" {
		t.Fatalf("unexpected inline referenced columns: %+v", inline.ReferencedCols)
	}
	if inline.OnDelete != parser.ForeignKeyActionRestrict {
		t.Fatalf("expected inline ON DELETE RESTRICT, got %v", inline.OnDelete)
	}
	if inline.OnUpdate != parser.ForeignKeyActionNoAction {
		t.Fatalf("expected inline ON UPDATE NO ACTION, got %v", inline.OnUpdate)
	}
	named := create.ForeignKeys[1]
	if named.Name != "fk_items_product" {
		t.Fatalf("expected named foreign key fk_items_product, got %s", named.Name)
	}
	if named.OnDelete != parser.ForeignKeyActionRestrict || named.OnUpdate != parser.ForeignKeyActionRestrict {
		t.Fatalf("expected named foreign key to default to RESTRICT actions")
	}
	if named.ReferencedTable != "products" {
		t.Fatalf("expected named foreign key to reference products, got %s", named.ReferencedTable)
	}
}

func TestCreateTableForeignKeyUnsupportedAction(t *testing.T) {
	_, err := parser.Parse("CREATE TABLE t(id INT, parent_id INT REFERENCES parents(id) ON DELETE CASCADE)")
	if err == nil || err.Error() != "referential action CASCADE is not supported (yet)" {
		t.Fatalf("expected CASCADE rejection, got %v", err)
	}
	_, err = parser.Parse("CREATE TABLE t2(id INT, parent_id INT REFERENCES parents(id) ON DELETE SET NULL)")
	if err == nil || err.Error() != "referential action SET NULL is not supported (yet)" {
		t.Fatalf("expected SET NULL rejection, got %v", err)
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
	if len(selectStmt.OrderBy) != 1 {
		t.Fatalf("expected single ORDER BY term")
	}
	term := selectStmt.OrderBy[0]
	col, ok := term.Expr.(*parser.ColumnRef)
	if !ok {
		t.Fatalf("expected column reference in ORDER BY, got %T", term.Expr)
	}
	if col.Table != "c" || col.Name != "id" {
		t.Fatalf("unexpected column reference: %+v", col)
	}
	if !term.Desc {
		t.Fatalf("expected DESC ordering")
	}
}

func TestSelectGroupByHavingOrder(t *testing.T) {
	query := "SELECT customer_id, COUNT(*) AS c FROM orders GROUP BY customer_id HAVING COUNT(*) > 1 ORDER BY c DESC, customer_id ASC"
	stmt, err := parser.Parse(query)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt := stmt.(*parser.SelectStmt)
	if len(selectStmt.GroupBy) != 1 {
		t.Fatalf("expected single GROUP BY expression")
	}
	if _, ok := selectStmt.GroupBy[0].(*parser.ColumnRef); !ok {
		t.Fatalf("expected column reference in GROUP BY")
	}
	if selectStmt.Having == nil {
		t.Fatalf("expected HAVING clause to be parsed")
	}
	if len(selectStmt.OrderBy) != 2 {
		t.Fatalf("expected two ORDER BY terms")
	}
	if !selectStmt.OrderBy[0].Desc {
		t.Fatalf("expected first ORDER BY term to be DESC")
	}
	if selectStmt.OrderBy[1].Desc {
		t.Fatalf("expected second ORDER BY term to default to ASC")
	}
}

func TestParseAggregateFunctions(t *testing.T) {
	stmt, err := parser.Parse("SELECT COUNT(*), COUNT(DISTINCT total) FROM orders")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt := stmt.(*parser.SelectStmt)
	if len(selectStmt.Items) != 2 {
		t.Fatalf("expected two projection items")
	}
	first := selectStmt.Items[0].(*parser.SelectExprItem)
	call, ok := first.Expr.(*parser.FunctionCallExpr)
	if !ok || call.Name != "COUNT" {
		t.Fatalf("expected COUNT function for first expression")
	}
	if len(call.Args) != 1 {
		t.Fatalf("expected COUNT(*) to have a single argument")
	}
	if _, ok := call.Args[0].(*parser.StarExpr); !ok {
		t.Fatalf("expected COUNT(*) to include star argument")
	}

	second := selectStmt.Items[1].(*parser.SelectExprItem)
	call2, ok := second.Expr.(*parser.FunctionCallExpr)
	if !ok || call2.Name != "COUNT" {
		t.Fatalf("expected COUNT function for second expression")
	}
	if !call2.Distinct {
		t.Fatalf("expected DISTINCT flag to be set")
	}
	if len(call2.Args) != 1 {
		t.Fatalf("expected COUNT(DISTINCT ...) to have single argument")
	}
	if _, ok := call2.Args[0].(*parser.ColumnRef); !ok {
		t.Fatalf("expected column reference argument for COUNT(DISTINCT ...)")
	}
}

func TestCreateIndexParsing(t *testing.T) {
	stmt, err := parser.Parse("CREATE INDEX idx_total ON orders(total);")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	create, ok := stmt.(*parser.CreateIndexStmt)
	if !ok {
		t.Fatalf("expected CreateIndexStmt, got %T", stmt)
	}
	if create.Name != "idx_total" || create.Table != "orders" {
		t.Fatalf("unexpected definition: %+v", create)
	}
	if create.Unique {
		t.Fatalf("expected non-unique index")
	}
	if len(create.Columns) != 1 || create.Columns[0] != "total" {
		t.Fatalf("unexpected columns: %+v", create.Columns)
	}
}

func TestCreateUniqueIndexParsing(t *testing.T) {
	stmt, err := parser.Parse("CREATE UNIQUE INDEX idx_name ON customers(name);")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	create := stmt.(*parser.CreateIndexStmt)
	if !create.Unique {
		t.Fatalf("expected UNIQUE flag")
	}
	if create.Table != "customers" {
		t.Fatalf("unexpected table %s", create.Table)
	}
}

func TestDropIndexParsing(t *testing.T) {
	stmt, err := parser.Parse("DROP INDEX idx_total;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	drop, ok := stmt.(*parser.DropIndexStmt)
	if !ok {
		t.Fatalf("expected DropIndexStmt, got %T", stmt)
	}
	if drop.Name != "idx_total" {
		t.Fatalf("unexpected index %s", drop.Name)
	}
}

func TestCreateIndexRequiresColumn(t *testing.T) {
	if _, err := parser.Parse("CREATE INDEX idx ON orders();"); err == nil {
		t.Fatalf("expected parse error for empty column list")
	}
}
