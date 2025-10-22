package validator_test

import (
	"path/filepath"
	"testing"

	"github.com/example/granite-db/engine/internal/catalog"
	"github.com/example/granite-db/engine/internal/sql/expr"
	"github.com/example/granite-db/engine/internal/sql/parser"
	"github.com/example/granite-db/engine/internal/sql/validator"
	"github.com/example/granite-db/engine/internal/storage"
)

func newTestCatalog(t *testing.T, definitions map[string][]catalog.Column) *catalog.Catalog {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.gdb")
	if err := storage.New(path); err != nil {
		t.Fatalf("create storage: %v", err)
	}
	mgr, err := storage.Open(path)
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	t.Cleanup(func() { mgr.Close() })
	cat, err := catalog.Load(mgr)
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	for name, cols := range definitions {
		if _, err := cat.CreateTable(name, cols, "", nil); err != nil {
			t.Fatalf("create table %s: %v", name, err)
		}
	}
	return cat
}

func TestValidateSelectArithmeticTypes(t *testing.T) {
	cat := newTestCatalog(t, map[string][]catalog.Column{
		"people": {
			{Name: "id", Type: catalog.ColumnTypeInt},
			{Name: "age", Type: catalog.ColumnTypeBigInt},
			{Name: "salary", Type: catalog.ColumnTypeBigInt},
		},
	})

	stmt, err := parser.Parse("SELECT id + 1 AS next, age + 1 AS bigger, age + 1.5 AS precise FROM people")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt := stmt.(*parser.SelectStmt)
	validated, err := validator.ValidateSelect(cat, selectStmt)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if len(validated.Outputs) != 3 {
		t.Fatalf("expected 3 outputs, got %d", len(validated.Outputs))
	}
	if validated.Outputs[0].Type.Kind != expr.TypeInt {
		t.Fatalf("expected INT result, got %v", validated.Outputs[0].Type.Kind)
	}
	if validated.Outputs[1].Type.Kind != expr.TypeBigInt {
		t.Fatalf("expected BIGINT result, got %v", validated.Outputs[1].Type.Kind)
	}
	if validated.Outputs[2].Type.Kind != expr.TypeDecimal {
		t.Fatalf("expected DECIMAL result, got %v", validated.Outputs[2].Type.Kind)
	}
}

func TestValidateSelectCoalesce(t *testing.T) {
	cat := newTestCatalog(t, map[string][]catalog.Column{
		"people": {
			{Name: "name", Type: catalog.ColumnTypeVarChar, Length: 50},
			{Name: "nick", Type: catalog.ColumnTypeVarChar, Length: 20},
		},
	})
	stmt, err := parser.Parse("SELECT COALESCE(nick, name) FROM people")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt := stmt.(*parser.SelectStmt)
	validated, err := validator.ValidateSelect(cat, selectStmt)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if len(validated.Outputs) != 1 {
		t.Fatalf("expected 1 output, got %d", len(validated.Outputs))
	}
	if validated.Outputs[0].Type.Kind != expr.TypeVarChar {
		t.Fatalf("expected VARCHAR result, got %v", validated.Outputs[0].Type.Kind)
	}
	if validated.Outputs[0].Name != "COALESCE(nick, name)" {
		t.Fatalf("unexpected derived name: %s", validated.Outputs[0].Name)
	}
}

func TestValidateSelectUnknownColumn(t *testing.T) {
	cat := newTestCatalog(t, map[string][]catalog.Column{
		"people": {{Name: "id", Type: catalog.ColumnTypeInt}},
	})
	stmt, err := parser.Parse("SELECT missing FROM people")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt := stmt.(*parser.SelectStmt)
	if _, err := validator.ValidateSelect(cat, selectStmt); err == nil {
		t.Fatalf("expected validation error for unknown column")
	}
}

func TestValidateSelectFunctionTypeError(t *testing.T) {
	cat := newTestCatalog(t, map[string][]catalog.Column{
		"people": {{Name: "id", Type: catalog.ColumnTypeInt}},
	})
	stmt, err := parser.Parse("SELECT UPPER(id) FROM people")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt := stmt.(*parser.SelectStmt)
	if _, err := validator.ValidateSelect(cat, selectStmt); err == nil {
		t.Fatalf("expected validation error for UPPER on INT")
	}
}

func TestValidateSelectWithoutFrom(t *testing.T) {
	stmt, err := parser.Parse("SELECT 1+2")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt := stmt.(*parser.SelectStmt)
	validated, err := validator.ValidateSelect(nil, selectStmt)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if len(validated.Outputs) != 1 {
		t.Fatalf("expected single output, got %d", len(validated.Outputs))
	}
	if validated.Outputs[0].Type.Kind != expr.TypeInt {
		t.Fatalf("expected INT result, got %v", validated.Outputs[0].Type.Kind)
	}
}

func TestValidateJoinEquiConditions(t *testing.T) {
	cat := newTestCatalog(t, map[string][]catalog.Column{
		"customers": {
			{Name: "id", Type: catalog.ColumnTypeInt},
			{Name: "name", Type: catalog.ColumnTypeVarChar, Length: 50},
		},
		"orders": {
			{Name: "id", Type: catalog.ColumnTypeInt},
			{Name: "customer_id", Type: catalog.ColumnTypeInt},
			{Name: "total", Type: catalog.ColumnTypeBigInt},
		},
	})
	stmt, err := parser.Parse("SELECT c.id, o.total FROM customers c INNER JOIN orders o ON c.id = o.customer_id")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt := stmt.(*parser.SelectStmt)
	validated, err := validator.ValidateSelect(cat, selectStmt)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if len(validated.Joins) != 1 {
		t.Fatalf("expected single join, got %d", len(validated.Joins))
	}
	join := validated.Joins[0]
	if join.Type != validator.JoinTypeInner {
		t.Fatalf("expected INNER join, got %v", join.Type)
	}
	if len(join.EquiConditions) != 1 {
		t.Fatalf("expected one equi condition, got %d", len(join.EquiConditions))
	}
	cond := join.EquiConditions[0]
	leftBinding := validated.Bindings[cond.LeftColumn]
	rightBinding := validated.Bindings[cond.RightColumn]
	if leftBinding.TableAlias != "c" || leftBinding.Column.Name != "id" {
		t.Fatalf("unexpected left binding: %+v", leftBinding)
	}
	if rightBinding.TableAlias != "o" || rightBinding.Column.Name != "customer_id" {
		t.Fatalf("unexpected right binding: %+v", rightBinding)
	}
	if len(join.Residuals) != 0 {
		t.Fatalf("expected no residual predicates")
	}
}

func TestValidateJoinAmbiguousColumn(t *testing.T) {
	cat := newTestCatalog(t, map[string][]catalog.Column{
		"customers": {{Name: "id", Type: catalog.ColumnTypeInt}},
		"orders":    {{Name: "id", Type: catalog.ColumnTypeInt}, {Name: "customer_id", Type: catalog.ColumnTypeInt}},
	})
	stmt, err := parser.Parse("SELECT id FROM customers c JOIN orders o ON c.id = o.customer_id")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt := stmt.(*parser.SelectStmt)
	if _, err := validator.ValidateSelect(cat, selectStmt); err == nil {
		t.Fatalf("expected ambiguous column error")
	}
}

func TestValidateJoinUnknownAlias(t *testing.T) {
	cat := newTestCatalog(t, map[string][]catalog.Column{
		"customers": {{Name: "id", Type: catalog.ColumnTypeInt}},
		"orders":    {{Name: "customer_id", Type: catalog.ColumnTypeInt}},
	})
	stmt, err := parser.Parse("SELECT x.id FROM customers c JOIN orders o ON c.id = o.customer_id")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt := stmt.(*parser.SelectStmt)
	if _, err := validator.ValidateSelect(cat, selectStmt); err == nil {
		t.Fatalf("expected error for unknown alias")
	}
}

func TestValidateJoinOnTypeMismatch(t *testing.T) {
	cat := newTestCatalog(t, map[string][]catalog.Column{
		"customers": {
			{Name: "id", Type: catalog.ColumnTypeInt},
			{Name: "name", Type: catalog.ColumnTypeVarChar, Length: 50},
		},
		"orders": {
			{Name: "customer_id", Type: catalog.ColumnTypeInt},
			{Name: "total", Type: catalog.ColumnTypeBigInt},
		},
	})
	stmt, err := parser.Parse("SELECT c.name FROM customers c JOIN orders o ON c.name = o.total")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt := stmt.(*parser.SelectStmt)
	if _, err := validator.ValidateSelect(cat, selectStmt); err == nil {
		t.Fatalf("expected type mismatch error in JOIN condition")
	}
}

func TestValidateSelectStarWithJoin(t *testing.T) {
	cat := newTestCatalog(t, map[string][]catalog.Column{
		"customers": {
			{Name: "id", Type: catalog.ColumnTypeInt},
			{Name: "name", Type: catalog.ColumnTypeVarChar, Length: 50},
		},
		"orders": {
			{Name: "customer_id", Type: catalog.ColumnTypeInt},
			{Name: "total", Type: catalog.ColumnTypeBigInt},
		},
	})
	stmt, err := parser.Parse("SELECT * FROM customers c JOIN orders o ON c.id = o.customer_id")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt := stmt.(*parser.SelectStmt)
	validated, err := validator.ValidateSelect(cat, selectStmt)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	expectedCols := 4
	if len(validated.Outputs) != expectedCols {
		t.Fatalf("expected %d outputs, got %d", expectedCols, len(validated.Outputs))
	}
	if validated.Outputs[0].Name != "c.id" || validated.Outputs[1].Name != "c.name" {
		t.Fatalf("unexpected column names: %v", []string{validated.Outputs[0].Name, validated.Outputs[1].Name})
	}
}
