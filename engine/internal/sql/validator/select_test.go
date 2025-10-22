package validator_test

import (
	"testing"

	"github.com/example/granite-db/engine/internal/catalog"
	"github.com/example/granite-db/engine/internal/sql/expr"
	"github.com/example/granite-db/engine/internal/sql/parser"
	"github.com/example/granite-db/engine/internal/sql/validator"
)

func TestValidateSelectArithmeticTypes(t *testing.T) {
	table := &catalog.Table{
		Name: "people",
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.ColumnTypeInt},
			{Name: "age", Type: catalog.ColumnTypeBigInt},
			{Name: "salary", Type: catalog.ColumnTypeBigInt},
		},
	}

	stmt, err := parser.Parse("SELECT id + 1 AS next, age + 1 AS bigger, age + 1.5 AS precise FROM people")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt := stmt.(*parser.SelectStmt)
	validated, err := validator.ValidateSelect(table, selectStmt)
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
	table := &catalog.Table{
		Name: "people",
		Columns: []catalog.Column{
			{Name: "name", Type: catalog.ColumnTypeVarChar, Length: 50},
			{Name: "nick", Type: catalog.ColumnTypeVarChar, Length: 20},
		},
	}
	stmt, err := parser.Parse("SELECT COALESCE(nick, name) FROM people")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt := stmt.(*parser.SelectStmt)
	validated, err := validator.ValidateSelect(table, selectStmt)
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
	table := &catalog.Table{Name: "people", Columns: []catalog.Column{{Name: "id", Type: catalog.ColumnTypeInt}}}
	stmt, err := parser.Parse("SELECT missing FROM people")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt := stmt.(*parser.SelectStmt)
	if _, err := validator.ValidateSelect(table, selectStmt); err == nil {
		t.Fatalf("expected validation error for unknown column")
	}
}

func TestValidateSelectFunctionTypeError(t *testing.T) {
	table := &catalog.Table{Name: "people", Columns: []catalog.Column{{Name: "id", Type: catalog.ColumnTypeInt}}}
	stmt, err := parser.Parse("SELECT UPPER(id) FROM people")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt := stmt.(*parser.SelectStmt)
	if _, err := validator.ValidateSelect(table, selectStmt); err == nil {
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
