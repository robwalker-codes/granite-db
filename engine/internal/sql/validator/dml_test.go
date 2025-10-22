package validator_test

import (
	"testing"

	"github.com/example/granite-db/engine/internal/catalog"
	"github.com/example/granite-db/engine/internal/sql/parser"
	"github.com/example/granite-db/engine/internal/sql/validator"
)

func TestValidateUpdateRejectsTooLongString(t *testing.T) {
	cat := newTestCatalog(t, map[string][]catalog.Column{
		"people": {
			{Name: "id", Type: catalog.ColumnTypeInt},
			{Name: "nick", Type: catalog.ColumnTypeVarChar, Length: 5},
		},
	})
	stmt, err := parser.Parse("UPDATE people SET nick = 'toolong'")
	if err != nil {
		t.Fatalf("parse update: %v", err)
	}
	update := stmt.(*parser.UpdateStmt)
	if _, err := validator.ValidateUpdate(cat, update); err == nil {
		t.Fatalf("expected validation error for oversized VARCHAR assignment")
	}
}

func TestValidateUpdateRejectsDecimalOverflow(t *testing.T) {
	cat := newTestCatalog(t, map[string][]catalog.Column{
		"accounts": {
			{Name: "id", Type: catalog.ColumnTypeInt},
			{Name: "balance", Type: catalog.ColumnTypeDecimal, Precision: 6, Scale: 2},
		},
	})
	stmt, err := parser.Parse("UPDATE accounts SET balance = 1234.567")
	if err != nil {
		t.Fatalf("parse update: %v", err)
	}
	update := stmt.(*parser.UpdateStmt)
	if _, err := validator.ValidateUpdate(cat, update); err == nil {
		t.Fatalf("expected validation error for DECIMAL overflow")
	}
}

func TestValidateUpdateNullability(t *testing.T) {
	cat := newTestCatalog(t, map[string][]catalog.Column{
		"people": {
			{Name: "id", Type: catalog.ColumnTypeInt, NotNull: true},
			{Name: "name", Type: catalog.ColumnTypeVarChar, Length: 32},
		},
	})
	stmtNull, err := parser.Parse("UPDATE people SET name = NULL")
	if err != nil {
		t.Fatalf("parse nullable update: %v", err)
	}
	if _, err := validator.ValidateUpdate(cat, stmtNull.(*parser.UpdateStmt)); err != nil {
		t.Fatalf("unexpected error validating nullable assignment: %v", err)
	}
	stmtNotNull, err := parser.Parse("UPDATE people SET id = NULL")
	if err != nil {
		t.Fatalf("parse not-null update: %v", err)
	}
	if _, err := validator.ValidateUpdate(cat, stmtNotNull.(*parser.UpdateStmt)); err == nil {
		t.Fatalf("expected error assigning NULL to NOT NULL column")
	}
}

func TestValidateUpdateAllowsNumericWidening(t *testing.T) {
	cat := newTestCatalog(t, map[string][]catalog.Column{
		"metrics": {
			{Name: "id", Type: catalog.ColumnTypeInt, NotNull: true},
			{Name: "total", Type: catalog.ColumnTypeBigInt},
		},
		"accounts": {
			{Name: "id", Type: catalog.ColumnTypeInt, NotNull: true},
			{Name: "balance", Type: catalog.ColumnTypeDecimal, Precision: 12, Scale: 2},
		},
	})
	stmtBigInt, err := parser.Parse("UPDATE metrics SET total = 5")
	if err != nil {
		t.Fatalf("parse bigint update: %v", err)
	}
	if _, err := validator.ValidateUpdate(cat, stmtBigInt.(*parser.UpdateStmt)); err != nil {
		t.Fatalf("unexpected error widening INT literal to BIGINT column: %v", err)
	}

	stmtDecimal, err := parser.Parse("UPDATE accounts SET balance = 10")
	if err != nil {
		t.Fatalf("parse decimal update: %v", err)
	}
	if _, err := validator.ValidateUpdate(cat, stmtDecimal.(*parser.UpdateStmt)); err != nil {
		t.Fatalf("unexpected error widening INT literal to DECIMAL column: %v", err)
	}
}

func TestValidateDeleteRequiresBooleanPredicate(t *testing.T) {
	cat := newTestCatalog(t, map[string][]catalog.Column{
		"people": {{Name: "id", Type: catalog.ColumnTypeInt}},
	})
	stmt, err := parser.Parse("DELETE FROM people WHERE id + 1")
	if err != nil {
		t.Fatalf("parse delete: %v", err)
	}
	deleteStmt := stmt.(*parser.DeleteStmt)
	if _, err := validator.ValidateDelete(cat, deleteStmt); err == nil {
		t.Fatalf("expected validation error for non-boolean WHERE clause")
	}
}
