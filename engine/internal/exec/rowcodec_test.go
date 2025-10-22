package exec

import (
	"testing"
	"time"

	"github.com/example/granite-db/engine/internal/catalog"
)

func TestRowCodecRoundTrip(t *testing.T) {
	columns := []catalog.Column{
		{Name: "id", Type: catalog.ColumnTypeInt},
		{Name: "name", Type: catalog.ColumnTypeVarChar, Length: 50},
		{Name: "active", Type: catalog.ColumnTypeBoolean},
		{Name: "joined", Type: catalog.ColumnTypeDate},
		{Name: "updated", Type: catalog.ColumnTypeTimestamp},
	}
	joined := time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC)
	updated := time.Date(2023, 5, 1, 12, 30, 0, 0, time.UTC)
	values := []interface{}{int32(7), "Ada", true, joined, updated}

	encoded, err := EncodeRow(columns, values)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := DecodeRow(columns, encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded) != len(values) {
		t.Fatalf("expected %d values, got %d", len(values), len(decoded))
	}
	if decoded[0].(int32) != values[0].(int32) {
		t.Fatalf("int mismatch: got %v want %v", decoded[0], values[0])
	}
	if decoded[1].(string) != values[1].(string) {
		t.Fatalf("string mismatch: got %v want %v", decoded[1], values[1])
	}
	if decoded[2].(bool) != values[2].(bool) {
		t.Fatalf("bool mismatch: got %v want %v", decoded[2], values[2])
	}
	if !decoded[3].(time.Time).Equal(joined) {
		t.Fatalf("date mismatch: got %v want %v", decoded[3], joined)
	}
	if !decoded[4].(time.Time).Equal(updated.UTC()) {
		t.Fatalf("timestamp mismatch: got %v want %v", decoded[4], updated)
	}
}
