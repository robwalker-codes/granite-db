package exec

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/example/granite-db/engine/internal/catalog"
)

// EncodeRow serialises a row according to the table schema.
func EncodeRow(columns []catalog.Column, values []interface{}) ([]byte, error) {
	buf := make([]byte, 0, 128)
	for i, col := range columns {
		value := values[i]
		if value == nil {
			buf = append(buf, 0)
			continue
		}
		buf = append(buf, 1)
		switch col.Type {
		case catalog.ColumnTypeInt:
			v, ok := value.(int32)
			if !ok {
				return nil, fmt.Errorf("exec: expected int32 value for column %s", col.Name)
			}
			tmp := make([]byte, 4)
			binary.LittleEndian.PutUint32(tmp, uint32(v))
			buf = append(buf, tmp...)
		case catalog.ColumnTypeBigInt:
			v, ok := value.(int64)
			if !ok {
				return nil, fmt.Errorf("exec: expected int64 value for column %s", col.Name)
			}
			tmp := make([]byte, 8)
			binary.LittleEndian.PutUint64(tmp, uint64(v))
			buf = append(buf, tmp...)
		case catalog.ColumnTypeBoolean:
			v, ok := value.(bool)
			if !ok {
				return nil, fmt.Errorf("exec: expected bool value for column %s", col.Name)
			}
			if v {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		case catalog.ColumnTypeVarChar:
			str, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("exec: expected string value for column %s", col.Name)
			}
			if col.Length > 0 && len(str) > col.Length {
				return nil, fmt.Errorf("exec: value for column %s exceeds length %d", col.Name, col.Length)
			}
			if len(str) > 0xFFFF {
				return nil, fmt.Errorf("exec: value for column %s too long", col.Name)
			}
			tmp := make([]byte, 2)
			binary.LittleEndian.PutUint16(tmp, uint16(len(str)))
			buf = append(buf, tmp...)
			buf = append(buf, []byte(str)...)
		case catalog.ColumnTypeDate:
			t, ok := value.(time.Time)
			if !ok {
				return nil, fmt.Errorf("exec: expected time value for column %s", col.Name)
			}
			unix := t.UTC().Truncate(24 * time.Hour).Unix()
			days := uint32(unix / 86400)
			tmp := make([]byte, 4)
			binary.LittleEndian.PutUint32(tmp, days)
			buf = append(buf, tmp...)
		case catalog.ColumnTypeTimestamp:
			t, ok := value.(time.Time)
			if !ok {
				return nil, fmt.Errorf("exec: expected time value for column %s", col.Name)
			}
			nanos := t.UTC().UnixNano()
			tmp := make([]byte, 8)
			binary.LittleEndian.PutUint64(tmp, uint64(nanos))
			buf = append(buf, tmp...)
		default:
			return nil, fmt.Errorf("exec: unsupported column type %d", col.Type)
		}
	}
	return buf, nil
}

// DecodeRow converts a binary record into typed values following the schema.
func DecodeRow(columns []catalog.Column, data []byte) ([]interface{}, error) {
	values := make([]interface{}, len(columns))
	pos := 0
	for i, col := range columns {
		if pos >= len(data) {
			return nil, fmt.Errorf("exec: truncated record for column %s", col.Name)
		}
		flag := data[pos]
		pos++
		if flag == 0 {
			values[i] = nil
			continue
		}
		switch col.Type {
		case catalog.ColumnTypeInt:
			if pos+4 > len(data) {
				return nil, fmt.Errorf("exec: truncated record for column %s", col.Name)
			}
			v := int32(binary.LittleEndian.Uint32(data[pos : pos+4]))
			values[i] = v
			pos += 4
		case catalog.ColumnTypeBigInt:
			if pos+8 > len(data) {
				return nil, fmt.Errorf("exec: truncated record for column %s", col.Name)
			}
			v := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
			values[i] = v
			pos += 8
		case catalog.ColumnTypeBoolean:
			if pos+1 > len(data) {
				return nil, fmt.Errorf("exec: truncated record for column %s", col.Name)
			}
			values[i] = data[pos] == 1
			pos++
		case catalog.ColumnTypeVarChar:
			if pos+2 > len(data) {
				return nil, fmt.Errorf("exec: truncated record for column %s", col.Name)
			}
			length := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
			pos += 2
			if pos+length > len(data) {
				return nil, fmt.Errorf("exec: truncated record for column %s", col.Name)
			}
			values[i] = string(data[pos : pos+length])
			pos += length
		case catalog.ColumnTypeDate:
			if pos+4 > len(data) {
				return nil, fmt.Errorf("exec: truncated record for column %s", col.Name)
			}
			days := binary.LittleEndian.Uint32(data[pos : pos+4])
			pos += 4
			values[i] = time.Unix(int64(days)*86400, 0).UTC()
		case catalog.ColumnTypeTimestamp:
			if pos+8 > len(data) {
				return nil, fmt.Errorf("exec: truncated record for column %s", col.Name)
			}
			nanos := binary.LittleEndian.Uint64(data[pos : pos+8])
			pos += 8
			values[i] = time.Unix(0, int64(nanos)).UTC()
		default:
			return nil, fmt.Errorf("exec: unsupported column type %d", col.Type)
		}
	}
	if pos != len(data) {
		return nil, fmt.Errorf("exec: record length mismatch (expected %d, used %d)", len(data), pos)
	}
	return values, nil
}
