package exec

import (
        "encoding/binary"
        "fmt"
        "time"

        "github.com/shopspring/decimal"

        "github.com/example/granite-db/engine/internal/catalog"
)

func buildIndexComponents(cols []catalog.Column, order []int, values []interface{}) ([][]byte, bool, error) {
        components := make([][]byte, len(order))
        for i, idx := range order {
                value := values[idx]
                if value == nil {
                        return nil, true, nil
                }
                comp, err := encodeComponent(cols[idx], value)
                if err != nil {
                        return nil, false, err
                }
                components[i] = comp
        }
        return components, false, nil
}

func encodeIndexKey(components [][]byte) []byte {
        size := 0
        for _, comp := range components {
                size += 2 + len(comp)
        }
        key := make([]byte, 0, size)
        for _, comp := range components {
                var lenBuf [2]byte
                binary.BigEndian.PutUint16(lenBuf[:], uint16(len(comp)))
                key = append(key, lenBuf[:]...)
                key = append(key, comp...)
        }
        return key
}

func encodeComponent(col catalog.Column, value interface{}) ([]byte, error) {
        switch col.Type {
        case catalog.ColumnTypeInt:
                v, ok := value.(int32)
                if !ok {
                        return nil, fmt.Errorf("exec: expected INT value for column %s", col.Name)
                }
                return encodeInt64(int64(v)), nil
        case catalog.ColumnTypeBigInt:
                v, ok := value.(int64)
                if !ok {
                        return nil, fmt.Errorf("exec: expected BIGINT value for column %s", col.Name)
                }
                return encodeInt64(v), nil
        case catalog.ColumnTypeBoolean:
                b, ok := value.(bool)
                if !ok {
                        return nil, fmt.Errorf("exec: expected BOOLEAN value for column %s", col.Name)
                }
                if b {
                        return []byte{1}, nil
                }
                return []byte{0}, nil
        case catalog.ColumnTypeVarChar:
                str, ok := value.(string)
                if !ok {
                        return nil, fmt.Errorf("exec: expected VARCHAR value for column %s", col.Name)
                }
                return []byte(str), nil
        case catalog.ColumnTypeDate:
                t, ok := value.(time.Time)
                if !ok {
                        return nil, fmt.Errorf("exec: expected DATE value for column %s", col.Name)
                }
                days := t.UTC().Truncate(24 * time.Hour).Unix() / 86400
                return encodeInt64(days), nil
        case catalog.ColumnTypeTimestamp:
                t, ok := value.(time.Time)
                if !ok {
                        return nil, fmt.Errorf("exec: expected TIMESTAMP value for column %s", col.Name)
                }
                return encodeInt64(t.UTC().UnixNano()), nil
        case catalog.ColumnTypeDecimal:
                decValue, ok := value.(decimal.Decimal)
                if !ok {
                        return nil, fmt.Errorf("exec: expected DECIMAL value for column %s", col.Name)
                }
                intVal, err := decimalToScaledInt(decValue, col.Scale)
                if err != nil {
                        return nil, err
                }
                return encodeInt64(intVal), nil
        default:
                return nil, fmt.Errorf("exec: unsupported index column type %d", col.Type)
        }
}

func encodeInt64(v int64) []byte {
        var buf [8]byte
        u := uint64(v) ^ (uint64(1) << 63)
        binary.BigEndian.PutUint64(buf[:], u)
        return buf[:]
}

func decimalToScaledInt(dec decimal.Decimal, scale int) (int64, error) {
        shifted := dec.Shift(int32(scale))
        if shifted.Exponent() != 0 {
                        return 0, fmt.Errorf("exec: decimal scaling mismatch")
        }
        coeff := shifted.Coefficient()
        if coeff.BitLen() > 63 {
                return 0, fmt.Errorf("exec: DECIMAL value exceeds index encoding range")
        }
        v := coeff.Int64()
        if shifted.Sign() < 0 {
                v = -v
        }
        return v, nil
}
