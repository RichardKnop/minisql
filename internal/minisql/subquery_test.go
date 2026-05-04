package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestScalarOperandType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value any
		want  OperandType
	}{
		{"int64", int64(42), OperandInteger},
		{"int32", int32(7), OperandInteger},
		{"float64", float64(3.14), OperandFloat},
		{"float32", float32(1.5), OperandFloat},
		{"bool", true, OperandBoolean},
		{"TimestampMicros", TimestampMicros(123456), OperandQuotedString},
		{"string", "hello", OperandQuotedString},
		{"TextPointer", NewTextPointer([]byte("hi")), OperandQuotedString},
		{"nil", nil, OperandQuotedString},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, scalarOperandType(tc.value))
		})
	}
}
