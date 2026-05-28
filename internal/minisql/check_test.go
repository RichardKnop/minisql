package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateCheckConstraints(t *testing.T) {
	t.Parallel()

	priceGtZero := &ConditionNode{
		Leaf: &Condition{
			Operand1: Operand{Type: OperandField, Value: Field{Name: "price"}},
			Operator: Gt,
			Operand2: Operand{Type: OperandInteger, Value: int64(0)},
		},
	}

	columns := []Column{
		{Name: "id", Kind: Int8, Size: 8},
		{Name: "price", Kind: Int8, Size: 8, Check: "price > 0", CheckCond: priceGtZero},
	}

	makeRow := func(id, price int64) Row {
		return NewRowWithValues(columns, []OptionalValue{
			{Value: id, Valid: true},
			{Value: price, Valid: true},
		})
	}

	t.Run("passes when all checks satisfied", func(t *testing.T) {
		t.Parallel()
		err := validateCheckConstraints(columns, makeRow(1, 10))
		require.NoError(t, err)
	})

	t.Run("fails when check violated", func(t *testing.T) {
		t.Parallel()
		err := validateCheckConstraints(columns, makeRow(1, 0))
		require.Error(t, err)
		var checkErr ErrCheckConstraintViolation
		require.ErrorAs(t, err, &checkErr)
		assert.Equal(t, "price", checkErr.ColumnName)
		assert.Equal(t, "price > 0", checkErr.Expr)
	})

	t.Run("no check constraints passes always", func(t *testing.T) {
		t.Parallel()
		bare := []Column{
			{Name: "id", Kind: Int8, Size: 8},
			{Name: "name", Kind: Text},
		}
		row := NewRowWithValues(bare, []OptionalValue{
			{Value: int64(1), Valid: true},
			{Value: NewTextPointer([]byte("foo")), Valid: true},
		})
		err := validateCheckConstraints(bare, row)
		require.NoError(t, err)
	})

	t.Run("error message contains column name and expression", func(t *testing.T) {
		t.Parallel()
		err := validateCheckConstraints(columns, makeRow(1, 0))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "price")
		assert.Contains(t, err.Error(), "price > 0")
	})
}

func TestErrCheckConstraintViolation_Error(t *testing.T) {
	t.Parallel()

	err := ErrCheckConstraintViolation{ColumnName: "age", Expr: "age >= 18"}
	assert.Contains(t, err.Error(), "age")
	assert.Contains(t, err.Error(), "age >= 18")
}
