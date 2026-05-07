package minisql

import (
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	internalminisql "github.com/RichardKnop/minisql/internal/minisql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToInternalArgs(t *testing.T) {
	t.Parallel()

	t.Run("nil value is preserved as nil element", func(t *testing.T) {
		t.Parallel()

		args := []driver.NamedValue{{Value: nil}}
		got, err := toInternalArgs(args)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Nil(t, got[0])
	})

	t.Run("empty args returns empty slice", func(t *testing.T) {
		t.Parallel()

		got, err := toInternalArgs([]driver.NamedValue{})
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("int64 is passed through unchanged", func(t *testing.T) {
		t.Parallel()

		args := []driver.NamedValue{{Value: int64(42)}}
		got, err := toInternalArgs(args)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, int64(42), got[0])
	})

	t.Run("float64 is passed through unchanged", func(t *testing.T) {
		t.Parallel()

		args := []driver.NamedValue{{Value: float64(3.14)}}
		got, err := toInternalArgs(args)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, float64(3.14), got[0])
	})

	t.Run("bool is passed through unchanged", func(t *testing.T) {
		t.Parallel()

		args := []driver.NamedValue{{Value: true}}
		got, err := toInternalArgs(args)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, true, got[0])
	})

	t.Run("string is converted to TextPointer", func(t *testing.T) {
		t.Parallel()

		args := []driver.NamedValue{{Value: "hello"}}
		got, err := toInternalArgs(args)
		require.NoError(t, err)
		require.Len(t, got, 1)
		// The value should be a TextPointer whose string representation matches.
		type stringer interface{ String() string }
		tp, ok := got[0].(stringer)
		require.True(t, ok, "expected TextPointer to implement String()")
		assert.Equal(t, "hello", tp.String())
	})

	t.Run("time.Time is converted to TimestampMicros", func(t *testing.T) {
		t.Parallel()

		ts := time.Date(2024, 6, 15, 12, 34, 56, 123000, time.UTC)
		args := []driver.NamedValue{{Value: ts}}
		got, err := toInternalArgs(args)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.NotNil(t, got[0])
		assert.Equal(t, "minisql.TimestampMicros", fmt.Sprintf("%T", got[0]))
	})

	t.Run("multiple args of different types", func(t *testing.T) {
		t.Parallel()

		args := []driver.NamedValue{
			{Value: int64(1)},
			{Value: float64(2.5)},
			{Value: true},
			{Value: "text"},
		}
		got, err := toInternalArgs(args)
		require.NoError(t, err)
		require.Len(t, got, 4)
		assert.Equal(t, int64(1), got[0])
		assert.Equal(t, float64(2.5), got[1])
		assert.Equal(t, true, got[2])
		// got[3] is a TextPointer
		assert.NotNil(t, got[3])
	})

	t.Run("unsupported type returns error", func(t *testing.T) {
		t.Parallel()

		type custom struct{ v int }
		args := []driver.NamedValue{{Value: custom{42}}}
		_, err := toInternalArgs(args)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported argument type")
	})
}

func TestResultLastInsertId(t *testing.T) {
	t.Parallel()

	r := Result{rowsAffected: 5}
	id, err := r.LastInsertId()
	require.NoError(t, err)
	assert.Equal(t, int64(0), id) // not yet implemented, always 0
}

func TestResultRowsAffected(t *testing.T) {
	t.Parallel()

	r := Result{rowsAffected: 7}
	n, err := r.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(7), n)
}

func TestStmtCloseNumInputAndUnsupportedLegacyMethods(t *testing.T) {
	t.Parallel()

	stmt := Stmt{
		statement: internalminisql.Statement{
			Kind:      internalminisql.Select,
			TableName: "users",
			Fields:    []internalminisql.Field{{Name: "*"}},
			Conditions: internalminisql.OneOrMore{
				{
					internalminisql.FieldIsEqual(
						internalminisql.Field{Name: "id"},
						internalminisql.OperandPlaceholder,
						internalminisql.Placeholder{},
					),
				},
			},
		},
	}

	require.NoError(t, stmt.Close())
	assert.Equal(t, 1, stmt.NumInput())

	result, err := stmt.Exec(nil)
	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "Exec without context is not supported")

	rows, err := stmt.Query(nil)
	require.Error(t, err)
	assert.Nil(t, rows)
	assert.Contains(t, err.Error(), "Query without context is not supported")
}
