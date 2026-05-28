package minisql

import (
	"context"
	"database/sql/driver"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/minisql"
)

func TestRowsColumnsCloseAndNext(t *testing.T) {
	t.Parallel()

	columns := []minisql.Column{
		{Name: "id", Kind: minisql.Int8, Size: 8},
		{Name: "name", Kind: minisql.Varchar, Size: minisql.MaxInlineVarchar},
		{Name: "created", Kind: minisql.Timestamp, Size: 8},
		{Name: "nullable", Kind: minisql.Varchar, Size: minisql.MaxInlineVarchar},
	}
	ts := minisql.MustParseTimestampMicros("2024-06-15 12:34:56.123456")
	rows := Rows{
		columns: columns,
		iter: minisql.NewSliceIterator([]minisql.Row{
			minisql.NewRowWithValues(columns, []minisql.OptionalValue{
				{Valid: true, Value: int64(1)},
				{Valid: true, Value: minisql.NewTextPointer([]byte("alice"))},
				{Valid: true, Value: minisql.TimestampMicros(ts)},
				{},
			}),
		}),
		ctx: context.Background(),
	}

	assert.Equal(t, []string{"id", "name", "created", "nullable"}, rows.Columns())

	dest := make([]driver.Value, len(columns))
	require.NoError(t, rows.Next(dest))
	assert.Equal(t, int64(1), dest[0])
	assert.Equal(t, "alice", dest[1])
	assert.Equal(t, time.Date(2024, 6, 15, 12, 34, 56, 123456000, time.UTC), dest[2])
	assert.Nil(t, dest[3])

	require.ErrorIs(t, rows.Next(dest), io.EOF)
	require.NoError(t, rows.Close())
}

func TestRowsNextReturnsIteratorError(t *testing.T) {
	t.Parallel()

	wantErr := assert.AnError
	rows := Rows{
		columns: []minisql.Column{{Name: "id", Kind: minisql.Int8}},
		iter: minisql.NewIterator(func(context.Context) (minisql.Row, error) {
			return minisql.Row{}, wantErr
		}),
		ctx: context.Background(),
	}

	err := rows.Next(make([]driver.Value, 1))
	assert.ErrorIs(t, err, wantErr)
}

func TestRowsNextValidatesDestinationWidth(t *testing.T) {
	t.Parallel()

	columns := []minisql.Column{{Name: "id", Kind: minisql.Int8}}
	rows := Rows{
		columns: columns,
		iter: minisql.NewSliceIterator([]minisql.Row{
			minisql.NewRowWithValues(columns, []minisql.OptionalValue{
				{Valid: true, Value: int64(1)},
			}),
		}),
		ctx: context.Background(),
	}

	err := rows.Next(make([]driver.Value, 2))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected 2 values, got 1")
}

func TestRowsNextUsesRowViews(t *testing.T) {
	t.Parallel()

	columns := []minisql.Column{
		{Name: "id", Kind: minisql.Int8, Size: 8},
		{Name: "name", Kind: minisql.Varchar, Size: minisql.MaxInlineVarchar},
		{Name: "created", Kind: minisql.Timestamp, Size: 8},
		{Name: "nullable", Kind: minisql.Varchar, Size: minisql.MaxInlineVarchar},
	}
	ts := minisql.MustParseTimestampMicros("2024-06-15 12:34:56.123456")
	row := minisql.NewRowWithValues(columns, []minisql.OptionalValue{
		{Valid: true, Value: int64(1)},
		{Valid: true, Value: minisql.NewTextPointer([]byte("alice"))},
		{Valid: true, Value: minisql.TimestampMicros(ts)},
		{},
	})
	data, err := row.Marshal()
	require.NoError(t, err)

	rows := Rows{
		columns:             columns,
		rowViewFieldIndexes: []int{0, 1, 2, 3},
		rowViewIter: minisql.NewSliceRowViewIterator([]minisql.RowView{
			minisql.NewRowView(columns, minisql.Cell{
				Value:       data,
				NullBitmask: row.NullBitmask(),
				TypeCodes:   minisql.TypeCodesFromColumns(columns),
				ColumnCount: uint8(len(columns)),
			}),
		}),
		ctx:         context.Background(),
		useRowViews: true,
	}

	dest := make([]driver.Value, len(columns))
	require.NoError(t, rows.Next(dest))
	assert.Equal(t, int64(1), dest[0])
	assert.Equal(t, "alice", dest[1])
	assert.Equal(t, time.Date(2024, 6, 15, 12, 34, 56, 123456000, time.UTC), dest[2])
	assert.Nil(t, dest[3])

	require.ErrorIs(t, rows.Next(dest), io.EOF)
}
