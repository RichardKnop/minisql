package minisql

import (
	"context"
	"database/sql/driver"
	"io"
	"testing"
	"time"

	internalminisql "github.com/RichardKnop/minisql/internal/minisql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRowsColumnsCloseAndNext(t *testing.T) {
	t.Parallel()

	columns := []internalminisql.Column{
		{Name: "id", Kind: internalminisql.Int8},
		{Name: "name", Kind: internalminisql.Text},
		{Name: "created", Kind: internalminisql.Timestamp},
		{Name: "nullable", Kind: internalminisql.Text},
	}
	ts := internalminisql.MustParseTimestampMicros("2024-06-15 12:34:56.123456")
	rows := Rows{
		columns: columns,
		iter: internalminisql.NewSliceIterator([]internalminisql.Row{
			internalminisql.NewRowWithValues(columns, []internalminisql.OptionalValue{
				{Valid: true, Value: int64(1)},
				{Valid: true, Value: internalminisql.NewTextPointer([]byte("alice"))},
				{Valid: true, Value: internalminisql.TimestampMicros(ts)},
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

	assert.ErrorIs(t, rows.Next(dest), io.EOF)
	require.NoError(t, rows.Close())
}

func TestRowsNextReturnsIteratorError(t *testing.T) {
	t.Parallel()

	wantErr := assert.AnError
	rows := Rows{
		columns: []internalminisql.Column{{Name: "id", Kind: internalminisql.Int8}},
		iter: internalminisql.NewIterator(func(context.Context) (internalminisql.Row, error) {
			return internalminisql.Row{}, wantErr
		}),
		ctx: context.Background(),
	}

	err := rows.Next(make([]driver.Value, 1))
	assert.ErrorIs(t, err, wantErr)
}

func TestRowsNextValidatesDestinationWidth(t *testing.T) {
	t.Parallel()

	columns := []internalminisql.Column{{Name: "id", Kind: internalminisql.Int8}}
	rows := Rows{
		columns: columns,
		iter: internalminisql.NewSliceIterator([]internalminisql.Row{
			internalminisql.NewRowWithValues(columns, []internalminisql.OptionalValue{
				{Valid: true, Value: int64(1)},
			}),
		}),
		ctx: context.Background(),
	}

	err := rows.Next(make([]driver.Value, 2))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected 2 values, got 1")
}
