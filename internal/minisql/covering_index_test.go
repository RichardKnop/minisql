package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// coveringIndexTestColumns mirrors a minimal table: id INT8 PK, email VARCHAR, name VARCHAR.
var coveringIndexTestColumns = []Column{
	{Kind: Int8, Size: 8, Name: "id"},
	{Kind: Varchar, Size: MaxInlineVarchar, Name: "email", Nullable: true},
	{Kind: Varchar, Size: MaxInlineVarchar, Name: "name", Nullable: true},
}

func TestCoveringIndexEligible(t *testing.T) {
	t.Parallel()

	emailCol := coveringIndexTestColumns[1:2] // [email]
	idCol := coveringIndexTestColumns[0:1]    // [id]

	tests := []struct {
		name         string
		stmt         Statement
		indexColumns []Column
		want         bool
	}{
		{
			name:         "SELECT * is never eligible",
			stmt:         Statement{Kind: Select, Fields: []Field{{Name: "*"}}},
			indexColumns: emailCol,
			want:         false,
		},
		{
			name:         "SELECT COUNT(*) is always eligible",
			stmt:         Statement{Kind: Select, Fields: []Field{{Name: "COUNT(*)"}}},
			indexColumns: emailCol,
			want:         true,
		},
		{
			name:         "SELECT indexed column - eligible",
			stmt:         Statement{Kind: Select, Fields: []Field{{Name: "email"}}},
			indexColumns: emailCol,
			want:         true,
		},
		{
			name:         "SELECT non-indexed column - not eligible",
			stmt:         Statement{Kind: Select, Fields: []Field{{Name: "name"}}},
			indexColumns: emailCol,
			want:         false,
		},
		{
			name: "SELECT indexed + non-indexed column - not eligible",
			stmt: Statement{Kind: Select, Fields: []Field{
				{Name: "email"},
				{Name: "name"},
			}},
			indexColumns: emailCol,
			want:         false,
		},
		{
			name: "SELECT two columns both in composite index - eligible",
			stmt: Statement{Kind: Select, Fields: []Field{
				{Name: "email"},
				{Name: "name"},
			}},
			indexColumns: []Column{
				{Kind: Varchar, Size: MaxInlineVarchar, Name: "email"},
				{Kind: Varchar, Size: MaxInlineVarchar, Name: "name"},
			},
			want: true,
		},
		{
			name: "IS NULL condition - not eligible",
			stmt: Statement{
				Kind:   Select,
				Fields: []Field{{Name: "email"}},
				Conditions: OneOrMore{
					{FieldIsNull(Field{Name: "email"})},
				},
			},
			indexColumns: emailCol,
			want:         false,
		},
		{
			name: "WHERE on non-indexed column - not eligible",
			stmt: Statement{
				Kind:   Select,
				Fields: []Field{{Name: "email"}},
				Conditions: OneOrMore{
					{FieldIsEqual(Field{Name: "name"}, OperandQuotedString, "Alice")},
				},
			},
			indexColumns: emailCol,
			want:         false,
		},
		{
			name: "WHERE on indexed column only - eligible",
			stmt: Statement{
				Kind:   Select,
				Fields: []Field{{Name: "email"}},
				Conditions: OneOrMore{
					{FieldIsEqual(Field{Name: "email"}, OperandQuotedString, "alice@example.com")},
				},
			},
			indexColumns: emailCol,
			want:         true,
		},
		{
			name: "ORDER BY indexed column - eligible",
			stmt: Statement{
				Kind:    Select,
				Fields:  []Field{{Name: "email"}},
				OrderBy: []OrderBy{{Field: Field{Name: "email"}}},
			},
			indexColumns: emailCol,
			want:         true,
		},
		{
			name: "ORDER BY non-indexed column - not eligible",
			stmt: Statement{
				Kind:    Select,
				Fields:  []Field{{Name: "email"}},
				OrderBy: []OrderBy{{Field: Field{Name: "name"}}},
			},
			indexColumns: emailCol,
			want:         false,
		},
		{
			name:         "nil Fields without aggregate - not eligible",
			stmt:         Statement{Kind: Select},
			indexColumns: emailCol,
			want:         false,
		},
		{
			name:         "SELECT pk column covered by pk index - eligible",
			stmt:         Statement{Kind: Select, Fields: []Field{{Name: "id"}}},
			indexColumns: idCol,
			want:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := coveringIndexEligible(tt.stmt, tt.indexColumns)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMarkCoveringIndexes(t *testing.T) {
	t.Parallel()

	indexName := "key__test_table__email"
	emailCol := coveringIndexTestColumns[1:2]

	table := NewTable(zap.NewNop(), nil, nil, testTableName, coveringIndexTestColumns, 0, nil,
		WithUniqueIndex(UniqueIndex{
			IndexInfo: IndexInfo{Name: indexName, Columns: emailCol},
		}),
	)

	t.Run("SELECT indexed column sets CoveringIndex", func(t *testing.T) {
		t.Parallel()
		stmt := Statement{
			Kind:   Select,
			Fields: []Field{{Name: "email"}},
			Conditions: OneOrMore{
				{FieldIsEqual(Field{Name: "email"}, OperandQuotedString, NewTextPointer([]byte("a@b.com")))},
			},
		}
		plan, err := table.PlanQuery(context.Background(), stmt)
		require.NoError(t, err)
		require.Len(t, plan.Scans, 1)
		assert.Equal(t, ScanTypeIndexPoint, plan.Scans[0].Type)
		assert.True(t, plan.Scans[0].CoveringIndex)
	})

	t.Run("SELECT * does not set CoveringIndex", func(t *testing.T) {
		t.Parallel()
		stmt := Statement{
			Kind:   Select,
			Fields: []Field{{Name: "*"}},
			Conditions: OneOrMore{
				{FieldIsEqual(Field{Name: "email"}, OperandQuotedString, NewTextPointer([]byte("a@b.com")))},
			},
		}
		plan, err := table.PlanQuery(context.Background(), stmt)
		require.NoError(t, err)
		require.Len(t, plan.Scans, 1)
		assert.Equal(t, ScanTypeIndexPoint, plan.Scans[0].Type)
		assert.False(t, plan.Scans[0].CoveringIndex)
	})

	t.Run("SELECT non-indexed column does not set CoveringIndex", func(t *testing.T) {
		t.Parallel()
		stmt := Statement{
			Kind:   Select,
			Fields: []Field{{Name: "name"}},
			Conditions: OneOrMore{
				{FieldIsEqual(Field{Name: "email"}, OperandQuotedString, NewTextPointer([]byte("a@b.com")))},
			},
		}
		plan, err := table.PlanQuery(context.Background(), stmt)
		require.NoError(t, err)
		require.Len(t, plan.Scans, 1)
		assert.Equal(t, ScanTypeIndexPoint, plan.Scans[0].Type)
		assert.False(t, plan.Scans[0].CoveringIndex)
	})

	t.Run("SELECT COUNT(*) sets CoveringIndex", func(t *testing.T) {
		t.Parallel()
		stmt := Statement{
			Kind:   Select,
			Fields: []Field{{Name: "COUNT(*)"}},
			Conditions: OneOrMore{
				{FieldIsEqual(Field{Name: "email"}, OperandQuotedString, NewTextPointer([]byte("a@b.com")))},
			},
		}
		plan, err := table.PlanQuery(context.Background(), stmt)
		require.NoError(t, err)
		require.Len(t, plan.Scans, 1)
		assert.Equal(t, ScanTypeIndexPoint, plan.Scans[0].Type)
		assert.True(t, plan.Scans[0].CoveringIndex)
	})

	t.Run("Sequential scan never sets CoveringIndex", func(t *testing.T) {
		t.Parallel()
		stmt := Statement{
			Kind:   Select,
			Fields: []Field{{Name: "email"}},
			// Condition on non-indexed column forces sequential scan
			Conditions: OneOrMore{
				{FieldIsEqual(Field{Name: "name"}, OperandQuotedString, "Alice")},
			},
		}
		plan, err := table.PlanQuery(context.Background(), stmt)
		require.NoError(t, err)
		require.Len(t, plan.Scans, 1)
		assert.Equal(t, ScanTypeSequential, plan.Scans[0].Type)
		assert.False(t, plan.Scans[0].CoveringIndex)
	})

	t.Run("DELETE on indexed column does not set CoveringIndex", func(t *testing.T) {
		t.Parallel()
		stmt := Statement{
			Kind:   Delete,
			Fields: []Field{{Name: "email"}},
			Conditions: OneOrMore{
				{FieldIsEqual(Field{Name: "email"}, OperandQuotedString, NewTextPointer([]byte("a@b.com")))},
			},
		}
		plan, err := table.PlanQuery(context.Background(), stmt)
		require.NoError(t, err)
		for _, scan := range plan.Scans {
			assert.False(t, scan.CoveringIndex, "DELETE scan must never be a covering index scan")
		}
	})

	t.Run("UPDATE on indexed column does not set CoveringIndex", func(t *testing.T) {
		t.Parallel()
		stmt := Statement{
			Kind:   Update,
			Fields: []Field{{Name: "email"}},
			Conditions: OneOrMore{
				{FieldIsEqual(Field{Name: "email"}, OperandQuotedString, NewTextPointer([]byte("a@b.com")))},
			},
		}
		plan, err := table.PlanQuery(context.Background(), stmt)
		require.NoError(t, err)
		for _, scan := range plan.Scans {
			assert.False(t, scan.CoveringIndex, "UPDATE scan must never be a covering index scan")
		}
	})
}
