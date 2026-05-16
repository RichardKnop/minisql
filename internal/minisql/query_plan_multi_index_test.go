package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTable_PlanQuery_MultipleIndexes(t *testing.T) {
	t.Parallel()

	var (
		columns = []Column{
			{
				Kind: Int8,
				Size: 8,
				Name: "id",
			},
			{
				Kind: Varchar,
				Size: MaxInlineVarchar,
				Name: "email",
			},
			{
				Kind:     Text,
				Name:     "Name",
				Nullable: true,
			},
			{
				Kind: Timestamp,
				Name: "dob",
			},
			{
				Kind:            Timestamp,
				Name:            "created",
				DefaultValueNow: true,
				// secondary index on this column
			},
		}
		pkIndexName        = "pkey__users"
		uniqueIndexName    = "key__users__email"
		secondaryIndexName = "idx__users__created"
		table              = NewTable(zap.NewNop(), nil, nil, "users", columns, 0, nil, WithPrimaryKey(
			NewPrimaryKey(pkIndexName, columns[0:1], true),
		), WithUniqueIndex(
			UniqueIndex{
				IndexInfo: IndexInfo{
					Name:    uniqueIndexName,
					Columns: columns[1:2],
				},
			},
		))
	)
	table.SetSecondaryIndex(SecondaryIndex{IndexInfo: IndexInfo{Name: secondaryIndexName, Columns: columns[4:5]}})

	testCases := []struct {
		Name     string
		Stmt     Statement
		Expected QueryPlan
	}{
		{
			"Sequential scan",
			Statement{
				Kind: Select,
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName: "users",
						Type:      ScanTypeSequential,
					},
				},
			},
		},
		{
			"Sequential scan with filters",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual(Field{Name: "name"}, OperandQuotedString, NewTextPointer([]byte("Richard"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName: "users",
						Type:      ScanTypeSequential,
						Filters: OneOrMore{
							{
								FieldIsEqual(Field{Name: "name"}, OperandQuotedString, NewTextPointer([]byte("Richard"))),
							},
						},
					},
				},
			},
		},
		{
			"Two index point scans for both primary and unique indexes",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual(Field{Name: "id"}, OperandInteger, int64(42)),
					},
					{
						FieldIsEqual(Field{Name: "email"}, OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{{
					TableName: "users",
					Type:      ScanTypeIndexUnion,
					SubScans: []Scan{
						{
							TableName:    "users",
							Type:         ScanTypeIndexPoint,
							IndexName:    pkIndexName,
							IndexColumns: columns[0:1],
							IndexKeys:    []any{int64(42)},
						},
						{
							TableName:    "users",
							Type:         ScanTypeIndexPoint,
							IndexName:    uniqueIndexName,
							IndexColumns: columns[1:2],
							IndexKeys:    []any{"foo@example.com"},
						},
					},
					Filters: OneOrMore{
						{FieldIsEqual(Field{Name: "id"}, OperandInteger, int64(42))},
						{FieldIsEqual(Field{Name: "email"}, OperandQuotedString, NewTextPointer([]byte("foo@example.com")))},
					},
				}},
			},
		},
		{
			"Index scan on unique index and range scan on secondary index",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual(Field{Name: "email"}, OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
					},
					{
						FieldIsGreaterOrEqual(Field{Name: "created"}, OperandQuotedString, MustParseTimestampMicros("2025-01-01 00:00:00")),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{{
					TableName: "users",
					Type:      ScanTypeIndexUnion,
					SubScans: []Scan{
						{
							TableName:    "users",
							Type:         ScanTypeIndexPoint,
							IndexName:    uniqueIndexName,
							IndexColumns: columns[1:2],
							IndexKeys:    []any{"foo@example.com"},
						},
						{
							TableName:    "users",
							Type:         ScanTypeIndexRange,
							IndexName:    secondaryIndexName,
							IndexColumns: columns[4:5],
							RangeCondition: RangeCondition{
								Lower: &RangeBound{
									Value:     int64(MustParseTimestampMicros("2025-01-01 00:00:00")),
									Inclusive: true,
								},
							},
						},
					},
					Filters: OneOrMore{
						{FieldIsEqual(Field{Name: "email"}, OperandQuotedString, NewTextPointer([]byte("foo@example.com")))},
						{FieldIsGreaterOrEqual(Field{Name: "created"}, OperandQuotedString, MustParseTimestampMicros("2025-01-01 00:00:00"))},
					},
				}},
			},
		},
		{
			"Primary index priority over unique and secondary indexes",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual(Field{Name: "email"}, OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
						FieldIsGreaterOrEqual(Field{Name: "created"}, OperandQuotedString, MustParseTimestampMicros("2025-01-01 00:00:00")),
						FieldIsEqual(Field{Name: "id"}, OperandInteger, int64(42)),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName:    "users",
						Type:         ScanTypeIndexPoint,
						IndexName:    pkIndexName,
						IndexColumns: columns[0:1],
						IndexKeys:    []any{int64(42)},
						Filters: OneOrMore{
							{
								FieldIsEqual(Field{Name: "email"}, OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
								FieldIsGreaterOrEqual(Field{Name: "created"}, OperandQuotedString, MustParseTimestampMicros("2025-01-01 00:00:00")),
							},
						},
					},
				},
			},
		},
		{
			"Unique index priority over secondary index",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual(Field{Name: "created"}, OperandQuotedString, MustParseTimestampMicros("2025-01-01 00:00:00")),
						FieldIsEqual(Field{Name: "email"}, OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName:    "users",
						Type:         ScanTypeIndexPoint,
						IndexName:    uniqueIndexName,
						IndexColumns: columns[1:2],
						IndexKeys:    []any{"foo@example.com"},
						Filters: OneOrMore{
							{
								FieldIsEqual(Field{Name: "created"}, OperandQuotedString, MustParseTimestampMicros("2025-01-01 00:00:00")),
							},
						},
					},
				},
			},
		},
		{
			"Combine sequential scans",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual(Field{Name: "dob"}, OperandQuotedString, MustParseTimestampMicros("1990-01-01 00:00:00")),
					},
					{
						FieldIsEqual(Field{Name: "email"}, OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
					},
					{
						FieldIsEqual(Field{Name: "name"}, OperandQuotedString, NewTextPointer([]byte("Richard"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						TableName: "users",
						Type:      ScanTypeSequential,
						Filters: OneOrMore{
							{
								FieldIsEqual(Field{Name: "dob"}, OperandQuotedString, MustParseTimestampMicros("1990-01-01 00:00:00")),
							},
							{
								FieldIsEqual(Field{Name: "email"}, OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
							},
							{
								FieldIsEqual(Field{Name: "name"}, OperandQuotedString, NewTextPointer([]byte("Richard"))),
							},
						},
					},
				},
			},
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			actual, err := table.PlanQuery(context.Background(), aTestCase.Stmt)
			require.NoError(t, err)
			assert.Equal(t, aTestCase.Expected, actual)
		})
	}
}

func TestTable_PlanQuery_Intersection(t *testing.T) {
	t.Parallel()

	var (
		columns = []Column{
			{Kind: Int8, Size: 8, Name: "id"},
			{Kind: Varchar, Size: MaxInlineVarchar, Name: "email"},
			{Kind: Text, Name: "name", Nullable: true},
			{Kind: Timestamp, Name: "dob"},
			{Kind: Timestamp, Name: "created", DefaultValueNow: true},
		}
		pkIndexName     = "pkey__users"
		uniqueIndexName = "key__users__email"
		idxCreated      = "idx__users__created"
		idxDob          = "idx__users__dob"
	)

	newTable := func() *Table {
		t.Helper()
		tbl := NewTable(zap.NewNop(), nil, nil, "users", columns, 0, nil, WithPrimaryKey(
			NewPrimaryKey(pkIndexName, columns[0:1], true),
		), WithUniqueIndex(
			UniqueIndex{IndexInfo: IndexInfo{Name: uniqueIndexName, Columns: columns[1:2]}},
		))
		tbl.SetSecondaryIndex(SecondaryIndex{IndexInfo: IndexInfo{Name: idxCreated, Columns: columns[4:5]}})
		tbl.SetSecondaryIndex(SecondaryIndex{IndexInfo: IndexInfo{Name: idxDob, Columns: columns[3:4]}})
		return tbl
	}

	subIndexNames := func(scan Scan) []string {
		names := make([]string, len(scan.SubScans))
		for i, s := range scan.SubScans {
			names[i] = s.IndexName
		}
		return names
	}

	t.Run("two secondary equality conditions → intersection", func(t *testing.T) {
		t.Parallel()
		tbl := newTable()
		stmt := Statement{
			Kind: Select,
			Conditions: OneOrMore{
				{
					FieldIsEqual(Field{Name: "dob"}, OperandQuotedString, MustParseTimestampMicros("1990-01-01 00:00:00")),
					FieldIsEqual(Field{Name: "created"}, OperandQuotedString, MustParseTimestampMicros("2025-01-01 00:00:00")),
				},
			},
		}
		plan, err := tbl.PlanQuery(context.Background(), stmt)
		require.NoError(t, err)
		require.Len(t, plan.Scans, 1)
		assert.Equal(t, ScanTypeIndexIntersect, plan.Scans[0].Type)
		require.Len(t, plan.Scans[0].SubScans, 2)
		assert.ElementsMatch(t, []string{idxDob, idxCreated}, subIndexNames(plan.Scans[0]))
		for _, sub := range plan.Scans[0].SubScans {
			assert.Equal(t, ScanTypeIndexPoint, sub.Type)
			assert.Len(t, sub.IndexKeys, 1)
		}
		assert.Nil(t, plan.Scans[0].Filters)
	})

	t.Run("two secondary range conditions → intersection", func(t *testing.T) {
		t.Parallel()
		tbl := newTable()
		stmt := Statement{
			Kind: Select,
			Conditions: OneOrMore{
				{
					FieldIsGreaterOrEqual(Field{Name: "created"}, OperandQuotedString, MustParseTimestampMicros("2025-01-01 00:00:00")),
					FieldIsGreaterOrEqual(Field{Name: "dob"}, OperandQuotedString, MustParseTimestampMicros("1990-01-01 00:00:00")),
				},
			},
		}
		plan, err := tbl.PlanQuery(context.Background(), stmt)
		require.NoError(t, err)
		require.Len(t, plan.Scans, 1)
		assert.Equal(t, ScanTypeIndexIntersect, plan.Scans[0].Type)
		require.Len(t, plan.Scans[0].SubScans, 2)
		assert.ElementsMatch(t, []string{idxCreated, idxDob}, subIndexNames(plan.Scans[0]))
		for _, sub := range plan.Scans[0].SubScans {
			assert.Equal(t, ScanTypeIndexRange, sub.Type)
			assert.NotNil(t, sub.RangeCondition.Lower)
		}
	})

	t.Run("unique 1-key stays single-index (no intersection overhead)", func(t *testing.T) {
		t.Parallel()
		tbl := newTable()
		stmt := Statement{
			Kind: Select,
			Conditions: OneOrMore{
				{
					FieldIsEqual(Field{Name: "email"}, OperandQuotedString, NewTextPointer([]byte("a@b.com"))),
					FieldIsEqual(Field{Name: "created"}, OperandQuotedString, MustParseTimestampMicros("2025-01-01 00:00:00")),
				},
			},
		}
		plan, err := tbl.PlanQuery(context.Background(), stmt)
		require.NoError(t, err)
		require.Len(t, plan.Scans, 1)
		assert.Equal(t, ScanTypeIndexPoint, plan.Scans[0].Type)
		assert.Equal(t, uniqueIndexName, plan.Scans[0].IndexName)
		assert.Len(t, plan.Scans[0].Filters, 1) // created goes to filter
	})

	t.Run("PK 1-key stays single-index", func(t *testing.T) {
		t.Parallel()
		tbl := newTable()
		stmt := Statement{
			Kind: Select,
			Conditions: OneOrMore{
				{
					FieldIsEqual(Field{Name: "id"}, OperandInteger, int64(42)),
					FieldIsEqual(Field{Name: "created"}, OperandQuotedString, MustParseTimestampMicros("2025-01-01 00:00:00")),
				},
			},
		}
		plan, err := tbl.PlanQuery(context.Background(), stmt)
		require.NoError(t, err)
		require.Len(t, plan.Scans, 1)
		assert.Equal(t, ScanTypeIndexPoint, plan.Scans[0].Type)
		assert.Equal(t, pkIndexName, plan.Scans[0].IndexName)
	})

	t.Run("secondary point + extra non-indexed condition → filter on parent", func(t *testing.T) {
		t.Parallel()
		tbl := newTable()
		stmt := Statement{
			Kind: Select,
			Conditions: OneOrMore{
				{
					FieldIsEqual(Field{Name: "dob"}, OperandQuotedString, MustParseTimestampMicros("1990-01-01 00:00:00")),
					FieldIsEqual(Field{Name: "created"}, OperandQuotedString, MustParseTimestampMicros("2025-01-01 00:00:00")),
					FieldIsEqual(Field{Name: "name"}, OperandQuotedString, NewTextPointer([]byte("Alice"))),
				},
			},
		}
		plan, err := tbl.PlanQuery(context.Background(), stmt)
		require.NoError(t, err)
		require.Len(t, plan.Scans, 1)
		assert.Equal(t, ScanTypeIndexIntersect, plan.Scans[0].Type)
		require.Len(t, plan.Scans[0].SubScans, 2)
		// name is non-indexed → captured in parent Filters
		assert.Equal(t, 1, conditionCount(plan.Scans[0].Filters))
	})
}
