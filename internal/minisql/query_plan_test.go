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
		aTable             = NewTable(zap.NewNop(), nil, nil, "users", columns, 0, WithPrimaryKey(
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
	aTable.SetSecondaryIndex(secondaryIndexName, columns[4:5], nil)

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
						Type: ScanTypeSequential,
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
						FieldIsEqual("name", OperandQuotedString, NewTextPointer([]byte("Richard"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type: ScanTypeSequential,
						Filters: OneOrMore{
							{
								FieldIsEqual("name", OperandQuotedString, NewTextPointer([]byte("Richard"))),
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
						FieldIsEqual("id", OperandInteger, int64(42)),
					},
					{
						FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type:         ScanTypeIndexPoint,
						IndexName:    pkIndexName,
						IndexColumns: columns[0:1],
						IndexKeys:    []any{int64(42)},
					},
					{
						Type:         ScanTypeIndexPoint,
						IndexName:    uniqueIndexName,
						IndexColumns: columns[1:2],
						IndexKeys:    []any{"foo@example.com"},
					},
				},
			},
		},
		{
			"Index scan on unique index and range scan on secondary index",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
					},
					{
						FieldIsGreaterOrEqual("created", OperandQuotedString, MustParseTimestamp("2025-01-01 00:00:00")),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type:         ScanTypeIndexPoint,
						IndexName:    uniqueIndexName,
						IndexColumns: columns[1:2],
						IndexKeys:    []any{"foo@example.com"},
					},
					{
						Type:         ScanTypeIndexRange,
						IndexName:    secondaryIndexName,
						IndexColumns: columns[4:5],
						RangeCondition: RangeCondition{
							Lower: &RangeBound{
								Value:     MustParseTimestamp("2025-01-01 00:00:00").TotalMicroseconds(),
								Inclusive: true,
							},
						},
					},
				},
			},
		},
		{
			"Primary index priority over unique and secondary indexes",
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
						FieldIsGreaterOrEqual("created", OperandQuotedString, MustParseTimestamp("2025-01-01 00:00:00")),
						FieldIsEqual("id", OperandInteger, int64(42)),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type:         ScanTypeIndexPoint,
						IndexName:    pkIndexName,
						IndexColumns: columns[0:1],
						IndexKeys:    []any{int64(42)},
						Filters: OneOrMore{
							{
								FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
								FieldIsGreaterOrEqual("created", OperandQuotedString, MustParseTimestamp("2025-01-01 00:00:00")),
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
						FieldIsEqual("created", OperandQuotedString, MustParseTimestamp("2025-01-01 00:00:00")),
						FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type:         ScanTypeIndexPoint,
						IndexName:    uniqueIndexName,
						IndexColumns: columns[1:2],
						IndexKeys:    []any{"foo@example.com"},
						Filters: OneOrMore{
							{
								FieldIsEqual("created", OperandQuotedString, MustParseTimestamp("2025-01-01 00:00:00")),
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
						FieldIsEqual("dob", OperandQuotedString, MustParseTimestamp("1990-01-01 00:00:00")),
					},
					{
						FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
					},
					{
						FieldIsEqual("name", OperandQuotedString, NewTextPointer([]byte("Richard"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type: ScanTypeSequential,
						Filters: OneOrMore{
							{
								FieldIsEqual("dob", OperandQuotedString, MustParseTimestamp("1990-01-01 00:00:00")),
							},
							{
								FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
							},
							{
								FieldIsEqual("name", OperandQuotedString, NewTextPointer([]byte("Richard"))),
							},
						},
					},
				},
			},
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			actual, err := aTable.PlanQuery(context.Background(), aTestCase.Stmt)
			require.NoError(t, err)
			assert.Equal(t, aTestCase.Expected, actual)
		})
	}
}
