package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTable_PlanQuery_CompositeIndex(t *testing.T) {
	t.Parallel()
	var (
		compositeColumns = []Column{
			{Kind: Int8, Size: 8, Name: "id"},
			{Kind: Varchar, Size: 100, Name: "first_name"},
			{Kind: Varchar, Size: 100, Name: "last_name"},
			{Kind: Varchar, Size: 100, Name: "email"},
			{Kind: Int4, Size: 4, Name: "age"},
		}

		// Table with composite primary key on (first_name, last_name)
		compositePKIndexName = "pkey__users"
		tableWithCompositePK = NewTable(
			zap.NewNop(), nil, nil, "users",
			compositeColumns,
			0,
			WithPrimaryKey(NewPrimaryKey(compositePKIndexName, compositeColumns[1:3], false)),
		)

		// Table with composite unique index on (first_name, last_name)
		compositeUniqueIndexName = "idx_name"
		tableWithCompositeUnique = NewTable(
			zap.NewNop(), nil, nil, "users",
			compositeColumns,
			0,
			WithPrimaryKey(NewPrimaryKey("pkey__users", compositeColumns[0:1], true)),
			WithUniqueIndex(UniqueIndex{
				IndexInfo: IndexInfo{
					Name:    compositeUniqueIndexName,
					Columns: compositeColumns[1:3],
				},
			}),
		)

		// Table with composite secondary index on (first_name, last_name)
		compositeSecondaryIndexName = "idx_name"
		tableWithCompositeSecondary = NewTable(
			zap.NewNop(), nil, nil, "users",
			compositeColumns,
			0,
			WithPrimaryKey(NewPrimaryKey("pkey__users", compositeColumns[0:1], true)),
			WithSecondaryIndex(SecondaryIndex{
				IndexInfo: IndexInfo{
					Name:    compositeSecondaryIndexName,
					Columns: compositeColumns[1:3],
				},
			}),
		)
	)

	testCases := []struct {
		Name     string
		Table    *Table
		Stmt     Statement
		Expected QueryPlan
	}{
		{
			"Composite PK: Match both columns - index point scan",
			tableWithCompositePK,
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual("first_name", OperandQuotedString, NewTextPointer([]byte("John"))),
						FieldIsEqual("last_name", OperandQuotedString, NewTextPointer([]byte("Doe"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type:         ScanTypeIndexPoint,
						IndexName:    compositePKIndexName,
						IndexColumns: compositeColumns[1:3],
						IndexKeys: []any{
							NewCompositeKey(compositeColumns[1:3], "John", "Doe"),
						},
					},
				},
			},
		},
		{
			"Composite PK: Match only first column - index range scan with partial key",
			tableWithCompositePK,
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual("first_name", OperandQuotedString, NewTextPointer([]byte("John"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type:         ScanTypeIndexRange,
						IndexName:    compositePKIndexName,
						IndexColumns: compositeColumns[1:3],
						RangeCondition: RangeCondition{
							Lower: &RangeBound{
								Value:     NewCompositeKey(compositeColumns[1:2], "John"),
								Inclusive: true,
							},
							Upper: &RangeBound{
								Value:     NewCompositeKey(compositeColumns[1:2], "John\xFF"),
								Inclusive: false,
							},
						},
					},
				},
			},
		},
		{
			"Composite PK: Match only second column - sequential scan (not a prefix)",
			tableWithCompositePK,
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual("last_name", OperandQuotedString, NewTextPointer([]byte("Doe"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type: ScanTypeSequential,
						Filters: OneOrMore{
							{
								FieldIsEqual("last_name", OperandQuotedString, NewTextPointer([]byte("Doe"))),
							},
						},
					},
				},
			},
		},
		{
			"Composite PK: Match both columns with extra filter - index point scan with remaining filter",
			tableWithCompositePK,
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual("first_name", OperandQuotedString, NewTextPointer([]byte("John"))),
						FieldIsEqual("last_name", OperandQuotedString, NewTextPointer([]byte("Doe"))),
						FieldIsEqual("age", OperandInteger, int64(30)),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type:         ScanTypeIndexPoint,
						IndexName:    compositePKIndexName,
						IndexColumns: compositeColumns[1:3],
						IndexKeys: []any{
							NewCompositeKey(compositeColumns[1:3], "John", "Doe"),
						},
						Filters: OneOrMore{
							{
								FieldIsEqual("age", OperandInteger, int64(30)),
							},
						},
					},
				},
			},
		},
		{
			"Composite PK: Multiple OR conditions, each matching composite key",
			tableWithCompositePK,
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual("first_name", OperandQuotedString, NewTextPointer([]byte("John"))),
						FieldIsEqual("last_name", OperandQuotedString, NewTextPointer([]byte("Doe"))),
					},
					{
						FieldIsEqual("first_name", OperandQuotedString, NewTextPointer([]byte("Jane"))),
						FieldIsEqual("last_name", OperandQuotedString, NewTextPointer([]byte("Smith"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type:         ScanTypeIndexPoint,
						IndexName:    compositePKIndexName,
						IndexColumns: compositeColumns[1:3],
						IndexKeys: []any{
							NewCompositeKey(compositeColumns[1:3], "John", "Doe"),
						},
					},
					{
						Type:         ScanTypeIndexPoint,
						IndexName:    compositePKIndexName,
						IndexColumns: compositeColumns[1:3],
						IndexKeys: []any{
							NewCompositeKey(compositeColumns[1:3], "Jane", "Smith"),
						},
					},
				},
			},
		},
		{
			"Composite Unique Index: Match both columns - index point scan",
			tableWithCompositeUnique,
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual("first_name", OperandQuotedString, NewTextPointer([]byte("John"))),
						FieldIsEqual("last_name", OperandQuotedString, NewTextPointer([]byte("Doe"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type:         ScanTypeIndexPoint,
						IndexName:    compositeUniqueIndexName,
						IndexColumns: compositeColumns[1:3],
						IndexKeys: []any{
							NewCompositeKey(compositeColumns[1:3], "John", "Doe"),
						},
					},
				},
			},
		},
		{
			"Composite Secondary Index: Match both columns - index point scan",
			tableWithCompositeSecondary,
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual("first_name", OperandQuotedString, NewTextPointer([]byte("John"))),
						FieldIsEqual("last_name", OperandQuotedString, NewTextPointer([]byte("Doe"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type:         ScanTypeIndexPoint,
						IndexName:    compositeSecondaryIndexName,
						IndexColumns: compositeColumns[1:3],
						IndexKeys: []any{
							NewCompositeKey(compositeColumns[1:3], "John", "Doe"),
						},
					},
				},
			},
		},
		{
			"Prefer PK over composite unique index",
			NewTable(
				zap.NewNop(), nil, nil, "users",
				compositeColumns,
				0,
				WithPrimaryKey(NewPrimaryKey("pkey__users", compositeColumns[0:1], true)),
				WithUniqueIndex(UniqueIndex{
					IndexInfo: IndexInfo{
						Name:    "idx_name",
						Columns: compositeColumns[1:3],
					},
				}),
			),
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual("id", OperandInteger, int64(42)),
						FieldIsEqual("first_name", OperandQuotedString, NewTextPointer([]byte("John"))),
						FieldIsEqual("last_name", OperandQuotedString, NewTextPointer([]byte("Doe"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type:         ScanTypeIndexPoint,
						IndexName:    "pkey__users",
						IndexColumns: compositeColumns[0:1],
						IndexKeys:    []any{int64(42)},
						Filters: OneOrMore{
							{
								FieldIsEqual("first_name", OperandQuotedString, NewTextPointer([]byte("John"))),
								FieldIsEqual("last_name", OperandQuotedString, NewTextPointer([]byte("Doe"))),
							},
						},
					},
				},
			},
		},
		{
			"Prefer composite unique index over composite secondary index",
			func() *Table {
				t := NewTable(
					zap.NewNop(), nil, nil, "users",
					compositeColumns,
					0,
					WithUniqueIndex(UniqueIndex{
						IndexInfo: IndexInfo{
							Name:    "idx_unique_name",
							Columns: compositeColumns[1:3],
						},
					}),
					WithSecondaryIndex(
						SecondaryIndex{
							IndexInfo: IndexInfo{
								Name:    "idx_secondary_name",
								Columns: compositeColumns[1:3],
							},
						},
					),
				)
				return t
			}(),
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual("first_name", OperandQuotedString, NewTextPointer([]byte("John"))),
						FieldIsEqual("last_name", OperandQuotedString, NewTextPointer([]byte("Doe"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type:         ScanTypeIndexPoint,
						IndexName:    "idx_unique_name",
						IndexColumns: compositeColumns[1:3],
						IndexKeys: []any{
							NewCompositeKey(compositeColumns[1:3], "John", "Doe"),
						},
					},
				},
			},
		},
		{
			"Prefer index with more matched columns",
			func() *Table {
				t := NewTable(
					zap.NewNop(), nil, nil, "users",
					compositeColumns,
					0,
				)
				t.SetSecondaryIndex("idx_first_name", compositeColumns[1:2], nil)
				t.SetSecondaryIndex("idx_full_name", compositeColumns[1:3], nil)
				return t
			}(),
			Statement{
				Kind: Select,
				Conditions: OneOrMore{
					{
						FieldIsEqual("first_name", OperandQuotedString, NewTextPointer([]byte("John"))),
						FieldIsEqual("last_name", OperandQuotedString, NewTextPointer([]byte("Doe"))),
					},
				},
			},
			QueryPlan{
				Scans: []Scan{
					{
						Type:         ScanTypeIndexPoint,
						IndexName:    "idx_full_name",
						IndexColumns: compositeColumns[1:3],
						IndexKeys: []any{
							NewCompositeKey(compositeColumns[1:3], "John", "Doe"),
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			plan, err := tc.Table.PlanQuery(context.Background(), tc.Stmt)
			require.NoError(t, err)
			assert.Equal(t, tc.Expected, plan)
		})
	}
}
