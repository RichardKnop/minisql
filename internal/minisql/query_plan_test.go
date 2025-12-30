package minisql

// func TestTable_PlanQuery_MultipleIndexes(t *testing.T) {
// 	t.Parallel()

// 	var (
// 		columns = []Column{
// 			{
// 				Kind:          Int8,
// 				Size:          8,
// 				Name:          "id",
// 				PrimaryKey:    true,
// 				Autoincrement: true,
// 			},
// 			{
// 				Kind:   Varchar,
// 				Size:   MaxInlineVarchar,
// 				Name:   "email",
// 				Unique: true,
// 			},
// 			{
// 				Kind:     Text,
// 				Name:     "Name",
// 				Nullable: true,
// 			},
// 			{
// 				Kind: Timestamp,
// 				Name: "dob",
// 			},
// 			{
// 				Kind:            Timestamp,
// 				Name:            "created",
// 				DefaultValueNow: true,
// 				// secondary index on this column
// 			},
// 		}
// 		pkIndexName        = "pkey__users"
// 		uniqueIndexName    = "key__users__email"
// 		secondaryIndexName = "idx__users__created"
// 		aTable             = NewTable(zap.NewNop(), nil, nil, "users", columns, 0)
// 	)
// 	aTable.SetSecondaryIndex(secondaryIndexName, columns[4:5], nil)

// 	testCases := []struct {
// 		Name     string
// 		Stmt     Statement
// 		Expected QueryPlan
// 	}{
// 		{
// 			"Sequential scan",
// 			Statement{
// 				Kind: Select,
// 			},
// 			QueryPlan{
// 				Scans: []Scan{
// 					{
// 						Type: ScanTypeSequential,
// 					},
// 				},
// 			},
// 		},
// 		{
// 			"Sequential scan with filters",
// 			Statement{
// 				Kind: Select,
// 				Conditions: OneOrMore{
// 					{
// 						FieldIsEqual("name", OperandQuotedString, NewTextPointer([]byte("Richard"))),
// 					},
// 				},
// 			},
// 			QueryPlan{
// 				Scans: []Scan{
// 					{
// 						Type: ScanTypeSequential,
// 						Filters: OneOrMore{
// 							{
// 								FieldIsEqual("name", OperandQuotedString, NewTextPointer([]byte("Richard"))),
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 		{
// 			"Two index point scans for both primary and unique indexes",
// 			Statement{
// 				Kind: Select,
// 				Conditions: OneOrMore{
// 					{
// 						FieldIsEqual("id", OperandInteger, int64(42)),
// 					},
// 					{
// 						FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
// 					},
// 				},
// 			},
// 			QueryPlan{
// 				Scans: []Scan{
// 					{
// 						Type:         ScanTypeIndexPoint,
// 						IndexName:    pkIndexName,
// 						IndexColumns: columns[0:1],
// 						IndexKeys:    []any{int64(42)},
// 						Filters:      OneOrMore{{}},
// 					},
// 					{
// 						Type:         ScanTypeIndexPoint,
// 						IndexName:    uniqueIndexName,
// 						IndexColumns: columns[1:2],
// 						IndexKeys:    []any{"foo@example.com"},
// 						Filters:      OneOrMore{{}},
// 					},
// 				},
// 			},
// 		},
// 		{
// 			"Index scan on unique index and range scan on secondary index",
// 			Statement{
// 				Kind: Select,
// 				Conditions: OneOrMore{
// 					{
// 						FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
// 					},
// 					{
// 						FieldIsGreaterOrEqual("created", OperandQuotedString, MustParseTimestamp("2025-01-01 00:00:00")),
// 					},
// 				},
// 			},
// 			QueryPlan{
// 				Scans: []Scan{
// 					{
// 						Type:         ScanTypeIndexPoint,
// 						IndexName:    uniqueIndexName,
// 						IndexColumns: columns[1:2],
// 						IndexKeys:    []any{"foo@example.com"},
// 						Filters:      OneOrMore{{}},
// 					},
// 					{
// 						Type:         ScanTypeIndexRange,
// 						IndexName:    secondaryIndexName,
// 						IndexColumns: columns[4:5],
// 						RangeCondition: RangeCondition{
// 							Lower: &RangeBound{
// 								Value:     MustParseTimestamp("2025-01-01 00:00:00").TotalMicroseconds(),
// 								Inclusive: true,
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 		{
// 			"Primary index priority over unique and secondary indexes",
// 			Statement{
// 				Kind: Select,
// 				Conditions: OneOrMore{
// 					{
// 						FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
// 						FieldIsGreaterOrEqual("created", OperandQuotedString, MustParseTimestamp("2025-01-01 00:00:00")),
// 						FieldIsEqual("id", OperandInteger, int64(42)),
// 					},
// 				},
// 			},
// 			QueryPlan{
// 				Scans: []Scan{
// 					{
// 						Type:         ScanTypeIndexPoint,
// 						IndexName:    pkIndexName,
// 						IndexColumns: columns[0:1],
// 						IndexKeys:    []any{int64(42)},
// 						Filters: OneOrMore{
// 							{
// 								FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
// 								FieldIsGreaterOrEqual("created", OperandQuotedString, MustParseTimestamp("2025-01-01 00:00:00")),
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 		{
// 			"Unique index priority over secondary index",
// 			Statement{
// 				Kind: Select,
// 				Conditions: OneOrMore{
// 					{
// 						FieldIsEqual("created", OperandQuotedString, MustParseTimestamp("2025-01-01 00:00:00")),
// 						FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
// 					},
// 				},
// 			},
// 			QueryPlan{
// 				Scans: []Scan{
// 					{
// 						Type:         ScanTypeIndexPoint,
// 						IndexName:    uniqueIndexName,
// 						IndexColumns: columns[1:2],
// 						IndexKeys:    []any{"foo@example.com"},
// 						Filters: OneOrMore{
// 							{
// 								FieldIsEqual("created", OperandQuotedString, MustParseTimestamp("2025-01-01 00:00:00")),
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 		{
// 			"Combine sequential scans",
// 			Statement{
// 				Kind: Select,
// 				Conditions: OneOrMore{
// 					{
// 						FieldIsEqual("dob", OperandQuotedString, MustParseTimestamp("1990-01-01 00:00:00")),
// 					},
// 					{
// 						FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
// 					},
// 					{
// 						FieldIsEqual("name", OperandQuotedString, NewTextPointer([]byte("Richard"))),
// 					},
// 				},
// 			},
// 			QueryPlan{
// 				Scans: []Scan{
// 					{
// 						Type: ScanTypeSequential,
// 						Filters: OneOrMore{
// 							{
// 								FieldIsEqual("dob", OperandQuotedString, MustParseTimestamp("1990-01-01 00:00:00")),
// 							},
// 							{
// 								FieldIsEqual("email", OperandQuotedString, NewTextPointer([]byte("foo@example.com"))),
// 							},
// 							{
// 								FieldIsEqual("name", OperandQuotedString, NewTextPointer([]byte("Richard"))),
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 	}

// 	for _, aTestCase := range testCases {
// 		t.Run(aTestCase.Name, func(t *testing.T) {
// 			actual, err := aTable.PlanQuery(context.Background(), aTestCase.Stmt)
// 			require.NoError(t, err)
// 			assert.Equal(t, aTestCase.Expected, actual)
// 		})
// 	}
// }

// func TestTryRangeScan(t *testing.T) {
// 	t.Parallel()

// 	indexName := "pkey__users"
// 	indexInfo := IndexInfo{
// 		Name:    indexName,
// 		Columns: testColumns[0:1],
// 	}
// 	testCases := []struct {
// 		Name         string
// 		Conditions   Conditions
// 		ExpectedScan Scan
// 		ExpectedOK   bool
// 	}{
// 		{
// 			"Equality operator does not qualify for range scan",
// 			Conditions{
// 				FieldIsEqual("id", OperandInteger, int64(10)),
// 			},
// 			Scan{},
// 			false,
// 		},
// 		{
// 			"Not equal operator",
// 			Conditions{
// 				FieldIsNotEqual("id", OperandInteger, int64(42)),
// 			},
// 			Scan{},
// 			false,
// 		},
// 		{
// 			"Range scan with lower bound only",
// 			Conditions{
// 				FieldIsGreater("id", OperandInteger, int64(10)),
// 			},
// 			Scan{
// 				Type:         ScanTypeIndexRange,
// 				IndexName:    indexName,
// 				IndexColumns: testColumns[0:1],
// 				RangeCondition: RangeCondition{
// 					Lower: &RangeBound{
// 						Value: int64(10),
// 					},
// 				},
// 			},
// 			true,
// 		},
// 		{
// 			"Range scan with lower bound only (inclusive)",
// 			Conditions{
// 				FieldIsGreaterOrEqual("id", OperandInteger, int64(10)),
// 			},
// 			Scan{
// 				Type:         ScanTypeIndexRange,
// 				IndexName:    indexName,
// 				IndexColumns: testColumns[0:1],
// 				RangeCondition: RangeCondition{
// 					Lower: &RangeBound{
// 						Value:     int64(10),
// 						Inclusive: true,
// 					},
// 				},
// 			},
// 			true,
// 		},
// 		{
// 			"Range scan with upper bound only",
// 			Conditions{
// 				FieldIsLess("id", OperandInteger, int64(10)),
// 			},
// 			Scan{
// 				Type:         ScanTypeIndexRange,
// 				IndexName:    indexName,
// 				IndexColumns: testColumns[0:1],
// 				RangeCondition: RangeCondition{
// 					Upper: &RangeBound{
// 						Value: int64(10),
// 					},
// 				},
// 			},
// 			true,
// 		},
// 		{
// 			"Range scan with upper bound only (inclusive)",
// 			Conditions{
// 				FieldIsLessOrEqual("id", OperandInteger, int64(10)),
// 			},
// 			Scan{
// 				Type:         ScanTypeIndexRange,
// 				IndexName:    indexName,
// 				IndexColumns: testColumns[0:1],
// 				RangeCondition: RangeCondition{
// 					Upper: &RangeBound{
// 						Value:     int64(10),
// 						Inclusive: true,
// 					},
// 				},
// 			},
// 			true,
// 		},
// 		{
// 			"Range scan with with both lower and upper bounds",
// 			Conditions{
// 				FieldIsLessOrEqual("id", OperandInteger, int64(10)),
// 				FieldIsGreater("id", OperandInteger, int64(5)),
// 			},
// 			Scan{
// 				Type:         ScanTypeIndexRange,
// 				IndexName:    indexName,
// 				IndexColumns: testColumns[0:1],
// 				RangeCondition: RangeCondition{
// 					Lower: &RangeBound{
// 						Value: int64(5),
// 					},
// 					Upper: &RangeBound{
// 						Value:     int64(10),
// 						Inclusive: true,
// 					},
// 				},
// 			},
// 			true,
// 		},
// 		{
// 			"Range scan with with both lower and upper bounds and remaining filters",
// 			Conditions{
// 				FieldIsLessOrEqual("id", OperandInteger, int64(10)),
// 				FieldIsGreater("id", OperandInteger, int64(5)),
// 				FieldIsEqual("name", OperandQuotedString, NewTextPointer([]byte("foo"))),
// 			},
// 			Scan{
// 				Type:         ScanTypeIndexRange,
// 				IndexName:    indexName,
// 				IndexColumns: testColumns[0:1],
// 				RangeCondition: RangeCondition{
// 					Lower: &RangeBound{
// 						Value: int64(5),
// 					},
// 					Upper: &RangeBound{
// 						Value:     int64(10),
// 						Inclusive: true,
// 					},
// 				},
// 				Filters: OneOrMore{{
// 					FieldIsEqual("name", OperandQuotedString, NewTextPointer([]byte("foo"))),
// 				}},
// 			},
// 			true,
// 		},
// 	}

// 	for _, aTestCase := range testCases {
// 		t.Run(aTestCase.Name, func(t *testing.T) {
// 			aScan, ok, err := tryRangeScan(indexInfo, aTestCase.Conditions)
// 			require.NoError(t, err)
// 			assert.Equal(t, aTestCase.ExpectedOK, ok)
// 			if ok {
// 				assert.Equal(t, aTestCase.ExpectedScan, aScan)
// 			}
// 		})
// 	}
// }
