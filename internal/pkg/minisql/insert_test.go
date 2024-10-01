package minisql

// func TestTable_Insert(t *testing.T) {
// 	t.Parallel()

// 	ctx := context.Background()
// 	pagerMock := new(MockPager)
// 	var (
// 		page0 = NewPage(0)
// 	)
// 	pagerMock.On("GetPage", mock.Anything, "foo", uint32(0)).Return(page0, nil)

// 	aDatabase, err := NewDatabase(ctx, "db", nil, pagerMock)
// 	require.NoError(t, err)

// 	aRow := gen.Row()

// 	insertStmt := Statement{
// 		Kind:      Insert,
// 		TableName: "foo",
// 		Fields:    []string{"id", "email", "age"},
// 		Inserts:   [][]any{aRow.Values},
// 	}

// 	aTable, err := aDatabase.CreateTable(ctx, "foo", testColumns)
// 	require.NoError(t, err)

// 	aResult, err := aTable.Insert(ctx, insertStmt)
// 	require.NoError(t, err)
// 	assert.Equal(t, 1, aResult.RowsAffected)

// 	selectStmt := Statement{
// 		Kind:      Select,
// 		TableName: "foo",
// 		Fields:    []string{"id", "email", "age"},
// 	}
// 	aResult, err = aTable.Select(ctx, selectStmt)
// 	require.NoError(t, err)

// 	selectRow, err := aResult.Rows(ctx)
// 	require.NoError(t, err)
// 	assert.Equal(t, aRow, selectRow)

// 	_, err = aResult.Rows(ctx)
// 	require.Error(t, err)
// 	assert.ErrorIs(t, err, ErrNoMoreRows)

// 	mock.AssertExpectationsForObjects(t, pagerMock)
// }
