package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestCursor_TableStart_Empty(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	aDatabase, err := NewDatabase(ctx, "db", nil, nil)
	require.NoError(t, err)
	aTable, err := aDatabase.CreateTable(ctx, "foo", testColumns)
	require.NoError(t, err)

	aCursor := TableStart(aTable)
	assert.Equal(t, 0, aCursor.RowNumber)
	assert.True(t, aCursor.EndOfTable)
}

func TestCursor_TableStart_NotEmpty(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	pagerMock := new(MockPager)
	var (
		page0 = NewPage(0)
	)
	pagerMock.On("GetPage", mock.Anything, "foo", uint32(0)).Return(page0, nil)

	aDatabase, err := NewDatabase(ctx, "db", nil, pagerMock)
	require.NoError(t, err)
	aTable, err := aDatabase.CreateTable(ctx, "foo", testColumns)
	require.NoError(t, err)

	insertRows(ctx, t, aTable, gen.Rows(1))

	aCursor := TableStart(aTable)
	assert.Equal(t, 0, aCursor.RowNumber)
	assert.False(t, aCursor.EndOfTable)

	mock.AssertExpectationsForObjects(t, pagerMock)
}

func TestCursor_TableEnd(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	pagerMock := new(MockPager)
	var (
		page0 = NewPage(0)
		page1 = NewPage(1)
	)
	pagerMock.On("GetPage", mock.Anything, "foo", uint32(0)).Return(page0, nil)
	pagerMock.On("GetPage", mock.Anything, "foo", uint32(1)).Return(page1, nil)

	aDatabase, err := NewDatabase(ctx, "db", nil, pagerMock)
	require.NoError(t, err)
	aTable, err := aDatabase.CreateTable(ctx, "foo", testColumns)
	require.NoError(t, err)

	rows := gen.Rows(20)
	insertRows(ctx, t, aTable, rows)

	aCursor := TableEnd(aTable)
	assert.Equal(t, 20, aCursor.RowNumber)
	assert.True(t, aCursor.EndOfTable)

	mock.AssertExpectationsForObjects(t, pagerMock)
}

func TestCursor_TableAt(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	pagerMock := new(MockPager)
	var (
		page0 = NewPage(0)
		page1 = NewPage(1)
	)
	pagerMock.On("GetPage", mock.Anything, "foo", uint32(0)).Return(page0, nil)
	pagerMock.On("GetPage", mock.Anything, "foo", uint32(1)).Return(page1, nil)

	aDatabase, err := NewDatabase(ctx, "db", nil, pagerMock)
	require.NoError(t, err)
	aTable, err := aDatabase.CreateTable(ctx, "foo", testColumns)
	require.NoError(t, err)

	rows := gen.Rows(20)
	insertRows(ctx, t, aTable, rows)

	aCursor := TableAt(aTable, 10)
	assert.Equal(t, 10, aCursor.RowNumber)
	assert.False(t, aCursor.EndOfTable)

	mock.AssertExpectationsForObjects(t, pagerMock)
}

func TestCursor_Value(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, "db", nil, nil)
	require.NoError(t, err)
	aTable, err := aDatabase.CreateTable(ctx, "foo", testColumns)
	require.NoError(t, err)

	// Row size is 267 bytes
	// 15 rows will fit into each 4096 bytes page
	// There are max 100 pages right now (temporary limitation)

	testCases := []struct {
		Name      string
		RowNumber int
		Page      uint32
		Offset    uint32
		Err       error
	}{
		{
			Name:      "First row in the table",
			RowNumber: 0,
			Page:      uint32(0),
			Offset:    uint32(0),
		},
		{
			Name:      "Second row in the table",
			RowNumber: 1,
			Page:      uint32(0),
			Offset:    uint32(267),
		},
		{
			Name:      "Third row in the table",
			RowNumber: 2,
			Page:      uint32(0),
			Offset:    uint32(267 * 2),
		},
		{
			Name:      "16th row should be the first row of the second page",
			RowNumber: 15,
			Page:      uint32(1),
			Offset:    uint32(0),
		},
		{
			Name:      "1486th row should be the first row of the last 100th page",
			RowNumber: 1485,
			Page:      uint32(99),
			Offset:    uint32(0),
		},
		{
			Name:      "1500th row should be the last row of the last 100th page",
			RowNumber: 1499,
			Page:      uint32(99),
			Offset:    uint32(267 * 14),
		},
		{
			Name:      "1501th row should cause error",
			RowNumber: 1500,
			Err:       errMaximumPagesReached,
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			aCursor := TableAt(aTable, aTestCase.RowNumber)
			pageIdx, offset, err := aCursor.Value()
			if aTestCase.Err != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, aTestCase.Err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, aTestCase.Page, pageIdx)
			assert.Equal(t, aTestCase.Offset, offset)
		})
	}
}
