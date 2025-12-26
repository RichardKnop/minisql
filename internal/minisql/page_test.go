package minisql

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTable_PageRecycling(t *testing.T) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", "testdb")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())
	aPager, err := NewPager(tempFile, PageSize)
	require.NoError(t, err)
	txManager := NewTransactionManager(zap.NewNop())
	tablePager := NewTransactionalPager(
		aPager.ForTable(testMediumColumns),
		txManager,
	)

	var (
		ctx     = context.Background()
		numRows = 100
		rows    = gen.MediumRows(numRows)
		aTable  = NewTable(testLogger, tablePager, txManager, testTableName, testMediumColumns, 0)
	)
	aTable.maximumICells = 5 // for testing purposes only, normally 340

	// Batch insert test rows
	stmt := Statement{
		Kind:    Insert,
		Fields:  fieldsFromColumns(testMediumColumns...),
		Inserts: [][]OptionalValue{},
	}
	for _, aRow := range rows {
		stmt.Inserts = append(stmt.Inserts, aRow.Values)
	}

	mustInsert(t, ctx, aTable, txManager, aPager, stmt)

	// require.NoError(t, aTable.print())

	assert.Equal(t, 47, int(aPager.TotalPages()))
	assert.Equal(t, 0, int(aPager.dbHeader.FreePageCount))
	checkRows(ctx, t, aTable, rows)

	// Now delete all rows, this will free up 46 pages
	// but the root page will remain in use
	var aResult StatementResult
	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		var err error
		aResult, err = aTable.Delete(ctx, Statement{
			Kind: Delete,
		})
		return err
	}, TxCommitter{aPager, nil})
	require.NoError(t, err)

	assert.Equal(t, len(rows), aResult.RowsAffected)

	checkRows(ctx, t, aTable, nil)
	assert.Equal(t, 47, int(aPager.TotalPages()))
	assert.Equal(t, 46, int(aPager.dbHeader.FreePageCount))

	// Now we reinsert the same rows again
	mustInsert(t, ctx, aTable, txManager, aPager, stmt)

	// We should still have the same number of pages in total
	// and no free pages
	assert.Equal(t, 47, int(aPager.TotalPages()))
	assert.Equal(t, 0, int(aPager.dbHeader.FreePageCount))
	checkRows(ctx, t, aTable, rows)
}

func TestPage_Clone(t *testing.T) {
	t.Parallel()

	t.Run("leaf page", func(t *testing.T) {
		original := &Page{
			Index: 5,
			LeafNode: &LeafNode{
				Header: LeafNodeHeader{
					Header: Header{
						IsInternal: false,
						IsRoot:     true,
						Parent:     7,
					},
					Cells:    2,
					NextLeaf: 9,
				},
				Cells: []Cell{
					{
						NullBitmask: 0,
						Key:         1,
						Value:       []byte("first value"),
					},
					{
						NullBitmask: 0,
						Key:         2,
						Value:       []byte("second value"),
					},
				},
			},
		}

		copied := original.Clone()

		require.Nil(t, copied.InternalNode)
		require.Nil(t, copied.FreePage)
		require.Nil(t, copied.IndexNode)
		require.NotNil(t, copied.LeafNode)
		assert.Equal(t, original.Index, copied.Index)
		assert.Equal(t, original.LeafNode, copied.LeafNode)
	})

	t.Run("internal page", func(t *testing.T) {
		iCells := [InternalNodeMaxCells]ICell{}
		iCells[0] = ICell{
			Key:   10,
			Child: 3,
		}
		iCells[1] = ICell{
			Key:   20,
			Child: 4,
		}
		original := &Page{
			Index: 5,
			InternalNode: &InternalNode{
				Header: InternalNodeHeader{
					Header: Header{
						IsInternal: false,
						IsRoot:     true,
						Parent:     7,
					},
					KeysNum:    2,
					RightChild: 9,
				},
				ICells: iCells,
			},
		}

		copied := original.Clone()

		require.Nil(t, copied.LeafNode)
		require.Nil(t, copied.FreePage)
		require.Nil(t, copied.IndexNode)
		require.NotNil(t, copied.InternalNode)
		assert.Equal(t, original.Index, copied.Index)
		assert.Equal(t, original.InternalNode, copied.InternalNode)
	})

	t.Run("free page", func(t *testing.T) {
		original := &Page{
			Index: 5,
			FreePage: &FreePage{
				NextFreePage: 10,
			},
		}

		copied := original.Clone()

		require.Nil(t, copied.LeafNode)
		require.Nil(t, copied.InternalNode)
		require.Nil(t, copied.IndexNode)
		require.NotNil(t, copied.FreePage)
		assert.Equal(t, original.Index, copied.Index)
		assert.Equal(t, original.FreePage, copied.FreePage)
	})

	t.Run("index page", func(t *testing.T) {
		original := &Page{
			Index: 5,
			IndexNode: &IndexNode[int64]{
				Header: IndexNodeHeader{
					IsRoot:     false,
					IsLeaf:     true,
					Parent:     7,
					Keys:       2,
					RightChild: 65,
				},
				Cells: []IndexCell[int64]{
					{
						Key:   100,
						Child: 3,
					},
					{
						Key:   200,
						Child: 4,
					},
				},
			},
		}

		copied := original.Clone()

		require.Nil(t, copied.LeafNode)
		require.Nil(t, copied.InternalNode)
		require.Nil(t, copied.FreePage)
		require.NotNil(t, copied.IndexNode)
		assert.Equal(t, original.Index, copied.Index)
		assert.Equal(t, original.IndexNode, copied.IndexNode)
	})
}
