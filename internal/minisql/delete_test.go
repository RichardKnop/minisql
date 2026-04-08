package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTable_Delete_RootLeafNode(t *testing.T) {
	/*
		In this test we will be deleting from a root leaf node only tree.
	*/
	var (
		pager, dbFile = initTest(t)
		ctx           = context.Background()
		numRows       = 5
		rows          = gen.MediumRows(numRows)
		tablePager    = pager.ForTable(testMediumColumns)
		txManager     = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager       = NewTransactionalPager(tablePager, txManager, testTableName, "")
		table         = NewTable(testLogger, txPager, txManager, testTableName, testMediumColumns, 0, nil)
	)

	// Set some values to NULL so we can test selecting/filtering on NULLs
	rows[1].Values[2] = OptionalValue{Valid: false}
	rows[3].Values[5] = OptionalValue{Valid: false}
	rows[4].Values[5] = OptionalValue{Valid: false}

	// Batch insert test rows
	stmt := Statement{
		Kind:    Insert,
		Fields:  fieldsFromColumns(testMediumColumns...),
		Inserts: [][]OptionalValue{},
	}
	for _, row := range rows {
		stmt.Inserts = append(stmt.Inserts, row.Values)
	}

	mustInsert(ctx, t, table, txManager, stmt)

	t.Run("Delete rows with NULL values when no rows match", func(t *testing.T) {
		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind:       Delete,
			Conditions: NewOneOrMore(Conditions{FieldIsNull(Field{Name: "id"})}),
		})

		assert.Equal(t, 0, result.RowsAffected)
		checkRows(ctx, t, table, rows)
	})

	t.Run("Delete one row", func(t *testing.T) {
		id, ok := rows[0].GetValue("id")
		require.True(t, ok)

		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsEqual(Field{Name: "id"}, OperandInteger, id.Value.(int64)),
				},
			},
		})

		assert.Equal(t, 1, result.RowsAffected)
		checkRows(ctx, t, table, rows[1:])
	})

	t.Run("Delete rows with NULL values", func(t *testing.T) {
		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind:       Delete,
			Conditions: NewOneOrMore(Conditions{FieldIsNull(Field{Name: "age"})}),
		})

		assert.Equal(t, 1, result.RowsAffected)
		checkRows(ctx, t, table, rows[2:])
	})

	t.Run("Delete rows with NOT NULL values", func(t *testing.T) {
		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind:       Delete,
			Conditions: NewOneOrMore(Conditions{FieldIsNotNull(Field{Name: "created"})}),
		})

		assert.Equal(t, 1, result.RowsAffected)
		checkRows(ctx, t, table, rows[3:])
	})

	t.Run("Delete all rows", func(t *testing.T) {
		result := mustDelete(ctx, t, table, txManager, pager, Statement{Kind: Delete})

		assert.Equal(t, 2, result.RowsAffected)
		checkRows(ctx, t, table, nil)
	})

	// Root page is never recycled
	assert.Equal(t, 0, int(pager.dbHeader.FirstFreePage))
	assert.Equal(t, 0, int(pager.dbHeader.FreePageCount))
}

func TestTable_Delete_LeafNodeRebalancing(t *testing.T) {
	var (
		pager, dbFile = initTest(t)
		ctx           = context.Background()
		numRows       = 20
		rows          = gen.MediumRows(numRows)
		tablePager    = pager.ForTable(testMediumColumns)
		txManager     = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager       = NewTransactionalPager(tablePager, txManager, testTableName, "")
		table         = NewTable(testLogger, txPager, txManager, testTableName, testMediumColumns, 0, nil)
	)

	// Batch insert test rows
	stmt := Statement{
		Kind:    Insert,
		Fields:  fieldsFromColumns(testMediumColumns...),
		Inserts: [][]OptionalValue{},
	}
	for _, row := range rows {
		stmt.Inserts = append(stmt.Inserts, row.Values)
	}

	mustInsert(ctx, t, table, txManager, stmt)

	/*
		Initial state of the tree:

		           +------------------------------------------------+
		           |   2,       5,       8,         11,        14   |
		           +------------------------------------------------+
		          /       /         /        /             /         \
		+-------+  +-------+  +-------+  +---------+  +----------+  +----------------+
		| 0,1,2 |  | 3,4,5 |  | 6,7,8 |  | 9,10,11 |  | 12,13,14 |  | 15,16,17,18,19 |
		+-------+  +-------+  +-------+  +---------+  +----------+  +----------------+
	*/

	// Check the root page
	assert.Equal(t, 5, int(pager.pages[0].InternalNode.Header.KeysNum))
	assert.Equal(t, []RowID{2, 5, 8, 11, 14}, pager.pages[0].InternalNode.Keys())
	// Check the leaf pages
	assert.Equal(t, []RowID{0, 1, 2}, pager.pages[2].LeafNode.Keys())
	assert.Equal(t, []RowID{3, 4, 5}, pager.pages[1].LeafNode.Keys())
	assert.Equal(t, []RowID{6, 7, 8}, pager.pages[3].LeafNode.Keys())
	assert.Equal(t, []RowID{9, 10, 11}, pager.pages[4].LeafNode.Keys())
	assert.Equal(t, []RowID{12, 13, 14}, pager.pages[5].LeafNode.Keys())
	assert.Equal(t, []RowID{15, 16, 17, 18, 19}, pager.pages[6].LeafNode.Keys())

	t.Run("Delete first row to force merging of first two leaves", func(t *testing.T) {
		ids := rowIDs(rows[0])

		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsInAny(Field{Name: "id"}, ids...),
				},
			},
		})

		assert.Equal(t, 1, result.RowsAffected)
		checkRows(ctx, t, table, rows[1:])

		/*
				          +----------------------------------------------+
				          |      5,        8,         11,        14      |
				          +----------------------------------------------+
				         /           /          /            /           \
			+-----------+     +-------+    +---------+    +----------+     +----------------+
			| 1,2,3,4,5 |     | 6,7,8 |    | 9,10,11 |    | 12,13,14 |     | 15,16,17,18,19 |
			+-----------+     +-------+    +---------+    +----------+     +----------------+
		*/

		// Check the root page
		assert.Equal(t, 4, int(pager.pages[0].InternalNode.Header.KeysNum))
		assert.Equal(t, []RowID{5, 8, 11, 14}, pager.pages[0].InternalNode.Keys())
		// Check the leaf pages
		assert.Equal(t, []RowID{1, 2, 3, 4, 5}, pager.pages[2].LeafNode.Keys())
		// leafs[1] has been merged into leafs[0]
		assert.Equal(t, []RowID{6, 7, 8}, pager.pages[3].LeafNode.Keys())
		assert.Equal(t, []RowID{9, 10, 11}, pager.pages[4].LeafNode.Keys())
		assert.Equal(t, []RowID{12, 13, 14}, pager.pages[5].LeafNode.Keys())
		assert.Equal(t, []RowID{15, 16, 17, 18, 19}, pager.pages[6].LeafNode.Keys())
		// Check that leafs[1] is now a free page
		assert.NotNil(t, pager.pages[1].FreePage)
		assert.Nil(t, pager.pages[1].LeafNode)
		assert.Nil(t, pager.pages[1].InternalNode)
		assert.Equal(t, 0, int(pager.pages[1].FreePage.NextFreePage))
		assert.Equal(t, int(pager.pages[1].Index), int(pager.dbHeader.FirstFreePage))
		assert.Equal(t, 1, int(pager.dbHeader.FreePageCount))
	})

	t.Run("Delete last three rows to force merging of last two leaves", func(t *testing.T) {
		ids := rowIDs(rows[17], rows[18], rows[19])

		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsInAny(Field{Name: "id"}, ids...),
				},
			},
		})

		assert.Equal(t, 3, result.RowsAffected)
		checkRows(ctx, t, table, rows[1:17])

		/*
				          +------------------------------------+
				          |      5,        8,         11,      |
				          +------------------------------------+
				         /           /          /               \
			+-----------+     +-------+    +---------+    +----------------+
			| 1,2,3,4,5 |     | 6,7,8 |    | 9,10,11 |    | 12,13,14,15,16 |
			+-----------+     +-------+    +---------+    +----------------+
		*/

		// Check the root page
		assert.Equal(t, 3, int(pager.pages[0].InternalNode.Header.KeysNum))
		assert.Equal(t, []RowID{5, 8, 11}, pager.pages[0].InternalNode.Keys())
		// Check the leaf pages
		assert.Equal(t, []RowID{1, 2, 3, 4, 5}, pager.pages[2].LeafNode.Keys())
		assert.Equal(t, []RowID{6, 7, 8}, pager.pages[3].LeafNode.Keys())
		assert.Equal(t, []RowID{9, 10, 11}, pager.pages[4].LeafNode.Keys())
		assert.Equal(t, []RowID{12, 13, 14, 15, 16}, pager.pages[5].LeafNode.Keys())
		// Check that leafs[6] is now a free page
		assert.NotNil(t, pager.pages[6].FreePage)
		assert.Nil(t, pager.pages[6].LeafNode)
		assert.Nil(t, pager.pages[6].InternalNode)
		assert.Equal(t, int(pager.pages[1].Index), int(pager.pages[6].FreePage.NextFreePage))
		assert.Equal(t, int(pager.pages[6].Index), int(pager.dbHeader.FirstFreePage))
		assert.Equal(t, 2, int(pager.dbHeader.FreePageCount))
	})

	t.Run("Keep deleting more rows, another merge", func(t *testing.T) {
		ids := rowIDs(rows[2], rows[4], rows[6])

		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsInAny(Field{Name: "id"}, ids...),
				},
			},
		})

		assert.Equal(t, 3, result.RowsAffected)
		checkRows(ctx, t, table, []Row{
			rows[1], rows[3], rows[5], rows[7], rows[8],
			rows[9], rows[10], rows[11],
			rows[12], rows[13], rows[14], rows[15], rows[16],
		})

		/*
			           +----------------------------+
			           |        8,         11,      |
			           +----------------------------+
			          /              /               \
			+-----------+      +---------+      +----------------+
			| 1,3,5,7,8 |      | 9,10,11 |      | 12,13,14,15,16 |
			+-----------+      +---------+      +----------------+
		*/

		// Check the root page
		assert.Equal(t, 2, int(pager.pages[0].InternalNode.Header.KeysNum))
		assert.Equal(t, []RowID{8, 11}, pager.pages[0].InternalNode.Keys())
		// Check the leaf pages
		assert.Equal(t, []RowID{1, 3, 5, 7, 8}, pager.pages[2].LeafNode.Keys())
		assert.Equal(t, []RowID{9, 10, 11}, pager.pages[4].LeafNode.Keys())
		assert.Equal(t, []RowID{12, 13, 14, 15, 16}, pager.pages[5].LeafNode.Keys())
		// Check that leafs[3] is now a free page
		assert.NotNil(t, pager.pages[3].FreePage)
		assert.Nil(t, pager.pages[3].LeafNode)
		assert.Nil(t, pager.pages[3].InternalNode)
		assert.Equal(t, int(pager.pages[6].Index), int(pager.pages[3].FreePage.NextFreePage))
		assert.Equal(t, int(pager.pages[3].Index), int(pager.dbHeader.FirstFreePage))
		assert.Equal(t, 3, int(pager.dbHeader.FreePageCount))
	})

	t.Run("Keep deleting more rows, no merge", func(t *testing.T) {
		ids := rowIDs(rows[9], rows[11], rows[13], rows[15])

		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsInAny(Field{Name: "id"}, ids...),
				},
			},
		})

		assert.Equal(t, 4, result.RowsAffected)
		checkRows(ctx, t, table, []Row{
			rows[1], rows[3], rows[5],
			rows[7], rows[8], rows[10],
			rows[12], rows[14], rows[16],
		})

		/*
			           +----------------------------+
			           |        5,         10,      |
			           +----------------------------+
			          /              /               \
			+--------+           +--------+          +----------+
			| 1,3,5, |           | 7,8,10 |          | 12,14,16 |
			+--------+           +--------+          +----------+
		*/

		// Check the root page
		assert.Equal(t, 2, int(pager.pages[0].InternalNode.Header.KeysNum))
		assert.Equal(t, []RowID{5, 10}, pager.pages[0].InternalNode.Keys())
		// Check the leaf pages
		assert.Equal(t, []RowID{1, 3, 5}, pager.pages[2].LeafNode.Keys())
		assert.Equal(t, []RowID{7, 8, 10}, pager.pages[4].LeafNode.Keys())
		assert.Equal(t, []RowID{12, 14, 16}, pager.pages[5].LeafNode.Keys())
	})

	t.Run("Keep deleting more rows, another merge and borrow", func(t *testing.T) {
		ids := rowIDs(rows[3], rows[12], rows[5])

		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsInAny(Field{Name: "id"}, ids...),
				},
			},
		})

		assert.Equal(t, 3, result.RowsAffected)
		checkRows(ctx, t, table, []Row{
			rows[1], rows[7], rows[8],
			rows[10], rows[14], rows[16],
		})

		/*
		           +-------------+
		           |      8      |
		           +-------------+
		          /               \
		 +-------+                +----------+
		 | 1,7,8 |                | 10,14,16 |
		 +-------+                +----------+
		*/

		// Check the root page
		assert.Equal(t, 1, int(pager.pages[0].InternalNode.Header.KeysNum))
		assert.Equal(t, []RowID{8}, pager.pages[0].InternalNode.Keys())
		// Check the leaf pages
		assert.Equal(t, []RowID{1, 7, 8}, pager.pages[2].LeafNode.Keys())
		assert.Equal(t, []RowID{10, 14, 16}, pager.pages[5].LeafNode.Keys())
		// Check that leafs[4] is now a free page
		assert.NotNil(t, pager.pages[4].FreePage)
		assert.Nil(t, pager.pages[4].LeafNode)
		assert.Nil(t, pager.pages[4].InternalNode)
		assert.Equal(t, int(pager.pages[3].Index), int(pager.pages[4].FreePage.NextFreePage))
		assert.Equal(t, int(pager.pages[4].Index), int(pager.dbHeader.FirstFreePage))
		assert.Equal(t, 4, int(pager.dbHeader.FreePageCount))
	})

	t.Run("Delete one more time, we are left with only root leaf node", func(t *testing.T) {
		ids := rowIDs(rows[14])

		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsInAny(Field{Name: "id"}, ids...),
				},
			},
		})

		assert.Equal(t, 1, result.RowsAffected)
		checkRows(ctx, t, table, []Row{
			rows[1], rows[7], rows[8],
			rows[10], rows[16],
		})

		/*
		   +-----------------+
		   | 1, 7, 8, 10, 16 |
		   +-----------------+
		*/

		assert.Nil(t, pager.pages[0].InternalNode)
		assert.Equal(t, 5, int(pager.pages[0].LeafNode.Header.Cells))
		assert.Equal(t, 0, int(pager.pages[0].LeafNode.Header.Parent))
		assert.Equal(t, 0, int(pager.pages[0].LeafNode.Header.NextLeaf))
		assert.Equal(t, []RowID{1, 7, 8, 10, 16}, pager.pages[0].LeafNode.Keys())
		// Check there are two more free pages (6 in total now)
		assert.NotNil(t, pager.pages[5].FreePage)
		assert.Nil(t, pager.pages[5].LeafNode)
		assert.Nil(t, pager.pages[5].InternalNode)
		assert.Equal(t, int(pager.pages[2].Index), int(pager.pages[5].FreePage.NextFreePage))
		assert.Equal(t, int(pager.pages[5].Index), int(pager.dbHeader.FirstFreePage))
		assert.Equal(t, 6, int(pager.dbHeader.FreePageCount))
	})

	t.Run("Delete all remaining rows", func(t *testing.T) {
		ids := rowIDs(rows[1], rows[7], rows[8], rows[10], rows[16])

		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsInAny(Field{Name: "id"}, ids...),
				},
			},
		})

		assert.Equal(t, 5, result.RowsAffected)
		checkRows(ctx, t, table, nil)

		assert.Equal(t, 0, int(pager.pages[0].LeafNode.Header.Cells))
	})

	assert.Equal(t, 7, int(pager.TotalPages()))
	// Root page cannot be recycled so there should still be just 6 free pages
	assert.NotNil(t, pager.pages[5].FreePage)
	assert.Nil(t, pager.pages[5].LeafNode)
	assert.Nil(t, pager.pages[5].InternalNode)
	assert.Nil(t, pager.pages[5].OverflowPage)
	assert.Equal(t, int(pager.pages[2].Index), int(pager.pages[5].FreePage.NextFreePage))
	assert.Equal(t, int(pager.pages[5].Index), int(pager.dbHeader.FirstFreePage))
	assert.Equal(t, 6, int(pager.dbHeader.FreePageCount))
}

func TestTable_Delete_InternalNodeRebalancing(t *testing.T) {
	var (
		pager, dbFile = initTest(t)
		ctx           = context.Background()
		numRows       = 1000
		rows          = gen.MediumRows(numRows)
		tablePager    = pager.ForTable(testMediumColumns)
		txManager     = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager       = NewTransactionalPager(tablePager, txManager, testTableName, "")
		table         = NewTable(testLogger, txPager, txManager, testTableName, testMediumColumns, 0, nil)
	)
	// maximumICells is normally ~340; set to 5 in production code to stress-test splits.

	// Batch insert test rows
	stmt := Statement{
		Kind:    Insert,
		Fields:  fieldsFromColumns(testMediumColumns...),
		Inserts: [][]OptionalValue{},
	}
	for _, row := range rows {
		stmt.Inserts = append(stmt.Inserts, row.Values)
	}

	mustInsert(ctx, t, table, txManager, stmt)

	checkRows(ctx, t, table, rows)
	assert.Equal(t, 336, int(pager.TotalPages()))

	result := mustDelete(ctx, t, table, txManager, pager, Statement{Kind: Delete})

	assert.Equal(t, len(rows), result.RowsAffected)

	checkRows(ctx, t, table, nil)

	assert.Equal(t, 336, int(pager.TotalPages()))
	assert.Equal(t, 335, int(pager.dbHeader.FreePageCount))
}

func TestTable_Delete_Overflow(t *testing.T) {
	var (
		pager, dbFile = initTest(t)
		ctx           = context.Background()
		tablePager    = pager.ForTable(testOverflowColumns)
		txManager     = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager       = NewTransactionalPager(tablePager, txManager, testTableName, "")
		table         = NewTable(testLogger, txPager, txManager, testTableName, testOverflowColumns, 0, nil)
		rows          = gen.OverflowRows(3, []uint32{
			MaxInlineVarchar,          // inline text
			MaxInlineVarchar + 100,    // text overflows to 1 page
			MaxOverflowPageData + 100, // text overflows to multiple pages
		})
	)

	// Batch insert test rows
	stmt := Statement{
		Kind:    Insert,
		Fields:  fieldsFromColumns(testOverflowColumns...),
		Inserts: [][]OptionalValue{},
	}
	for _, row := range rows {
		stmt.Inserts = append(stmt.Inserts, row.Values)
	}

	mustInsert(ctx, t, table, txManager, stmt)

	require.Equal(t, 4, int(pager.TotalPages()))

	t.Run("Delete inline non overflowing row", func(t *testing.T) {
		ids := rowIDs(rows[0])

		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsInAny(Field{Name: "id"}, ids...),
				},
			},
		})

		assert.Equal(t, 1, result.RowsAffected)
		checkRows(ctx, t, table, rows[1:])

		require.Equal(t, 4, int(pager.TotalPages()))
		assertFreePages(t, tablePager, nil)
	})

	t.Run("Delete overflowing rows", func(t *testing.T) {
		ids := rowIDs(rows[1], rows[2])

		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsInAny(Field{Name: "id"}, ids...),
				},
			},
		})

		assert.Equal(t, 2, result.RowsAffected)
		checkRows(ctx, t, table, nil)

		require.Equal(t, 4, int(pager.TotalPages()))
		assertFreePages(t, tablePager, []PageIndex{3, 2, 1})
	})
}

func rowIDs(rows ...Row) []any {
	ids := make([]any, 0, len(rows))
	for _, r := range rows {
		id, ok := r.GetValue("id")
		if ok {
			ids = append(ids, id.Value.(int64))
		}
	}
	return ids
}
