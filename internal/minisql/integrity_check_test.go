package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestDatabase_QuickCheck(t *testing.T) {
	t.Parallel()

	t.Run("healthy database has no issues", func(t *testing.T) {
		pager, dbFile := initTest(t)
		db, err := NewDatabase(context.Background(), testLogger, dbFile.Name(), nil, pager, pager, nil)
		require.NoError(t, err)

		report, err := db.QuickCheck(context.Background())
		require.NoError(t, err)
		assert.True(t, report.Ok())
		assert.NotZero(t, report.CheckedRootPages)
	})

	t.Run("free page count mismatch is reported", func(t *testing.T) {
		pager, dbFile := initTest(t)
		db, err := NewDatabase(context.Background(), testLogger, dbFile.Name(), nil, pager, pager, nil)
		require.NoError(t, err)

		pager.dbHeader.FreePageCount = 1

		report, err := db.QuickCheck(context.Background())
		require.NoError(t, err)
		assert.False(t, report.Ok())
		assert.Contains(t, issueCodes(report), "free_page_count_mismatch")
	})

	t.Run("invalid free list entry is reported", func(t *testing.T) {
		pager, dbFile := initTest(t)
		db, err := NewDatabase(context.Background(), testLogger, dbFile.Name(), nil, pager, pager, nil)
		require.NoError(t, err)
		userTable := addQuickCheckTestTable(db, pager, "users", 1)
		pager.dbHeader.FirstFreePage = userTable.GetRootPageIdx()
		pager.dbHeader.FreePageCount = 1

		report, err := db.QuickCheck(context.Background())
		require.NoError(t, err)
		assert.False(t, report.Ok())
		assert.Contains(t, issueCodes(report), "free_list_page_not_free")
	})

	t.Run("free list head out of range is reported", func(t *testing.T) {
		pager, dbFile := initTest(t)
		db, err := NewDatabase(context.Background(), testLogger, dbFile.Name(), nil, pager, pager, nil)
		require.NoError(t, err)

		pager.dbHeader.FirstFreePage = PageIndex(99)

		report, err := db.QuickCheck(context.Background())
		require.NoError(t, err)
		assert.False(t, report.Ok())
		assert.Contains(t, issueCodes(report), "free_list_head_out_of_range")
	})

	t.Run("free list cycle is reported", func(t *testing.T) {
		pager, dbFile := initTest(t)
		db, err := NewDatabase(context.Background(), testLogger, dbFile.Name(), nil, pager, pager, nil)
		require.NoError(t, err)

		for len(pager.pages) <= 1 {
			pager.pages = append(pager.pages, nil)
		}
		pager.pages[1] = &Page{
			Index:    1,
			FreePage: &FreePage{NextFreePage: 1},
		}
		pager.totalPages = 2
		pager.dbHeader.FirstFreePage = 1
		pager.dbHeader.FreePageCount = 1

		report, err := db.QuickCheck(context.Background())
		require.NoError(t, err)
		assert.False(t, report.Ok())
		assert.Contains(t, issueCodes(report), "free_list_cycle")
	})

	t.Run("invalid table root page type is reported", func(t *testing.T) {
		pager, dbFile := initTest(t)
		db, err := NewDatabase(context.Background(), testLogger, dbFile.Name(), nil, pager, pager, nil)
		require.NoError(t, err)

		pager.pages[0] = &Page{
			Index:    0,
			FreePage: &FreePage{},
		}

		report, err := db.QuickCheck(context.Background())
		require.NoError(t, err)
		assert.False(t, report.Ok())
		assert.Contains(t, issueCodes(report), "table_root_invalid_type")
	})

	t.Run("table root out of range is reported", func(t *testing.T) {
		pager, dbFile := initTest(t)
		db, err := NewDatabase(context.Background(), testLogger, dbFile.Name(), nil, pager, pager, nil)
		require.NoError(t, err)

		db.tables["users"] = NewTable(
			testLogger,
			NewTransactionalPager(pager.ForTable(testColumns[:1]), db.txManager, "users", ""),
			db.txManager,
			"users",
			testColumns[:1],
			5,
			db.lockedProvider,
		)

		report, err := db.QuickCheck(context.Background())
		require.NoError(t, err)
		assert.False(t, report.Ok())
		assert.Contains(t, issueCodes(report), "table_root_out_of_range")
	})

	t.Run("index roots are checked", func(t *testing.T) {
		pager, dbFile := initTest(t)
		db, err := NewDatabase(context.Background(), testLogger, dbFile.Name(), nil, pager, pager, nil)
		require.NoError(t, err)

		addQuickCheckTestTableWithSecondaryIndex(db, pager, testTableName, 1, "test_table_email_idx", 2)

		report, err := db.QuickCheck(context.Background())
		require.NoError(t, err)
		assert.True(t, report.Ok())
		assert.GreaterOrEqual(t, report.CheckedRootPages, 3)
	})
}

func TestDatabase_IntegrityCheck(t *testing.T) {
	t.Parallel()

	t.Run("orphan page is reported", func(t *testing.T) {
		pager, dbFile := initTest(t)
		db, err := NewDatabase(context.Background(), testLogger, dbFile.Name(), nil, pager, pager, nil)
		require.NoError(t, err)

		orphanPageIdx := PageIndex(1)
		for len(pager.pages) <= int(orphanPageIdx) {
			pager.pages = append(pager.pages, nil)
		}
		pager.pages[orphanPageIdx] = &Page{
			Index: orphanPageIdx,
			LeafNode: &LeafNode{
				Header: LeafNodeHeader{
					Header: Header{IsRoot: true},
				},
			},
		}
		pager.totalPages = 2

		report, err := db.IntegrityCheck(context.Background())
		require.NoError(t, err)
		assert.False(t, report.Ok())
		assert.Contains(t, issueCodes(report), "orphan_page")
	})

	t.Run("page reachable from multiple objects is reported", func(t *testing.T) {
		pager, dbFile := initTest(t)
		db, err := NewDatabase(context.Background(), testLogger, dbFile.Name(), nil, pager, pager, nil)
		require.NoError(t, err)

		addQuickCheckTestTable(db, pager, "users", 1)
		addQuickCheckTestTable(db, pager, "orders", 1)

		report, err := db.IntegrityCheck(context.Background())
		require.NoError(t, err)
		assert.False(t, report.Ok())
		assert.Contains(t, issueCodes(report), "page_reachable_from_multiple_objects")
	})

	t.Run("table page out of range is reported", func(t *testing.T) {
		pager, dbFile := initTest(t)
		db, err := NewDatabase(context.Background(), testLogger, dbFile.Name(), nil, pager, pager, nil)
		require.NoError(t, err)

		rootPageIdx := PageIndex(1)
		addQuickCheckTestTable(db, pager, "users", rootPageIdx)
		pager.pages[rootPageIdx] = &Page{
			Index: rootPageIdx,
			InternalNode: &InternalNode{
				Header: InternalNodeHeader{
					Header:     Header{IsRoot: true},
					KeysNum:    1,
					RightChild: RightChildNotSet,
				},
				ICells: [InternalNodeMaxCells]ICell{
					{Child: 99},
				},
			},
		}
		pager.totalPages = 2

		report, err := db.IntegrityCheck(context.Background())
		require.NoError(t, err)
		assert.False(t, report.Ok())
		assert.Contains(t, issueCodes(report), "table_internal_missing_right_child")
		assert.Contains(t, issueCodes(report), "table_page_out_of_range")
	})

	t.Run("index pages are traversed", func(t *testing.T) {
		pager, dbFile := initTest(t)
		db, err := NewDatabase(context.Background(), testLogger, dbFile.Name(), nil, pager, pager, nil)
		require.NoError(t, err)

		addQuickCheckTestTableWithSecondaryIndex(db, pager, testTableName, 1, "test_table_email_idx", 2)

		report, err := db.IntegrityCheck(context.Background())
		require.NoError(t, err)
		assert.True(t, report.Ok())
		assert.Greater(t, report.CheckedLivePages, 0)
	})

	t.Run("missing unique index entry is reported", func(t *testing.T) {
		pager, dbFile := initTest(t)
		mockParser := new(MockParser)
		db, err := NewDatabase(context.Background(), testLogger, dbFile.Name(), mockParser, pager, pager, nil)
		require.NoError(t, err)

		createTableStmt := Statement{
			Kind:      CreateTable,
			TableName: testTableName,
			Columns:   append([]Column{}, testColumns[:2]...),
			UniqueIndexes: []UniqueIndex{
				{
					IndexInfo: IndexInfo{
						Name:    UniqueIndexName(testTableName, "email"),
						Columns: testColumns[1:2],
					},
				},
			},
		}
		mockParser.EXPECT().Parse(mock.Anything, createTableStmt.DDL()).Return([]Statement{createTableStmt}, nil).Once()

		err = db.txManager.ExecuteInTransaction(context.Background(), func(ctx context.Context) error {
			_, err := db.ExecuteStatement(ctx, createTableStmt)
			return err
		})
		require.NoError(t, err)

		insertStmt := Statement{
			Kind:      Insert,
			TableName: testTableName,
			Fields: []Field{
				{Name: "email"},
			},
			Inserts: [][]OptionalValue{{
				{Value: NewTextPointer([]byte("alice@example.com")), Valid: true},
			}},
		}
		err = db.txManager.ExecuteInTransaction(context.Background(), func(ctx context.Context) error {
			_, err := db.tables[testTableName].Insert(ctx, insertStmt)
			return err
		})
		require.NoError(t, err)

		uniqueIndex := db.tables[testTableName].UniqueIndexes[UniqueIndexName(testTableName, "email")]
		err = db.txManager.ExecuteInTransaction(context.Background(), func(ctx context.Context) error {
			return uniqueIndex.Index.Delete(ctx, "alice@example.com", 1)
		})
		require.NoError(t, err)

		report, err := db.IntegrityCheck(context.Background())
		require.NoError(t, err)
		assert.Contains(t, issueCodes(report), "index_missing_entry")
	})

	t.Run("orphan secondary index entry is reported", func(t *testing.T) {
		pager, dbFile := initTest(t)
		mockParser := new(MockParser)
		db, err := NewDatabase(context.Background(), testLogger, dbFile.Name(), mockParser, pager, pager, nil)
		require.NoError(t, err)

		createTableStmt := Statement{
			Kind:      CreateTable,
			TableName: testTableName,
			Columns:   append([]Column{}, testColumns[:2]...),
		}
		createIndexStmt := Statement{
			Kind:      CreateIndex,
			TableName: testTableName,
			IndexName: "idx_email",
			Columns:   testColumns[1:2],
		}
		mockParser.EXPECT().Parse(mock.Anything, createTableStmt.DDL()).Return([]Statement{createTableStmt}, nil).Once()

		err = db.txManager.ExecuteInTransaction(context.Background(), func(ctx context.Context) error {
			_, err := db.ExecuteStatement(ctx, createTableStmt)
			if err != nil {
				return err
			}
			_, err = db.ExecuteStatement(ctx, createIndexStmt)
			return err
		})
		require.NoError(t, err)

		insertStmt := Statement{
			Kind:      Insert,
			TableName: testTableName,
			Fields: []Field{
				{Name: "email"},
			},
			Inserts: [][]OptionalValue{{
				{Value: NewTextPointer([]byte("alice@example.com")), Valid: true},
			}},
		}
		err = db.txManager.ExecuteInTransaction(context.Background(), func(ctx context.Context) error {
			_, err := db.tables[testTableName].Insert(ctx, insertStmt)
			return err
		})
		require.NoError(t, err)

		secondaryIndex := db.tables[testTableName].SecondaryIndexes["idx_email"]
		err = db.txManager.ExecuteInTransaction(context.Background(), func(ctx context.Context) error {
			return secondaryIndex.Index.Insert(ctx, "ghost@example.com", 999)
		})
		require.NoError(t, err)

		report, err := db.IntegrityCheck(context.Background())
		require.NoError(t, err)
		assert.Contains(t, issueCodes(report), "index_orphan_entry")
	})

	t.Run("missing full-text index posting is reported", func(t *testing.T) {
		pager, dbFile := initTest(t)
		mockParser := new(MockParser)
		db, err := NewDatabase(context.Background(), testLogger, dbFile.Name(), mockParser, pager, pager, nil)
		require.NoError(t, err)

		createTableStmt := Statement{
			Kind:      CreateTable,
			TableName: "articles_integrity",
			Columns: []Column{
				{Name: "body", Kind: Text},
			},
		}
		createIndexStmt := Statement{
			Kind:           CreateIndex,
			TableName:      "articles_integrity",
			IndexName:      "idx_articles_integrity_body",
			Columns:        []Column{{Name: "body"}},
			IndexMethod:    IndexMethodFullText,
			IndexTokenizer: TextSearchTokenizerSimple,
		}
		mockParser.EXPECT().Parse(mock.Anything, createTableStmt.DDL()).Return([]Statement{createTableStmt}, nil).Once()

		err = db.txManager.ExecuteInTransaction(context.Background(), func(ctx context.Context) error {
			_, err := db.ExecuteStatement(ctx, createTableStmt)
			if err != nil {
				return err
			}
			_, err = db.ExecuteStatement(ctx, createIndexStmt)
			return err
		})
		require.NoError(t, err)

		insertStmt := Statement{
			Kind:      Insert,
			TableName: "articles_integrity",
			Fields:    []Field{{Name: "body"}},
			Inserts: [][]OptionalValue{{
				{Value: NewTextPointer([]byte("database pages")), Valid: true},
			}},
		}
		err = db.txManager.ExecuteInTransaction(context.Background(), func(ctx context.Context) error {
			_, err := db.tables["articles_integrity"].Insert(ctx, insertStmt)
			return err
		})
		require.NoError(t, err)

		secondaryIndex := db.tables["articles_integrity"].SecondaryIndexes["idx_articles_integrity_body"]
		err = db.txManager.ExecuteInTransaction(context.Background(), func(ctx context.Context) error {
			return secondaryIndex.InvertedIndex.Delete(ctx, "database", invertedPosting{RowID: 0, Positions: []uint32{0}})
		})
		require.NoError(t, err)

		report, err := db.IntegrityCheck(context.Background())
		require.NoError(t, err)
		assert.Contains(t, issueCodes(report), "index_missing_entry")
	})

	t.Run("orphan JSON inverted index posting is reported", func(t *testing.T) {
		pager, dbFile := initTest(t)
		mockParser := new(MockParser)
		db, err := NewDatabase(context.Background(), testLogger, dbFile.Name(), mockParser, pager, pager, nil)
		require.NoError(t, err)

		createTableStmt := Statement{
			Kind:      CreateTable,
			TableName: "events_integrity",
			Columns: []Column{
				{Name: "payload", Kind: JSON},
			},
		}
		createIndexStmt := Statement{
			Kind:        CreateIndex,
			TableName:   "events_integrity",
			IndexName:   "idx_events_integrity_payload",
			Columns:     []Column{{Name: "payload"}},
			IndexMethod: IndexMethodInverted,
		}
		mockParser.EXPECT().Parse(mock.Anything, createTableStmt.DDL()).Return([]Statement{createTableStmt}, nil).Once()

		err = db.txManager.ExecuteInTransaction(context.Background(), func(ctx context.Context) error {
			_, err := db.ExecuteStatement(ctx, createTableStmt)
			if err != nil {
				return err
			}
			_, err = db.ExecuteStatement(ctx, createIndexStmt)
			return err
		})
		require.NoError(t, err)

		insertStmt := Statement{
			Kind:      Insert,
			TableName: "events_integrity",
			Fields:    []Field{{Name: "payload"}},
			Inserts: [][]OptionalValue{{
				{Value: NewTextPointer([]byte(`{"type":"click"}`)), Valid: true},
			}},
		}
		err = db.txManager.ExecuteInTransaction(context.Background(), func(ctx context.Context) error {
			_, err := db.tables["events_integrity"].Insert(ctx, insertStmt)
			return err
		})
		require.NoError(t, err)

		secondaryIndex := db.tables["events_integrity"].SecondaryIndexes["idx_events_integrity_payload"]
		err = db.txManager.ExecuteInTransaction(context.Background(), func(ctx context.Context) error {
			return secondaryIndex.InvertedIndex.Insert(ctx, `kv:type:s:"ghost"`, invertedPosting{RowID: 99})
		})
		require.NoError(t, err)

		report, err := db.IntegrityCheck(context.Background())
		require.NoError(t, err)
		assert.Contains(t, issueCodes(report), "index_orphan_entry")
	})
}

func issueCodes(report IntegrityReport) []string {
	codes := make([]string, 0, len(report.Issues))
	for _, issue := range report.Issues {
		codes = append(codes, issue.Code)
	}
	return codes
}

func addQuickCheckTestTable(db *Database, pager *pagerImpl, name string, rootPageIdx PageIndex) *Table {
	return addQuickCheckTestTableWithColumns(db, pager, name, rootPageIdx, []Column{{
		Kind: Int8,
		Size: 8,
		Name: "id",
	}})
}

func addQuickCheckTestTableWithColumns(db *Database, pager *pagerImpl, name string, rootPageIdx PageIndex, columns []Column) *Table {
	for len(pager.pages) <= int(rootPageIdx) {
		pager.pages = append(pager.pages, nil)
	}
	pager.pages[rootPageIdx] = &Page{
		Index: rootPageIdx,
		LeafNode: &LeafNode{
			Header: LeafNodeHeader{
				Header: Header{
					IsRoot: true,
				},
			},
		},
	}
	if pager.totalPages <= uint32(rootPageIdx) {
		pager.totalPages = uint32(rootPageIdx) + 1
	}

	table := NewTable(
		testLogger,
		NewTransactionalPager(pager.ForTable(columns), db.txManager, name, ""),
		db.txManager,
		name,
		columns,
		rootPageIdx,
		db.lockedProvider,
	)
	db.tables[name] = table
	return table
}

func addQuickCheckTestTableWithSecondaryIndex(db *Database, pager *pagerImpl, tableName string, tableRootPageIdx PageIndex, indexName string, indexRootPageIdx PageIndex) *Table {
	columns := []Column{
		{
			Kind: Int8,
			Size: 8,
			Name: "id",
		},
		{
			Kind:     Varchar,
			Size:     MaxInlineVarchar,
			Name:     "email",
			Nullable: true,
		},
	}
	table := addQuickCheckTestTableWithColumns(db, pager, tableName, tableRootPageIdx, columns)

	for len(pager.pages) <= int(indexRootPageIdx) {
		pager.pages = append(pager.pages, nil)
	}
	indexNode := NewIndexNode[string](false)
	indexNode.Header.IsRoot = true
	indexNode.Header.IsLeaf = true
	pager.pages[indexRootPageIdx] = &Page{
		Index:     indexRootPageIdx,
		IndexNode: indexNode,
	}
	if pager.totalPages <= uint32(indexRootPageIdx) {
		pager.totalPages = uint32(indexRootPageIdx) + 1
	}

	index, err := NewNonUniqueIndex[string](
		testLogger,
		db.txManager,
		indexName,
		columns[1:2],
		NewTransactionalPager(pager.ForIndex(columns[1:2], false), db.txManager, tableName, indexName),
		indexRootPageIdx,
	)
	if err != nil {
		panic(err)
	}
	table.SetSecondaryIndex(SecondaryIndex{IndexInfo: IndexInfo{Name: indexName, Columns: columns[1:2]}, Index: index})
	return table
}
