package minisql

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	testDbName = "test_db"
)

func TestNewDatabase(t *testing.T) {
	aPager, dbFile := initTest(t)

	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, testLogger, dbFile.Name(), nil, aPager, aPager)
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 1)
	assert.Equal(t, dbFile.Name(), aDatabase.GetFileName())
	assert.Equal(t, SchemaTableName, aDatabase.tables[SchemaTableName].Name)
	assert.Equal(t, PageIndex(0), aDatabase.tables[SchemaTableName].GetRootPageIdx())
	assert.Contains(t, aDatabase.ListTableNames(ctx), SchemaTableName)
}

func TestNewDatabase_MultipleTablesWithIndexes(t *testing.T) {
	var (
		aPager, dbFile  = initTest(t)
		mockParser      = new(MockParser)
		ctx             = context.Background()
		uniqueIndexName = UniqueIndexName(testTableName3, "email")
	)

	aDatabase, err := NewDatabase(ctx, testLogger, dbFile.Name(), mockParser, aPager, aPager)
	require.NoError(t, err)

	// Let's create 4 tables:
	// - one without any index
	// - one with a primary key
	// - one with a unique index
	stmt := Statement{
		Kind:      CreateTable,
		TableName: testTableName,
		Columns:   append([]Column{}, testColumns...),
	}
	stmt2 := Statement{
		Kind:       CreateTable,
		TableName:  testTableName2,
		Columns:    append([]Column{}, testColumns[0:2]...),
		PrimaryKey: NewPrimaryKey(PrimaryKeyName(testTableName2), testColumns[0:1], true),
	}
	stmt3 := Statement{
		Kind:      CreateTable,
		TableName: testTableName3,
		Columns:   append([]Column{}, testColumns[0:2]...),
		UniqueIndexes: []UniqueIndex{
			{
				IndexInfo: IndexInfo{
					Name:    uniqueIndexName,
					Columns: testColumns[1:2],
				},
			},
		},
	}

	for _, s := range []Statement{stmt, stmt2, stmt3} {
		err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			_, err := aDatabase.ExecuteStatement(ctx, s)
			return err
		})
		require.NoError(t, err)
	}

	// Now, let's re-initialize the database to load existing tables
	mockParser.On("Parse", mock.Anything, stmt.DDL()).Return([]Statement{stmt}, nil)
	mockParser.On("Parse", mock.Anything, stmt2.DDL()).Return([]Statement{stmt2}, nil)
	mockParser.On("Parse", mock.Anything, stmt3.DDL()).Return([]Statement{stmt3}, nil)

	aDatabase, err = NewDatabase(ctx, testLogger, dbFile.Name(), mockParser, aPager, aPager)
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 4)
	assert.ElementsMatch(t, []string{
		SchemaTableName,
		testTableName,
		testTableName2,
		testTableName3,
	}, aDatabase.ListTableNames(ctx))

	assert.Equal(t, testTableName, aDatabase.tables[testTableName].Name)
	assert.Equal(t, testColumns, aDatabase.tables[testTableName].Columns)

	assert.Equal(t, testTableName2, aDatabase.tables[testTableName2].Name)
	assert.Equal(t, testColumns[0:2], aDatabase.tables[testTableName2].Columns)
	assert.Empty(t, aDatabase.tables[testTableName2].UniqueIndexes)
	assert.Empty(t, aDatabase.tables[testTableName2].SecondaryIndexes)
	assert.NotNil(t, aDatabase.tables[testTableName2].PrimaryKey.Index)

	assert.Equal(t, testTableName3, aDatabase.tables[testTableName3].Name)
	assert.Equal(t, testColumns[0:2], aDatabase.tables[testTableName3].Columns)
	assert.Empty(t, aDatabase.tables[testTableName3].PrimaryKey)
	assert.Empty(t, aDatabase.tables[testTableName3].SecondaryIndexes)
	assert.Len(t, aDatabase.tables[testTableName3].UniqueIndexes, 1)
	assert.NotNil(t, aDatabase.tables[testTableName3].UniqueIndexes[uniqueIndexName])

	assert.Equal(t, PageIndex(0), aDatabase.tables[SchemaTableName].GetRootPageIdx())
	assert.Equal(t, PageIndex(1), aDatabase.tables[testTableName].GetRootPageIdx())
	assert.Equal(t, PageIndex(2), aDatabase.tables[testTableName2].GetRootPageIdx())
	assert.Equal(t, PageIndex(3), aDatabase.tables[testTableName2].PrimaryKey.Index.GetRootPageIdx())
	assert.Equal(t, PageIndex(4), aDatabase.tables[testTableName3].GetRootPageIdx())
	assert.Equal(t, PageIndex(5), aDatabase.tables[testTableName3].UniqueIndexes[uniqueIndexName].Index.GetRootPageIdx())

	mock.AssertExpectationsForObjects(t, mockParser)

	// Check system schema table contents
	schemas := collectMainSchemas(t, ctx, aDatabase)
	assert.Len(t, schemas, 6)

	// First created table without any index
	assert.Equal(t, SchemaTable, schemas[1].Type)
	assert.Equal(t, testTableName, schemas[1].Name)
	assert.Empty(t, schemas[1].TableName)
	assert.Equal(t, 1, int(schemas[1].RootPage))

	// Second created table with primary key
	assert.Equal(t, SchemaTable, schemas[2].Type)
	assert.Equal(t, testTableName2, schemas[2].Name)
	assert.Empty(t, schemas[2].TableName)
	assert.Equal(t, 2, int(schemas[2].RootPage))

	// Primary key for the second table
	assert.Equal(t, SchemaPrimaryKey, schemas[3].Type)
	assert.Equal(t, PrimaryKeyName(testTableName2), schemas[3].Name)
	assert.Equal(t, testTableName2, schemas[3].TableName)
	assert.Equal(t, 3, int(schemas[3].RootPage))

	// Third created table with unique index
	assert.Equal(t, SchemaTable, schemas[4].Type)
	assert.Equal(t, testTableName3, schemas[4].Name)
	assert.Empty(t, schemas[4].TableName)
	assert.Equal(t, 4, int(schemas[4].RootPage))

	// Unique index for the third table
	assert.Equal(t, SchemaUniqueIndex, schemas[5].Type)
	assert.Equal(t, uniqueIndexName, schemas[5].Name)
	assert.Equal(t, testTableName3, schemas[5].TableName)
	assert.Equal(t, 5, int(schemas[5].RootPage))

	mock.AssertExpectationsForObjects(t, mockParser)
}

func TestDatabase_CreateTable(t *testing.T) {
	aPager, dbFile := initTest(t)

	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, testLogger, dbFile.Name(), nil, aPager, aPager)
	require.NoError(t, err)

	t.Run("Create table", func(t *testing.T) {
		stmt := Statement{
			Kind:      CreateTable,
			TableName: testTableName,
			Columns:   append([]Column{}, testColumns...),
		}
		err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			_, err := aDatabase.ExecuteStatement(ctx, stmt)
			return err
		})
		require.NoError(t, err)

		assert.Len(t, aDatabase.tables, 2)
		assert.ElementsMatch(t, []string{
			SchemaTableName,
			testTableName,
		}, aDatabase.ListTableNames(ctx))

		assert.Equal(t, testTableName, aDatabase.tables[testTableName].Name)
		assert.Equal(t, testColumns, aDatabase.tables[testTableName].Columns)
		assert.Equal(t, PageIndex(0), aDatabase.tables[SchemaTableName].GetRootPageIdx())
		assert.Equal(t, PageIndex(1), aDatabase.tables[testTableName].GetRootPageIdx())

		// Root page plus a new page for table, should be 2 in total
		assert.Len(t, aPager.pages, 2)

		// Check system schema table contents
		schemas := collectMainSchemas(t, ctx, aDatabase)
		assert.Len(t, schemas, 2)

		// Created table without any index
		assert.Equal(t, SchemaTable, schemas[1].Type)
		assert.Equal(t, testTableName, schemas[1].Name)
		assert.Equal(t, 1, int(schemas[1].RootPage))
	})

	t.Run("Drop table", func(t *testing.T) {
		stmt := Statement{
			Kind:      DropTable,
			TableName: testTableName,
		}
		err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			_, err := aDatabase.ExecuteStatement(ctx, stmt)
			return err
		})
		require.NoError(t, err)

		assert.Len(t, aDatabase.tables, 1)
		assert.Equal(t, dbFile.Name(), aDatabase.GetFileName())
		assert.Equal(t, SchemaTableName, aDatabase.tables[SchemaTableName].Name)
		assert.Equal(t, PageIndex(0), aDatabase.tables[SchemaTableName].GetRootPageIdx())
		assert.Equal(t, []string{SchemaTableName}, aDatabase.ListTableNames(ctx))

		tablePager := aDatabase.factory.ForTable(testColumns)
		assertFreePages(t, tablePager, []PageIndex{1})

		// Check system table contents
		schemas := collectMainSchemas(t, ctx, aDatabase)
		assert.Len(t, schemas, 1)
	})
}

func TestDatabase_CreateTable_WithPrimaryKey(t *testing.T) {
	aPager, dbFile := initTest(t)

	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, testLogger, dbFile.Name(), nil, aPager, aPager)
	require.NoError(t, err)

	t.Run("Create table", func(t *testing.T) {
		stmt := Statement{
			Kind:       CreateTable,
			TableName:  testTableName,
			Columns:    append([]Column{}, testColumns[0:2]...),
			PrimaryKey: NewPrimaryKey(PrimaryKeyName(testTableName), testColumns[0:1], true),
		}
		err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			_, err := aDatabase.ExecuteStatement(ctx, stmt)
			return err
		})
		require.NoError(t, err)

		assert.Len(t, aDatabase.tables, 2)
		assert.ElementsMatch(t, []string{
			SchemaTableName,
			testTableName,
		}, aDatabase.ListTableNames(ctx))

		assert.Equal(t, testTableName, aDatabase.tables[testTableName].Name)
		assert.Equal(t, testColumns[0:2], aDatabase.tables[testTableName].Columns)
		assert.Empty(t, aDatabase.tables[testTableName].UniqueIndexes)
		assert.Empty(t, aDatabase.tables[testTableName].SecondaryIndexes)
		assert.NotNil(t, aDatabase.tables[testTableName].PrimaryKey.Index)

		assert.Equal(t, PageIndex(1), aDatabase.tables[testTableName].GetRootPageIdx())
		assert.Equal(t, PageIndex(0), aDatabase.tables[SchemaTableName].GetRootPageIdx())
		assert.Equal(t, PageIndex(2), aDatabase.tables[testTableName].PrimaryKey.Index.GetRootPageIdx())
		// Root page plus a new page for table and index, should be 3 in total
		assert.Len(t, aPager.pages, 3)

		// Check system schema table contents
		schemas := collectMainSchemas(t, ctx, aDatabase)
		assert.Len(t, schemas, 3)

		// Created table with primary key
		assert.Equal(t, SchemaTable, schemas[1].Type)
		assert.Equal(t, testTableName, schemas[1].Name)
		assert.Equal(t, 1, int(schemas[1].RootPage))

		// Primary key for the created table
		assert.Equal(t, SchemaPrimaryKey, schemas[2].Type)
		assert.Equal(t, PrimaryKeyName(testTableName), schemas[2].Name)
		assert.Equal(t, 2, int(schemas[2].RootPage))
	})

	t.Run("Drop table", func(t *testing.T) {
		stmt := Statement{
			Kind:      DropTable,
			TableName: testTableName,
		}
		err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			_, err := aDatabase.ExecuteStatement(ctx, stmt)
			return err
		})
		require.NoError(t, err)

		assert.Len(t, aDatabase.tables, 1)
		assert.Equal(t, dbFile.Name(), aDatabase.GetFileName())
		assert.Equal(t, SchemaTableName, aDatabase.tables[SchemaTableName].Name)
		assert.Equal(t, PageIndex(0), aDatabase.tables[SchemaTableName].GetRootPageIdx())
		assert.Equal(t, []string{SchemaTableName}, aDatabase.ListTableNames(ctx))

		tablePager := aDatabase.factory.ForTable(testColumns)
		assertFreePages(t, tablePager, []PageIndex{2, 1})

		// Check system table contents
		schemas := collectMainSchemas(t, ctx, aDatabase)
		assert.Len(t, schemas, 1)
	})
}

func TestDatabase_CreateTable_WithUniqueIndex(t *testing.T) {
	aPager, dbFile := initTest(t)
	indexName := UniqueIndexName(testTableName, testColumns[1].Name)

	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, testLogger, dbFile.Name(), nil, aPager, aPager)
	require.NoError(t, err)

	t.Run("Create table", func(t *testing.T) {
		stmt := Statement{
			Kind:      CreateTable,
			TableName: testTableName,
			Columns:   append([]Column{}, testColumns[0:2]...),
			UniqueIndexes: []UniqueIndex{
				{
					IndexInfo: IndexInfo{
						Name:    indexName,
						Columns: testColumns[1:2],
					},
				},
			},
		}
		err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			_, err := aDatabase.ExecuteStatement(ctx, stmt)
			return err
		})
		require.NoError(t, err)

		assert.Len(t, aDatabase.tables, 2)
		assert.ElementsMatch(t, []string{
			SchemaTableName,
			testTableName,
		}, aDatabase.ListTableNames(ctx))

		assert.Equal(t, testTableName, aDatabase.tables[testTableName].Name)
		assert.Equal(t, testColumns[0:2], aDatabase.tables[testTableName].Columns)
		assert.Empty(t, aDatabase.tables[testTableName].PrimaryKey)
		assert.Empty(t, aDatabase.tables[testTableName].SecondaryIndexes)
		assert.Len(t, aDatabase.tables[testTableName].UniqueIndexes, 1)
		assert.NotNil(t, aDatabase.tables[testTableName].UniqueIndexes[indexName])

		assert.Equal(t, PageIndex(1), aDatabase.tables[testTableName].GetRootPageIdx())
		assert.Equal(t, PageIndex(0), aDatabase.tables[SchemaTableName].GetRootPageIdx())
		assert.Equal(t, PageIndex(2), aDatabase.tables[testTableName].UniqueIndexes[indexName].Index.GetRootPageIdx())

		// Root page plus a new page for table and index, should be 3 in total
		assert.Len(t, aPager.pages, 3)

		// Check system table contents
		schemas := collectMainSchemas(t, ctx, aDatabase)
		assert.Len(t, schemas, 3)

		// Created table with unique index
		assert.Equal(t, SchemaTable, schemas[1].Type)
		assert.Equal(t, testTableName, schemas[1].Name)
		assert.Equal(t, 1, int(schemas[1].RootPage))

		// Unique index for the created table
		assert.Equal(t, SchemaUniqueIndex, schemas[2].Type)
		assert.Equal(t, indexName, schemas[2].Name)
		assert.Equal(t, 2, int(schemas[2].RootPage))
	})

	t.Run("Drop table", func(t *testing.T) {
		stmt := Statement{
			Kind:      DropTable,
			TableName: testTableName,
		}
		err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			_, err := aDatabase.ExecuteStatement(ctx, stmt)
			return err
		})
		require.NoError(t, err)

		assert.Len(t, aDatabase.tables, 1)
		assert.Equal(t, dbFile.Name(), aDatabase.GetFileName())
		assert.Equal(t, SchemaTableName, aDatabase.tables[SchemaTableName].Name)
		assert.Equal(t, PageIndex(0), aDatabase.tables[SchemaTableName].GetRootPageIdx())
		assert.Equal(t, []string{SchemaTableName}, aDatabase.ListTableNames(ctx))

		tablePager := aDatabase.factory.ForTable(testColumns)
		assertFreePages(t, tablePager, []PageIndex{2, 1})

		// Check system table contents
		schemas := collectMainSchemas(t, ctx, aDatabase)
		assert.Len(t, schemas, 1)
	})
}

func TestDatabase_CreateIndex(t *testing.T) {
	var (
		aPager, dbFile = initTest(t)
		mockParser     = new(MockParser)
		ctx            = context.Background()
	)

	aDatabase, err := NewDatabase(ctx, testLogger, dbFile.Name(), mockParser, aPager, aPager)
	require.NoError(t, err)

	// First create a test table
	stmt := Statement{
		Kind:      CreateTable,
		TableName: testTableName,
		Columns:   append([]Column{}, testColumns...),
	}

	mockParser.On("Parse", mock.Anything, stmt.DDL()).Return([]Statement{stmt}, nil)

	err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := aDatabase.ExecuteStatement(ctx, stmt)
		return err
	})
	require.NoError(t, err)

	t.Run("Create index when table does not exist", func(t *testing.T) {
		createIndexStmt := Statement{
			Kind:      CreateIndex,
			IndexName: "foo_bar",
			TableName: "bogus",
			Columns:   []Column{{Name: "created"}},
		}
		err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			_, err := aDatabase.ExecuteStatement(ctx, createIndexStmt)
			return err
		})
		require.Error(t, err)
		assert.ErrorContains(t, err, "table does not exist")
	})

	t.Run("Create index", func(t *testing.T) {
		createIndexStmt := Statement{
			Kind:      CreateIndex,
			IndexName: "foo_bar",
			TableName: testTableName,
			Columns: []Column{
				{
					Name: "created",
				},
			},
		}
		err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			_, err := aDatabase.ExecuteStatement(ctx, createIndexStmt)
			return err
		})
		require.NoError(t, err)

		// Verify index exists has been added to the table
		assert.Equal(t, testColumns[len(testColumns)-1], aDatabase.tables[testTableName].SecondaryIndexes["foo_bar"].Columns[0])
		assert.Equal(t, "foo_bar", aDatabase.tables[testTableName].SecondaryIndexes["foo_bar"].Name)
		assert.NotNil(t, aDatabase.tables[testTableName].SecondaryIndexes["foo_bar"].Index)
		assert.Equal(t, PageIndex(2), aDatabase.tables[testTableName].SecondaryIndexes["foo_bar"].Index.GetRootPageIdx())

		// Root page plus a page for the test table, plus a new page for index, should be 3 in total
		assert.Len(t, aPager.pages, 3)

		// Check system table contents
		schemas := collectMainSchemas(t, ctx, aDatabase)
		assert.Len(t, schemas, 3)

		// Created table without any index
		assert.Equal(t, SchemaSecondaryIndex, schemas[2].Type)
		assert.Equal(t, "foo_bar", schemas[2].Name)
		assert.Equal(t, testTableName, schemas[2].TableName)
		assert.Equal(t, 2, int(schemas[2].RootPage))
	})

	t.Run("Drop index when index does not exist", func(t *testing.T) {
		createIndexStmt := Statement{
			Kind:      DropIndex,
			IndexName: "bogus",
		}
		err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			_, err := aDatabase.ExecuteStatement(ctx, createIndexStmt)
			return err
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, errIndexDoesNotExist)
	})

	t.Run("Drop index", func(t *testing.T) {
		// Now create an index on the test table
		createStmt := Statement{
			Kind:      CreateIndex,
			IndexName: "foo_bar",
			TableName: testTableName,
			Columns: []Column{
				{
					Name: "created",
				},
			},
		}
		mockParser.On("Parse", mock.Anything, createStmt.DDL()).Return([]Statement{createStmt}, nil).Once()

		deleteStmt := Statement{
			Kind:      DropIndex,
			IndexName: "foo_bar",
		}
		err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			_, err := aDatabase.ExecuteStatement(ctx, deleteStmt)
			return err
		})
		require.NoError(t, err)

		// Verify index has been removed from the table
		assert.Empty(t, aDatabase.tables[testTableName].SecondaryIndexes)

		// Root page plus a page for the test table, we should be back to 2 in total
		// Third page should have been freed
		assert.Len(t, aPager.pages, 3)

		tablePager := aDatabase.factory.ForTable(testColumns)
		assertFreePages(t, tablePager, []PageIndex{2})

		// Check system table contents
		schemas := collectMainSchemas(t, ctx, aDatabase)
		assert.Len(t, schemas, 2)

		assert.Equal(t, SchemaTable, schemas[0].Type)
		assert.Equal(t, SchemaTable, schemas[1].Type)
	})

	mock.AssertExpectationsForObjects(t, mockParser)
}

func initTest(t *testing.T) (*pagerImpl, *os.File) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", testDbName)
	require.NoError(t, err)
	t.Cleanup(func() { os.Remove(tempFile.Name()) })

	aPager, err := NewPager(tempFile, PageSize, 1000)
	require.NoError(t, err)

	return aPager, tempFile
}

func collectMainSchemas(t *testing.T, ctx context.Context, aDatabase *Database) []Schema {
	mainTable := aDatabase.tables[SchemaTableName]
	schemaResults, err := mainTable.Select(ctx, Statement{
		Kind:   Select,
		Fields: mainTableFields,
	})
	require.NoError(t, err)

	var schemas []Schema
	for schemaResults.Rows.Next(ctx) {
		schemas = append(schemas, scanSchema(schemaResults.Rows.Row()))
	}
	require.NoError(t, schemaResults.Rows.Err())
	return schemas
}

func mockPagerFactory(aPager Pager) TxPagerFactory {
	return func(ctx context.Context, tableName, indexName string) (Pager, error) {
		return aPager, nil
	}
}
