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
	aPager := initTest(t)

	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, testLogger, testDbName, nil, aPager, aPager)
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 1)
	assert.Equal(t, testDbName, aDatabase.Name)
	assert.Equal(t, SchemaTableName, aDatabase.tables[SchemaTableName].Name)
	assert.Equal(t, PageIndex(0), aDatabase.tables[SchemaTableName].GetRootPageIdx())
	assert.Contains(t, aDatabase.ListTableNames(ctx), SchemaTableName)
}

func TestNewDatabase_MultipleTablesWithIndexes(t *testing.T) {
	var (
		aPager             = initTest(t)
		mockParser         = new(MockParser)
		ctx                = context.Background()
		uniqueIndexName    = uniqueIndexName(testTableName3, "email")
		secondaryIndexName = secondaryIndexName(testTableName4, "email")
	)

	aDatabase, err := NewDatabase(ctx, testLogger, testDbName, mockParser, aPager, aPager)
	require.NoError(t, err)

	// Let's create 4 tables:
	// - one without any index
	// - one with a primary key
	// - one with a unique index
	// - one with a secondary index
	stmt := Statement{
		Kind:      CreateTable,
		TableName: testTableName,
		Columns:   append([]Column{}, testColumns...),
	}
	stmt2 := Statement{
		Kind:      CreateTable,
		TableName: testTableName2,
		Columns:   append([]Column{}, testColumnsWithPrimaryKey...),
	}
	stmt3 := Statement{
		Kind:      CreateTable,
		TableName: testTableName3,
		Columns:   append([]Column{}, testColumnsWithUniqueIndex...),
	}
	stmt4 := Statement{
		Kind:      CreateTable,
		TableName: testTableName4,
		Columns:   append([]Column{}, testColumnsWithSecondaryIndex...),
	}

	for _, s := range []Statement{stmt, stmt2, stmt3, stmt4} {
		err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			_, err := aDatabase.ExecuteStatement(ctx, s)
			return err
		}, TxCommitter{aDatabase.saver, aDatabase})
		require.NoError(t, err)
	}

	// Now, let's re-initialize the database to load existing tables
	mockParser.On("Parse", mock.Anything, stmt.CreateTableDDL()).Return([]Statement{stmt}, nil).Once()
	mockParser.On("Parse", mock.Anything, stmt2.CreateTableDDL()).Return([]Statement{stmt2}, nil).Once()
	mockParser.On("Parse", mock.Anything, stmt3.CreateTableDDL()).Return([]Statement{stmt3}, nil).Once()
	mockParser.On("Parse", mock.Anything, stmt4.CreateTableDDL()).Return([]Statement{stmt4}, nil).Once()

	aDatabase, err = NewDatabase(ctx, testLogger, testDbName, mockParser, aPager, aPager)
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 5)
	assert.ElementsMatch(t, []string{
		SchemaTableName,
		testTableName,
		testTableName2,
		testTableName3,
		testTableName4,
	}, aDatabase.ListTableNames(ctx))

	assert.Equal(t, testTableName, aDatabase.tables[testTableName].Name)
	assert.Equal(t, testColumns, aDatabase.tables[testTableName].Columns)

	assert.Equal(t, testTableName2, aDatabase.tables[testTableName2].Name)
	assert.Equal(t, testColumnsWithPrimaryKey, aDatabase.tables[testTableName2].Columns)
	assert.Empty(t, aDatabase.tables[testTableName2].UniqueIndexes)
	assert.Empty(t, aDatabase.tables[testTableName2].SecondaryIndexes)
	assert.NotNil(t, aDatabase.tables[testTableName2].PrimaryKey.Index)

	assert.Equal(t, testTableName3, aDatabase.tables[testTableName3].Name)
	assert.Equal(t, testColumnsWithUniqueIndex, aDatabase.tables[testTableName3].Columns)
	assert.Empty(t, aDatabase.tables[testTableName3].PrimaryKey)
	assert.Empty(t, aDatabase.tables[testTableName3].SecondaryIndexes)
	assert.Len(t, aDatabase.tables[testTableName3].UniqueIndexes, 1)
	assert.NotNil(t, aDatabase.tables[testTableName3].UniqueIndexes[uniqueIndexName])

	assert.Equal(t, testTableName4, aDatabase.tables[testTableName4].Name)
	assert.Equal(t, testColumnsWithSecondaryIndex, aDatabase.tables[testTableName4].Columns)
	assert.Empty(t, aDatabase.tables[testTableName4].PrimaryKey)
	assert.Empty(t, aDatabase.tables[testTableName4].UniqueIndexes)
	assert.Len(t, aDatabase.tables[testTableName4].SecondaryIndexes, 1)
	assert.NotNil(t, aDatabase.tables[testTableName4].SecondaryIndexes[secondaryIndexName])

	assert.Equal(t, PageIndex(0), aDatabase.tables[SchemaTableName].GetRootPageIdx())
	assert.Equal(t, PageIndex(1), aDatabase.tables[testTableName].GetRootPageIdx())
	assert.Equal(t, PageIndex(2), aDatabase.tables[testTableName2].GetRootPageIdx())
	assert.Equal(t, PageIndex(3), aDatabase.tables[testTableName2].PrimaryKey.Index.GetRootPageIdx())
	assert.Equal(t, PageIndex(4), aDatabase.tables[testTableName3].GetRootPageIdx())
	assert.Equal(t, PageIndex(5), aDatabase.tables[testTableName3].UniqueIndexes[uniqueIndexName].Index.GetRootPageIdx())
	assert.Equal(t, PageIndex(6), aDatabase.tables[testTableName4].GetRootPageIdx())
	assert.Equal(t, PageIndex(7), aDatabase.tables[testTableName4].SecondaryIndexes[secondaryIndexName].Index.GetRootPageIdx())

	mock.AssertExpectationsForObjects(t, mockParser)

	// Check system schema table contents
	mainRows := collectMainTableRows(t, ctx, aDatabase)
	assert.Len(t, mainRows, 8)

	// First created table without any index
	assert.Equal(t, SchemaTable, SchemaType(mainRows[1].Values[0].Value.(int32)))
	assert.Equal(t, testTableName, mainRows[1].Values[1].Value.(TextPointer).String())
	assert.False(t, mainRows[1].Values[2].Valid)
	assert.Equal(t, 1, int(mainRows[1].Values[3].Value.(int32)))

	// Second created table with primary key
	assert.Equal(t, SchemaTable, SchemaType(mainRows[2].Values[0].Value.(int32)))
	assert.Equal(t, testTableName2, mainRows[2].Values[1].Value.(TextPointer).String())
	assert.False(t, mainRows[2].Values[2].Valid)
	assert.Equal(t, 2, int(mainRows[2].Values[3].Value.(int32)))

	// Primary key for the second table
	assert.Equal(t, SchemaPrimaryKey, SchemaType(mainRows[3].Values[0].Value.(int32)))
	assert.Equal(t, primaryKeyName(testTableName2), mainRows[3].Values[1].Value.(TextPointer).String())
	assert.Equal(t, testTableName2, mainRows[3].Values[2].Value.(TextPointer).String())
	assert.Equal(t, 3, int(mainRows[3].Values[3].Value.(int32)))

	// Third created table with unique index
	assert.Equal(t, SchemaTable, SchemaType(mainRows[4].Values[0].Value.(int32)))
	assert.Equal(t, testTableName3, mainRows[4].Values[1].Value.(TextPointer).String())
	assert.False(t, mainRows[4].Values[2].Valid)
	assert.Equal(t, 4, int(mainRows[4].Values[3].Value.(int32)))

	// Unique index for the third table
	assert.Equal(t, SchemaUniqueIndex, SchemaType(mainRows[5].Values[0].Value.(int32)))
	assert.Equal(t, uniqueIndexName, mainRows[5].Values[1].Value.(TextPointer).String())
	assert.Equal(t, testTableName3, mainRows[5].Values[2].Value.(TextPointer).String())
	assert.Equal(t, 5, int(mainRows[5].Values[3].Value.(int32)))

	// Fourth created table with unique index
	assert.Equal(t, SchemaTable, SchemaType(mainRows[6].Values[0].Value.(int32)))
	assert.Equal(t, testTableName4, mainRows[6].Values[1].Value.(TextPointer).String())
	assert.False(t, mainRows[6].Values[2].Valid)
	assert.Equal(t, 6, int(mainRows[6].Values[3].Value.(int32)))

	// Secondary index for the fourth table
	assert.Equal(t, SchemaSecondaryIndex, SchemaType(mainRows[7].Values[0].Value.(int32)))
	assert.Equal(t, secondaryIndexName, mainRows[7].Values[1].Value.(TextPointer).String())
	assert.Equal(t, testTableName4, mainRows[7].Values[2].Value.(TextPointer).String())
	assert.Equal(t, 7, int(mainRows[7].Values[3].Value.(int32)))
}

func TestDatabase_CreateTable(t *testing.T) {
	aPager := initTest(t)

	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, testLogger, testDbName, nil, aPager, aPager)
	require.NoError(t, err)

	stmt := Statement{
		Kind:      CreateTable,
		TableName: testTableName,
		Columns:   append([]Column{}, testColumns...),
	}
	err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := aDatabase.ExecuteStatement(ctx, stmt)
		return err
	}, TxCommitter{aDatabase.saver, aDatabase})
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
	mainRows := collectMainTableRows(t, ctx, aDatabase)
	assert.Len(t, mainRows, 2)
	// Created table without any index
	assert.Equal(t, SchemaTable, SchemaType(mainRows[1].Values[0].Value.(int32)))
	assert.Equal(t, testTableName, mainRows[1].Values[1].Value.(TextPointer).String())
	assert.Equal(t, 1, int(mainRows[1].Values[3].Value.(int32)))
}

func TestDatabase_CreateTable_WithPrimaryKey(t *testing.T) {
	aPager := initTest(t)

	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, testLogger, testDbName, nil, aPager, aPager)
	require.NoError(t, err)

	stmt := Statement{
		Kind:      CreateTable,
		TableName: testTableName,
		Columns:   append([]Column{}, testColumnsWithPrimaryKey...),
	}
	err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := aDatabase.ExecuteStatement(ctx, stmt)
		return err
	}, TxCommitter{aDatabase.saver, aDatabase})
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 2)
	assert.ElementsMatch(t, []string{
		SchemaTableName,
		testTableName,
	}, aDatabase.ListTableNames(ctx))

	assert.Equal(t, testTableName, aDatabase.tables[testTableName].Name)
	assert.Equal(t, testColumnsWithPrimaryKey, aDatabase.tables[testTableName].Columns)
	assert.Empty(t, aDatabase.tables[testTableName].UniqueIndexes)
	assert.Empty(t, aDatabase.tables[testTableName].SecondaryIndexes)
	assert.NotNil(t, aDatabase.tables[testTableName].PrimaryKey.Index)

	assert.Equal(t, PageIndex(1), aDatabase.tables[testTableName].GetRootPageIdx())
	assert.Equal(t, PageIndex(0), aDatabase.tables[SchemaTableName].GetRootPageIdx())
	assert.Equal(t, PageIndex(2), aDatabase.tables[testTableName].PrimaryKey.Index.GetRootPageIdx())
	// Root page plus a new page for table and index, should be 3 in total
	assert.Len(t, aPager.pages, 3)

	// Check system schema table contents
	mainRows := collectMainTableRows(t, ctx, aDatabase)
	assert.Len(t, mainRows, 3)
	// Created table with primary key
	assert.Equal(t, SchemaTable, SchemaType(mainRows[1].Values[0].Value.(int32)))
	assert.Equal(t, testTableName, mainRows[1].Values[1].Value.(TextPointer).String())
	assert.Equal(t, 1, int(mainRows[1].Values[3].Value.(int32)))
	// Primary key for the created table
	assert.Equal(t, SchemaPrimaryKey, SchemaType(mainRows[2].Values[0].Value.(int32)))
	assert.Equal(t, primaryKeyName(testTableName), mainRows[2].Values[1].Value.(TextPointer).String())
	assert.Equal(t, 2, int(mainRows[2].Values[3].Value.(int32)))
}

func TestDatabase_CreateTable_WithUniqueIndex(t *testing.T) {
	aPager := initTest(t)
	indexName := uniqueIndexName(testTableName, "email")

	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, testLogger, testDbName, nil, aPager, aPager)
	require.NoError(t, err)

	stmt := Statement{
		Kind:      CreateTable,
		TableName: testTableName,
		Columns:   append([]Column{}, testColumnsWithUniqueIndex...),
	}
	err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := aDatabase.ExecuteStatement(ctx, stmt)
		return err
	}, TxCommitter{aDatabase.saver, aDatabase})
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 2)
	assert.ElementsMatch(t, []string{
		SchemaTableName,
		testTableName,
	}, aDatabase.ListTableNames(ctx))

	assert.Equal(t, testTableName, aDatabase.tables[testTableName].Name)
	assert.Equal(t, testColumnsWithUniqueIndex, aDatabase.tables[testTableName].Columns)
	assert.Empty(t, aDatabase.tables[testTableName].PrimaryKey)
	assert.Empty(t, aDatabase.tables[testTableName].SecondaryIndexes)
	assert.Len(t, aDatabase.tables[testTableName].UniqueIndexes, 1)
	assert.NotNil(t, aDatabase.tables[testTableName].UniqueIndexes[indexName])

	assert.Equal(t, PageIndex(1), aDatabase.tables[testTableName].GetRootPageIdx())
	assert.Equal(t, PageIndex(0), aDatabase.tables[SchemaTableName].GetRootPageIdx())
	assert.Equal(t, PageIndex(2), aDatabase.tables[testTableName].UniqueIndexes[indexName].Index.GetRootPageIdx())
	// Root page plus a new page for table and index, should be 3 in total
	assert.Len(t, aPager.pages, 3)

	// Check system schema table contents
	mainRows := collectMainTableRows(t, ctx, aDatabase)
	assert.Len(t, mainRows, 3)
	// Created table with unique index
	assert.Equal(t, SchemaTable, SchemaType(mainRows[1].Values[0].Value.(int32)))
	assert.Equal(t, testTableName, mainRows[1].Values[1].Value.(TextPointer).String())
	assert.Equal(t, 1, int(mainRows[1].Values[3].Value.(int32)))
	// Unique index for the created table
	assert.Equal(t, SchemaUniqueIndex, SchemaType(mainRows[2].Values[0].Value.(int32)))
	assert.Equal(t, indexName, mainRows[2].Values[1].Value.(TextPointer).String())
	assert.Equal(t, 2, int(mainRows[2].Values[3].Value.(int32)))
}

func TestDatabase_CreateTable_WithSecondaryIndex(t *testing.T) {
	aPager := initTest(t)
	indexName := secondaryIndexName(testTableName, "email")

	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, testLogger, testDbName, nil, aPager, aPager)
	require.NoError(t, err)

	stmt := Statement{
		Kind:      CreateTable,
		TableName: testTableName,
		Columns:   append([]Column{}, testColumnsWithSecondaryIndex...),
	}
	err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := aDatabase.ExecuteStatement(ctx, stmt)
		return err
	}, TxCommitter{aDatabase.saver, aDatabase})
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 2)
	assert.ElementsMatch(t, []string{
		SchemaTableName,
		testTableName,
	}, aDatabase.ListTableNames(ctx))

	assert.Equal(t, testTableName, aDatabase.tables[testTableName].Name)
	assert.Equal(t, testColumnsWithSecondaryIndex, aDatabase.tables[testTableName].Columns)
	assert.Empty(t, aDatabase.tables[testTableName].PrimaryKey)
	assert.Empty(t, aDatabase.tables[testTableName].UniqueIndexes)
	assert.Len(t, aDatabase.tables[testTableName].SecondaryIndexes, 1)
	assert.NotNil(t, aDatabase.tables[testTableName].SecondaryIndexes[indexName])

	assert.Equal(t, PageIndex(1), aDatabase.tables[testTableName].GetRootPageIdx())
	assert.Equal(t, PageIndex(0), aDatabase.tables[SchemaTableName].GetRootPageIdx())
	assert.Equal(t, PageIndex(2), aDatabase.tables[testTableName].SecondaryIndexes[indexName].Index.GetRootPageIdx())
	// Root page plus a new page for table and index, should be 3 in total
	assert.Len(t, aPager.pages, 3)

	// Check system schema table contents
	mainRows := collectMainTableRows(t, ctx, aDatabase)
	assert.Len(t, mainRows, 3)
	// Created table with secondary index
	assert.Equal(t, SchemaTable, SchemaType(mainRows[1].Values[0].Value.(int32)))
	assert.Equal(t, testTableName, mainRows[1].Values[1].Value.(TextPointer).String())
	assert.Equal(t, 1, int(mainRows[1].Values[3].Value.(int32)))
	// Secondary index for the created table
	assert.Equal(t, SchemaSecondaryIndex, SchemaType(mainRows[2].Values[0].Value.(int32)))
	assert.Equal(t, indexName, mainRows[2].Values[1].Value.(TextPointer).String())
	assert.Equal(t, 2, int(mainRows[2].Values[3].Value.(int32)))
}

func TestDatabase_DropTable(t *testing.T) {
	aPager := initTest(t)

	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, testLogger, testDbName, nil, aPager, aPager)
	require.NoError(t, err)

	stmt := Statement{
		Kind:      CreateTable,
		TableName: testTableName,
		Columns:   testColumns,
	}
	err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := aDatabase.ExecuteStatement(ctx, stmt)
		return err
	}, TxCommitter{aDatabase.saver, aDatabase})
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 2)

	stmt = Statement{
		Kind:      DropTable,
		TableName: testTableName,
	}
	err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := aDatabase.ExecuteStatement(ctx, stmt)
		return err
	}, TxCommitter{aDatabase.saver, aDatabase})
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 1)
	assert.Equal(t, testDbName, aDatabase.Name)
	assert.Equal(t, SchemaTableName, aDatabase.tables[SchemaTableName].Name)
	assert.Equal(t, PageIndex(0), aDatabase.tables[SchemaTableName].GetRootPageIdx())
	assert.Equal(t, []string{SchemaTableName}, aDatabase.ListTableNames(ctx))

	tablePager := aDatabase.factory.ForTable(testColumns)
	assertFreePages(t, tablePager, []PageIndex{1})

	// Check system schema table contents
	mainRows := collectMainTableRows(t, ctx, aDatabase)
	assert.Len(t, mainRows, 1)
}

func TestDatabase_DropTable_WithPrimaryKey(t *testing.T) {
	aPager := initTest(t)

	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, testLogger, testDbName, nil, aPager, aPager)
	require.NoError(t, err)

	stmt := Statement{
		Kind:      CreateTable,
		TableName: testTableName,
		Columns:   testColumnsWithPrimaryKey,
	}
	err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := aDatabase.ExecuteStatement(ctx, stmt)
		return err
	}, TxCommitter{aDatabase.saver, aDatabase})
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 2)

	stmt = Statement{
		Kind:      DropTable,
		TableName: testTableName,
	}
	err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := aDatabase.ExecuteStatement(ctx, stmt)
		return err
	}, TxCommitter{aDatabase.saver, aDatabase})
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 1)
	assert.Equal(t, testDbName, aDatabase.Name)
	assert.Equal(t, SchemaTableName, aDatabase.tables[SchemaTableName].Name)
	assert.Equal(t, PageIndex(0), aDatabase.tables[SchemaTableName].GetRootPageIdx())
	assert.Equal(t, []string{SchemaTableName}, aDatabase.ListTableNames(ctx))

	tablePager := aDatabase.factory.ForTable(testColumns)
	assertFreePages(t, tablePager, []PageIndex{2, 1})

	// Check system schema table contents
	mainRows := collectMainTableRows(t, ctx, aDatabase)
	assert.Len(t, mainRows, 1)
}

func TestDatabase_DropTable_WithUniqueIndex(t *testing.T) {
	aPager := initTest(t)

	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, testLogger, testDbName, nil, aPager, aPager)
	require.NoError(t, err)

	stmt := Statement{
		Kind:      CreateTable,
		TableName: testTableName,
		Columns:   testColumnsWithUniqueIndex,
	}
	err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := aDatabase.ExecuteStatement(ctx, stmt)
		return err
	}, TxCommitter{aDatabase.saver, aDatabase})
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 2)

	stmt = Statement{
		Kind:      DropTable,
		TableName: testTableName,
	}
	err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := aDatabase.ExecuteStatement(ctx, stmt)
		return err
	}, TxCommitter{aDatabase.saver, aDatabase})
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 1)
	assert.Equal(t, testDbName, aDatabase.Name)
	assert.Equal(t, SchemaTableName, aDatabase.tables[SchemaTableName].Name)
	assert.Equal(t, PageIndex(0), aDatabase.tables[SchemaTableName].GetRootPageIdx())
	assert.Equal(t, []string{SchemaTableName}, aDatabase.ListTableNames(ctx))

	tablePager := aDatabase.factory.ForTable(testColumns)
	assertFreePages(t, tablePager, []PageIndex{2, 1})

	// Check system schema table contents
	mainRows := collectMainTableRows(t, ctx, aDatabase)
	assert.Len(t, mainRows, 1)
}

func TestDatabase_DropTable_WithSecondaryIndex(t *testing.T) {
	aPager := initTest(t)

	ctx := context.Background()
	aDatabase, err := NewDatabase(ctx, testLogger, testDbName, nil, aPager, aPager)
	require.NoError(t, err)

	stmt := Statement{
		Kind:      CreateTable,
		TableName: testTableName,
		Columns:   testColumnsWithSecondaryIndex,
	}
	err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := aDatabase.ExecuteStatement(ctx, stmt)
		return err
	}, TxCommitter{aDatabase.saver, aDatabase})
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 2)

	stmt = Statement{
		Kind:      DropTable,
		TableName: testTableName,
	}
	err = aDatabase.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := aDatabase.ExecuteStatement(ctx, stmt)
		return err
	}, TxCommitter{aDatabase.saver, aDatabase})
	require.NoError(t, err)

	assert.Len(t, aDatabase.tables, 1)
	assert.Equal(t, testDbName, aDatabase.Name)
	assert.Equal(t, SchemaTableName, aDatabase.tables[SchemaTableName].Name)
	assert.Equal(t, PageIndex(0), aDatabase.tables[SchemaTableName].GetRootPageIdx())
	assert.Equal(t, []string{SchemaTableName}, aDatabase.ListTableNames(ctx))

	tablePager := aDatabase.factory.ForTable(testColumns)
	assertFreePages(t, tablePager, []PageIndex{2, 1})

	// Check system schema table contents
	mainRows := collectMainTableRows(t, ctx, aDatabase)
	assert.Len(t, mainRows, 1)
}

func initTest(t *testing.T) *pagerImpl {
	t.Parallel()

	tempFile, err := os.CreateTemp("", "testdb")
	require.NoError(t, err)
	t.Cleanup(func() { os.Remove(tempFile.Name()) })

	aPager, err := NewPager(tempFile, PageSize)
	require.NoError(t, err)

	return aPager
}

func collectMainTableRows(t *testing.T, ctx context.Context, aDatabase *Database) []Row {
	mainTable := aDatabase.tables[SchemaTableName]
	schemaResults, err := mainTable.Select(ctx, Statement{
		Kind:   Select,
		Fields: mainTableFields,
	})
	require.NoError(t, err)

	var rows []Row
	for schemaResults.Rows.Next(ctx) {
		rows = append(rows, schemaResults.Rows.Row())
	}
	require.NoError(t, schemaResults.Rows.Err())
	return rows
}
