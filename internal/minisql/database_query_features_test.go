package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var queryFeatureColumns = []Column{
	{Name: "id", Kind: Int8, Size: 8},
	{Name: "name", Kind: Varchar, Size: MaxInlineVarchar},
	{Name: "age", Kind: Int8, Size: 8},
}

func newQueryFeatureDatabase(t *testing.T) (*Database, context.Context) {
	t.Helper()

	pager, dbFile := initTest(t)
	db, err := NewDatabase(context.Background(), testLogger, dbFile.Name(), nil, pager, pager, nil)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, db.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := db.ExecuteStatement(ctx, Statement{
			Kind:      CreateTable,
			TableName: "people",
			Columns:   queryFeatureColumns,
		})
		if err != nil {
			return err
		}
		return nil
	}))
	require.NoError(t, db.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := db.ExecuteStatement(ctx, Statement{
			Kind:      Insert,
			TableName: "people",
			Fields:    fieldsFromColumns(queryFeatureColumns...),
			Inserts: [][]OptionalValue{
				queryFeatureValues(1, "alice", 41),
				queryFeatureValues(2, "bob", 29),
				queryFeatureValues(3, "carol", 35),
			},
		})
		return err
	}))

	return db, ctx
}

func queryFeatureValues(id int64, name string, age int64) []OptionalValue {
	return []OptionalValue{
		{Valid: true, Value: id},
		{Valid: true, Value: NewTextPointer([]byte(name))},
		{Valid: true, Value: age},
	}
}

func queryFeatureSelect(fields ...Field) Statement {
	return Statement{
		Kind:      Select,
		TableName: "people",
		Fields:    fields,
	}
}

func queryFeatureCollectRows(t *testing.T, ctx context.Context, db *Database, stmt Statement) []Row {
	t.Helper()

	var rows []Row
	require.NoError(t, db.txManager.ExecuteReadOnlyTransaction(ctx, func(ctx context.Context) error {
		result, err := db.ExecuteStatement(ctx, stmt)
		if err != nil {
			return err
		}
		for result.Rows.Next(ctx) {
			rows = append(rows, result.Rows.Row())
		}
		return result.Rows.Err()
	}))
	return rows
}

func TestDatabase_ExecuteUnionAllStreaming(t *testing.T) {
	db, ctx := newQueryFeatureDatabase(t)

	rows := queryFeatureCollectRows(t, ctx, db, Statement{
		Kind:      Select,
		TableName: "people",
		Fields:    []Field{{Name: "name"}},
		Conditions: NewOneOrMore(Conditions{
			FieldIsEqual(Field{Name: "id"}, OperandInteger, int64(1)),
		}),
		Unions: []UnionClause{{
			All:  true,
			Stmt: queryFeatureSelect(Field{Name: "name"}),
		}},
	})

	require.Len(t, rows, 4)
	assert.Equal(t, "alice", rows[0].Values[0].Value.(TextPointer).String())
}

func TestDatabase_ExecuteUnionDeduplicates(t *testing.T) {
	db, ctx := newQueryFeatureDatabase(t)

	rows := queryFeatureCollectRows(t, ctx, db, Statement{
		Kind:      Select,
		TableName: "people",
		Fields:    []Field{{Name: "age"}},
		Unions: []UnionClause{{
			Stmt: queryFeatureSelect(Field{Name: "age"}),
		}},
	})

	require.Len(t, rows, 3)
}

func TestDatabase_ExecuteCTESelectMaterializes(t *testing.T) {
	db, ctx := newQueryFeatureDatabase(t)

	rows := queryFeatureCollectRows(t, ctx, db, Statement{
		Kind:      Select,
		TableName: "older",
		Fields:    []Field{{Name: "name"}},
		CTEs: []CTE{{
			Name: "older",
			Body: &Statement{
				Kind:      Select,
				TableName: "people",
				Fields:    fieldsFromColumns(queryFeatureColumns...),
				Conditions: NewOneOrMore(Conditions{
					FieldIsGreater(Field{Name: "age"}, OperandInteger, int64(30)),
				}),
				Limit: OptionalValue{Valid: true, Value: int64(10)},
			},
		}},
	})

	require.Len(t, rows, 2)
	assert.Equal(t, "alice", rows[0].Values[0].Value.(TextPointer).String())
	assert.Equal(t, "carol", rows[1].Values[0].Value.(TextPointer).String())
}

func TestDatabase_ExecuteSelectFromDerivedTable(t *testing.T) {
	db, ctx := newQueryFeatureDatabase(t)

	rows := queryFeatureCollectRows(t, ctx, db, Statement{
		Kind:              Select,
		FromSubqueryAlias: "p",
		FromSubquery: &Statement{
			Kind:      Select,
			TableName: "people",
			Fields:    []Field{{Name: "name"}, {Name: "age"}},
		},
		Fields: []Field{{AliasPrefix: "p", Name: "name"}},
		Conditions: NewOneOrMore(Conditions{
			FieldIsGreater(Field{AliasPrefix: "p", Name: "age"}, OperandInteger, int64(30)),
		}),
	})

	require.Len(t, rows, 2)
	assert.Equal(t, "alice", rows[0].Values[0].Value.(TextPointer).String())
	assert.Equal(t, "carol", rows[1].Values[0].Value.(TextPointer).String())
}

func TestDatabase_ExecuteExplainSelectVariants(t *testing.T) {
	db, ctx := newQueryFeatureDatabase(t)

	for name, stmt := range map[string]Statement{
		"plain": {
			Kind: Select, TableName: "people", Fields: []Field{{Name: "name"}},
		},
		"derived": {
			Kind:              Select,
			FromSubqueryAlias: "p",
			FromSubquery:      &Statement{Kind: Select, TableName: "people", Fields: []Field{{Name: "name"}}},
			Fields:            []Field{{AliasPrefix: "p", Name: "name"}},
		},
		"cte": {
			Kind:      Select,
			TableName: "p",
			Fields:    []Field{{Name: "name"}},
			CTEs: []CTE{{
				Name: "p",
				Body: &Statement{
					Kind:      Select,
					TableName: "people",
					Fields:    []Field{{Name: "name"}},
					Limit:     OptionalValue{Valid: true, Value: int64(10)},
				},
			}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			rows := queryFeatureCollectRows(t, ctx, db, Statement{
				Kind:             Explain,
				ExplainAnalyze:   true,
				ExplainStatement: &stmt,
			})
			require.NotEmpty(t, rows)
		})
	}
}
