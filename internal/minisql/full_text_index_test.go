package minisql

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestTable_FullTextIndexScanAndMaintenance(t *testing.T) {
	pager, dbFile := initTest(t)
	ctx := context.Background()
	mockParser := new(MockParser)
	database, err := NewDatabase(ctx, testLogger, dbFile.Name(), mockParser, pager, pager, nil)
	require.NoError(t, err)

	const tableName = "articles"
	columns := []Column{
		{Kind: Int8, Size: 8, Name: "id"},
		{Kind: Varchar, Size: MaxInlineVarchar, Name: "title"},
		{Kind: Text, Size: 0, Name: "body"},
	}
	createStmt := Statement{
		Kind:      CreateTable,
		TableName: tableName,
		Columns:   columns,
	}
	require.NoError(t, database.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := database.ExecuteStatement(ctx, createStmt)
		return err
	}))

	require.NoError(t, database.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		for _, row := range [][]OptionalValue{
			{{Valid: true, Value: int64(1)}, {Valid: true, Value: NewTextPointer([]byte("MiniSQL"))}, {Valid: true, Value: NewTextPointer([]byte("MiniSQL stores rows in B tree pages and database pages."))}},
			{{Valid: true, Value: int64(2)}, {Valid: true, Value: NewTextPointer([]byte("Postgres"))}, {Valid: true, Value: NewTextPointer([]byte("Postgres has generalized inverted indexes."))}},
			{{Valid: true, Value: int64(3)}, {Valid: true, Value: NewTextPointer([]byte("Storage"))}, {Valid: true, Value: NewTextPointer([]byte("A small database stores data in pages."))}},
		} {
			_, err := database.ExecuteStatement(ctx, Statement{
				Kind:      Insert,
				TableName: tableName,
				Columns:   columns,
				Fields:    fieldsFromColumns(columns...),
				Inserts:   [][]OptionalValue{row},
			})
			if err != nil {
				return err
			}
		}
		return nil
	}))

	fullTextIndexStmt := Statement{
		Kind:           CreateIndex,
		TableName:      tableName,
		IndexName:      "idx_articles_body_fts",
		Columns:        []Column{{Name: "body"}},
		IndexMethod:    IndexMethodFullText,
		IndexTokenizer: TextSearchTokenizerSimple,
	}
	mockParser.On("Parse", mock.Anything, createStmt.DDL()).Return([]Statement{createStmt}, nil).Once()
	require.NoError(t, database.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := database.ExecuteStatement(ctx, fullTextIndexStmt)
		return err
	}))

	table, ok := database.GetTable(ctx, tableName)
	require.True(t, ok)

	matchDatabasePages := fullTextMatchCondition("body", "database pages")
	plan, err := table.PlanQuery(ctx, Statement{
		Kind:       Select,
		TableName:  tableName,
		Columns:    columns,
		Fields:     []Field{{Name: "title"}},
		Conditions: OneOrMore{{matchDatabasePages}},
	})
	require.NoError(t, err)
	require.Len(t, plan.Scans, 1)
	assert.Equal(t, ScanTypeFullText, plan.Scans[0].Type)
	assert.Equal(t, []any{"database", "pages"}, plan.Scans[0].IndexKeys)

	titles := selectTitlesWithCondition(t, ctx, database, table, matchDatabasePages)
	assert.Equal(t, []string{"MiniSQL", "Storage"}, titles)
	assert.Equal(t, []string{"MiniSQL"}, selectTitlesWithCondition(t, ctx, database, table, fullTextMatchCondition("body", `"database pages"`)))

	require.NoError(t, database.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := table.Update(ctx, Statement{
			Kind:    Update,
			Columns: columns,
			Updates: map[string]OptionalValue{
				"body": {Valid: true, Value: NewTextPointer([]byte("Fresh token document about index maintenance."))},
			},
			Conditions: OneOrMore{{
				{
					Operand1: Operand{Type: OperandField, Value: Field{Name: "title"}},
					Operator: Eq,
					Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("Postgres"))},
				},
			}},
		})
		return err
	}))

	assert.Equal(t, []string{"Postgres"}, selectTitlesWithCondition(t, ctx, database, table, fullTextMatchCondition("body", "fresh token")))
	assert.Empty(t, selectTitlesWithCondition(t, ctx, database, table, fullTextMatchCondition("body", "generalized inverted")))

	require.NoError(t, database.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := table.Delete(ctx, Statement{
			Kind:       Delete,
			Columns:    columns,
			Conditions: OneOrMore{{fullTextMatchCondition("body", "fresh token")}},
		})
		return err
	}))
	assert.Empty(t, selectTitlesWithCondition(t, ctx, database, table, fullTextMatchCondition("body", "fresh token")))

	mockParser.AssertExpectations(t)
}

func TestFullTextIndexHelpers(t *testing.T) {
	t.Parallel()

	sourceColumn := Column{Name: "body", Kind: Text}
	storageColumns := secondaryIndexStorageColumns(SecondaryIndex{
		IndexInfo: IndexInfo{
			Method:  IndexMethodFullText,
			Columns: []Column{sourceColumn},
		},
	})
	require.Len(t, storageColumns, 1)
	assert.Equal(t, "__fts_token__", storageColumns[0].Name)
	assert.Equal(t, Varchar, storageColumns[0].Kind)

	plainColumns := []Column{{Name: "title", Kind: Varchar, Size: MaxInlineVarchar}}
	assert.Equal(t, plainColumns, secondaryIndexStorageColumns(SecondaryIndex{
		IndexInfo: IndexInfo{Method: IndexMethodBTree, Columns: plainColumns},
	}))

	row := Row{
		Columns: []Column{sourceColumn},
		Values:  []OptionalValue{{Valid: true, Value: NewTextPointer([]byte("MiniSQL minisql database and pages"))}},
	}
	tokens, err := fullTextTokensForRow(SecondaryIndex{
		IndexInfo: IndexInfo{Name: "idx_body", Columns: []Column{sourceColumn}},
	}, row)
	require.NoError(t, err)
	assert.Equal(t, []string{"minisql", "database", "pages"}, tokens)

	_, err = fullTextTokensForRow(SecondaryIndex{
		IndexInfo: IndexInfo{Name: "idx_bad", Columns: []Column{sourceColumn, {Name: "title", Kind: Varchar}}},
	}, row)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires exactly one source column")
}

func TestFullTextIndexKeyMaintenanceHelpers(t *testing.T) {
	t.Parallel()

	bodyColumn := Column{Name: "body", Kind: Text}
	index := &fakeFullTextInvertedIndex{postings: make(map[string][]invertedPosting)}
	secondaryIndex := SecondaryIndex{
		IndexInfo: IndexInfo{
			Name:    "idx_body_fts",
			Method:  IndexMethodFullText,
			Columns: []Column{bodyColumn},
		},
		InvertedIndex: index,
	}
	table := NewTable(testLogger, nil, nil, "articles", []Column{bodyColumn}, 0, nil)
	ctx := context.Background()

	oldRow := Row{
		Key:     7,
		Columns: []Column{bodyColumn},
		Values:  []OptionalValue{{Valid: true, Value: NewTextPointer([]byte("old token value"))}},
	}
	newRow := Row{
		Key:     7,
		Columns: []Column{bodyColumn},
		Values:  []OptionalValue{{Valid: true, Value: NewTextPointer([]byte("new token value"))}},
	}

	require.NoError(t, table.insertFullTextIndexKeys(ctx, secondaryIndex, newRow.Key, newRow))
	assert.Contains(t, index.inserted, "new")
	assert.Contains(t, index.inserted, "token")
	assert.Contains(t, index.inserted, "value")

	require.NoError(t, table.updateFullTextIndexKeys(ctx, secondaryIndex, oldRow, newRow))
	assert.Contains(t, index.deleted, "old")
	assert.Contains(t, index.inserted, "new")

	require.NoError(t, table.deleteFullTextIndexKeys(ctx, secondaryIndex, newRow.Key, newRow))
	assert.Contains(t, index.deleted, "new")
}

func TestFullTextIndexScanMissingIndex(t *testing.T) {
	t.Parallel()

	table := NewTable(testLogger, nil, nil, "articles", []Column{{Name: "body", Kind: Text}}, 0, nil)
	err := table.fullTextIndexScan(context.Background(), Scan{
		Type:      ScanTypeFullText,
		IndexName: "missing",
		IndexKeys: []any{"database"},
	}, nil, func(Row) error {
		return nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no index found for full-text scan")
}

func TestPlanQuery_FullTextIndexSkipsOverlongQueryToken(t *testing.T) {
	t.Parallel()

	bodyColumn := Column{Name: "body", Kind: Text}
	table := NewTable(testLogger, nil, nil, "articles", []Column{bodyColumn}, 0, nil, WithSecondaryIndex(SecondaryIndex{
		IndexInfo: IndexInfo{
			Name:    "idx_body_fts",
			Method:  IndexMethodFullText,
			Columns: []Column{bodyColumn},
		},
	}))

	plan, err := table.PlanQuery(context.Background(), Statement{
		Kind:       Select,
		TableName:  "articles",
		Columns:    table.Columns,
		Fields:     []Field{{Name: "body"}},
		Conditions: OneOrMore{{fullTextMatchCondition("body", "database "+strings.Repeat("x", MaxIndexKeySize+1))}},
	})
	require.NoError(t, err)
	require.Len(t, plan.Scans, 1)
	assert.Equal(t, ScanTypeSequential, plan.Scans[0].Type)
}

func TestIndexMethodStringAndTokenizer(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "btree", IndexMethodBTree.String())
	assert.Equal(t, "fulltext", IndexMethodFullText.String())
	assert.Equal(t, "inverted", IndexMethodInverted.String())
	assert.Equal(t, "unknown", IndexMethod(99).String())
	assert.True(t, isSupportedIndexTokenizer(TextSearchTokenizerSimple))
	assert.False(t, isSupportedIndexTokenizer("porter"))
}

func TestStatement_ValidateCreateIndexMethods(t *testing.T) {
	t.Parallel()

	table := NewTable(testLogger, nil, nil, "docs", []Column{
		{Name: "id", Kind: Int8, Size: 8},
		{Name: "body", Kind: Text},
		{Name: "title", Kind: Varchar, Size: MaxInlineVarchar},
		{Name: "payload", Kind: JSON},
	}, 0, nil)

	cases := []struct {
		stmt    Statement
		name    string
		wantErr string
	}{
		{
			name: "btree rejects tokenizer",
			stmt: Statement{
				IndexMethod:    IndexMethodBTree,
				Columns:        []Column{{Name: "title"}},
				IndexTokenizer: TextSearchTokenizerSimple,
			},
			wantErr: "btree indexes do not support tokenizer options",
		},
		{
			name: "fulltext rejects expression",
			stmt: Statement{
				IndexMethod:    IndexMethodFullText,
				Columns:        []Column{{Name: "body"}},
				IndexTokenizer: TextSearchTokenizerSimple,
				IndexExpression: &Expr{
					Column: "body",
				},
			},
			wantErr: "full-text indexes do not support expression keys yet",
		},
		{
			name: "fulltext rejects multiple columns",
			stmt: Statement{
				IndexMethod:    IndexMethodFullText,
				Columns:        []Column{{Name: "body"}, {Name: "title"}},
				IndexTokenizer: TextSearchTokenizerSimple,
			},
			wantErr: "full-text indexes require exactly one column",
		},
		{
			name: "fulltext requires tokenizer",
			stmt: Statement{
				IndexMethod: IndexMethodFullText,
				Columns:     []Column{{Name: "body"}},
			},
			wantErr: "full-text indexes require a tokenizer",
		},
		{
			name: "fulltext rejects unsupported tokenizer",
			stmt: Statement{
				IndexMethod:    IndexMethodFullText,
				Columns:        []Column{{Name: "body"}},
				IndexTokenizer: "porter",
			},
			wantErr: `unsupported full-text tokenizer "porter"`,
		},
		{
			name: "fulltext rejects missing column",
			stmt: Statement{
				IndexMethod:    IndexMethodFullText,
				Columns:        []Column{{Name: "missing"}},
				IndexTokenizer: TextSearchTokenizerSimple,
				TableName:      "docs",
			},
			wantErr: "column missing does not exist on table docs",
		},
		{
			name: "fulltext rejects non text column",
			stmt: Statement{
				IndexMethod:    IndexMethodFullText,
				Columns:        []Column{{Name: "id"}},
				IndexTokenizer: TextSearchTokenizerSimple,
			},
			wantErr: `full-text index column "id" must be TEXT or VARCHAR`,
		},
		{
			name: "inverted rejects expression",
			stmt: Statement{
				IndexMethod:     IndexMethodInverted,
				Columns:         []Column{{Name: "payload"}},
				IndexExpression: &Expr{Column: "payload"},
			},
			wantErr: "inverted indexes do not support expression keys yet",
		},
		{
			name: "inverted rejects tokenizer",
			stmt: Statement{
				IndexMethod:    IndexMethodInverted,
				Columns:        []Column{{Name: "payload"}},
				IndexTokenizer: TextSearchTokenizerSimple,
			},
			wantErr: "inverted indexes do not support tokenizer options",
		},
		{
			name: "inverted rejects multiple columns",
			stmt: Statement{
				IndexMethod: IndexMethodInverted,
				Columns:     []Column{{Name: "payload"}, {Name: "title"}},
			},
			wantErr: "inverted indexes require exactly one column",
		},
		{
			name: "inverted rejects missing column",
			stmt: Statement{
				IndexMethod: IndexMethodInverted,
				Columns:     []Column{{Name: "missing"}},
				TableName:   "docs",
			},
			wantErr: "column missing does not exist on table docs",
		},
		{
			name: "inverted rejects non json column",
			stmt: Statement{
				IndexMethod: IndexMethodInverted,
				Columns:     []Column{{Name: "title"}},
			},
			wantErr: `inverted index column "title" must be JSON`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.stmt.validateCreateIndexMethod(table)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}

	require.NoError(t, (Statement{
		IndexMethod:    IndexMethodFullText,
		Columns:        []Column{{Name: "body"}},
		IndexTokenizer: TextSearchTokenizerSimple,
	}).validateCreateIndexMethod(table))
	require.NoError(t, (Statement{
		IndexMethod: IndexMethodInverted,
		Columns:     []Column{{Name: "payload"}},
	}).validateCreateIndexMethod(table))
}

func selectTitlesWithCondition(t *testing.T, ctx context.Context, database *Database, table *Table, cond Condition) []string {
	t.Helper()

	var titles []string
	err := database.txManager.ExecuteReadOnlyTransaction(ctx, func(ctx context.Context) error {
		result, err := table.Select(ctx, Statement{
			Kind:       Select,
			TableName:  table.Name,
			Columns:    table.Columns,
			Fields:     []Field{{Name: "title"}},
			Conditions: OneOrMore{{cond}},
		})
		if err != nil {
			return err
		}
		for result.Rows.Next(ctx) {
			row := result.Rows.Row()
			value, ok := row.GetValue("title")
			if !ok || !value.Valid {
				continue
			}
			title, ok := toStringVal(value.Value)
			if ok {
				titles = append(titles, title)
			}
		}
		return result.Rows.Err()
	})
	require.NoError(t, err)
	return titles
}

func fullTextMatchCondition(columnName, query string) Condition {
	return Condition{
		Operand1: Operand{
			Type: OperandExpr,
			Value: &Expr{
				FuncName: "MATCH",
				Args: []*Expr{
					{Column: columnName},
					{Literal: NewTextPointer([]byte(query))},
				},
			},
		},
		Operator: Eq,
		Operand2: Operand{Type: OperandBoolean, Value: true},
	}
}

type fakeFullTextInvertedIndex struct {
	postings map[string][]invertedPosting
	inserted []string
	deleted  []string
	replaced []string
	mode     invertedPostingMode
}

func (f *fakeFullTextInvertedIndex) GetRootPageIdx() PageIndex {
	return 0
}

func (f *fakeFullTextInvertedIndex) Mode() invertedIndexPostingMode {
	return invertedIndexPostingMode(f.postingMode())
}

func (f *fakeFullTextInvertedIndex) Insert(_ context.Context, term string, posting invertedPosting) error {
	f.inserted = append(f.inserted, term)
	f.postings[term] = append(f.postings[term], posting)
	return nil
}

func (f *fakeFullTextInvertedIndex) InsertMany(_ context.Context, term string, postings []invertedPosting) error {
	f.inserted = append(f.inserted, term)
	f.postings[term] = append(f.postings[term], postings...)
	return nil
}

func (f *fakeFullTextInvertedIndex) Replace(_ context.Context, term string, oldPosting, newPosting invertedPosting) error {
	f.replaced = append(f.replaced, term)
	if err := f.Delete(context.Background(), term, oldPosting); err != nil {
		return err
	}
	return f.Insert(context.Background(), term, newPosting)
}

func (f *fakeFullTextInvertedIndex) Delete(_ context.Context, term string, posting invertedPosting) error {
	f.deleted = append(f.deleted, term)
	postings := f.postings[term]
	for i, existing := range postings {
		if existing.RowID == posting.RowID {
			f.postings[term] = append(postings[:i], postings[i+1:]...)
			break
		}
	}
	return nil
}

func (f *fakeFullTextInvertedIndex) Lookup(_ context.Context, term string) (invertedPostingIterator, error) {
	payload, err := encodeInvertedPostingList(f.postingMode(), f.postings[term])
	if err != nil {
		return nil, err
	}
	return &singleBlockInvertedPostingIterator{
		block: invertedPostingBlock{
			Payload:      payload,
			CodecVersion: invertedPostingCodecVersion,
		},
		hasBlock: true,
	}, nil
}

func (f *fakeFullTextInvertedIndex) Stats(_ context.Context, term string) (invertedPostingStats, error) {
	postings := groupInvertedPostings(f.postingMode(), f.postings[term])
	return invertedPostingStats{
		DocFreq:      uint32(len(postings)),
		PostingCount: countInvertedPostings(f.postingMode(), postings),
	}, nil
}

func (f *fakeFullTextInvertedIndex) postingMode() invertedPostingMode {
	if f.mode != 0 {
		return f.mode
	}
	return invertedPostingModePositions
}
