package minisql

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// twoTextColumns is a minimal schema with two TEXT overflow columns so that
// updateOverflowTexts can be tested for the "unchanged column is skipped" case.
var twoTextColumns = []Column{
	{Kind: Int8, Size: 8, Name: "id"},
	{Kind: Text, Name: "col_a", Nullable: true},
	{Kind: Text, Name: "col_b", Nullable: true},
}

// overflowTestSetup holds everything needed for a text-pointer test:
// a Table wired to a real pager, the txManager for transactions, the
// column-level Pager (from ForTable), and the TransactionalPager that
// the table uses internally so tests can call internal functions directly.
type overflowTestSetup struct {
	ctx        context.Context
	table      *Table
	txManager  *TransactionManager
	txPager    *TransactionalPager
	tablePager Pager
}

// newOverflowSetup creates a fresh database, inserts one sentinel row (so
// that the B-tree root page 0 is allocated before any overflow pages), and
// returns the test setup wired for the given column schema.
func newOverflowSetup(t *testing.T, columns []Column) overflowTestSetup {
	t.Helper()

	pager, dbFile := initTest(t) // also calls t.Parallel()
	tablePager := pager.ForTable(columns)
	txManager := NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
	txPager := NewTransactionalPager(tablePager, txManager, testTableName, "")
	table := NewTable(testLogger, txPager, txManager, testTableName, columns, 0, nil)

	ctx := context.Background()

	// Insert a single sentinel row so the B-tree root leaf is created at page 0.
	// Without this the first GetFreePage call would return index 0, making
	// tp.FirstPage == 0 indistinguishable from "not set".
	sentinelValues := make([]OptionalValue, len(columns))
	for i, col := range columns {
		switch col.Kind {
		case Int8:
			sentinelValues[i] = OptionalValue{Value: int64(9999), Valid: true}
		default:
			sentinelValues[i] = OptionalValue{Value: NewTextPointer([]byte("sentinel")), Valid: true}
		}
	}
	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := table.Insert(ctx, Statement{
			Kind:    Insert,
			Fields:  fieldsFromColumns(columns...),
			Inserts: [][]OptionalValue{sentinelValues},
		})
		return err
	})
	require.NoError(t, err, "newOverflowSetup: sentinel insert")

	return overflowTestSetup{ctx: ctx, table: table, txManager: txManager, txPager: txPager, tablePager: tablePager}
}

// insertOverflowRow inserts a row whose text column (at colIdx) contains data
// of the given size, and returns the row as read back from the table (so
// TextPointers have FirstPage populated).
func (s overflowTestSetup) insertOverflowRow(t *testing.T, columns []Column, colIdx int, data []byte) Row {
	t.Helper()

	values := make([]OptionalValue, len(columns))
	var idVal int64
	for i, col := range columns {
		switch col.Kind {
		case Int8:
			idVal = gen.Int64()
			values[i] = OptionalValue{Value: idVal, Valid: true}
		default:
			if i == colIdx {
				values[i] = OptionalValue{Value: NewTextPointer(data), Valid: true}
			} else {
				values[i] = OptionalValue{Value: NewTextPointer([]byte("placeholder")), Valid: true}
			}
		}
	}

	err := s.txManager.ExecuteInTransaction(s.ctx, func(ctx context.Context) error {
		_, err := s.table.Insert(ctx, Statement{
			Kind:    Insert,
			Fields:  fieldsFromColumns(columns...),
			Inserts: [][]OptionalValue{values},
		})
		return err
	})
	require.NoError(t, err)

	// Read back the row so overflow TextPointers have FirstPage set.
	var found Row
	err = s.txManager.ExecuteInTransaction(s.ctx, func(ctx context.Context) error {
		r, err := s.table.Select(ctx, Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(columns...),
		})
		if err != nil {
			return err
		}
		for r.Rows.Next(ctx) {
			row := r.Rows.Row()
			v, _ := row.GetValue(columns[0].Name)
			if v.Value == idVal {
				found = row
			}
		}
		return r.Rows.Err()
	})
	require.NoError(t, err)
	if found.Values == nil {
		t.Fatalf("insertOverflowRow: inserted row not found in SELECT result")
	}
	return found
}

// ── updateOverflowText ────────────────────────────────────────────────────────

// When oldFirstPage is zero the call falls through to storeOverflowText:
// new overflow pages are allocated and FirstPage is set to a non-zero index.
func TestTextPointer_UpdateOverflowText_NoOldChain(t *testing.T) {
	s := newOverflowSetup(t, testOverflowColumns)

	newTP := NewTextPointer(bytes.Repeat([]byte("X"), int(MaxOverflowPageData)+100))
	require.False(t, newTP.IsInline())

	err := s.txManager.ExecuteInTransaction(s.ctx, func(ctx context.Context) error {
		return newTP.updateOverflowText(ctx, s.txPager, 0)
	})
	require.NoError(t, err)

	assert.NotZero(t, newTP.FirstPage)

	// Read back content to verify the data was written correctly.
	var got TextPointer
	err = s.txManager.ExecuteInTransaction(s.ctx, func(ctx context.Context) error {
		var err error
		got, err = TextPointer{FirstPage: newTP.FirstPage, Length: newTP.Length}.readOverflowText(s.ctx, s.txPager)
		return err
	})
	require.NoError(t, err)
	assert.Equal(t, string(newTP.Data), string(got.Data))
}

// Same-size update: all old pages are reused in-place; FirstPage is unchanged;
// no free-list mutations occur.
func TestTextPointer_UpdateOverflowText_SamePageCount(t *testing.T) {
	s := newOverflowSetup(t, testOverflowColumns)

	oldData := bytes.Repeat([]byte("A"), int(MaxOverflowPageData)+100) // 2 overflow pages
	oldTP := NewTextPointer(oldData)
	err := s.txManager.ExecuteInTransaction(s.ctx, func(ctx context.Context) error {
		return oldTP.storeOverflowText(ctx, s.txPager)
	})
	require.NoError(t, err)
	require.NotZero(t, oldTP.FirstPage)

	newData := bytes.Repeat([]byte("B"), int(MaxOverflowPageData)+50) // still 2 pages
	newTP := NewTextPointer(newData)

	err = s.txManager.ExecuteInTransaction(s.ctx, func(ctx context.Context) error {
		return newTP.updateOverflowText(ctx, s.txPager, oldTP.FirstPage)
	})
	require.NoError(t, err)

	// First page must be reused — not reallocated.
	assert.Equal(t, oldTP.FirstPage, newTP.FirstPage)

	var got TextPointer
	err = s.txManager.ExecuteInTransaction(s.ctx, func(ctx context.Context) error {
		var err error
		got, err = TextPointer{FirstPage: newTP.FirstPage, Length: newTP.Length}.readOverflowText(s.ctx, s.txPager)
		return err
	})
	require.NoError(t, err)
	assert.Equal(t, string(newData), string(got.Data))

	// No pages freed.
	assertFreePages(t, s.tablePager, nil)
}

// Shrink: new text fits in fewer pages; first page is reused; excess tail page
// is returned to the free list.
func TestTextPointer_UpdateOverflowText_Shrink(t *testing.T) {
	s := newOverflowSetup(t, testOverflowColumns)

	oldData := bytes.Repeat([]byte("A"), int(MaxOverflowPageData)+100) // 2 pages
	oldTP := NewTextPointer(oldData)
	err := s.txManager.ExecuteInTransaction(s.ctx, func(ctx context.Context) error {
		return oldTP.storeOverflowText(ctx, s.txPager)
	})
	require.NoError(t, err)
	require.NotZero(t, oldTP.FirstPage)

	newData := bytes.Repeat([]byte("C"), int(MaxOverflowPageData)-50) // 1 page
	newTP := NewTextPointer(newData)

	err = s.txManager.ExecuteInTransaction(s.ctx, func(ctx context.Context) error {
		return newTP.updateOverflowText(ctx, s.txPager, oldTP.FirstPage)
	})
	require.NoError(t, err)

	// First old page is reused.
	assert.Equal(t, oldTP.FirstPage, newTP.FirstPage)

	var got TextPointer
	err = s.txManager.ExecuteInTransaction(s.ctx, func(ctx context.Context) error {
		var err error
		got, err = TextPointer{FirstPage: newTP.FirstPage, Length: newTP.Length}.readOverflowText(s.ctx, s.txPager)
		return err
	})
	require.NoError(t, err)
	assert.Equal(t, string(newData), string(got.Data))

	// One excess page freed.
	dbHeader := s.tablePager.GetHeader(s.ctx)
	assert.Equal(t, uint32(1), dbHeader.FreePageCount)
}

// Expand: new text requires more pages; old page is reused and an extra page
// is allocated from the free list (or as a new page).
func TestTextPointer_UpdateOverflowText_Expand(t *testing.T) {
	s := newOverflowSetup(t, testOverflowColumns)

	oldData := bytes.Repeat([]byte("A"), int(MaxInlineVarchar)+200) // 1 page
	oldTP := NewTextPointer(oldData)
	err := s.txManager.ExecuteInTransaction(s.ctx, func(ctx context.Context) error {
		return oldTP.storeOverflowText(ctx, s.txPager)
	})
	require.NoError(t, err)
	require.NotZero(t, oldTP.FirstPage)

	pagesBefore := s.tablePager.TotalPages()

	newData := bytes.Repeat([]byte("D"), int(MaxOverflowPageData)+200) // 2 pages
	newTP := NewTextPointer(newData)

	err = s.txManager.ExecuteInTransaction(s.ctx, func(ctx context.Context) error {
		return newTP.updateOverflowText(ctx, s.txPager, oldTP.FirstPage)
	})
	require.NoError(t, err)

	// First old page is reused; a second page is added.
	assert.Equal(t, oldTP.FirstPage, newTP.FirstPage)
	assert.Equal(t, uint32(pagesBefore)+1, s.tablePager.TotalPages())

	var got TextPointer
	err = s.txManager.ExecuteInTransaction(s.ctx, func(ctx context.Context) error {
		var err error
		got, err = TextPointer{FirstPage: newTP.FirstPage, Length: newTP.Length}.readOverflowText(s.ctx, s.txPager)
		return err
	})
	require.NoError(t, err)
	assert.Equal(t, string(newData), string(got.Data))

	assertFreePages(t, s.tablePager, nil)
}

// ── updateOverflowTexts ───────────────────────────────────────────────────────

// Changed overflow → overflow: the old overflow chain is reused in-place.
func TestRow_UpdateOverflowTexts_OverflowToOverflow(t *testing.T) {
	s := newOverflowSetup(t, testOverflowColumns)
	profileColIdx := 2
	profileCol := testOverflowColumns[profileColIdx]

	oldData := bytes.Repeat([]byte("E"), int(MaxOverflowPageData)+100)
	oldRow := s.insertOverflowRow(t, testOverflowColumns, profileColIdx, oldData)

	oldTP := oldRow.Values[profileColIdx].Value.(TextPointer)
	require.NotZero(t, oldTP.FirstPage)

	newData := bytes.Repeat([]byte("F"), int(MaxOverflowPageData)+80) // same page count
	newTP := NewTextPointer(newData)
	newRow := oldRow.Clone()
	newRow.Values[profileColIdx] = OptionalValue{Value: newTP, Valid: true}

	changedCols := map[string]Column{profileCol.Name: profileCol}

	var resultRow Row
	err := s.txManager.ExecuteInTransaction(s.ctx, func(ctx context.Context) error {
		var err error
		resultRow, err = newRow.updateOverflowTexts(ctx, s.txPager, oldRow, changedCols)
		return err
	})
	require.NoError(t, err)

	// Old FirstPage reused.
	resultTP := resultRow.Values[profileColIdx].Value.(TextPointer)
	assert.Equal(t, oldTP.FirstPage, resultTP.FirstPage)

	var got TextPointer
	err = s.txManager.ExecuteInTransaction(s.ctx, func(ctx context.Context) error {
		var err error
		got, err = TextPointer{FirstPage: resultTP.FirstPage, Length: resultTP.Length}.readOverflowText(s.ctx, s.txPager)
		return err
	})
	require.NoError(t, err)
	assert.Equal(t, string(newData), string(got.Data))

	assertFreePages(t, s.tablePager, nil)
}

// Changed overflow → inline: the old overflow chain is freed; no new overflow
// pages are allocated.
func TestRow_UpdateOverflowTexts_OverflowToInline(t *testing.T) {
	s := newOverflowSetup(t, testOverflowColumns)
	profileColIdx := 2
	profileCol := testOverflowColumns[profileColIdx]

	// 2 overflow pages.
	oldData := bytes.Repeat([]byte("G"), int(MaxOverflowPageData)+100)
	oldRow := s.insertOverflowRow(t, testOverflowColumns, profileColIdx, oldData)

	oldTP := oldRow.Values[profileColIdx].Value.(TextPointer)
	require.NotZero(t, oldTP.FirstPage)

	inlineTP := NewTextPointer([]byte("short")) // inline
	require.True(t, inlineTP.IsInline())

	newRow := oldRow.Clone()
	newRow.Values[profileColIdx] = OptionalValue{Value: inlineTP, Valid: true}
	changedCols := map[string]Column{profileCol.Name: profileCol}

	err := s.txManager.ExecuteInTransaction(s.ctx, func(ctx context.Context) error {
		var err error
		_, err = newRow.updateOverflowTexts(ctx, s.txPager, oldRow, changedCols)
		return err
	})
	require.NoError(t, err)

	// Both old overflow pages (2) should be freed.
	dbHeader := s.tablePager.GetHeader(s.ctx)
	assert.Equal(t, uint32(2), dbHeader.FreePageCount)
}

// Changed inline → overflow: oldFirstPage is zero; falls through to
// storeOverflowText and allocates fresh overflow pages.
func TestRow_UpdateOverflowTexts_InlineToOverflow(t *testing.T) {
	s := newOverflowSetup(t, testOverflowColumns)
	profileColIdx := 2
	profileCol := testOverflowColumns[profileColIdx]

	// Row with inline profile text.
	oldRow := s.insertOverflowRow(t, testOverflowColumns, profileColIdx, []byte("inline"))

	oldTP := oldRow.Values[profileColIdx].Value.(TextPointer)
	require.True(t, oldTP.IsInline(), "expected inline TextPointer")

	newData := bytes.Repeat([]byte("H"), int(MaxOverflowPageData)+50)
	newTP := NewTextPointer(newData)
	newRow := oldRow.Clone()
	newRow.Values[profileColIdx] = OptionalValue{Value: newTP, Valid: true}
	changedCols := map[string]Column{profileCol.Name: profileCol}

	var resultRow Row
	err := s.txManager.ExecuteInTransaction(s.ctx, func(ctx context.Context) error {
		var err error
		resultRow, err = newRow.updateOverflowTexts(ctx, s.txPager, oldRow, changedCols)
		return err
	})
	require.NoError(t, err)

	resultTP := resultRow.Values[profileColIdx].Value.(TextPointer)
	assert.NotZero(t, resultTP.FirstPage)

	var got TextPointer
	err = s.txManager.ExecuteInTransaction(s.ctx, func(ctx context.Context) error {
		var err error
		got, err = TextPointer{FirstPage: resultTP.FirstPage, Length: resultTP.Length}.readOverflowText(s.ctx, s.txPager)
		return err
	})
	require.NoError(t, err)
	assert.Equal(t, string(newData), string(got.Data))

	assertFreePages(t, s.tablePager, nil)
}

// Unchanged overflow column: updateOverflowTexts must skip it entirely —
// neither re-storing it (which would create duplicate chains) nor freeing its
// pages. The unchanged column's FirstPage must remain valid and readable.
func TestRow_UpdateOverflowTexts_UnchangedColumnSkipped(t *testing.T) {
	s := newOverflowSetup(t, twoTextColumns)
	colAIdx := 1
	colBIdx := 2
	colA := twoTextColumns[colAIdx]

	dataA := bytes.Repeat([]byte("A"), int(MaxOverflowPageData)+100)
	dataB := bytes.Repeat([]byte("B"), int(MaxOverflowPageData)+100)

	// Insert a row with both columns as overflow text.
	values := []OptionalValue{
		{Value: int64(1), Valid: true},
		{Value: NewTextPointer(dataA), Valid: true},
		{Value: NewTextPointer(dataB), Valid: true},
	}
	const insertedID int64 = 1
	err := s.txManager.ExecuteInTransaction(s.ctx, func(ctx context.Context) error {
		_, err := s.table.Insert(ctx, Statement{
			Kind:    Insert,
			Fields:  fieldsFromColumns(twoTextColumns...),
			Inserts: [][]OptionalValue{values},
		})
		return err
	})
	require.NoError(t, err)

	// Read back to get TextPointers with FirstPage set.
	var oldRow Row
	err = s.txManager.ExecuteInTransaction(s.ctx, func(ctx context.Context) error {
		r, err := s.table.Select(ctx, Statement{
			Kind:   Select,
			Fields: fieldsFromColumns(twoTextColumns...),
		})
		if err != nil {
			return err
		}
		for r.Rows.Next(ctx) {
			row := r.Rows.Row()
			v, _ := row.GetValue("id")
			if v.Value == insertedID {
				oldRow = row
			}
		}
		return r.Rows.Err()
	})
	require.NoError(t, err)
	require.NotNil(t, oldRow.Values)

	tpA := oldRow.Values[colAIdx].Value.(TextPointer)
	tpB := oldRow.Values[colBIdx].Value.(TextPointer)
	require.NotZero(t, tpA.FirstPage)
	require.NotZero(t, tpB.FirstPage)

	// Only col_a changes; col_b stays the same.
	newDataA := bytes.Repeat([]byte("Z"), int(MaxOverflowPageData)+80)
	newTPa := NewTextPointer(newDataA)
	newRow := oldRow.Clone()
	newRow.Values[colAIdx] = OptionalValue{Value: newTPa, Valid: true}

	changedCols := map[string]Column{colA.Name: colA}

	var resultRow Row
	err = s.txManager.ExecuteInTransaction(s.ctx, func(ctx context.Context) error {
		var err error
		resultRow, err = newRow.updateOverflowTexts(ctx, s.txPager, oldRow, changedCols)
		return err
	})
	require.NoError(t, err)

	// col_a: reused in-place (same FirstPage), new content.
	resultA := resultRow.Values[colAIdx].Value.(TextPointer)
	assert.Equal(t, tpA.FirstPage, resultA.FirstPage)

	var gotA TextPointer
	err = s.txManager.ExecuteInTransaction(s.ctx, func(ctx context.Context) error {
		var err error
		gotA, err = TextPointer{FirstPage: resultA.FirstPage, Length: resultA.Length}.readOverflowText(s.ctx, s.txPager)
		return err
	})
	require.NoError(t, err)
	assert.Equal(t, string(newDataA), string(gotA.Data))

	// col_b: completely untouched — original FirstPage preserved, content unchanged.
	resultB := resultRow.Values[colBIdx].Value.(TextPointer)
	assert.Equal(t, tpB.FirstPage, resultB.FirstPage)

	var gotB TextPointer
	err = s.txManager.ExecuteInTransaction(s.ctx, func(ctx context.Context) error {
		var err error
		gotB, err = TextPointer{FirstPage: tpB.FirstPage, Length: tpB.Length}.readOverflowText(s.ctx, s.txPager)
		return err
	})
	require.NoError(t, err)
	assert.Equal(t, string(dataB), string(gotB.Data))

	// No pages freed: col_a same-size reuse, col_b untouched.
	assertFreePages(t, s.tablePager, nil)
}
