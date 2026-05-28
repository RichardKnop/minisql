package minisql

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/RichardKnop/minisql/internal/pkg/logging"
)

const (
	testDBName    = "test_db"
	testTableName = "test_table"
	// testTableName2 / testTableName3 are used only in table_test.go so they stay there.
)

var (
	gen = newDataGen(uint64(time.Now().Unix()))

	testColumns = []Column{
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
		{
			Kind:     Int4,
			Size:     4,
			Name:     "age",
			Nullable: true,
		},
		{
			Kind:         Boolean,
			Size:         1,
			Name:         "verified",
			Nullable:     true,
			DefaultValue: OptionalValue{Value: false, Valid: true},
		},
		{
			Kind:     Real,
			Size:     4,
			Name:     "score",
			Nullable: true,
		},
		{
			Kind:         Timestamp,
			Size:         8,
			Name:         "created",
			Nullable:     true,
			DefaultValue: OptionalValue{Value: MustParseTimestampMicros("0001-01-01 00:00:00.000000"), Valid: true},
		},
	}
	testRowSize = uint64(8 + 4 + 255 + 4 + 1 + 4 + 8) // calculated size of testColumns

	mediumRowBaseSize = uint32(8 + (varcharLengthPrefixSize + MaxInlineVarchar) + 4 + 1 + 4 + 8)
	// Append varchars until row size is so that 5 of these full rows can fit into a page
	testMediumColumns = appendUntilSize(
		[]Column{
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
			{
				Kind:     Int4,
				Size:     4,
				Name:     "age",
				Nullable: true,
			},
			{
				Kind:         Boolean,
				Size:         1,
				Name:         "verified",
				Nullable:     true,
				DefaultValue: OptionalValue{Value: false, Valid: true},
			},
			{
				Kind:     Real,
				Size:     4,
				Name:     "score",
				Nullable: true,
			},
			{
				Kind:         Timestamp,
				Size:         8,
				Name:         "created",
				Nullable:     true,
				DefaultValue: OptionalValue{Value: MustParseTimestampMicros("0001-01-01 00:00:00.000000"), Valid: true},
			},
		},
		int((PageSize-uint32(RootPageConfigSize)-
			uint32(pageChecksumSize)- // 4-byte CRC32 checksum at end of page
			7-                        // base header
			8-                        // leaf header
			5*8-                      // 5 keys
			5*8-                      // 5 null bitmasks
			5*1-                      // 5 ColumnCount bytes (self-describing cell format)
			5*8-                      // 5×8 TypeCode bytes (8 columns: 6 base + 2 varchars, self-consistent)
			5*mediumRowBaseSize)/5),
	)

	bigRowBaseSize = uint32(8 + (varcharLengthPrefixSize + MaxInlineVarchar) + 4 + 1 + 4 + 8)
	// Append varcharts until row size is so that one full row fills en entire page
	testBigColumns = appendUntilSize(
		[]Column{
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
			{
				Kind:     Int4,
				Size:     4,
				Name:     "age",
				Nullable: true,
			},
			{
				Kind:         Boolean,
				Size:         1,
				Name:         "verified",
				Nullable:     true,
				DefaultValue: OptionalValue{Value: false, Valid: true},
			},
			{
				Kind:     Real,
				Size:     4,
				Name:     "score",
				Nullable: true,
			},
			{
				Kind:         Timestamp,
				Size:         8,
				Name:         "created",
				Nullable:     true,
				DefaultValue: OptionalValue{Value: MustParseTimestampMicros("0001-01-01 00:00:00.000000"), Valid: true},
			},
		},
		int((PageSize - uint32(RootPageConfigSize) -
			uint32(pageChecksumSize) - // 4-byte CRC32 checksum at end of page
			7 -                        // base header
			8 -                        // leaf header
			8 -                        // 1 key
			8 -                        // 1 null bitmask
			1 -                        // 1 ColumnCount byte (self-describing cell format)
			21 -                       // 21 TypeCode bytes (21 columns: 6 base + 15 varchars, self-consistent)
			bigRowBaseSize)),
	)

	testLogger *zap.Logger
)

func appendUntilSize(columns []Column, targetSize int) []Column {
	size := 0
	i := 0
	for size < targetSize {
		columns = append(columns, Column{
			Kind:     Varchar,
			Size:     MaxInlineVarchar,
			Name:     fmt.Sprintf("test_varchar_%d", i),
			Nullable: true,
		})
		i += 1
		size += varcharLengthPrefixSize + MaxInlineVarchar
	}
	if size > targetSize {
		columns[len(columns)-1].Size -= uint32(size - targetSize)
	}
	return columns
}

func init() {
	logConf := logging.DefaultConfig()

	level := os.Getenv("LOG_LEVEL")
	if level == "" {
		level = "debug"
	}

	l, err := logging.ParseLevel(level)
	if err != nil {
		panic(err)
	}
	logConf.Level = zap.NewAtomicLevelAt(l)

	testLogger, err = logConf.Build()
	if err != nil {
		panic(err)
	}
}

type dataGen struct {
	*gofakeit.Faker
}

func newDataGen(seed uint64) *dataGen {
	g := dataGen{
		Faker: gofakeit.New(seed),
	}

	return &g
}

func (g *dataGen) Row() Row {
	return NewRowWithValues(testColumns, []OptionalValue{
		{Value: g.Int64(), Valid: true},
		{Value: g.paddedEmail(), Valid: true},
		{Value: int32(g.IntRange(18, 100)), Valid: true},
		{Value: g.Bool(), Valid: true},
		{Value: g.Float32(), Valid: true},
		{Value: MustParseTimestampMicros(g.PastDate().Format(timestampFormat)), Valid: true},
	})
}

func (g *dataGen) paddedEmail() TextPointer {
	email := g.Email()
	paddingLength := MaxInlineVarchar - len(email)
	parts := strings.Split(email, "@")
	parts[0] += "+"
	for i := 0; i < paddingLength-1; i++ {
		parts[0] += g.Letter()
	}
	return NewTextPointer([]byte(strings.Join(parts, "@")))
}

func (g *dataGen) Rows(number int) []Row {
	var (
		idMap    = map[int64]struct{}{}
		emailMap = map[string]struct{}{}
		rows     = make([]Row, 0, number)
	)
	for i := range number {
		row := g.Row()

		// Ensure unique ID
		_, ok := idMap[row.Values[0].Value.(int64)]
		for ok {
			row = g.Row()
			_, ok = idMap[row.Values[0].Value.(int64)]
		}

		// Ensure unique email
		_, ok = emailMap[row.Values[1].Value.(TextPointer).String()]
		for ok {
			row = g.Row()
			_, ok = emailMap[row.Values[1].Value.(TextPointer).String()]
		}

		row.Key = RowID(i)
		rows = append(rows, row)

		idMap[row.Values[0].Value.(int64)] = struct{}{}
		emailMap[row.Values[1].Value.(TextPointer).String()] = struct{}{}
	}
	return rows
}

func (g *dataGen) MediumRow() Row {
	row := NewRowWithValues(testMediumColumns, []OptionalValue{
		{Value: g.Int64(), Valid: true},
		{Value: g.paddedEmail(), Valid: true},
		{Value: int32(g.IntRange(18, 100)), Valid: true},
		{Value: g.Bool(), Valid: true},
		{Value: g.Float32(), Valid: true},
		{Value: MustParseTimestampMicros(g.PastDate().Format(timestampFormat)), Valid: true},
	})

	for len(row.Values) < len(testMediumColumns) {
		row.Values = append(
			row.Values,
			OptionalValue{Value: g.textOfLength(testMediumColumns[len(row.Values)].Size), Valid: true},
		)
	}
	return row
}

func (g *dataGen) textOfLength(length uint32) TextPointer {
	txt := ""
	for len(txt) < int(length) {
		txt += g.Sentence(10)
	}
	return NewTextPointer([]byte(txt[0:length]))
}

func (g *dataGen) MediumRows(number int) []Row {
	var (
		idMap    = map[int64]struct{}{}
		emailMap = map[string]struct{}{}
		rows     = make([]Row, 0, number)
	)
	for i := range number {
		row := g.MediumRow()

		// Ensure unique ID
		_, ok := idMap[row.Values[0].Value.(int64)]
		for ok {
			row = g.MediumRow()
			_, ok = idMap[row.Values[0].Value.(int64)]
		}

		// Ensure unique email
		_, ok = emailMap[row.Values[1].Value.(TextPointer).String()]
		for ok {
			row = g.MediumRow()
			_, ok = emailMap[row.Values[1].Value.(TextPointer).String()]
		}

		row.Key = RowID(i)
		rows = append(rows, row)

		idMap[row.Values[0].Value.(int64)] = struct{}{}
		emailMap[row.Values[1].Value.(TextPointer).String()] = struct{}{}
	}
	return rows
}

func (g *dataGen) BigRow() Row {
	row := NewRowWithValues(testBigColumns, []OptionalValue{
		{Value: g.Int64(), Valid: true},
		{Value: g.paddedEmail(), Valid: true},
		{Value: int32(g.IntRange(18, 100)), Valid: true},
		{Value: g.Bool(), Valid: true},
		{Value: g.Float32(), Valid: true},
		{Value: MustParseTimestampMicros(g.PastDate().Format(timestampFormat)), Valid: true},
	})

	for len(row.Values) < len(testBigColumns) {
		row.Values = append(
			row.Values,
			OptionalValue{Value: g.textOfLength(testBigColumns[len(row.Values)].Size), Valid: true},
		)
	}
	return row
}

func (g *dataGen) BigRows(number int) []Row {
	var (
		idMap    = map[int64]struct{}{}
		emailMap = map[string]struct{}{}
		rows     = make([]Row, 0, number)
	)
	for i := range number {
		row := g.BigRow()

		// Ensure unique ID
		_, ok := idMap[row.Values[0].Value.(int64)]
		for ok {
			row = g.BigRow()
			_, ok = idMap[row.Values[0].Value.(int64)]
		}

		// Ensure unique email
		_, ok = emailMap[row.Values[1].Value.(TextPointer).String()]
		for ok {
			row = g.BigRow()
			_, ok = emailMap[row.Values[1].Value.(TextPointer).String()]
		}

		row.Key = RowID(i)
		rows = append(rows, row)

		idMap[row.Values[0].Value.(int64)] = struct{}{}
		emailMap[row.Values[1].Value.(TextPointer).String()] = struct{}{}
	}
	return rows
}

func (g *dataGen) RowWithPrimaryKey(primaryKey int64) Row {
	return NewRowWithValues(testColumns[0:2], []OptionalValue{
		{Value: primaryKey, Valid: true},
		{Value: NewTextPointer([]byte(g.Email())), Valid: true},
	})
}

func (g *dataGen) RowsWithPrimaryKey(number int) []Row {
	var (
		emailMap = map[string]struct{}{}
		rows     = make([]Row, 0, number)
	)
	for i := range number {
		row := g.RowWithPrimaryKey(int64(i + 1))

		// Ensure unique email
		_, ok := emailMap[row.Values[1].Value.(TextPointer).String()]
		for ok {
			row = g.RowWithPrimaryKey(int64(i + 1))
			_, ok = emailMap[row.Values[1].Value.(TextPointer).String()]
		}

		row.Key = RowID(i)
		rows = append(rows, row)

		emailMap[row.Values[1].Value.(TextPointer).String()] = struct{}{}
	}
	return rows
}

func (g *dataGen) RowWithUniqueIndex() Row {
	return NewRowWithValues(testColumns[0:2], []OptionalValue{
		{Value: g.Int64(), Valid: true},
		{Value: NewTextPointer([]byte(g.Email())), Valid: true},
	})
}

func (g *dataGen) RowsWithUniqueIndex(number int) []Row {
	var (
		emailMap = map[string]struct{}{}
		rows     = make([]Row, 0, number)
	)
	for i := range number {
		row := g.RowWithUniqueIndex()

		// Ensure unique email
		_, ok := emailMap[row.Values[1].Value.(TextPointer).String()]
		for ok {
			row = g.RowWithUniqueIndex()
			_, ok = emailMap[row.Values[1].Value.(TextPointer).String()]
		}

		row.Key = RowID(i)
		rows = append(rows, row)

		emailMap[row.Values[1].Value.(TextPointer).String()] = struct{}{}
	}
	return rows
}

var testOverflowColumns = []Column{
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
	{
		Kind:     Text,
		Name:     "profile",
		Nullable: true,
	},
}

func (g *dataGen) OverflowRow(textSize uint32) Row {
	return NewRowWithValues(testOverflowColumns, []OptionalValue{
		{Value: g.Int64(), Valid: true},
		{Value: g.paddedEmail(), Valid: true},
		{Value: g.textOfLength(textSize), Valid: true},
	})
}

func (g *dataGen) OverflowRows(number int, sizes []uint32) []Row {
	if len(sizes) != number {
		panic("sizes length must match number of rows")
	}
	// Make sure all rows will have unique ID, this is important in some tests
	idMap := map[int64]struct{}{}
	rows := make([]Row, 0, number)
	for i := range number {
		row := g.OverflowRow(sizes[i])
		_, ok := idMap[row.Values[0].Value.(int64)]
		for ok {
			row = g.OverflowRow(sizes[i])
			_, ok = idMap[row.Values[0].Value.(int64)]
		}
		row.Key = RowID(i)
		rows = append(rows, row)
		idMap[row.Values[0].Value.(int64)] = struct{}{}
	}
	return rows
}

var testCompositeKeyColumns = []Column{
	{
		Kind: Int8,
		Size: 8,
		Name: "id",
	},
	{
		Kind: Varchar,
		Size: 100,
		Name: "first_name",
	},
	{
		Kind: Varchar,
		Size: 100,
		Name: "last_name",
	},
	{
		Kind: Varchar,
		Size: 100,
		Name: "email",
	},
	{
		Kind: Timestamp,
		Size: 8,
		Name: "dob",
	},
}

func (g *dataGen) RowWithCompositeKey() Row {
	return NewRowWithValues(testCompositeKeyColumns, []OptionalValue{
		{Value: g.Int64(), Valid: true},
		{Value: NewTextPointer([]byte(g.FirstName())), Valid: true},
		{Value: NewTextPointer([]byte(g.LastName())), Valid: true},
		{Value: NewTextPointer([]byte(g.Email())), Valid: true},
		{Value: MustParseTimestampMicros(g.PastDate().Format(timestampFormat)), Valid: true},
	})
}

func (g *dataGen) RowsWithCompositeKey(number int) []Row {
	var (
		uniqueMap = map[string]struct{}{}
		rows      = make([]Row, 0, number)
	)
	for i := range number {
		row := g.RowWithCompositeKey()

		// Ensure unique composite key
		uniqueHash := fmt.Sprintf("%s|%s", row.Values[1].Value.(TextPointer).String(), row.Values[2].Value.(TextPointer).String())
		_, ok := uniqueMap[uniqueHash]
		for ok {
			row = g.RowWithCompositeKey()
			uniqueHash = fmt.Sprintf("%s|%s", row.Values[1].Value.(TextPointer).String(), row.Values[2].Value.(TextPointer).String())
			_, ok = uniqueMap[uniqueHash]
		}

		row.Key = RowID(i)
		rows = append(rows, row)

		uniqueMap[uniqueHash] = struct{}{}
	}
	return rows
}

func (g *dataGen) UniqueCountries(n int) []string {
	countryMap := map[string]struct{}{}
	countries := make([]string, 0, n)
	for len(countries) < n {
		country := g.Country()
		_, ok := countryMap[country]
		if !ok {
			countryMap[country] = struct{}{}
			countries = append(countries, country)
		}
	}
	return countries
}

func (g *dataGen) UniqueCities(n int) []string {
	cityMap := map[string]struct{}{}
	cities := make([]string, 0, n)
	for len(cities) < n {
		city := g.City()
		_, ok := cityMap[city]
		if !ok {
			cityMap[city] = struct{}{}
			cities = append(cities, city)
		}
	}
	return cities
}

func (g *dataGen) UniqueStreets(n int) []string {
	streetMap := map[string]struct{}{}
	streets := make([]string, 0, n)
	for len(streets) < n {
		street := g.Street()
		_, ok := streetMap[street]
		if !ok {
			streetMap[street] = struct{}{}
			streets = append(streets, street)
		}
	}
	return streets
}

func newRootLeafPageWithCells(cells, rowSize int) *Page {
	aRootLeaf := NewLeafNode()
	aRootLeaf.Header.IsRoot = true
	aRootLeaf.Header.Cells = uint32(cells)

	for i := range cells {
		aRootLeaf.Cells = append(aRootLeaf.Cells, Cell{
			Key:   RowID(i),
			Value: bytes.Repeat([]byte{byte(i)}, rowSize),
		})
	}

	return &Page{LeafNode: aRootLeaf}
}

/*
Below is a simple B tree for testing purposes

		           +-------------------+
		           |       *,5,*       |
		           +-------------------+
		          /                     \
		     +-------+                  +--------+
		     | *,2,* |                  | *,18,* |
		     +-------+                  +--------+
		    /         \                /          \
	 +---------+     +-----+     +-----------+    +------+
	 | 1:c,2:d |     | 5:a |     | 12:b,18:f |    | 21:g |
	 +---------+     +-----+     +-----------+    +------+
*/
func newTestBtree() (*Page, []*Page, []*Page) {
	var (
		// page 0
		rootPage = &Page{
			Index: 0,
			InternalNode: &InternalNode{
				Header: InternalNodeHeader{
					Header: Header{
						IsInternal: true,
						IsRoot:     true,
					},
					KeysNum:    1,
					RightChild: 2, // page 2
				},
				ICells: [InternalNodeMaxCells]ICell{
					{
						Key:   5,
						Child: 1, // page 1
					},
				},
			},
		}
		// page 1
		internalPage1 = &Page{
			Index: 1,
			InternalNode: &InternalNode{
				Header: InternalNodeHeader{
					Header: Header{
						IsInternal: true,
						Parent:     0, // page 0
					},
					KeysNum:    1,
					RightChild: 4, // page 4
				},
				ICells: [InternalNodeMaxCells]ICell{
					{
						Key:   2,
						Child: 3, // page 3
					},
				},
			},
		}
		// page 2
		internalPage2 = &Page{
			Index: 2,
			InternalNode: &InternalNode{
				Header: InternalNodeHeader{
					Header: Header{
						IsInternal: true,
						Parent:     0,
					},
					KeysNum:    1,
					RightChild: 6, // page 6
				},
				ICells: [InternalNodeMaxCells]ICell{
					{
						Key:   18,
						Child: 5, // page 5
					},
				},
			},
		}
		// page 3
		leafPage1 = &Page{
			Index: 3,
			LeafNode: &LeafNode{
				Header: LeafNodeHeader{
					Header: Header{
						IsInternal: false,
						Parent:     1,
					},
					Cells:    2,
					NextLeaf: 4,
				},
				Cells: []Cell{
					{
						Key:         1,
						Value:       prefixWithLength([]byte("ccc")),
						TypeCodes:   []byte{byte(TypeCodeText)},
						ColumnCount: 1,
						isOwned:     true,
					},
					{
						Key:         2,
						Value:       prefixWithLength([]byte("ddd")),
						TypeCodes:   []byte{byte(TypeCodeText)},
						ColumnCount: 1,
						isOwned:     true,
					},
				},
			},
		}
		// page 4
		leafPage2 = &Page{
			Index: 4,
			LeafNode: &LeafNode{
				Header: LeafNodeHeader{
					Header: Header{
						IsInternal: false,
						Parent:     1,
					},
					Cells:    1,
					NextLeaf: 5,
				},
				Cells: []Cell{
					{
						Key:         5,
						Value:       prefixWithLength([]byte("aaa")),
						TypeCodes:   []byte{byte(TypeCodeText)},
						ColumnCount: 1,
						isOwned:     true,
					},
				},
			},
		}
		// page 5
		leafPage3 = &Page{
			Index: 5,
			LeafNode: &LeafNode{
				Header: LeafNodeHeader{
					Header: Header{
						IsInternal: false,
						Parent:     2,
					},
					Cells:    2,
					NextLeaf: 6,
				},
				Cells: []Cell{
					{
						Key:         12,
						Value:       prefixWithLength([]byte("bbb")),
						TypeCodes:   []byte{byte(TypeCodeText)},
						ColumnCount: 1,
						isOwned:     true,
					},
					{
						Key:         18,
						Value:       prefixWithLength([]byte("fff")),
						TypeCodes:   []byte{byte(TypeCodeText)},
						ColumnCount: 1,
						isOwned:     true,
					},
				},
			},
		}
		// page 6
		leafPage4 = &Page{
			Index: 6,
			LeafNode: &LeafNode{
				Header: LeafNodeHeader{
					Header: Header{
						IsInternal: false,
						Parent:     2,
					},
					Cells: 1,
				},
				Cells: []Cell{
					{
						Key:         21,
						Value:       prefixWithLength([]byte("ggg")),
						TypeCodes:   []byte{byte(TypeCodeText)},
						ColumnCount: 1,
						isOwned:     true,
					},
				},
			},
		}

		internalPages = []*Page{
			internalPage1,
			internalPage2,
		}

		leafPages = []*Page{
			leafPage1,
			leafPage2,
			leafPage3,
			leafPage4,
		}
	)

	return rootPage, internalPages, leafPages
}

func resetMocks(mocks ...*mock.Mock) {
	for _, aMock := range mocks {
		aMock.ExpectedCalls = nil
		aMock.Calls = nil
	}
}

// initTest creates a temp DB file + pager for a single test function.
// It calls t.Parallel() and registers cleanup automatically.
func initTest(t *testing.T) (*pagerImpl, *os.File) {
	t.Helper()
	t.Parallel()

	tempFile, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	t.Cleanup(func() { os.Remove(tempFile.Name()) })

	pager, err := NewPager(tempFile, PageSize, 1000)
	require.NoError(t, err)

	return pager, tempFile
}

// mockPagerFactory returns a TxPagerFactory that always returns the given pager.
func mockPagerFactory(pager Pager) TxPagerFactory {
	return func(_ context.Context, _, _ string) (Pager, error) {
		return pager, nil
	}
}

// newTestTable creates a Table wired to a fresh transactional pager, calling
// initTest internally (which calls t.Parallel()).  It returns the table, its
// transaction manager, and the underlying pager (useful for low-level assertions).
// Options are forwarded to NewTable.
func newTestTable(t *testing.T, columns []Column, opts ...TableOption) (*Table, *TransactionManager, *pagerImpl) {
	t.Helper()
	pager, dbFile := initTest(t)
	tablePager := pager.ForTable(columns)
	txManager := NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
	txPager := NewTransactionalPager(tablePager, txManager, testTableName, "")
	table := NewTable(testLogger, txPager, txManager, testTableName, columns, 0, nil, opts...)
	return table, txManager, pager
}

// mustInsert runs an INSERT inside a transaction and fails the test on any error.
func mustInsert(ctx context.Context, t *testing.T, table *Table, txManager *TransactionManager, stmt Statement) {
	t.Helper()
	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := table.Insert(ctx, stmt)
		return err
	})
	require.NoError(t, err)
}

// mustDelete runs a DELETE inside a transaction and fails the test on any error.
func mustDelete(ctx context.Context, t *testing.T, table *Table, txManager *TransactionManager, _ PageSaver, stmt Statement) StatementResult {
	t.Helper()
	var result StatementResult
	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		var err error
		result, err = table.Delete(ctx, stmt)
		return err
	})
	require.NoError(t, err)
	return result
}

// checkRows selects all rows from table and asserts they match expectedRows (order-preserving).
func checkRows(ctx context.Context, t *testing.T, table *Table, expectedRows []Row) {
	t.Helper()
	selectResult, err := table.Select(ctx, Statement{
		Kind:   Select,
		Fields: fieldsFromColumns(table.Columns...),
	})
	require.NoError(t, err)

	expectedIDMap := map[int64]struct{}{}
	for _, r := range expectedRows {
		id, ok := r.GetValue("id")
		require.True(t, ok)
		expectedIDMap[id.Value.(int64)] = struct{}{}
	}

	var actual []Row
	for selectResult.Rows.Next(ctx) {
		row := selectResult.Rows.Row()
		actual = append(actual, row)
		if len(expectedIDMap) > 0 {
			_, ok := expectedIDMap[row.Values[0].Value.(int64)]
			assert.True(t, ok)
		}
	}
	require.NoError(t, selectResult.Rows.Err())

	require.Len(t, actual, len(expectedRows))
	for i := range len(expectedRows) {
		assert.Equal(t, expectedRows[i].Key, actual[i].Key, "row key %d does not match expected %d", i)
		assert.Equal(t, expectedRows[i].Columns, actual[i].Columns, "row columns %d does not match expected", i)
		for j, val := range expectedRows[i].Values {
			tp, ok := val.Value.(TextPointer)
			if ok {
				assert.Equal(t, int(tp.Length), int(actual[i].Values[j].Value.(TextPointer).Length), "row %d text pointer length %d does not match expected", i, j)
				assert.Equal(t, tp.Data, actual[i].Values[j].Value.(TextPointer).Data, "row %d text pointer data %d does not match expected", i, j)
			} else {
				assert.Equal(t, actual[i].Values[j], expectedRows[i].Values[j], "row %d value %d does not match expected", i, j)
			}
		}
		assert.Equal(t, expectedRows[i].NullBitmask(), actual[i].NullBitmask(), "row %d null bitmask does not match expected", i)
	}
}
