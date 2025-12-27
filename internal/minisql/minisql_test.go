package minisql

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/RichardKnop/minisql/internal/pkg/logging"
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
			DefaultValue: OptionalValue{Value: MustParseTimestamp("0001-01-01 00:00:00.000000"), Valid: true},
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
				DefaultValue: OptionalValue{Value: MustParseTimestamp("0001-01-01 00:00:00.000000"), Valid: true},
			},
		},
		int((PageSize-uint32(RootPageConfigSize)-
			7- // base header
			8- // leaf header
			5*8- // 5 keys
			5*8- // 5 null bitmasks
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
				DefaultValue: OptionalValue{Value: MustParseTimestamp("0001-01-01 00:00:00.000000"), Valid: true},
			},
		},
		int((PageSize - uint32(RootPageConfigSize) -
			7 - // base header
			8 - // leaf header
			8 - // 5 keys
			8 - // 5 null bitmasks
			bigRowBaseSize)),
	)

	testColumnsWithPrimaryKey = []Column{
		{
			Kind:          Int8,
			Size:          8,
			Name:          "id",
			PrimaryKey:    true,
			Autoincrement: true,
		},
		{
			Kind:     Varchar,
			Size:     MaxInlineVarchar,
			Name:     "email",
			Nullable: true,
		},
	}

	testColumnsWithUniqueIndex = []Column{
		{
			Kind: Int8,
			Size: 8,
			Name: "id",
		},
		{
			Kind:   Varchar,
			Size:   MaxInlineVarchar,
			Name:   "email",
			Unique: true,
		},
	}

	testOverflowColumns = []Column{
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
		i++
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
	return Row{
		Columns: testColumns,
		Values: []OptionalValue{
			{Value: g.Int64(), Valid: true},
			{Value: g.paddedEmail(), Valid: true},
			{Value: int32(g.IntRange(18, 100)), Valid: true},
			{Value: g.Bool(), Valid: true},
			{Value: g.Float32(), Valid: true},
			{Value: MustParseTimestamp(g.PastDate().Format(timestampFormat)), Valid: true},
		},
	}
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
		aRow := g.Row()

		// Ensure unique ID
		_, ok := idMap[aRow.Values[0].Value.(int64)]
		for ok {
			aRow = g.Row()
			_, ok = idMap[aRow.Values[0].Value.(int64)]
		}

		// Ensure unique email
		_, ok = emailMap[aRow.Values[1].Value.(TextPointer).String()]
		for ok {
			aRow = g.Row()
			_, ok = emailMap[aRow.Values[1].Value.(TextPointer).String()]
		}

		aRow.Key = RowID(i)
		rows = append(rows, aRow)

		idMap[aRow.Values[0].Value.(int64)] = struct{}{}
		emailMap[aRow.Values[1].Value.(TextPointer).String()] = struct{}{}
	}
	return rows
}

func (g *dataGen) MediumRow() Row {
	aRow := Row{
		Columns: testMediumColumns,
		Values: []OptionalValue{
			{Value: g.Int64(), Valid: true},
			{Value: g.paddedEmail(), Valid: true},
			{Value: int32(g.IntRange(18, 100)), Valid: true},
			{Value: g.Bool(), Valid: true},
			{Value: g.Float32(), Valid: true},
			{Value: MustParseTimestamp(g.PastDate().Format(timestampFormat)), Valid: true},
		},
	}
	for len(aRow.Values) < len(testMediumColumns) {
		aRow.Values = append(
			aRow.Values,
			OptionalValue{Value: g.textOfLength(testMediumColumns[len(aRow.Values)].Size), Valid: true},
		)
	}
	return aRow
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
		aRow := g.MediumRow()

		// Ensure unique ID
		_, ok := idMap[aRow.Values[0].Value.(int64)]
		for ok {
			aRow = g.MediumRow()
			_, ok = idMap[aRow.Values[0].Value.(int64)]
		}

		// Ensure unique email
		_, ok = emailMap[aRow.Values[1].Value.(TextPointer).String()]
		for ok {
			aRow = g.MediumRow()
			_, ok = emailMap[aRow.Values[1].Value.(TextPointer).String()]
		}

		aRow.Key = RowID(i)
		rows = append(rows, aRow)

		idMap[aRow.Values[0].Value.(int64)] = struct{}{}
		emailMap[aRow.Values[1].Value.(TextPointer).String()] = struct{}{}
	}
	return rows
}

func (g *dataGen) BigRow() Row {
	aRow := Row{
		Columns: testBigColumns,
		Values: []OptionalValue{
			{Value: g.Int64(), Valid: true},
			{Value: g.paddedEmail(), Valid: true},
			{Value: int32(g.IntRange(18, 100)), Valid: true},
			{Value: g.Bool(), Valid: true},
			{Value: g.Float32(), Valid: true},
			{Value: MustParseTimestamp(g.PastDate().Format(timestampFormat)), Valid: true},
		},
	}
	for len(aRow.Values) < len(testBigColumns) {
		aRow.Values = append(
			aRow.Values,
			OptionalValue{Value: g.textOfLength(testBigColumns[len(aRow.Values)].Size), Valid: true},
		)
	}
	return aRow
}

func (g *dataGen) BigRows(number int) []Row {
	var (
		idMap    = map[int64]struct{}{}
		emailMap = map[string]struct{}{}
		rows     = make([]Row, 0, number)
	)
	for i := range number {
		aRow := g.BigRow()

		// Ensure unique ID
		_, ok := idMap[aRow.Values[0].Value.(int64)]
		for ok {
			aRow = g.BigRow()
			_, ok = idMap[aRow.Values[0].Value.(int64)]
		}

		// Ensure unique email
		_, ok = emailMap[aRow.Values[1].Value.(TextPointer).String()]
		for ok {
			aRow = g.BigRow()
			_, ok = emailMap[aRow.Values[1].Value.(TextPointer).String()]
		}

		aRow.Key = RowID(i)
		rows = append(rows, aRow)

		idMap[aRow.Values[0].Value.(int64)] = struct{}{}
		emailMap[aRow.Values[1].Value.(TextPointer).String()] = struct{}{}
	}
	return rows
}

func (g *dataGen) RowWithPrimaryKey(primaryKey int64) Row {
	return Row{
		Columns: testColumnsWithPrimaryKey,
		Values: []OptionalValue{
			{Value: primaryKey, Valid: true},
			{Value: NewTextPointer([]byte(g.Email())), Valid: true},
		},
	}
}

func (g *dataGen) RowsWithPrimaryKey(number int) []Row {
	var (
		emailMap = map[string]struct{}{}
		rows     = make([]Row, 0, number)
	)
	for i := range number {
		aRow := g.RowWithPrimaryKey(int64(i + 1))

		// Ensure unique email
		_, ok := emailMap[aRow.Values[1].Value.(TextPointer).String()]
		for ok {
			aRow = g.RowWithPrimaryKey(int64(i + 1))
			_, ok = emailMap[aRow.Values[1].Value.(TextPointer).String()]
		}

		aRow.Key = RowID(i)
		rows = append(rows, aRow)

		emailMap[aRow.Values[1].Value.(TextPointer).String()] = struct{}{}
	}
	return rows
}

func (g *dataGen) RowWithUniqueIndex() Row {
	return Row{
		Columns: testColumnsWithUniqueIndex,
		Values: []OptionalValue{
			{Value: g.Int64(), Valid: true},
			{Value: NewTextPointer([]byte(g.Email())), Valid: true},
		},
	}
}

func (g *dataGen) RowsWithUniqueIndex(number int) []Row {
	var (
		emailMap = map[string]struct{}{}
		rows     = make([]Row, 0, number)
	)
	for i := range number {
		aRow := g.RowWithUniqueIndex()

		// Ensure unique email
		_, ok := emailMap[aRow.Values[1].Value.(TextPointer).String()]
		for ok {
			aRow = g.RowWithUniqueIndex()
			_, ok = emailMap[aRow.Values[1].Value.(TextPointer).String()]
		}

		aRow.Key = RowID(i)
		rows = append(rows, aRow)

		emailMap[aRow.Values[1].Value.(TextPointer).String()] = struct{}{}
	}
	return rows
}

func (g *dataGen) OverflowRow(textSize uint32) Row {
	return Row{
		Columns: testOverflowColumns,
		Values: []OptionalValue{
			{Value: g.Int64(), Valid: true},
			{Value: g.paddedEmail(), Valid: true},
			{Value: g.textOfLength(textSize), Valid: true},
		},
	}
}

func (g *dataGen) OverflowRows(number int, sizes []uint32) []Row {
	if len(sizes) != number {
		panic("sizes length must match number of rows")
	}
	// Make sure all rows will have unique ID, this is important in some tests
	idMap := map[int64]struct{}{}
	rows := make([]Row, 0, number)
	for i := range number {
		aRow := g.OverflowRow(sizes[i])
		_, ok := idMap[aRow.Values[0].Value.(int64)]
		for ok {
			aRow = g.OverflowRow(sizes[i])
			_, ok = idMap[aRow.Values[0].Value.(int64)]
		}
		aRow.Key = RowID(i)
		rows = append(rows, aRow)
		idMap[aRow.Values[0].Value.(int64)] = struct{}{}
	}
	return rows
}

func newRootLeafPageWithCells(cells, rowSize int) *Page {
	aRootLeaf := NewLeafNode()
	aRootLeaf.Header.Header.IsRoot = true
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
		aRootPage = &Page{
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
						Key:   1,
						Value: prefixWithLength(bytes.Repeat([]byte{byte(1)}, 270)),
					},
					{
						Key:   2,
						Value: prefixWithLength(bytes.Repeat([]byte{byte(2)}, 270)),
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
						Key:   5,
						Value: prefixWithLength(bytes.Repeat([]byte{byte(3)}, 270)),
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
						Key:   12,
						Value: prefixWithLength(bytes.Repeat([]byte{byte(4)}, 270)),
					},
					{
						Key:   18,
						Value: prefixWithLength(bytes.Repeat([]byte{byte(5)}, 270)),
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
						Key:   21,
						Value: prefixWithLength(bytes.Repeat([]byte{byte(6)}, 270)),
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

	return aRootPage, internalPages, leafPages
}

func resetMock(aMock *mock.Mock) {
	aMock.ExpectedCalls = nil
	aMock.Calls = nil
}

func intPtr(i int64) *int64 {
	return &i
}
