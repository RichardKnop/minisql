package minisql

import (
	"bytes"
	"os"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"go.uber.org/zap"

	"github.com/RichardKnop/minisql/internal/pkg/logging"
)

//go:generate mockery --name=Pager --structname=MockPager --inpackage --case=snake --testonly
//go:generate mockery --name=Parser --structname=MockParser --inpackage --case=snake --testonly

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
			Size:     255,
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
			Kind:     Boolean,
			Size:     1,
			Name:     "verified",
			Nullable: true,
		},
		{
			Kind:     Real,
			Size:     4,
			Name:     "test_real",
			Nullable: true,
		},
		{
			Kind:     Double,
			Size:     8,
			Name:     "test_double",
			Nullable: true,
		},
	}

	testMediumColumns = []Column{
		{
			Kind: Int8,
			Size: 8,
			Name: "id",
		},
		{
			Kind:     Varchar,
			Size:     255,
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
			Kind:     Boolean,
			Size:     1,
			Name:     "verified",
			Nullable: true,
		},
		{
			Kind:     Real,
			Size:     4,
			Name:     "test_real",
			Nullable: true,
		},
		{
			Kind:     Double,
			Size:     8,
			Name:     "test_double",
			Nullable: true,
		},
		{
			Kind: Varchar,
			// Size is defined so 5 of these columns can fit into a single page
			Size:     (PageSize - 6 - 8 - 5*8 - 5*8 - 5*(8+255+4+1+4+8)) / 5,
			Name:     "test_varchar",
			Nullable: true,
		},
	}

	testBigColumns = []Column{
		{
			Kind: Int8,
			Size: 8,
			Name: "id",
		},
		{
			Kind:     Varchar,
			Size:     255,
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
			Kind:     Boolean,
			Size:     1,
			Name:     "verified",
			Nullable: true,
		},
		{
			Kind:     Real,
			Size:     4,
			Name:     "test_real",
			Nullable: true,
		},
		{
			Kind:     Double,
			Size:     8,
			Name:     "test_double",
			Nullable: true,
		},
		{
			Kind:     Varchar,
			Size:     PageSize - 6 - 8 - 8 - 8 - (8 + 255 + 4 + 1 + 4 + 8),
			Name:     "test_varchar",
			Nullable: true,
		},
	}

	testLogger *zap.Logger
)

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

func columnNames(columns ...Column) []string {
	names := make([]string, 0, len(columns))
	for _, aColumn := range columns {
		names = append(names, aColumn.Name)
	}
	return names
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
			{Value: g.Email(), Valid: true},
			{Value: int32(g.IntRange(18, 100)), Valid: true},
			{Value: g.Bool(), Valid: true},
			{Value: g.Float32(), Valid: true},
			{Value: g.Float64(), Valid: true},
		},
	}
}

func (g *dataGen) Rows(number int) []Row {
	// Make sure all rows will have unique ID, this is important in some tests
	idMap := map[int64]struct{}{}
	rows := make([]Row, 0, number)
	for i := range number {
		aRow := g.Row()
		_, ok := idMap[aRow.Values[0].Value.(int64)]
		for ok {
			aRow = g.Row()
			_, ok = idMap[aRow.Values[0].Value.(int64)]
		}
		aRow.Key = uint64(i)
		rows = append(rows, aRow)
		idMap[aRow.Values[0].Value.(int64)] = struct{}{}
	}
	return rows
}

func (g *dataGen) MediumRow() Row {
	return Row{
		Columns: testMediumColumns,
		Values: []OptionalValue{
			{Value: g.Int64(), Valid: true},
			{Value: g.Email(), Valid: true},
			{Value: int32(g.IntRange(18, 100)), Valid: true},
			{Value: g.Bool(), Valid: true},
			{Value: g.Float32(), Valid: true},
			{Value: g.Float64(), Valid: true},
			{Value: g.Sentence(5), Valid: true},
		},
	}
}

func (g *dataGen) MediumRows(number int) []Row {
	// Make sure all rows will have unique ID, this is important in some tests
	idMap := map[int64]struct{}{}
	rows := make([]Row, 0, number)
	for i := range number {
		aRow := g.MediumRow()
		_, ok := idMap[aRow.Values[0].Value.(int64)]
		for ok {
			aRow = g.MediumRow()
			_, ok = idMap[aRow.Values[0].Value.(int64)]
		}
		aRow.Key = uint64(i)
		rows = append(rows, aRow)
		idMap[aRow.Values[0].Value.(int64)] = struct{}{}
	}
	return rows
}

func (g *dataGen) BigRow() Row {
	return Row{
		Columns: testBigColumns,
		Values: []OptionalValue{
			{Value: g.Int64(), Valid: true},
			{Value: g.Email(), Valid: true},
			{Value: int32(g.IntRange(18, 100)), Valid: true},
			{Value: g.Bool(), Valid: true},
			{Value: g.Float32(), Valid: true},
			{Value: g.Float64(), Valid: true},
			{Value: g.Sentence(15), Valid: true},
		},
	}
}

func (g *dataGen) BigRows(number int) []Row {
	// Make sure all rows will have unique ID, this is important in some tests
	idMap := map[int64]struct{}{}
	rows := make([]Row, 0, number)
	for i := range number {
		aRow := g.BigRow()
		_, ok := idMap[aRow.Values[0].Value.(int64)]
		for ok {
			aRow = g.BigRow()
			_, ok = idMap[aRow.Values[0].Value.(int64)]
		}
		aRow.Key = uint64(i)
		rows = append(rows, aRow)
		idMap[aRow.Values[0].Value.(int64)] = struct{}{}
	}
	return rows
}

func newRootLeafPageWithCells(cells, rowSize int) *Page {
	aRootLeaf := NewLeafNode(uint64(rowSize))
	aRootLeaf.Header.Header.IsRoot = true
	aRootLeaf.Header.Cells = uint32(cells)

	for i := 0; i < cells; i++ {
		aRootLeaf.Cells[i] = Cell{
			Key:   uint64(i),
			Value: bytes.Repeat([]byte{byte(i)}, rowSize),
		}
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
	defaultCell := NewLeafNode(270)
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
				Cells: append([]Cell{
					{
						Key:   1,
						Value: bytes.Repeat([]byte{byte(1)}, 270),
					},
					{
						Key:   2,
						Value: bytes.Repeat([]byte{byte(2)}, 270),
					},
				}, defaultCell.Cells[2:]...),
				RowSize: 270,
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
				Cells: append([]Cell{
					{
						Key:   5,
						Value: bytes.Repeat([]byte{byte(3)}, 270),
					},
				}, defaultCell.Cells[1:]...),
				RowSize: 270,
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
				Cells: append([]Cell{
					{
						Key:   12,
						Value: bytes.Repeat([]byte{byte(4)}, 270),
					},
					{
						Key:   18,
						Value: bytes.Repeat([]byte{byte(5)}, 270),
					},
				}, defaultCell.Cells[2:]...),
				RowSize: 270,
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
				Cells: append([]Cell{
					{
						Key:   21,
						Value: bytes.Repeat([]byte{byte(6)}, 270),
					},
				}, defaultCell.Cells[1:]...),
				RowSize: 270,
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
