package minisqltest

import (
	"bytes"

	"github.com/brianvoe/gofakeit/v7"

	"github.com/RichardKnop/minisql/internal/core/minisql"
)

var (
	testColumns = []minisql.Column{
		{
			Kind: minisql.Int8,
			Size: 8,
			Name: "id",
		},
		{
			Kind: minisql.Varchar,
			Size: 255,
			Name: "email",
		},
		{
			Kind: minisql.Int4,
			Size: 4,
			Name: "age",
		},
		{
			Kind: minisql.Boolean,
			Size: 1,
			Name: "verified",
		},
		{
			Kind: minisql.Real,
			Size: 4,
			Name: "test_real",
		},
		{
			Kind: minisql.Double,
			Size: 8,
			Name: "test_double",
		},
	}

	testBigColumns = []minisql.Column{
		{
			Kind: minisql.Int8,
			Size: 8,
			Name: "id",
		},
		{
			Kind: minisql.Varchar,
			Size: 255,
			Name: "email",
		},
		{
			Kind: minisql.Int4,
			Size: 4,
			Name: "age",
		},
		{
			Kind: minisql.Boolean,
			Size: 1,
			Name: "verified",
		},
		{
			Kind: minisql.Real,
			Size: 4,
			Name: "test_real",
		},
		{
			Kind: minisql.Double,
			Size: 8,
			Name: "test_double",
		},
		{
			Kind: minisql.Varchar,
			Size: minisql.PageSize - 6 - 8 - 8 - 8 - (8 + 255 + 4 + 1 + 4 + 8),
			Name: "test_varchar",
		},
	}
)

type DataGen struct {
	*gofakeit.Faker
}

func NewDataGen(seed uint64) *DataGen {
	g := DataGen{
		Faker: gofakeit.New(seed),
	}

	return &g
}

func (g *DataGen) Row() minisql.Row {
	return minisql.Row{
		Columns: testColumns,
		Values: []any{
			g.Int64(),
			g.Email(),
			int32(g.IntRange(18, 100)),
			g.Bool(),
			g.Float32(),
			g.Float64(),
		},
	}
}

func (g *DataGen) Rows(number int) []minisql.Row {
	// Make sure all rows will have unique ID, this is important in some tests
	idMap := map[int64]struct{}{}
	rows := make([]minisql.Row, 0, number)
	for i := 0; i < number; i++ {
		aRow := g.Row()
		_, ok := idMap[aRow.Values[0].(int64)]
		for ok {
			aRow = g.Row()
			_, ok = idMap[aRow.Values[0].(int64)]
		}
		rows = append(rows, aRow)
		idMap[aRow.Values[0].(int64)] = struct{}{}

	}
	return rows
}

func (g *DataGen) BigRow() minisql.Row {
	return minisql.Row{
		Columns: testBigColumns,
		Values: []any{
			g.Int64(),
			g.Email(),
			int32(g.IntRange(18, 100)),
			g.Bool(),
			g.Float32(),
			g.Float64(),
			g.Sentence(15),
		},
	}
}

func (g *DataGen) BigRows(number int) []minisql.Row {
	// Make sure all rows will have unique ID, this is important in some tests
	idMap := map[int64]struct{}{}
	rows := make([]minisql.Row, 0, number)
	for i := 0; i < number; i++ {
		aRow := g.BigRow()
		_, ok := idMap[aRow.Values[0].(int64)]
		for ok {
			aRow = g.BigRow()
			_, ok = idMap[aRow.Values[0].(int64)]
		}
		rows = append(rows, aRow)
		idMap[aRow.Values[0].(int64)] = struct{}{}
	}
	return rows
}

func (g *DataGen) NewRootLeafPageWithCells(cells, rowSize int) *minisql.Page {
	aRootLeaf := minisql.NewLeafNode(uint64(rowSize))
	aRootLeaf.Header.Header.IsRoot = true
	aRootLeaf.Header.Cells = uint32(cells)

	for i := 0; i < cells; i++ {
		aRootLeaf.Cells[i] = minisql.Cell{
			Key:   uint64(i),
			Value: bytes.Repeat([]byte{byte(i)}, rowSize),
		}
	}

	return &minisql.Page{LeafNode: aRootLeaf}
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
func (g *DataGen) NewTestBtree() (*minisql.Page, []*minisql.Page, []*minisql.Page) {
	defaultCell := minisql.NewLeafNode(270)
	var (
		// page 0
		aRootPage = &minisql.Page{
			Index: 0,
			InternalNode: &minisql.InternalNode{
				Header: minisql.InternalNodeHeader{
					Header: minisql.Header{
						IsInternal: true,
						IsRoot:     true,
					},
					KeysNum:    1,
					RightChild: 2, // page 2
				},
				ICells: [minisql.InternalNodeMaxCells]minisql.ICell{
					{
						Key:   5,
						Child: 1, // page 1
					},
				},
			},
		}
		// page 1
		internalPage1 = &minisql.Page{
			Index: 1,
			InternalNode: &minisql.InternalNode{
				Header: minisql.InternalNodeHeader{
					Header: minisql.Header{
						IsInternal: true,
						Parent:     0, // page 0
					},
					KeysNum:    1,
					RightChild: 4, // page 4
				},
				ICells: [minisql.InternalNodeMaxCells]minisql.ICell{
					{
						Key:   2,
						Child: 3, // page 3
					},
				},
			},
		}
		// page 2
		internalPage2 = &minisql.Page{
			Index: 2,
			InternalNode: &minisql.InternalNode{
				Header: minisql.InternalNodeHeader{
					Header: minisql.Header{
						IsInternal: true,
						Parent:     0,
					},
					KeysNum:    1,
					RightChild: 6, // page 6
				},
				ICells: [minisql.InternalNodeMaxCells]minisql.ICell{
					{
						Key:   18,
						Child: 5, // page 5
					},
				},
			},
		}
		// page 3
		leafPage1 = &minisql.Page{
			Index: 3,
			LeafNode: &minisql.LeafNode{
				Header: minisql.LeafNodeHeader{
					Header: minisql.Header{
						IsInternal: false,
						Parent:     1,
					},
					Cells:    2,
					NextLeaf: 4,
				},
				Cells: append([]minisql.Cell{
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
		leafPage2 = &minisql.Page{
			Index: 4,
			LeafNode: &minisql.LeafNode{
				Header: minisql.LeafNodeHeader{
					Header: minisql.Header{
						IsInternal: false,
						Parent:     1,
					},
					Cells:    1,
					NextLeaf: 5,
				},
				Cells: append([]minisql.Cell{
					{
						Key:   5,
						Value: bytes.Repeat([]byte{byte(3)}, 270),
					},
				}, defaultCell.Cells[1:]...),
				RowSize: 270,
			},
		}
		// page 5
		leafPage3 = &minisql.Page{
			Index: 5,
			LeafNode: &minisql.LeafNode{
				Header: minisql.LeafNodeHeader{
					Header: minisql.Header{
						IsInternal: false,
						Parent:     2,
					},
					Cells:    2,
					NextLeaf: 6,
				},
				Cells: append([]minisql.Cell{
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
		leafPage4 = &minisql.Page{
			Index: 6,
			LeafNode: &minisql.LeafNode{
				Header: minisql.LeafNodeHeader{
					Header: minisql.Header{
						IsInternal: false,
						Parent:     2,
					},
					Cells: 1,
				},
				Cells: append([]minisql.Cell{
					{
						Key:   21,
						Value: bytes.Repeat([]byte{byte(6)}, 270),
					},
				}, defaultCell.Cells[1:]...),
				RowSize: 270,
			},
		}

		internalPages = []*minisql.Page{
			internalPage1,
			internalPage2,
		}

		leafPages = []*minisql.Page{
			leafPage1,
			leafPage2,
			leafPage3,
			leafPage4,
		}
	)

	return aRootPage, internalPages, leafPages
}
