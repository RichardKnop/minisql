package minisqltest

import (
	"bytes"
	"time"

	"github.com/brianvoe/gofakeit/v6"

	"github.com/RichardKnop/minisql/internal/pkg/minisql"
)

var (
	gen = NewDataGen(time.Now().Unix())

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
	}
)

type DataGen struct {
	*gofakeit.Faker
}

func NewDataGen(seed int64) *DataGen {
	g := DataGen{
		Faker: gofakeit.New(seed),
	}

	return &g
}

func (g *DataGen) Rows(number int) []minisql.Row {
	rows := make([]minisql.Row, 0, number)
	for i := 0; i < number; i++ {
		rows = append(rows, g.Row())
	}
	return rows
}

func (g *DataGen) Row() minisql.Row {
	return minisql.Row{
		Columns: testColumns,
		Values: []any{
			g.Int64(),
			g.Email(),
			int32(g.IntRange(18, 100)),
		},
	}
}

func (g *DataGen) NewRootLeafPageWithCells(cells, size int) *minisql.Page {
	aRootLeaf := minisql.LeafNode{
		Header: minisql.LeafNodeHeader{
			Header: minisql.Header{
				IsRoot: true,
			},
			Cells: uint32(cells),
		},
		Cells: []minisql.Cell{
			{
				Key:     0,
				Value:   bytes.Repeat([]byte{byte(1)}, 270),
				RowSize: 270,
			},
			{
				Key:     1,
				Value:   bytes.Repeat([]byte{byte(2)}, 270),
				RowSize: 270,
			},
			{
				Key:     2,
				Value:   bytes.Repeat([]byte{byte(3)}, 270),
				RowSize: 270,
			},
		},
	}
	aRootLeaf.Cells = make([]minisql.Cell, 0, cells)
	for i := 0; i < cells; i++ {
		aRootLeaf.Cells = append(aRootLeaf.Cells, minisql.Cell{
			Key:     uint32(i),
			Value:   bytes.Repeat([]byte{byte(i)}, 270),
			RowSize: uint64(size),
		})
	}

	return &minisql.Page{LeafNode: &aRootLeaf}
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
	defaultCell := minisql.NewLeafNode(14, 270)
	var (
		// page 0
		aRootPage = &minisql.Page{
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
						Key:     1,
						Value:   bytes.Repeat([]byte{byte(1)}, 270),
						RowSize: 270,
					},
					{
						Key:     2,
						Value:   bytes.Repeat([]byte{byte(2)}, 270),
						RowSize: 270,
					},
				}, defaultCell.Cells[2:]...),
			},
		}
		// page 4
		leafPage2 = &minisql.Page{
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
						Key:     5,
						Value:   bytes.Repeat([]byte{byte(3)}, 270),
						RowSize: 270,
					},
				}, defaultCell.Cells[1:]...),
			},
		}
		// page 5
		leafPage3 = &minisql.Page{
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
						Key:     12,
						Value:   bytes.Repeat([]byte{byte(4)}, 270),
						RowSize: 270,
					},
					{
						Key:     18,
						Value:   bytes.Repeat([]byte{byte(5)}, 270),
						RowSize: 270,
					},
				}, defaultCell.Cells[2:]...),
			},
		}
		// page 6
		leafPage4 = &minisql.Page{
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
						Key:     21,
						Value:   bytes.Repeat([]byte{byte(6)}, 270),
						RowSize: 270,
					},
				}, defaultCell.Cells[1:]...),
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
