package minisql

import (
	"bytes"
	"time"

	"github.com/brianvoe/gofakeit/v6"
)

var (
	gen = newDataGen(time.Now().Unix())

	testColumns = []Column{
		{
			Kind: Int8,
			Size: 8,
			Name: "id",
		},
		{
			Kind: Varchar,
			Size: 255,
			Name: "email",
		},
		{
			Kind: Int4,
			Size: 4,
			Name: "age",
		},
	}
)

type dataGen struct {
	*gofakeit.Faker
}

func newDataGen(seed int64) *dataGen {
	g := dataGen{
		Faker: gofakeit.New(seed),
	}

	return &g
}

func (g *dataGen) Rows(number int) []Row {
	rows := make([]Row, 0, number)
	for i := 0; i < number; i++ {
		rows = append(rows, g.Row())
	}
	return rows
}

func (g *dataGen) Row() Row {
	return Row{
		Columns: testColumns,
		Values: []any{
			g.Int64(),
			g.Email(),
			int32(g.IntRange(18, 100)),
		},
	}
}

func newRootLeafPageWithCells(cells, rowSize int) *Page {
	aRootLeaf := NewLeafNode(uint64(rowSize))
	aRootLeaf.Header.Header.IsRoot = true
	aRootLeaf.Header.Cells = uint32(cells)

	for i := 0; i < cells; i++ {
		aRootLeaf.Cells[i] = Cell{
			Key:     uint64(i),
			Value:   bytes.Repeat([]byte{byte(i)}, rowSize),
			RowSize: uint64(rowSize),
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
		leafPage2 = &Page{
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
						Key:     5,
						Value:   bytes.Repeat([]byte{byte(3)}, 270),
						RowSize: 270,
					},
				}, defaultCell.Cells[1:]...),
			},
		}
		// page 5
		leafPage3 = &Page{
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
		leafPage4 = &Page{
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
						Key:     21,
						Value:   bytes.Repeat([]byte{byte(6)}, 270),
						RowSize: 270,
					},
				}, defaultCell.Cells[1:]...),
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
