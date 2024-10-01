package minisql

import (
	"bytes"
	"time"

	"github.com/brianvoe/gofakeit/v6"

	"github.com/RichardKnop/minisql/internal/pkg/node"
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

// Below is a simple B tree for testing purposes
//
//	           +-----------------+
//	           |      *,5,*      |
//	           +-----------------+
//	          /                   \
//	 +-------+                    +--------+
//	 | *,2,* |                    | *,18,* |
//	 +-------+                    +--------+
//	/         \                  /          \
//
// +---------+    +-----+       +-----------+   +------+
// | 1:c,2:d |    | 5:a |       | 12:b,18:f |   | 21:g |
// +---------+    +-----+       +-----------+   +------+
func newTestBtree() (*Page, []*Page, []*Page) {
	var (
		// page 0
		rootPage = &Page{
			InternalNode: &node.InternalNode{
				Header: node.InternalNodeHeader{
					Header: node.Header{
						IsRoot: true,
					},
					KeysNum:    1,
					RightChild: 2, // page 2
				},
				ICells: [node.InternalNodeMaxCells]node.ICell{
					{
						Key:   5,
						Child: 1, // page 1
					},
				},
			},
		}
		// page 1
		internalPage1 = &Page{
			InternalNode: &node.InternalNode{
				Header: node.InternalNodeHeader{
					Header: node.Header{
						IsInternal: true,
						Parent:     0, // page 0
					},
					KeysNum:    1,
					RightChild: 4, // page 4
				},
				ICells: [node.InternalNodeMaxCells]node.ICell{
					{
						Key:   1,
						Child: 3, // page 3
					},
				},
			},
		}
		// page 2
		internalPage2 = &Page{
			InternalNode: &node.InternalNode{
				Header: node.InternalNodeHeader{
					Header: node.Header{
						IsInternal: true,
						Parent:     0,
					},
					KeysNum:    1,
					RightChild: 6,
				},
				ICells: [node.InternalNodeMaxCells]node.ICell{
					{
						Key:   12,
						Child: 12,
					},
				},
			},
		}
		// page 3
		leafPage1 = &Page{
			LeafNode: &node.LeafNode{
				Header: node.LeafNodeHeader{
					Header: node.Header{
						IsInternal: false,
						Parent:     1,
					},
					Cells:    2,
					NextLeaf: 4,
				},
				Cells: []node.Cell{
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
				},
			},
		}
		// page 4
		leafPage2 = &Page{
			LeafNode: &node.LeafNode{
				Header: node.LeafNodeHeader{
					Header: node.Header{
						IsInternal: false,
						Parent:     1,
					},
					Cells:    1,
					NextLeaf: 5,
				},
				Cells: []node.Cell{
					{
						Key:     5,
						Value:   bytes.Repeat([]byte{byte(3)}, 270),
						RowSize: 270,
					},
				},
			},
		}
		// page 5
		leafPage3 = &Page{
			LeafNode: &node.LeafNode{
				Header: node.LeafNodeHeader{
					Header: node.Header{
						IsInternal: false,
						Parent:     2,
					},
					Cells:    2,
					NextLeaf: 6,
				},
				Cells: []node.Cell{
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
				},
			},
		}
		// page 6
		leafPage4 = &Page{
			LeafNode: &node.LeafNode{
				Header: node.LeafNodeHeader{
					Header: node.Header{
						IsInternal: false,
						Parent:     2,
					},
					Cells: 1,
				},
				Cells: []node.Cell{
					{
						Key:     21,
						Value:   bytes.Repeat([]byte{byte(6)}, 270),
						RowSize: 270,
					},
				},
			},
		}
	)

	return rootPage, []*Page{internalPage1, internalPage2}, []*Page{leafPage1, leafPage2, leafPage3, leafPage4}
}
