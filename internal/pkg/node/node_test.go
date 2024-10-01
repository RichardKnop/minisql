package node

import (
	"testing"
)

func TestFindChildByKey(t *testing.T) {
	t.Parallel()

	// aRootNode := &InternalNode{
	// 	Header: InternalNodeHeader{
	// 		Header: Header{
	// 			IsRoot:     true,
	// 		},
	// 		KeysNum:    1,
	// 		RightChild: 18,
	// 	},
	// 	ICells: [InternalNodeMaxCells]ICell{
	// 		{
	// 			Key:   5,
	// 			Child: 2,
	// 		},
	// 	},
	// }
	// internalNode1 := &InternalNode{
	// 	Header: InternalNodeHeader{
	// 		Header: Header{
	// 			IsInternal: true,
	// 			Parent:     0,
	// 		},
	// 		KeysNum:    1,
	// 		RightChild: 5,
	// 	},
	// 	ICells: [InternalNodeMaxCells]ICell{
	// 		{
	// 			Key:   2,
	// 			Child: 1,
	// 		},
	// 	},
	// }
	// internalNode2 := &InternalNode{
	// 	Header: InternalNodeHeader{
	// 		Header: Header{
	// 			IsInternal: true,
	// 			Parent:     0,
	// 		},
	// 		KeysNum:    1,
	// 		RightChild: 21,
	// 	},
	// 	ICells: [InternalNodeMaxCells]ICell{
	// 		{
	// 			Key:   18,
	// 			Child: 12,
	// 		},
	// 	},
	// }

	// leafNode1 := &LeafNode{
	// 	Header: LeafNodeHeader{
	// 		Header: Header{
	// 			IsInternal: false,
	// 			Parent:     2,
	// 		},
	// 		Cells:    2,
	// 		NextLeaf: 5,
	// 	},
	// 	Cells: []Cell{
	// 		{
	// 			Key: 1,
	// 			Value: bytes.Repeat([]byte{byte(1)}, 270),
	// 			rowSize: 270,
	// 		},
	// 		{
	// 			Key: 15,
	// 			Value: bytes.Repeat([]byte{byte(2)}, 270),
	// 			rowSize: 270,
	// 		},
	// 	},
	// }

	// leafNode2 := &LeafNode{
	// 	Header: LeafNodeHeader{
	// 		Header: Header{
	// 			IsInternal: false,
	// 			Parent:     2,
	// 		},
	// 		Cells:    1,
	// 		NextLeaf: 12,
	// 	},
	// 	Cells: []Cell{
	// 		{
	// 			Key: 5,
	// 			Value: bytes.Repeat([]byte{byte(3)}, 270),
	// 			rowSize: 270,
	// 		},
	// 	},
	// }

	// leafNode3 := &LeafNode{
	// 	Header: LeafNodeHeader{
	// 		Header: Header{
	// 			IsInternal: false,
	// 			Parent:     18,
	// 		},
	// 		Cells:    2,
	// 		NextLeaf: 21,
	// 	},
	// 	Cells: []Cell{
	// 		{
	// 			Key: 12,
	// 			Value: bytes.Repeat([]byte{byte(4)}, 270),
	// 			rowSize: 270,
	// 		},
	// 		{
	// 			Key: 12,
	// 			Value: bytes.Repeat([]byte{byte(5)}, 270),
	// 			rowSize: 270,
	// 		},
	// 	},
	// }

	// leafNode4 := &LeafNode{
	// 	Header: LeafNodeHeader{
	// 		Header: Header{
	// 			IsInternal: false,
	// 			Parent:     18,
	// 		},
	// 		Cells:    1,
	// 	},
	// 	Cells: []Cell{
	// 		{
	// 			Key: 51,
	// 			Value: bytes.Repeat([]byte{byte(6)}, 270),
	// 			rowSize: 270,
	// 		},
	// 	},
	// }
}
