package minisql

import (
	"bytes"
	"fmt"
)

type IndexNodeHeader struct {
	IsRoot     bool
	IsLeaf     bool
	Parent     uint32
	Keys       uint32
	RightChild uint32
}

func (h *IndexNodeHeader) Size() (s uint64) {
	return 1 + 1 + 4 + 4 + 4
}

func (h *IndexNodeHeader) Marshal(buf []byte) ([]byte, error) {
	size := h.Size()
	if uint64(cap(buf)) >= size {
		buf = buf[:size]
	} else {
		buf = make([]byte, size)
	}

	i := uint64(0)

	if h.IsRoot {
		buf[0] = 1
	} else {
		buf[0] = 0
	}
	i += 1

	if h.IsLeaf {
		buf[1] = 1
	} else {
		buf[1] = 0
	}
	i += 1

	marshalUint32(buf, h.Parent, i)
	i += 4

	marshalUint32(buf, h.Keys, i)
	i += 4

	marshalUint32(buf, h.RightChild, i)
	i += 4

	return buf[:size], nil
}

func (h *IndexNodeHeader) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)
	h.IsRoot = buf[i] == 1
	i += 1
	h.IsLeaf = buf[i] == 1
	i += 1
	h.Parent = unmarshalUint32(buf, i)
	i += 4
	h.Keys = unmarshalUint32(buf, i)
	i += 4
	h.RightChild = unmarshalUint32(buf, i)
	return h.Size(), nil
}

// Use int8 for bool so we can use comparison operators
type IndexCell[T int8 | int32 | int64 | float32 | float64 | string] struct {
	Key   T
	RowID uint64
	Child uint32
}

func (c *IndexCell[T]) Size(keySize uint64) uint64 {
	return keySize + 8 + 4
}

func (c *IndexCell[T]) Marshal(keySize uint64, buf []byte) ([]byte, error) {
	size := c.Size(keySize)
	if uint64(cap(buf)) >= size {
		buf = buf[:size]
	} else {
		buf = make([]byte, size)
	}

	i := uint64(0)

	// Marshal the key based on its type
	keyAny := any(c.Key)
	switch v := keyAny.(type) {
	case int8:
		if v == 1 {
			buf[i] = 1
		} else {
			buf[i] = 0
		}
		i += 1
	case int32:
		marshalInt32(buf, v, i)
	case int64:
		marshalInt64(buf, v, i)
	case float32:
		marshalFloat32(buf, v, i)
	case float64:
		marshalFloat64(buf, v, i)
	case string:
		b := make([]byte, keySize)
		copy(b, []byte(v))
		copy(buf[i:], b)
	default:
		return nil, fmt.Errorf("unsupported key type: %T", v)
	}
	i += keySize

	marshalUint64(buf, c.RowID, i)
	i += 8

	marshalUint32(buf, c.Child, i)
	i += 4

	return buf[:i], nil
}

func (c *IndexCell[T]) Unmarshal(keySize uint64, buf []byte) (uint64, error) {
	i := uint64(0)

	// Unmarshal the key based on its type
	keyAny := any(c.Key)
	switch v := keyAny.(type) {
	case int8:
		c.Key = any(buf[i] == 1).(T)
		i += 1
	case int32:
		c.Key = any(unmarshalInt32(buf, i)).(T)
		i += 4
	case int64:
		c.Key = any(unmarshalInt64(buf, i)).(T)
		i += 8
	case float32:
		c.Key = any(unmarshalFloat32(buf, i)).(T)
		i += 4
	case float64:
		c.Key = any(unmarshalFloat64(buf, i)).(T)
		i += 8
	case string:
		c.Key = any(string(bytes.Trim(buf[i:i+keySize], "\x00"))).(T)
		i += keySize
	default:
		return 0, fmt.Errorf("unsupported column type: %T", v)
	}

	c.RowID = unmarshalUint64(buf, i)
	i += 8

	c.Child = unmarshalUint32(buf, i)
	i += 4

	return i, nil
}

// Use int8 for bool so we can use comparison operators
type IndexNode[T int8 | int32 | int64 | float32 | float64 | string] struct {
	Header  IndexNodeHeader
	Cells   []IndexCell[T] // (PageSize - (5)) / (CellSize + 4 + 8)
	KeySize uint64
}

// func NewIndexCell[T bool | int32 | int64 | float32 | float64 | string](key T, rowID uint64, child uint32) IndexCell[T] {
// 	return IndexCell[T]{
// 		Key:   key,
// 		RowID: rowID,
// 		Child: child,
// 	}
// }

const MinimumIndexCells = 4

func maxIndexKeys(keySize uint64) uint32 {
	// index header = 13
	// each cell = keySize + 8 + 4
	return uint32((PageSize - 13) / (keySize + 8 + 4))
}

// Use int8 for bool so we can use comparison operators
func NewIndexNode[T int8 | int32 | int64 | float32 | float64 | string](keySize uint64, cells ...IndexCell[T]) *IndexNode[T] {
	aNode := IndexNode[T]{
		Header: IndexNodeHeader{
			RightChild: RIGHT_CHILD_NOT_SET,
		},
		Cells:   make([]IndexCell[T], 0, maxIndexKeys(keySize)),
		KeySize: keySize,
	}
	for i := 0; i < int(maxIndexKeys(keySize)); i++ {
		aNode.Cells = append(aNode.Cells, IndexCell[T]{})
	}
	if len(cells) > 0 {
		aNode.Header.Keys = uint32(len(cells)) - 1
		copy(aNode.Cells, cells)
	}
	return &aNode
}

func (n *IndexNode[T]) Size() uint64 {
	size := uint64(0)
	size += n.Header.Size()

	for idx := range n.Cells {
		size += n.Cells[idx].Size(n.KeySize)
	}

	return size
}

func (n *IndexNode[T]) Marshal(buf []byte) ([]byte, error) {
	size := n.Size()
	if uint64(cap(buf)) >= size {
		buf = buf[:size]
	} else {
		buf = make([]byte, size)
	}

	i := uint64(0)

	hbuf, err := n.Header.Marshal(buf[i:])
	if err != nil {
		return nil, err
	}
	i += uint64(len(hbuf))

	for idx := 0; idx < int(n.Header.Keys); idx++ {
		cbuf, err := n.Cells[idx].Marshal(n.KeySize, buf[i:])
		if err != nil {
			return nil, err
		}
		i += uint64(len(cbuf))
	}

	return buf[:i], nil
}

func (n *IndexNode[T]) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	hi, err := n.Header.Unmarshal(buf[i:])
	if err != nil {
		return 0, err
	}
	i += hi

	for idx := 0; idx < int(n.Header.Keys); idx++ {
		ci, err := n.Cells[idx].Unmarshal(n.KeySize, buf[i:])
		if err != nil {
			return 0, err
		}
		i += ci
	}

	return i, nil
}

// Child returns a node index of nth child of the node marked by its index
// (0 for the leftmost child, index equal to number of keys means the rightmost child).
func (n *IndexNode[T]) Child(childIdx uint32) (uint32, error) {
	keysNum := n.Header.Keys
	if childIdx > keysNum {
		return 0, fmt.Errorf("childIdx %d out of keysNum %d", childIdx, keysNum)
	}

	if childIdx == keysNum {
		return n.Header.RightChild, nil
	}

	return n.Cells[childIdx].Child, nil
}

func (n *IndexNode[T]) Keys() []T {
	keys := make([]T, 0, n.Header.Keys)
	for i := range n.Header.Keys {
		keys = append(keys, n.Cells[i].Key)
	}
	return keys
}

func (n *IndexNode[T]) Children() []uint32 {
	children := make([]uint32, 0, n.Header.Keys+1)
	for i := range n.Header.Keys {
		if n.Cells[i].Child == 0 {
			continue
		}
		children = append(children, n.Cells[i].Child)
	}
	if n.Header.RightChild > 0 && n.Header.RightChild != RIGHT_CHILD_NOT_SET {
		children = append(children, n.Header.RightChild)
	}
	return children
}
