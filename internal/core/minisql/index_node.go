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
type IndexCell[T IndexKey] struct {
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
type IndexNode[T IndexKey] struct {
	Header  IndexNodeHeader
	Cells   []IndexCell[T] // (PageSize - (5)) / (CellSize + 4 + 8)
	KeySize uint64
}

const MinimumIndexCells = 4

func maxIndexKeys(keySize uint64) uint32 {
	// index header = 14
	// each cell = keySize + 8 + 4
	return uint32((PageSize - 14) / (keySize + 8 + 4))
}

// Use int8 for bool so we can use comparison operators
func NewIndexNode[T IndexKey](keySize uint64, cells ...IndexCell[T]) *IndexNode[T] {
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

func (n *IndexNode[T]) SetChild(idx, childPage uint32) error {
	keysNum := n.Header.Keys
	if idx > keysNum {
		return fmt.Errorf("childIdx %d out of keysNum %d", idx, keysNum)
	}

	if idx == keysNum {
		n.Header.RightChild = childPage
		return nil
	}

	n.Cells[idx].Child = childPage
	return nil
}

func (n *IndexNode[T]) Keys() []T {
	if n.Header.Keys == 0 {
		return nil
	}
	keys := make([]T, 0, n.Header.Keys)
	for i := range n.Header.Keys {
		keys = append(keys, n.Cells[i].Key)
	}
	return keys
}

func (n *IndexNode[T]) RowIDs() []uint64 {
	rowIDs := make([]uint64, 0, n.Header.Keys)
	for i := range n.Header.Keys {
		rowIDs = append(rowIDs, n.Cells[i].RowID)
	}
	return rowIDs
}

func (n *IndexNode[T]) Children() []uint32 {
	if n.Header.IsLeaf {
		return nil
	}
	children := make([]uint32, 0, n.Header.Keys+1)
	for i := range n.Header.Keys {
		children = append(children, n.Cells[i].Child)
	}
	if n.Header.RightChild > 0 && n.Header.RightChild != RIGHT_CHILD_NOT_SET {
		children = append(children, n.Header.RightChild)
	}
	return children
}

func (n *IndexNode[T]) DeleteKeyByIndex(idx uint32) {
	if n.Header.Keys == 0 {
		return
	}

	if idx == n.Header.Keys {
		idx -= 1
	}

	if idx == n.Header.Keys-1 {
		n.Header.RightChild = n.Cells[idx].Child
	} else {
		n.Cells[idx+1].Child = n.Cells[idx].Child
		for i := int(idx); i < int(n.Header.Keys-1); i++ {
			n.Cells[i] = n.Cells[i+1]
		}
	}

	n.Cells[int(n.Header.Keys)-1] = IndexCell[T]{}
	n.Header.Keys -= 1
}

func (n *IndexNode[T]) AtLeastHalfFull(maxCells int) bool {
	return int(n.Header.Keys) >= (maxCells+1)/2
}

func (n *IndexNode[T]) MoreThanHalfFull(maxCells int) bool {
	return int(n.Header.Keys) > (maxCells+1)/2
}

func (n *IndexNode[T]) GetRightChildByIndex(idx uint32) uint32 {
	if idx == n.Header.Keys-1 {
		return n.Header.RightChild
	}

	return n.Cells[idx+1].Child
}

func (n *IndexNode[T]) FirstCell() IndexCell[T] {
	return n.Cells[0]
}

func (n *IndexNode[T]) LastCell() IndexCell[T] {
	return n.Cells[n.Header.Keys-1]
}

func (n *IndexNode[T]) RemoveFirstCell() {
	for i := 0; i < int(n.Header.Keys)-1; i++ {
		n.Cells[i] = n.Cells[i+1]
	}
	n.Cells[n.Header.Keys-1] = IndexCell[T]{}
	n.Header.Keys -= 1
}

func (n *IndexNode[T]) RemoveLastCell() {
	idx := n.Header.Keys - 1
	n.Header.RightChild = n.Cells[idx].Child
	n.Cells[idx] = IndexCell[T]{}
	n.Header.Keys -= 1
}

func (n *IndexNode[T]) PrependCell(aCell IndexCell[T]) {
	if n.Header.Keys == 1 {

	}
	for i := int(n.Header.Keys) - 1; i >= 0; i-- {
		n.Cells[i+1] = n.Cells[i]
	}
	n.Cells[0] = aCell
	n.Header.Keys += 1
}

func (n *IndexNode[T]) AppendCells(cells ...IndexCell[T]) {
	for _, aCell := range cells {
		n.Cells[n.Header.Keys] = aCell
		n.Header.Keys += 1
	}
}

func (n *IndexNode[T]) setParent(parentIdx uint32) {
	n.Header.Parent = parentIdx
}

func marshalIndexNode(anyNode any, buf []byte) ([]byte, error) {
	switch aNode := anyNode.(type) {
	case *IndexNode[int8]:
		return aNode.Marshal(buf)
	case *IndexNode[int32]:
		return aNode.Marshal(buf)
	case *IndexNode[int64]:
		return aNode.Marshal(buf)
	case *IndexNode[float32]:
		return aNode.Marshal(buf)
	case *IndexNode[float64]:
		return aNode.Marshal(buf)
	case *IndexNode[string]:
		return aNode.Marshal(buf)
	default:
		return nil, fmt.Errorf("unknown index node type: %T", aNode)
	}
}

func copyIndexNode(anyNode any) any {
	switch aNode := anyNode.(type) {
	case *IndexNode[int8]:
		aCopy := &IndexNode[int8]{
			Header:  aNode.Header,
			Cells:   make([]IndexCell[int8], 0, maxIndexKeys(aNode.KeySize)),
			KeySize: aNode.KeySize,
		}
		for _, aCell := range aNode.Cells {
			aCopy.Cells = append(aCopy.Cells, aCell)
		}
		return aCopy
	case *IndexNode[int32]:
		aCopy := &IndexNode[int32]{
			Header:  aNode.Header,
			Cells:   make([]IndexCell[int32], 0, maxIndexKeys(aNode.KeySize)),
			KeySize: aNode.KeySize,
		}
		for _, aCell := range aNode.Cells {
			aCopy.Cells = append(aCopy.Cells, aCell)
		}
		return aCopy
	case *IndexNode[int64]:
		aCopy := &IndexNode[int64]{
			Header:  aNode.Header,
			Cells:   make([]IndexCell[int64], 0, maxIndexKeys(aNode.KeySize)),
			KeySize: aNode.KeySize,
		}
		for _, aCell := range aNode.Cells {
			aCopy.Cells = append(aCopy.Cells, aCell)
		}
		return aCopy
	case *IndexNode[float32]:
		aCopy := &IndexNode[float32]{
			Header:  aNode.Header,
			Cells:   make([]IndexCell[float32], 0, maxIndexKeys(aNode.KeySize)),
			KeySize: aNode.KeySize,
		}
		for _, aCell := range aNode.Cells {
			aCopy.Cells = append(aCopy.Cells, aCell)
		}
		return aCopy
	case *IndexNode[float64]:
		aCopy := &IndexNode[float64]{
			Header:  aNode.Header,
			Cells:   make([]IndexCell[float64], 0, maxIndexKeys(aNode.KeySize)),
			KeySize: aNode.KeySize,
		}
		for _, aCell := range aNode.Cells {
			aCopy.Cells = append(aCopy.Cells, aCell)
		}
		return aCopy
	case *IndexNode[string]:
		aCopy := &IndexNode[string]{
			Header:  aNode.Header,
			Cells:   make([]IndexCell[string], 0, maxIndexKeys(aNode.KeySize)),
			KeySize: aNode.KeySize,
		}
		for _, aCell := range aNode.Cells {
			aCopy.Cells = append(aCopy.Cells, aCell)
		}
		return aCopy
	default:
		return nil
	}
}
