package minisql

import (
	"bytes"
	"fmt"
)

type IndexNodeHeader struct {
	IsRoot     bool
	IsLeaf     bool
	Parent     PageIndex
	Keys       uint32
	RightChild PageIndex
}

func (h *IndexNodeHeader) Size() (s uint64) {
	return indexHeaderSize()
}

func indexHeaderSize() uint64 {
	return 1 + 1 + 1 + 4 + 4 + 4
}

func (h *IndexNodeHeader) Marshal(buf []byte) ([]byte, error) {
	size := h.Size()
	if uint64(cap(buf)) >= size {
		buf = buf[:size]
	} else {
		buf = make([]byte, size)
	}

	i := uint64(0)
	buf[0] = PageTypeIndex
	i += 1

	buf = marshalBool(buf, h.IsRoot, i)
	i += 1

	buf = marshalBool(buf, h.IsLeaf, i)
	i += 1

	marshalUint32(buf, uint32(h.Parent), i)
	i += 4

	marshalUint32(buf, h.Keys, i)
	i += 4

	marshalUint32(buf, uint32(h.RightChild), i)
	i += 4

	return buf[:size], nil
}

func (h *IndexNodeHeader) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)
	if buf[i] != PageTypeIndex {
		return 0, fmt.Errorf("unmarshal index node header: invalid page type %d", buf[i])
	}
	i += 1
	h.IsRoot = unmarshalBool(buf, i)
	i += 1
	h.IsLeaf = unmarshalBool(buf, i)
	i += 1
	h.Parent = PageIndex(unmarshalUint32(buf, i))
	i += 4
	h.Keys = unmarshalUint32(buf, i)
	i += 4
	h.RightChild = PageIndex(unmarshalUint32(buf, i))
	return h.Size(), nil
}

// Use int8 for bool so we can use comparison operators
type UniqueIndexCell[T IndexKey] struct {
	Key   T
	RowID RowID
	Child PageIndex
}

func (c *UniqueIndexCell[T]) Size() uint64 {
	size := uint64(8 + 4)
	size += keySize(c.Key)
	return size
}

func keySize[T IndexKey](key T) uint64 {
	switch v := any(key).(type) {
	case int8:
		return 1
	case int32:
		return 4
	case int64:
		return 8
	case float32:
		return 4
	case float64:
		return 8
	case string:
		return varcharLengthPrefixSize + uint64(len(v))
	}
	return 0
}

func (c *UniqueIndexCell[T]) Marshal(buf []byte) ([]byte, error) {
	size := c.Size()
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
		marshalInt8(buf, v, i)
		i += 1
	case int32:
		marshalInt32(buf, v, i)
		i += 4
	case int64:
		marshalInt64(buf, v, i)
		i += 8
	case float32:
		marshalFloat32(buf, v, i)
		i += 4
	case float64:
		marshalFloat64(buf, v, i)
		i += 8
	case string:
		marshalUint32(buf, uint32(len(v)), i)
		i += varcharLengthPrefixSize
		copy(buf[i:i+uint64(len([]byte(v)))], []byte(v))
		i += uint64(len([]byte(v)))
	default:
		return nil, fmt.Errorf("unsupported key type: %T", v)
	}

	marshalUint64(buf, uint64(c.RowID), i)
	i += 8

	marshalUint32(buf, uint32(c.Child), i)
	i += 4

	return buf[:i], nil
}

func (c *UniqueIndexCell[T]) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	// Unmarshal the key based on its type
	keyAny := any(c.Key)
	switch v := keyAny.(type) {
	case int8:
		c.Key = any(unmarshalInt8(buf, i)).(T)
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
		length := unmarshalUint32(buf, i)
		i += varcharLengthPrefixSize
		keySize := uint64(length)
		c.Key = any(string(bytes.Trim(buf[i:i+keySize], "\x00"))).(T)
		i += keySize
	default:
		return 0, fmt.Errorf("unsupported column type: %T", v)
	}

	c.RowID = RowID(unmarshalUint64(buf, i))
	i += 8

	c.Child = PageIndex(unmarshalUint32(buf, i))
	i += 4

	return i, nil
}

// Use int8 for bool so we can use comparison operators
type UniqueIndexNode[T IndexKey] struct {
	Header IndexNodeHeader
	Cells  []UniqueIndexCell[T] // (PageSize - (5)) / (CellSize + 4 + 8)
}

// TODO - this is not used currently
const MinimumIndexCells = 4

// Use int8 for bool so we can use comparison operators
func NewUniqueIndexNode[T IndexKey](cells ...UniqueIndexCell[T]) *UniqueIndexNode[T] {
	aNode := UniqueIndexNode[T]{
		Header: IndexNodeHeader{
			RightChild: RIGHT_CHILD_NOT_SET,
		},
		Cells: make([]UniqueIndexCell[T], 0, MinimumIndexCells),
	}
	for i := 0; i < MinimumIndexCells; i++ {
		aNode.Cells = append(aNode.Cells, UniqueIndexCell[T]{})
	}
	if len(cells) > 0 {
		aNode.Header.Keys = uint32(len(cells)) - 1
		copy(aNode.Cells, cells)
	}
	return &aNode
}

func (n *UniqueIndexNode[T]) Size() uint64 {
	size := n.Header.Size()

	for idx := range n.Header.Keys {
		size += n.Cells[idx].Size()
	}

	return size
}

func (n *UniqueIndexNode[T]) Marshal(buf []byte) ([]byte, error) {
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
		cbuf, err := n.Cells[idx].Marshal(buf[i:])
		if err != nil {
			return nil, err
		}
		i += uint64(len(cbuf))
	}

	return buf[:i], nil
}

func (n *UniqueIndexNode[T]) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	hi, err := n.Header.Unmarshal(buf[i:])
	if err != nil {
		return 0, err
	}
	i += hi

	for idx := 0; idx < int(n.Header.Keys); idx++ {
		if len(n.Cells) == idx {
			n.Cells = append(n.Cells, UniqueIndexCell[T]{})
		}
		ci, err := n.Cells[idx].Unmarshal(buf[i:])
		if err != nil {
			return 0, err
		}
		i += ci
	}

	return i, nil
}

// Child returns a node index of nth child of the node marked by its index
// (0 for the leftmost child, index equal to number of keys means the rightmost child).
func (n *UniqueIndexNode[T]) Child(childIdx uint32) (PageIndex, error) {
	keysNum := n.Header.Keys
	if childIdx > keysNum {
		return 0, fmt.Errorf("childIdx %d out of keys num %d", childIdx, keysNum)
	}

	if childIdx == keysNum {
		return n.Header.RightChild, nil
	}

	return n.Cells[childIdx].Child, nil
}

func (n *UniqueIndexNode[T]) SetChild(idx uint32, childPage PageIndex) error {
	keysNum := n.Header.Keys
	if idx > keysNum {
		return fmt.Errorf("childIdx %d out of keys num %d", idx, keysNum)
	}

	if idx == keysNum {
		n.Header.RightChild = childPage
		return nil
	}

	n.Cells[idx].Child = childPage
	return nil
}

func (n *UniqueIndexNode[T]) Keys() []T {
	if n.Header.Keys == 0 {
		return nil
	}
	keys := make([]T, 0, n.Header.Keys)
	for i := range n.Header.Keys {
		keys = append(keys, n.Cells[i].Key)
	}
	return keys
}

func (n *UniqueIndexNode[T]) RowIDs() []RowID {
	rowIDs := make([]RowID, 0, n.Header.Keys)
	for i := range n.Header.Keys {
		rowIDs = append(rowIDs, n.Cells[i].RowID)
	}
	return rowIDs
}

func (n *UniqueIndexNode[T]) Children() []PageIndex {
	if n.Header.IsLeaf {
		return nil
	}
	children := make([]PageIndex, 0, n.Header.Keys+1)
	for i := range n.Header.Keys {
		children = append(children, n.Cells[i].Child)
	}
	if n.Header.RightChild > 0 && n.Header.RightChild != RIGHT_CHILD_NOT_SET {
		children = append(children, n.Header.RightChild)
	}
	return children
}

func (n *UniqueIndexNode[T]) DeleteKeyByIndex(idx uint32) {
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

	n.Cells[int(n.Header.Keys)-1] = UniqueIndexCell[T]{}
	n.Header.Keys -= 1
}

func (n *UniqueIndexNode[T]) GetRightChildByIndex(idx uint32) PageIndex {
	if idx == n.Header.Keys-1 {
		return n.Header.RightChild
	}

	return n.Cells[idx+1].Child
}

func (n *UniqueIndexNode[T]) FirstCell() UniqueIndexCell[T] {
	return n.Cells[0]
}

func (n *UniqueIndexNode[T]) LastCell() UniqueIndexCell[T] {
	return n.Cells[n.Header.Keys-1]
}

func (n *UniqueIndexNode[T]) RemoveFirstCell() {
	for i := 0; i < int(n.Header.Keys)-1; i++ {
		n.Cells[i] = n.Cells[i+1]
	}
	n.Cells[n.Header.Keys-1] = UniqueIndexCell[T]{}
	n.Header.Keys -= 1
}

func (n *UniqueIndexNode[T]) RemoveLastCell() {
	idx := n.Header.Keys - 1
	n.Header.RightChild = n.Cells[idx].Child
	n.Cells[idx] = UniqueIndexCell[T]{}
	n.Header.Keys -= 1
}

func (n *UniqueIndexNode[T]) PrependCell(aCell UniqueIndexCell[T]) {
	if len(n.Cells) <= int(n.Header.Keys) {
		n.Cells = append(n.Cells, UniqueIndexCell[T]{})
	}
	for i := int(n.Header.Keys) - 1; i >= 0; i-- {
		n.Cells[i+1] = n.Cells[i]
	}
	n.Cells[0] = aCell
	n.Header.Keys += 1
}

func (n *UniqueIndexNode[T]) AppendCells(cells ...UniqueIndexCell[T]) {
	for _, aCell := range cells {
		if len(n.Cells) <= int(n.Header.Keys) {
			n.Cells = append(n.Cells, UniqueIndexCell[T]{})
		}
		n.Cells[n.Header.Keys] = aCell
		n.Header.Keys += 1
	}
}

func (n *UniqueIndexNode[T]) setParent(parentIdx PageIndex) {
	n.Header.Parent = parentIdx
}

func marshalIndexNode(anyNode any, buf []byte) ([]byte, error) {
	switch aNode := anyNode.(type) {
	case *UniqueIndexNode[int8]:
		return aNode.Marshal(buf)
	case *UniqueIndexNode[int32]:
		return aNode.Marshal(buf)
	case *UniqueIndexNode[int64]:
		return aNode.Marshal(buf)
	case *UniqueIndexNode[float32]:
		return aNode.Marshal(buf)
	case *UniqueIndexNode[float64]:
		return aNode.Marshal(buf)
	case *UniqueIndexNode[string]:
		return aNode.Marshal(buf)
	default:
		return nil, fmt.Errorf("unknown index node type: %T", aNode)
	}
}

func copyIndexNode(anyNode any) any {
	switch aNode := anyNode.(type) {
	case *UniqueIndexNode[int8]:
		aCopy := &UniqueIndexNode[int8]{
			Header: aNode.Header,
			Cells:  make([]UniqueIndexCell[int8], 0, MinimumIndexCells),
		}
		for _, aCell := range aNode.Cells {
			aCopy.Cells = append(aCopy.Cells, aCell)
		}
		return aCopy
	case *UniqueIndexNode[int32]:
		aCopy := &UniqueIndexNode[int32]{
			Header: aNode.Header,
			Cells:  make([]UniqueIndexCell[int32], 0, MinimumIndexCells),
		}
		for _, aCell := range aNode.Cells {
			aCopy.Cells = append(aCopy.Cells, aCell)
		}
		return aCopy
	case *UniqueIndexNode[int64]:
		aCopy := &UniqueIndexNode[int64]{
			Header: aNode.Header,
			Cells:  make([]UniqueIndexCell[int64], 0, MinimumIndexCells),
		}
		for _, aCell := range aNode.Cells {
			aCopy.Cells = append(aCopy.Cells, aCell)
		}
		return aCopy
	case *UniqueIndexNode[float32]:
		aCopy := &UniqueIndexNode[float32]{
			Header: aNode.Header,
			Cells:  make([]UniqueIndexCell[float32], 0, MinimumIndexCells),
		}
		for _, aCell := range aNode.Cells {
			aCopy.Cells = append(aCopy.Cells, aCell)
		}
		return aCopy
	case *UniqueIndexNode[float64]:
		aCopy := &UniqueIndexNode[float64]{
			Header: aNode.Header,
			Cells:  make([]UniqueIndexCell[float64], 0, MinimumIndexCells),
		}
		for _, aCell := range aNode.Cells {
			aCopy.Cells = append(aCopy.Cells, aCell)
		}
		return aCopy
	case *UniqueIndexNode[string]:
		aCopy := &UniqueIndexNode[string]{
			Header: aNode.Header,
			Cells:  make([]UniqueIndexCell[string], 0, MinimumIndexCells),
		}
		for _, aCell := range aNode.Cells {
			aCopy.Cells = append(aCopy.Cells, aCell)
		}
		return aCopy
	default:
		return nil
	}
}

func (n *UniqueIndexNode[T]) MaxSpace() uint64 {
	maxSpace := PageSize - indexHeaderSize()
	return maxSpace
}

func (n *UniqueIndexNode[T]) TakenSpace() uint64 {
	takenPageSize := uint64(0)
	for i := uint32(0); i < n.Header.Keys; i++ {
		takenPageSize += n.Cells[i].Size()
	}
	return takenPageSize
}

func (n *UniqueIndexNode[T]) AvailableSpace() uint64 {
	return n.MaxSpace() - n.TakenSpace()
}

func (n *UniqueIndexNode[T]) HasSpaceForKey(key T) bool {
	return (keySize(key) + 8 + 4) <= n.AvailableSpace()
}

func (n *UniqueIndexNode[T]) AtLeastHalfFull() bool {
	return n.AvailableSpace() < (n.MaxSpace())/2
}

func (n *UniqueIndexNode[T]) CanMergeWith(n2 *UniqueIndexNode[T]) bool {
	return n2.TakenSpace() <= n.AvailableSpace()
}

func (n *UniqueIndexNode[T]) CanBorrowFirst() bool {
	firstCellSize := n.Cells[0].Size()
	return n.AvailableSpace()+firstCellSize < n.MaxSpace()/2
}

func (n *UniqueIndexNode[T]) CanBorrowLast() bool {
	lastCellSize := n.Cells[n.Header.Keys-1].Size()
	return n.AvailableSpace()+lastCellSize < n.MaxSpace()/2
}
