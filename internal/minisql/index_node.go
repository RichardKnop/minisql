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
type IndexCell[T IndexKey] struct {
	Key          T
	InlineRowIDs uint32 // only for non-unique indexes
	RowIDs       []RowID
	Overflow     PageIndex // 0 if not used (only for non-unique indexes)
	Child        PageIndex
	unique       bool // true for non-unique index cells
}

func NewIndexCell[T IndexKey](unique bool) IndexCell[T] {
	return IndexCell[T]{
		unique: unique,
		RowIDs: make([]RowID, 0, 1),
	}
}

func (c *IndexCell[T]) Size() uint64 {
	// Key size  + child pointer size
	size := keySize(c.Key) + 4
	if c.unique {
		// Single row ID
		size += 8
	} else {
		// Row IDs length prefix + inlined row IDs + overflow pointer
		size += uint64(rowIDsLengthPrefixSize + c.InlineRowIDs*8 + 4)
	}
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

func (c *IndexCell[T]) Marshal(buf []byte) ([]byte, error) {
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

	if c.unique {
		// Single row ID
		marshalUint64(buf, uint64(c.RowIDs[0]), i)
		i += 8
	} else {
		// Row IDs length prefix
		marshalUint32(buf, c.InlineRowIDs, i)
		i += 4

		// Inlined row IDs
		for j := 0; j < int(c.InlineRowIDs); j++ {
			marshalUint64(buf, uint64(c.RowIDs[j]), i)
			i += 8
		}

		// Overflow pointer
		marshalUint32(buf, uint32(c.Overflow), i)
		i += 4
	}

	marshalUint32(buf, uint32(c.Child), i)
	i += 4

	return buf[:i], nil
}

func (c *IndexCell[T]) Unmarshal(buf []byte) (uint64, error) {
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

	if c.unique {
		c.RowIDs = []RowID{RowID(unmarshalUint64(buf, i))}
		i += 8
	} else {
		c.InlineRowIDs = unmarshalUint32(buf, i)
		i += 4

		c.RowIDs = make([]RowID, 0, c.InlineRowIDs)
		for j := uint32(0); j < c.InlineRowIDs; j++ {
			c.RowIDs = append(c.RowIDs, RowID(unmarshalUint64(buf, i)))
			i += 8
		}

		c.Overflow = PageIndex(unmarshalUint32(buf, i))
		i += 4
	}

	c.Child = PageIndex(unmarshalUint32(buf, i))
	i += 4

	return i, nil
}

func (c *IndexCell[T]) RemoveRowID(id RowID) int {
	for i := range c.RowIDs {
		if c.RowIDs[i] == id {
			c.RowIDs = append(c.RowIDs[:i], c.RowIDs[i+1:]...)
			c.InlineRowIDs -= 1
			return i
		}
	}
	return -1
}

func (c *IndexCell[T]) ReplaceRowID(id, newID RowID) int {
	for i := range c.RowIDs {
		if c.RowIDs[i] == id {
			c.RowIDs[i] = newID
			return i
		}
	}
	return -1
}

// Use int8 for bool so we can use comparison operators
type IndexNode[T IndexKey] struct {
	Header IndexNodeHeader
	Cells  []IndexCell[T] // (PageSize - (5)) / (CellSize + 4 + 8)
}

// TODO - this is not used currently
const MinimumIndexCells = 4

// Use int8 for bool so we can use comparison operators
func NewIndexNode[T IndexKey](unique bool, cells ...IndexCell[T]) *IndexNode[T] {
	aNode := IndexNode[T]{
		Header: IndexNodeHeader{
			RightChild: RIGHT_CHILD_NOT_SET,
		},
		Cells: make([]IndexCell[T], 0, MinimumIndexCells),
	}
	for range MinimumIndexCells {
		aNode.Cells = append(aNode.Cells, NewIndexCell[T](unique))
	}
	if len(cells) > 0 {
		aNode.Header.Keys = uint32(len(cells)) - 1
		copy(aNode.Cells, cells)
	}
	return &aNode
}

func (n *IndexNode[T]) Size() uint64 {
	size := n.Header.Size()

	for idx := range n.Header.Keys {
		size += n.Cells[idx].Size()
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
		cbuf, err := n.Cells[idx].Marshal(buf[i:])
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
		if len(n.Cells) == idx {
			n.Cells = append(n.Cells, NewIndexCell[T](n.Cells[0].unique))
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
func (n *IndexNode[T]) Child(childIdx uint32) (PageIndex, error) {
	keysNum := n.Header.Keys
	if childIdx > keysNum {
		return 0, fmt.Errorf("childIdx %d out of keys num %d", childIdx, keysNum)
	}

	if childIdx == keysNum {
		return n.Header.RightChild, nil
	}

	return n.Cells[childIdx].Child, nil
}

func (n *IndexNode[T]) SetChild(idx uint32, childPage PageIndex) error {
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

// RowIDs returns inlined row IDs; there could be more row IDs in overflow pages,
// so keep that in mind when working with non-unique indexes.
func (n *IndexNode[T]) RowIDs() []RowID {
	rowIDs := make([]RowID, 0, n.Header.Keys)
	for i := range n.Header.Keys {
		rowIDs = append(rowIDs, n.Cells[i].RowIDs...)
	}
	return rowIDs
}

func (n *IndexNode[T]) Children() []PageIndex {
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

// Removes key from plus the right child pointer
func (n *IndexNode[T]) DeleteKeyAndRightChild(idx uint32) error {
	if n.Header.Keys == 0 {
		return nil
	}

	if idx >= n.Header.Keys {
		return fmt.Errorf("index %d out of range for keys %d", idx, n.Header.Keys)
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

	return nil
}

func (n *IndexNode[T]) GetRightChildByIndex(idx uint32) PageIndex {
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

func (n *IndexNode[T]) RemoveLastCell() IndexCell[T] {
	idx := n.Header.Keys - 1
	n.Header.RightChild = n.Cells[idx].Child
	aCellToRemove := n.Cells[idx]
	n.Cells[idx] = IndexCell[T]{}
	n.Header.Keys -= 1
	return aCellToRemove
}

func (n *IndexNode[T]) PrependCell(aCell IndexCell[T]) {
	if len(n.Cells) <= int(n.Header.Keys) {
		n.Cells = append(n.Cells, IndexCell[T]{})
	}
	for i := int(n.Header.Keys) - 1; i >= 0; i-- {
		n.Cells[i+1] = n.Cells[i]
	}
	n.Cells[0] = aCell
	n.Header.Keys += 1
}

func (n *IndexNode[T]) AppendCells(cells ...IndexCell[T]) {
	for _, aCell := range cells {
		if len(n.Cells) <= int(n.Header.Keys) {
			n.Cells = append(n.Cells, IndexCell[T]{})
		}
		n.Cells[n.Header.Keys] = aCell
		n.Header.Keys += 1
	}
}

func (n *IndexNode[T]) setParent(parentIdx PageIndex) {
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
			Header: aNode.Header,
			Cells:  make([]IndexCell[int8], 0, MinimumIndexCells),
		}
		for _, aCell := range aNode.Cells {
			aCopy.Cells = append(aCopy.Cells, aCell)
		}
		return aCopy
	case *IndexNode[int32]:
		aCopy := &IndexNode[int32]{
			Header: aNode.Header,
			Cells:  make([]IndexCell[int32], 0, MinimumIndexCells),
		}
		for _, aCell := range aNode.Cells {
			aCopy.Cells = append(aCopy.Cells, aCell)
		}
		return aCopy
	case *IndexNode[int64]:
		aCopy := &IndexNode[int64]{
			Header: aNode.Header,
			Cells:  make([]IndexCell[int64], 0, MinimumIndexCells),
		}
		for _, aCell := range aNode.Cells {
			aCopy.Cells = append(aCopy.Cells, aCell)
		}
		return aCopy
	case *IndexNode[float32]:
		aCopy := &IndexNode[float32]{
			Header: aNode.Header,
			Cells:  make([]IndexCell[float32], 0, MinimumIndexCells),
		}
		for _, aCell := range aNode.Cells {
			aCopy.Cells = append(aCopy.Cells, aCell)
		}
		return aCopy
	case *IndexNode[float64]:
		aCopy := &IndexNode[float64]{
			Header: aNode.Header,
			Cells:  make([]IndexCell[float64], 0, MinimumIndexCells),
		}
		for _, aCell := range aNode.Cells {
			aCopy.Cells = append(aCopy.Cells, aCell)
		}
		return aCopy
	case *IndexNode[string]:
		aCopy := &IndexNode[string]{
			Header: aNode.Header,
			Cells:  make([]IndexCell[string], 0, MinimumIndexCells),
		}
		for _, aCell := range aNode.Cells {
			aCopy.Cells = append(aCopy.Cells, aCell)
		}
		return aCopy
	default:
		return nil
	}
}

func (n *IndexNode[T]) MaxSpace() uint64 {
	maxSpace := PageSize - indexHeaderSize()
	return maxSpace
}

func (n *IndexNode[T]) TakenSpace() uint64 {
	takenPageSize := uint64(0)
	for i := uint32(0); i < n.Header.Keys; i++ {
		takenPageSize += n.Cells[i].Size()
	}
	return takenPageSize
}

func (n *IndexNode[T]) AvailableSpace() uint64 {
	return n.MaxSpace() - n.TakenSpace()
}

func (n *IndexNode[T]) HasSpaceForKey(key T) bool {
	// In case of a unique index we need space for key + rowID + child pointer
	// In case of a non-unique index there if key doesn't exist yet, it will be
	// key + child pointer + length prefix + offset ID + at least one rowID
	// there for there will be extra 8 bytes needed. We assume the worst case here.
	return (keySize(key) + 4 + rowIDsLengthPrefixSize + 4 + 8) <= n.AvailableSpace()
}

func (n *IndexNode[T]) AtLeastHalfFull() bool {
	return n.AvailableSpace() < (n.MaxSpace())/2
}

func (n *IndexNode[T]) SplitInHalves(unique bool) (uint32, uint32) {
	if !unique {
		halfSpace := int(n.TakenSpace() / 2)
		for i := uint32(0); i < n.Header.Keys; i++ {
			halfSpace -= int(n.Cells[i].Size())
			if halfSpace < 0 {
				leftCount := i + 1
				rightCount := n.Header.Keys - leftCount
				return leftCount, rightCount
			}
		}
	}
	rightCount := (n.Header.Keys+1)/2 - 1
	leftCount := n.Header.Keys - rightCount
	return leftCount, rightCount
}
