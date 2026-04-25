package minisql

import (
	"bytes"
	"fmt"
	"unsafe"
)

// IndexNodeHeader ...
type IndexNodeHeader struct {
	IsRoot     bool
	IsLeaf     bool
	Parent     PageIndex
	Keys       uint32
	RightChild PageIndex
}

// Size ...
func (h *IndexNodeHeader) Size() (s uint64) {
	return indexHeaderSize()
}

func indexHeaderSize() uint64 {
	return 1 + 1 + 1 + 4 + 4 + 4
}

// Marshal ...
func (h *IndexNodeHeader) Marshal(buf []byte) {
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
}

// Unmarshal ...
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

// IndexCell holds a single key entry within an index node, along with its associated row IDs and child pointer.
type IndexCell[T IndexKey] struct {
	Key          T
	RowIDs       []RowID
	UniqueRowID  RowID
	InlineRowIDs uint32
	Overflow     PageIndex
	Child        PageIndex
	unique       bool
}

// NewIndexCell ...
func NewIndexCell[T IndexKey](unique bool) IndexCell[T] {
	c := IndexCell[T]{unique: unique}
	if !unique {
		// Pre-allocate RowIDs slice only for non-unique cells.
		// Unique cells use inlineRowID instead (no heap allocation needed).
		c.RowIDs = make([]RowID, 0, 1)
	}
	return c
}

// Size ...
func (c *IndexCell[T]) Size() uint64 {
	// Key size + child pointer size
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

// keySize returns the serialised byte size of a single index key.
// For fixed-size numeric types, unsafe.Sizeof is a compile-time constant that
// the Go compiler folds away — no type switch needed at runtime.
// String and CompositeKey require runtime inspection for their variable sizes.
func keySize[T IndexKey](key T) uint64 {
	switch v := any(key).(type) {
	case string:
		return varcharLengthPrefixSize + uint64(len(v))
	case CompositeKey:
		return v.Size()
	default:
		// int8, int32, int64, float32, float64 — all fixed-size; Sizeof is constant.
		return uint64(unsafe.Sizeof(key))
	}
}

// Marshal ...
func (c *IndexCell[T]) Marshal(buf []byte) error {
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
	case CompositeKey:
		size := v.Size()
		if err := v.Marshal(buf, i); err != nil {
			return err
		}
		i += size
	}

	if c.unique {
		// Single row ID stored inline — no slice allocation.
		marshalUint64(buf, uint64(c.UniqueRowID), i)
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

	return nil
}

// Unmarshal ...
func (c *IndexCell[T]) Unmarshal(columns []Column, buf []byte) (uint64, error) {
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
	case CompositeKey:
		compositeKey := NewCompositeKey(columns)
		ci, err := compositeKey.Unmarshal(buf, i)
		if err != nil {
			return 0, err
		}
		i += ci
	default:
		return 0, fmt.Errorf("unsupported column type: %T", v)
	}

	if c.unique {
		// Store directly in UniqueRowID — no heap allocation.
		c.UniqueRowID = RowID(unmarshalUint64(buf, i))
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

// RemoveRowID ...
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

// ReplaceRowID ...
func (c *IndexCell[T]) ReplaceRowID(id, newID RowID) int {
	for i := range c.RowIDs {
		if c.RowIDs[i] == id {
			c.RowIDs[i] = newID
			return i
		}
	}
	return -1
}

// IndexNode is a B+ tree node used by the index, containing a header and a slice of index cells.
type IndexNode[T IndexKey] struct {
	Cells  []IndexCell[T]
	Header IndexNodeHeader
}

// MinimumIndexCells is the minimum number of cells required in an index node.
// TODO - this is not used currently
const MinimumIndexCells = 4

// NewIndexNode creates a new IndexNode with the given uniqueness flag and optional initial cells.
func NewIndexNode[T IndexKey](unique bool, cells ...IndexCell[T]) *IndexNode[T] {
	node := IndexNode[T]{
		Header: IndexNodeHeader{
			RightChild: RightChildNotSet,
		},
	}

	if len(cells) > 0 {
		node.Header.Keys = uint32(len(cells)) - 1
		node.Cells = make([]IndexCell[T], len(cells))
		for i := range cells {
			node.Cells[i] = cells[i].Clone()
			node.Cells[i].unique = unique
		}
		return &node
	}

	node.Cells = make([]IndexCell[T], 0, MinimumIndexCells)
	for range MinimumIndexCells {
		node.Cells = append(node.Cells, NewIndexCell[T](unique))
	}
	return &node
}

// NewRootIndexNode ...
func NewRootIndexNode[T IndexKey](unique bool, cells ...IndexCell[T]) *IndexNode[T] {
	node := NewIndexNode[T](unique, cells...)
	node.Header.IsRoot = true
	node.Header.IsLeaf = true
	return node
}

// Size ...
func (n *IndexNode[T]) Size() uint64 {
	size := n.Header.Size()

	for idx := range n.Header.Keys {
		size += n.Cells[idx].Size()
	}

	return size
}

// Marshal ...
func (n *IndexNode[T]) Marshal(buf []byte) error {
	i := uint64(0)

	n.Header.Marshal(buf[i:])
	i += n.Header.Size()

	for idx := 0; idx < int(n.Header.Keys); idx++ {
		if err := n.Cells[idx].Marshal(buf[i:]); err != nil {
			return err
		}
		i += n.Cells[idx].Size()
	}

	return nil
}

// Unmarshal ...
func (n *IndexNode[T]) Unmarshal(columns []Column, buf []byte) (uint64, error) {
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
		ci, err := n.Cells[idx].Unmarshal(columns, buf[i:])
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

// SetChild ...
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

// Keys ...
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
		if n.Cells[i].unique {
			rowIDs = append(rowIDs, n.Cells[i].UniqueRowID)
		} else {
			rowIDs = append(rowIDs, n.Cells[i].RowIDs...)
		}
	}
	return rowIDs
}

// Children ...
func (n *IndexNode[T]) Children() []PageIndex {
	if n.Header.IsLeaf {
		return nil
	}
	children := make([]PageIndex, 0, n.Header.Keys+1)
	for i := range n.Header.Keys {
		children = append(children, n.Cells[i].Child)
	}
	if n.Header.RightChild > 0 && n.Header.RightChild != RightChildNotSet {
		children = append(children, n.Header.RightChild)
	}
	return children
}

// DeleteKeyAndRightChild removes the key at idx plus the right child pointer from the index node.
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

	n.Cells[int(n.Header.Keys)-1] = NewIndexCell[T](n.Cells[int(n.Header.Keys)-1].unique)
	n.Header.Keys -= 1

	return nil
}

// GetRightChildByIndex ...
func (n *IndexNode[T]) GetRightChildByIndex(idx uint32) PageIndex {
	if idx == n.Header.Keys-1 {
		return n.Header.RightChild
	}

	return n.Cells[idx+1].Child
}

// FirstCell ...
func (n *IndexNode[T]) FirstCell() IndexCell[T] {
	return n.Cells[0]
}

// LastCell ...
func (n *IndexNode[T]) LastCell() IndexCell[T] {
	return n.Cells[n.Header.Keys-1]
}

// RemoveFirstCell ...
func (n *IndexNode[T]) RemoveFirstCell() {
	for i := 0; i < int(n.Header.Keys)-1; i++ {
		n.Cells[i] = n.Cells[i+1]
	}
	n.Cells[n.Header.Keys-1] = NewIndexCell[T](n.Cells[n.Header.Keys-1].unique)
	n.Header.Keys -= 1
}

// RemoveLastCell ...
func (n *IndexNode[T]) RemoveLastCell() IndexCell[T] {
	idx := n.Header.Keys - 1
	n.Header.RightChild = n.Cells[idx].Child
	cellToRemove := n.Cells[idx]
	n.Cells[idx] = NewIndexCell[T](cellToRemove.unique)
	n.Header.Keys -= 1
	return cellToRemove
}

// PrependCell ...
func (n *IndexNode[T]) PrependCell(cell IndexCell[T]) {
	if len(n.Cells) <= int(n.Header.Keys) {
		n.Cells = append(n.Cells, NewIndexCell[T](n.Cells[0].unique))
	}
	for i := int(n.Header.Keys) - 1; i >= 0; i-- {
		n.Cells[i+1] = n.Cells[i]
	}
	n.Cells[0] = cell
	n.Header.Keys += 1
}

// AppendCells ...
func (n *IndexNode[T]) AppendCells(cells ...IndexCell[T]) {
	needed := int(n.Header.Keys) + len(cells)
	if needed > len(n.Cells) {
		// Grow the slice in one shot instead of one cell at a time.
		n.Cells = append(n.Cells, make([]IndexCell[T], needed-len(n.Cells))...)
		if len(n.Cells) > 0 {
			unique := n.Cells[0].unique
			for i := int(n.Header.Keys); i < len(n.Cells); i++ {
				n.Cells[i].unique = unique
			}
		}
	}
	for _, cell := range cells {
		n.Cells[n.Header.Keys] = cell
		n.Header.Keys += 1
	}
}

func (n *IndexNode[T]) setParent(parentIdx PageIndex) {
	n.Header.Parent = parentIdx
}

func marshalIndexNode(anyNode any, buf []byte) error {
	switch node := anyNode.(type) {
	case *IndexNode[int8]:
		return node.Marshal(buf)
	case *IndexNode[int32]:
		return node.Marshal(buf)
	case *IndexNode[int64]:
		return node.Marshal(buf)
	case *IndexNode[float32]:
		return node.Marshal(buf)
	case *IndexNode[float64]:
		return node.Marshal(buf)
	case *IndexNode[string]:
		return node.Marshal(buf)
	case *IndexNode[CompositeKey]:
		return node.Marshal(buf)
	default:
		return fmt.Errorf("unknown index node type: %T", node)
	}
}

// Clone ...
func (n *IndexNode[T]) Clone() *IndexNode[T] {
	nodeCopy := &IndexNode[T]{
		Header: n.Header,
	}
	if n.Header.Keys == 0 {
		return nodeCopy
	}
	// Allocate a few extra slots so that splitChild (which appends one median key
	// to the parent) and AppendCells (which appends up to rightCount cells to the
	// new right child) don't immediately trigger a slice reallocation.
	nodeCopy.Cells = make([]IndexCell[T], n.Header.Keys, n.Header.Keys+4)
	for i := range n.Header.Keys {
		nodeCopy.Cells[i] = n.Cells[i].Clone()
	}
	return nodeCopy
}

// Clone ...
func (c *IndexCell[T]) Clone() IndexCell[T] {
	nodeCopy := IndexCell[T]{
		Key:          c.Key,
		InlineRowIDs: c.InlineRowIDs,
		Overflow:     c.Overflow,
		Child:        c.Child,
		unique:       c.unique,
		UniqueRowID:  c.UniqueRowID, // zero for non-unique; populated for unique
	}
	// For unique cells, RowIDs is nil — UniqueRowID already copied above.
	// For non-unique cells, deep-copy the RowIDs slice.
	if len(c.RowIDs) > 0 {
		nodeCopy.RowIDs = make([]RowID, len(c.RowIDs))
		copy(nodeCopy.RowIDs, c.RowIDs)
	}
	return nodeCopy
}

func copyIndexNode(anyNode any) any {
	switch node := anyNode.(type) {
	case *IndexNode[int8]:
		return node.Clone()
	case *IndexNode[int32]:
		return node.Clone()
	case *IndexNode[int64]:
		return node.Clone()
	case *IndexNode[float32]:
		return node.Clone()
	case *IndexNode[float64]:
		return node.Clone()
	case *IndexNode[string]:
		return node.Clone()
	case *IndexNode[CompositeKey]:
		return node.Clone()
	default:
		return nil
	}
}

// MaxSpace ...
func (n *IndexNode[T]) MaxSpace() uint64 {
	maxSpace := PageSize - indexHeaderSize()
	return maxSpace
}

// TakenSpace ...
func (n *IndexNode[T]) TakenSpace() uint64 {
	takenPageSize := uint64(0)
	for i := uint32(0); i < n.Header.Keys; i++ {
		takenPageSize += n.Cells[i].Size()
	}
	return takenPageSize
}

// AvailableSpace ...
func (n *IndexNode[T]) AvailableSpace() uint64 {
	return n.MaxSpace() - n.TakenSpace()
}

// HasSpaceForKey ...
func (n *IndexNode[T]) HasSpaceForKey(key T) bool {
	// In case of a unique index we need space for key + rowID + child pointer
	// In case of a non-unique index there if key doesn't exist yet, it will be
	// key + child pointer + length prefix + offset ID + at least one rowID
	// there for there will be extra 8 bytes needed. We assume the worst case here.
	return (keySize(key) + 4 + rowIDsLengthPrefixSize + 4 + 8) <= n.AvailableSpace()
}

// AtLeastHalfFull ...
func (n *IndexNode[T]) AtLeastHalfFull() bool {
	return n.AvailableSpace() < (n.MaxSpace())/2
}

// SplitInHalves ...
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
