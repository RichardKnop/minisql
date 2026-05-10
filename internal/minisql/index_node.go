package minisql

import (
	"bytes"
	"fmt"
	"unsafe"
)

// IndexNodeHeader is the on-disk header for an index B+ tree node. It records
// whether the node is the root and whether it is a leaf, its parent page,
// the number of keys stored, and the page index of the rightmost child.
type IndexNodeHeader struct {
	IsRoot     bool
	IsLeaf     bool
	Parent     PageIndex
	Keys       uint32
	RightChild PageIndex
}

// Size returns the fixed serialised byte size of the index node header.
func (h *IndexNodeHeader) Size() (s uint64) {
	return indexHeaderSize()
}

func indexHeaderSize() uint64 {
	return 1 + 1 + 1 + 4 + 4 + 4
}

// Marshal serialises the header into buf, writing the PageTypeIndex byte first.
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

// Unmarshal deserialises the header from buf, validates the page type byte,
// and returns the number of bytes consumed.
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

// NewIndexCell allocates a new IndexCell for the given uniqueness mode.
// Non-unique cells get a pre-allocated RowIDs slice; unique cells omit it.
func NewIndexCell[T IndexKey](unique bool) IndexCell[T] {
	c := IndexCell[T]{unique: unique}
	if !unique {
		// Pre-allocate RowIDs slice only for non-unique cells.
		// Unique cells use inlineRowID instead (no heap allocation needed).
		c.RowIDs = make([]RowID, 0, 1)
	}
	return c
}

// Size returns the serialised byte size of the cell: key + child pointer +
// either a single UniqueRowID (unique) or an inline row-ID list with overflow pointer.
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

// Marshal serialises the cell into buf: key bytes, then row ID data (unique
// inline or non-unique list + overflow pointer), then child page index.
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
	case UUIDValue:
		copy(buf[i:i+16], v[:])
		i += 16
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

// Unmarshal deserialises a cell from buf using the column schema for composite
// key decoding, and returns the number of bytes consumed.
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
	case UUIDValue:
		var uv UUIDValue
		copy(uv[:], buf[i:i+16])
		c.Key = any(uv).(T)
		i += 16
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

// RemoveRowID removes the first occurrence of id from the cell's inline RowIDs
// slice and decrements InlineRowIDs. Returns the removed index, or -1 if not found.
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

// ReplaceRowID replaces the first occurrence of id in the cell's inline RowIDs
// slice with newID. Returns the replaced index, or -1 if id was not found.
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
	Cells     []IndexCell[T]
	Header    IndexNodeHeader
	freeBytes uint64 // cached free space in bytes; maintained on every insert/delete
}

// indexCellsPrealloc is the initial Cells slice capacity for a new IndexNode.
// Sized to avoid most slice reallocations during sequential-insert workloads
// without bloating memory for small or short-lived nodes.
const indexCellsPrealloc = 32

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
		takenSpace := uint64(0)
		for i := range cells {
			node.Cells[i] = cells[i].Clone()
			node.Cells[i].unique = unique
			if uint32(i) < node.Header.Keys {
				takenSpace += node.Cells[i].Size()
			}
		}
		node.freeBytes = node.MaxSpace() - takenSpace
		return &node
	}

	// Pre-fill 4 cells so tests and Unmarshal can access Cells[0..3] directly.
	// The capacity is set larger to avoid reallocations on the first insert
	// (old cap==len==4 caused an immediate realloc on every first insert).
	node.Cells = make([]IndexCell[T], 4, indexCellsPrealloc)
	for i := range 4 {
		node.Cells[i] = NewIndexCell[T](unique)
	}
	node.freeBytes = node.MaxSpace()
	return &node
}

// NewRootIndexNode creates a new index node with IsRoot and IsLeaf set to true.
func NewRootIndexNode[T IndexKey](unique bool, cells ...IndexCell[T]) *IndexNode[T] {
	node := NewIndexNode[T](unique, cells...)
	node.Header.IsRoot = true
	node.Header.IsLeaf = true
	return node
}

// Size returns the total serialised byte size of the index node (header + keys).
func (n *IndexNode[T]) Size() uint64 {
	size := n.Header.Size()

	for idx := range n.Header.Keys {
		size += n.Cells[idx].Size()
	}

	return size
}

// Marshal serialises the node into buf: header first, then each cell up to Keys.
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

// Unmarshal deserialises the node from buf and also recomputes the cached
// freeBytes value. Returns the total number of bytes consumed.
func (n *IndexNode[T]) Unmarshal(columns []Column, buf []byte) (uint64, error) {
	i := uint64(0)

	hi, err := n.Header.Unmarshal(buf[i:])
	if err != nil {
		return 0, err
	}
	i += hi

	takenSpace := uint64(0)
	for idx := 0; idx < int(n.Header.Keys); idx++ {
		if len(n.Cells) == idx {
			n.Cells = append(n.Cells, NewIndexCell[T](n.Cells[0].unique))
		}
		ci, err := n.Cells[idx].Unmarshal(columns, buf[i:])
		if err != nil {
			return 0, err
		}
		i += ci
		takenSpace += ci
	}
	n.freeBytes = n.MaxSpace() - takenSpace

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

// SetChild updates the child pointer at position idx. idx == Keys sets the
// right-child pointer in the header; any value in [0, Keys) updates the cell.
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

// Keys returns a slice of all keys stored in the node, in order.
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

// Children returns all child page indices of the node (left children of each
// cell plus the right-child from the header). Returns nil for leaf nodes.
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

	removedSize := n.Cells[idx].Size()

	if idx == n.Header.Keys-1 {
		n.Header.RightChild = n.Cells[idx].Child
	} else {
		n.Cells[idx+1].Child = n.Cells[idx].Child
		for i := int(idx); i < int(n.Header.Keys-1); i++ {
			n.Cells[i] = n.Cells[i+1]
		}
	}

	n.Cells[int(n.Header.Keys)-1] = NewIndexCell[T](n.Cells[int(n.Header.Keys)-1].unique)
	n.freeBytes += removedSize
	n.Header.Keys -= 1

	return nil
}

// GetRightChildByIndex returns the right child of the cell at idx: the
// right-child header field for the last key, or the next cell's child otherwise.
func (n *IndexNode[T]) GetRightChildByIndex(idx uint32) PageIndex {
	if idx == n.Header.Keys-1 {
		return n.Header.RightChild
	}

	return n.Cells[idx+1].Child
}

// FirstCell returns the leftmost cell in the node.
func (n *IndexNode[T]) FirstCell() IndexCell[T] {
	return n.Cells[0]
}

// LastCell returns the rightmost cell currently stored in the node.
func (n *IndexNode[T]) LastCell() IndexCell[T] {
	return n.Cells[n.Header.Keys-1]
}

// RemoveFirstCell shifts all cells left by one, clears the last slot, updates
// the cached free space, and decrements the key count.
func (n *IndexNode[T]) RemoveFirstCell() {
	removedSize := n.Cells[0].Size()
	for i := 0; i < int(n.Header.Keys)-1; i++ {
		n.Cells[i] = n.Cells[i+1]
	}
	n.Cells[n.Header.Keys-1] = NewIndexCell[T](n.Cells[n.Header.Keys-1].unique)
	n.freeBytes += removedSize
	n.Header.Keys -= 1
}

// RemoveLastCell demotes the current right-child pointer into the removed cell's
// child slot, updates cached free space, decrements the key count, and returns
// the removed cell.
func (n *IndexNode[T]) RemoveLastCell() IndexCell[T] {
	idx := n.Header.Keys - 1
	n.Header.RightChild = n.Cells[idx].Child
	cellToRemove := n.Cells[idx]
	n.freeBytes += cellToRemove.Size()
	n.Cells[idx] = NewIndexCell[T](cellToRemove.unique)
	n.Header.Keys -= 1
	return cellToRemove
}

// PrependCell shifts all existing cells right by one, inserts cell at position 0,
// updates cached free space, and increments the key count.
func (n *IndexNode[T]) PrependCell(cell IndexCell[T]) {
	if len(n.Cells) <= int(n.Header.Keys) {
		n.Cells = append(n.Cells, NewIndexCell[T](n.Cells[0].unique))
	}
	for i := int(n.Header.Keys) - 1; i >= 0; i-- {
		n.Cells[i+1] = n.Cells[i]
	}
	n.Cells[0] = cell
	n.freeBytes -= cell.Size()
	n.Header.Keys += 1
}

// AppendCells appends one or more cells to the end of the node, growing the
// backing slice in one allocation if needed, and updates cached free space.
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
		n.freeBytes -= cell.Size()
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
	case *IndexNode[UUIDValue]:
		return node.Marshal(buf)
	default:
		return fmt.Errorf("unknown index node type: %T", node)
	}
}

// Clone returns a deep copy of the index node with independent cell slices.
func (n *IndexNode[T]) Clone() *IndexNode[T] {
	nodeCopy := &IndexNode[T]{
		Header:    n.Header,
		freeBytes: n.freeBytes,
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

// Clone returns a deep copy of the cell with its own RowIDs slice.
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
	case *IndexNode[UUIDValue]:
		return node.Clone()
	default:
		return nil
	}
}

// MaxSpace returns the total bytes available for cells in a page (page size minus header).
func (n *IndexNode[T]) MaxSpace() uint64 {
	maxSpace := PageSize - indexHeaderSize()
	return maxSpace
}

// TakenSpace returns the total number of bytes currently occupied by all cells
// (computed by iterating; prefer AvailableSpace for hot paths).
func (n *IndexNode[T]) TakenSpace() uint64 {
	takenPageSize := uint64(0)
	for i := uint32(0); i < n.Header.Keys; i++ {
		takenPageSize += n.Cells[i].Size()
	}
	return takenPageSize
}

// AvailableSpace returns the number of free bytes in this node (O(1) via cached freeBytes).
func (n *IndexNode[T]) AvailableSpace() uint64 {
	return n.freeBytes
}

// HasSpaceForKey reports whether the node has enough free space to accommodate
// a new entry for key, using worst-case sizing (key + child + row ID overhead).
func (n *IndexNode[T]) HasSpaceForKey(key T) bool {
	// In case of a unique index we need space for key + rowID + child pointer
	// In case of a non-unique index there if key doesn't exist yet, it will be
	// key + child pointer + length prefix + offset ID + at least one rowID
	// there for there will be extra 8 bytes needed. We assume the worst case here.
	return (keySize(key) + 4 + rowIDsLengthPrefixSize + 4 + 8) <= n.AvailableSpace()
}

// AtLeastHalfFull reports whether the node is at least half full by space,
// the minimum occupancy required to avoid a merge after a deletion.
func (n *IndexNode[T]) AtLeastHalfFull() bool {
	return n.AvailableSpace() < (n.MaxSpace())/2
}

// SplitInHalves computes the left and right cell counts for a node split.
// For non-unique nodes it splits by space so both halves are roughly equal.
// For unique nodes it splits by key count (ceil/floor).
func (n *IndexNode[T]) SplitInHalves(unique bool) (uint32, uint32) {
	if !unique {
		halfSpace := int((n.MaxSpace() - n.freeBytes) / 2)
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
