package minisql

import (
	"github.com/RichardKnop/minisql/pkg/bitwise"
)

// LeafNodeHeader is the on-disk header for a leaf B+ tree node. It extends
// the base Header with the number of cells currently stored and the page index
// of the next leaf (used for linked-list traversal in range scans).
type LeafNodeHeader struct {
	Header
	Cells    uint32
	NextLeaf PageIndex
}

// Size returns the serialised byte size of LeafNodeHeader (base Header + 8 bytes).
func (h *LeafNodeHeader) Size() uint64 {
	return h.Header.Size() + 8
}

// Marshal serialises the header into buf in little-endian byte order.
func (h *LeafNodeHeader) Marshal(buf []byte) {
	i := uint64(0)

	h.Header.Marshal(buf[i:])
	i += h.Header.Size()

	marshalUint32(buf, h.Cells, i)
	i += 4
	marshalUint32(buf, uint32(h.NextLeaf), i)
	i += 4
}

// Unmarshal deserialises the header from buf and returns the number of bytes consumed.
func (h *LeafNodeHeader) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	hi, err := h.Header.Unmarshal(buf[i:])
	if err != nil {
		return 0, err
	}
	i += hi

	h.Cells = unmarshalUint32(buf, i)
	i += 4
	h.NextLeaf = PageIndex(unmarshalUint32(buf, i))

	return h.Size(), nil
}

// Cell is a single row stored in a leaf B+ tree node. Value is a raw byte slice
// containing all non-NULL column values packed sequentially; NullBitmask tracks
// which column positions are NULL. isOwned controls copy-on-write behaviour.
type Cell struct {
	Value       []byte
	NullBitmask uint64
	Key         RowID
	isOwned     bool
}

// Size returns the serialised byte size of the cell: 8-byte NullBitmask, 8-byte key,
// plus the length of the packed value slice.
func (c *Cell) Size() uint64 {
	// 8 bytes for null bitmask, 8 bytes for key
	return 8 + 8 + uint64(len(c.Value))
}

// Marshal serialises the cell into buf: NullBitmask, key, then raw value bytes.
func (c *Cell) Marshal(buf []byte) {
	i := uint64(0)

	marshalUint64(buf, c.NullBitmask, i)
	i += 8

	marshalUint64(buf, uint64(c.Key), i)
	i += 8

	n := copy(buf[i:], c.Value)
	i += uint64(n)
}

// Unmarshal deserialises a cell from buf using the column schema to determine
// each field's size. Value is copied into owned memory (isOwned=true) so that
// the cell does not alias the source buffer, which may be returned to a pool.
func (c *Cell) Unmarshal(columns []Column, buf []byte) (uint64, error) {
	offset := uint64(0)

	c.NullBitmask = unmarshalUint64(buf, offset)
	offset += 8

	c.Key = RowID(unmarshalUint64(buf, offset))
	offset += 8

	// Pass 1: Calculate total size needed for all column values
	totalSize := uint64(0)
	scanOffset := offset
	for i, col := range columns {
		if bitwise.IsSet(c.NullBitmask, i) {
			continue
		}
		if col.Kind.IsText() {
			size := unmarshalInt32(buf, scanOffset)
			totalSize += 4 + uint64(size)
			scanOffset += 4 + uint64(size)
		} else {
			totalSize += uint64(col.Size)
			scanOffset += uint64(col.Size)
		}
	}

	// Pass 2: Copy value bytes into owned memory. A zero-copy sub-slice would alias
	// the source buffer (WAL frame buffers are returned to pageDataPool on the next
	// commit and reused as pageBuf, corrupting any c.Value still pointing into them).
	if totalSize > 0 {
		c.Value = make([]byte, totalSize)
		copy(c.Value, buf[offset:offset+totalSize])
		c.isOwned = true
		offset += totalSize
	}

	return offset, nil
}

// LeafNode is a leaf page in the B+ tree. It stores an ordered sequence of
// Cells (one per row) and a NextLeaf pointer that links sibling leaves for
// efficient range scans.
type LeafNode struct {
	Cells  []Cell
	Header LeafNodeHeader
}

// Clone cretes a shallow copy of the leaf node, sharing value slices
// until they are about to be modified at which point PrepareModifyCell
// should be called to clone the value slice for that cell.
func (n *LeafNode) Clone() *LeafNode {
	nodeCopy := &LeafNode{
		Header: n.Header,
	}

	if len(n.Cells) == 0 {
		return nodeCopy
	}

	// Shallow copy - share Value slices
	nodeCopy.Cells = make([]Cell, len(n.Cells))
	for i := range n.Cells {
		nodeCopy.Cells[i] = Cell{
			NullBitmask: n.Cells[i].NullBitmask,
			Key:         n.Cells[i].Key,
			Value:       n.Cells[i].Value, // Share the slice!
			isOwned:     false,            // Mark as shared
		}
	}
	return nodeCopy
}

// DeepClone returns a fully independent copy of the leaf node with its own
// value slice for every cell (no shared memory with the original).
func (n *LeafNode) DeepClone() *LeafNode {
	nodeCopy := &LeafNode{
		Header: n.Header,
	}

	if len(n.Cells) == 0 {
		return nodeCopy
	}

	nodeCopy.Cells = make([]Cell, len(n.Cells))
	for i := range n.Cells {
		nodeCopy.Cells[i] = Cell{
			NullBitmask: n.Cells[i].NullBitmask,
			Key:         n.Cells[i].Key,
			Value:       make([]byte, len(n.Cells[i].Value)),
			isOwned:     true, // Mark as owned
		}
		copy(nodeCopy.Cells[i].Value, n.Cells[i].Value)
	}
	return nodeCopy
}

// PrepareModifyCell ensures the cell at idx is copy-on-write safe before modification.
func (n *LeafNode) PrepareModifyCell(idx uint32) {
	if n.Cells[idx].isOwned {
		return
	}
	// Clone the Value slice on first write
	oldValue := n.Cells[idx].Value
	n.Cells[idx].Value = make([]byte, len(oldValue))
	copy(n.Cells[idx].Value, oldValue)
	n.Cells[idx].isOwned = true
}

// NewLeafNode allocates a new leaf node, optionally pre-populated with the
// given cells. The header cell count is initialised to match the provided cells.
func NewLeafNode(cells ...Cell) *LeafNode {
	node := LeafNode{
		Cells: make([]Cell, 0, len(cells)),
	}
	if len(cells) > 0 {
		node.Header.Cells = uint32(len(cells))
		node.Cells = append(node.Cells, cells...)
	}
	return &node
}

// Size returns the total serialised byte size of the leaf node (header + all cells).
func (n *LeafNode) Size() uint64 {
	size := uint64(0)
	size += n.Header.Size()

	for idx := range n.Cells {
		size += n.Cells[idx].Size()
	}

	return size
}

// Marshal serialises the leaf node into buf: header first, then each cell up to Header.Cells.
func (n *LeafNode) Marshal(buf []byte) error {
	i := uint64(0)

	n.Header.Marshal(buf[i:])
	i += n.Header.Size()

	for idx := range n.Cells[0:n.Header.Cells] {
		n.Cells[idx].Marshal(buf[i:])
		i += uint64(n.Cells[idx].Size())
	}

	return nil
}

// Unmarshal deserialises the leaf node from buf and returns the total bytes consumed.
func (n *LeafNode) Unmarshal(columns []Column, buf []byte) (uint64, error) {
	i := uint64(0)

	hi, err := n.Header.Unmarshal(buf[i:])
	if err != nil {
		return 0, err
	}
	i += hi

	if cap(n.Cells) < int(n.Header.Cells) {
		n.Cells = make([]Cell, n.Header.Cells)
	} else {
		n.Cells = n.Cells[:n.Header.Cells] // Reuse capacity
	}

	for idx := 0; idx < int(n.Header.Cells); idx++ {
		ci, err := n.Cells[idx].Unmarshal(columns, buf[i:])
		if err != nil {
			return 0, err
		}
		i += ci
	}

	return i, nil
}

// Delete removes the cell with the given key from the node, shifting subsequent
// cells left to fill the gap. Returns the deleted cell and true, or the zero
// Cell and false if no matching key was found.
func (n *LeafNode) Delete(key RowID) (Cell, bool) {
	if n.Header.Cells == 0 {
		return Cell{}, false
	}

	cellIdx := -1
	for i := 0; i < int(n.Header.Cells); i++ {
		if n.Cells[i].Key == key {
			cellIdx = i
			break
		}
	}

	if cellIdx < 0 {
		return Cell{}, false
	}

	n.PrepareModifyCell(uint32(cellIdx))
	cellToDelete := n.Cells[cellIdx]

	for i := uint32(cellIdx); i < n.Header.Cells-1; i++ {
		n.PrepareModifyCell(i + 1)
		n.Cells[i] = n.Cells[i+1]
	}
	n.Cells[int(n.Header.Cells)-1] = Cell{}

	n.Header.Cells -= 1

	return cellToDelete, true
}

// FirstCell returns the first (lowest-key) cell in the leaf node.
func (n *LeafNode) FirstCell() Cell {
	return n.Cells[0]
}

// LastCell returns the last (highest-key) cell currently stored in the leaf node.
func (n *LeafNode) LastCell() Cell {
	return n.Cells[n.Header.Cells-1]
}

// RemoveLastCell zeroes the last cell slot and decrements the cell count.
func (n *LeafNode) RemoveLastCell() {
	n.Cells[n.Header.Cells-1] = Cell{}
	n.Header.Cells -= 1
}

// RemoveFirstCell shifts all cells left by one, zeroes the last slot, and
// decrements the cell count.
func (n *LeafNode) RemoveFirstCell() {
	for i := uint32(0); i < n.Header.Cells-1; i++ {
		n.PrepareModifyCell(i + 1)
		n.Cells[i] = n.Cells[i+1]
	}
	n.Cells[n.Header.Cells-1] = Cell{}
	n.Header.Cells -= 1
}

// PrependCell shifts all existing cells right by one and inserts cell at
// position 0, incrementing the cell count.
func (n *LeafNode) PrependCell(cell Cell) {
	for i := n.Header.Cells; i > 0; i-- {
		n.PrepareModifyCell(i - 1)
		n.Cells[i] = n.Cells[i-1]
	}
	n.Cells[0] = cell
	n.Header.Cells += 1
}

// AppendCells appends one or more cells to the end of the node, growing the
// backing slice if necessary, and increments the cell count for each.
func (n *LeafNode) AppendCells(cells ...Cell) {
	for _, cell := range cells {
		if int(n.Header.Cells) < len(n.Cells) {
			n.Cells[n.Header.Cells] = cell
		} else {
			n.Cells = append(n.Cells, cell)
		}
		n.Header.Cells += 1
	}
}

// Keys returns a slice of all row IDs stored in the leaf node, in order.
func (n *LeafNode) Keys() []RowID {
	keys := make([]RowID, 0, n.Header.Cells)
	for idx := range n.Header.Cells {
		keys = append(keys, n.Cells[idx].Key)
	}
	return keys
}

// MaxSpace returns the maximum number of bytes available for cell data in the
// page, accounting for the larger root-page header when IsRoot is set.
func (n *LeafNode) MaxSpace() uint64 {
	maxSpace := PageSize - headerSize()
	if n.Header.IsRoot {
		maxSpace = PageSize - rootHeaderSize()
	}
	return maxSpace
}

// TakenSpace returns the total number of bytes currently occupied by all cells.
func (n *LeafNode) TakenSpace() uint64 {
	takenPageSize := uint64(0)
	for i := uint32(0); i < n.Header.Cells; i++ {
		takenPageSize += n.Cells[i].Size()
	}
	return takenPageSize
}

// AvailableSpace returns the number of bytes still free for new cells.
func (n *LeafNode) AvailableSpace() uint64 {
	return n.MaxSpace() - n.TakenSpace()
}

// HasSpaceForRow reports whether the node has enough free space to store the
// given row (row size + 8-byte key + 8-byte null bitmask).
func (n *LeafNode) HasSpaceForRow(row Row) bool {
	return row.Size()+8+8 <= n.AvailableSpace()
}

// AtLeastHalfFull reports whether the node is at least half full by space,
// the minimum occupancy required to avoid a merge after a deletion.
func (n *LeafNode) AtLeastHalfFull() bool {
	return n.AvailableSpace() < n.MaxSpace()/2
}

// CanMergeWith reports whether all cells from n2 fit into the remaining space
// in n, meaning the two nodes can be merged into one.
func (n *LeafNode) CanMergeWith(n2 *LeafNode) bool {
	return n2.TakenSpace() <= n.AvailableSpace()
}

// CanBorrowFirst reports whether this node can donate its first cell to an
// under-full sibling while remaining at least half full itself.
func (n *LeafNode) CanBorrowFirst() bool {
	firstCellSize := n.Cells[0].Size()
	return n.AvailableSpace()+firstCellSize < n.MaxSpace()/2
}

// CanBorrowLast reports whether this node can donate its last cell to an
// under-full sibling while remaining at least half full itself.
func (n *LeafNode) CanBorrowLast() bool {
	lastCellSize := n.Cells[n.Header.Cells-1].Size()
	return n.AvailableSpace()+lastCellSize < n.MaxSpace()/2
}
