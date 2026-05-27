package minisql

import (
	"fmt"

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

// Cell is a single row stored in a leaf B+ tree node.
//
// On-disk layout (v2 self-describing format):
//
//	[8B NullBitmask][8B Key][1B ColumnCount][ColumnCount TypeCode bytes][packed non-NULL values]
//
// TypeCodes carries one TypeCode per column as written; together with NullBitmask
// it fully describes the byte layout without needing the current table schema.
// ColumnCount < len(schema) signals a row written before ADD COLUMN (lazy migration).
type Cell struct {
	Value       []byte
	TypeCodes   []byte // one TypeCode per column; len == ColumnCount
	NullBitmask uint64
	Key         RowID
	ColumnCount uint8
	isOwned     bool
}

// Size returns the serialised byte size of the cell.
func (c *Cell) Size() uint64 {
	return 8 + 8 + 1 + uint64(c.ColumnCount) + uint64(len(c.Value))
}

// Marshal serialises the cell into buf.
func (c *Cell) Marshal(buf []byte) {
	i := uint64(0)

	marshalUint64(buf, c.NullBitmask, i)
	i += 8

	marshalUint64(buf, uint64(c.Key), i)
	i += 8

	buf[i] = c.ColumnCount
	i++

	copy(buf[i:], c.TypeCodes)
	i += uint64(c.ColumnCount)

	copy(buf[i:], c.Value)
}

// Unmarshal deserialises a cell from buf. The cell is self-describing: column
// widths are determined from the stored TypeCodes, not from an external schema.
// Value is copied into owned memory so that the cell does not alias the source
// buffer (WAL frame buffers are pooled and reused across commits).
func (c *Cell) Unmarshal(buf []byte) (uint64, error) {
	offset := uint64(0)

	c.NullBitmask = unmarshalUint64(buf, offset)
	offset += 8

	c.Key = RowID(unmarshalUint64(buf, offset))
	offset += 8

	c.ColumnCount = buf[offset]
	offset++

	// Read TypeCodes (nil when ColumnCount == 0 to match uninitialized Cell).
	if c.ColumnCount > 0 {
		c.TypeCodes = make([]byte, c.ColumnCount)
		copy(c.TypeCodes, buf[offset:offset+uint64(c.ColumnCount)])
		offset += uint64(c.ColumnCount)
	}

	// Pass 1: calculate total value bytes using TypeCodes + NullBitmask.
	totalSize := uint64(0)
	scanOffset := offset
	for i := 0; i < int(c.ColumnCount); i++ {
		if bitwise.IsSet(c.NullBitmask, i) {
			continue
		}
		tc := TypeCode(c.TypeCodes[i])
		sz := typeCodeFixedSize(tc)
		if sz >= 0 {
			totalSize += uint64(sz)
			scanOffset += uint64(sz)
		} else {
			// TypeCodeText: 4-byte length prefix + data.
			if scanOffset+4 > uint64(len(buf)) {
				return 0, fmt.Errorf("cell unmarshal: text column %d offset %d exceeds buffer length %d", i, scanOffset, len(buf))
			}
			length := unmarshalInt32(buf, scanOffset)
			totalSize += 4 + uint64(length)
			scanOffset += 4 + uint64(length)
		}
	}

	// Pass 2: copy value bytes into owned memory.
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

	// Shallow copy - share Value and TypeCodes slices
	nodeCopy.Cells = make([]Cell, len(n.Cells))
	for i := range n.Cells {
		nodeCopy.Cells[i] = Cell{
			NullBitmask: n.Cells[i].NullBitmask,
			Key:         n.Cells[i].Key,
			Value:       n.Cells[i].Value,     // Share the slice!
			TypeCodes:   n.Cells[i].TypeCodes, // Share the slice!
			ColumnCount: n.Cells[i].ColumnCount,
			isOwned:     false, // Mark as shared
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
			TypeCodes:   make([]byte, len(n.Cells[i].TypeCodes)),
			ColumnCount: n.Cells[i].ColumnCount,
			isOwned:     true,
		}
		copy(nodeCopy.Cells[i].Value, n.Cells[i].Value)
		copy(nodeCopy.Cells[i].TypeCodes, n.Cells[i].TypeCodes)
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
// Cells are self-describing (TypeCodes stored per cell) so no schema is needed.
func (n *LeafNode) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	hi, err := n.Header.Unmarshal(buf[i:])
	if err != nil {
		return 0, err
	}
	i += hi

	if cap(n.Cells) <= int(n.Header.Cells) {
		// Allocate one extra slot so the next append (insert into this page)
		// does not immediately trigger a reallocation.
		n.Cells = make([]Cell, n.Header.Cells, n.Header.Cells+1)
	} else {
		n.Cells = n.Cells[:n.Header.Cells] // Reuse capacity (already has headroom)
	}

	for idx := 0; idx < int(n.Header.Cells); idx++ {
		ci, err := n.Cells[idx].Unmarshal(buf[i:])
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
// given row.  The cell overhead is: 8B NullBitmask + 8B Key + 1B ColumnCount
// + 1B per column (TypeCodes) + value bytes.
func (n *LeafNode) HasSpaceForRow(row Row) bool {
	return row.Size()+8+8+1+uint64(len(row.Columns)) <= n.AvailableSpace()
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
