package minisql

import (
	"fmt"
)

// IndexOfChild returns the index of the child which should contain the given key.
// For example, if node has 2 keys, this could return 0 for the leftmost child,
// 1 for the middle child or 2 for the rightmost child.
// The returned value is not a node index!
func (n *InternalNode) IndexOfChild(key RowID) uint32 {
	// Binary search
	var (
		minIdx = uint32(0)
		maxIdx = n.Header.KeysNum
	)
	for minIdx != maxIdx {
		idx := (minIdx + maxIdx) / 2
		rightKey := n.ICells[idx].Key
		if rightKey >= key {
			maxIdx = idx
		} else {
			minIdx = idx + 1
		}
	}

	return minIdx
}

// IndexOfPage returns index of child which contains page number
func (n *InternalNode) IndexOfPage(pageIdx PageIndex) (uint32, error) {
	for idx, cell := range n.ICells {
		if cell.Child == pageIdx {
			return uint32(idx), nil
		}
	}
	if n.Header.RightChild == pageIdx {
		return n.Header.KeysNum, nil
	}
	return 0, fmt.Errorf("pageIdx %d not found", pageIdx)
}

// Child returns a node index of nth child of the node marked by its index
// (0 for the leftmost child, index equal to number of keys means the rightmost child).
func (n *InternalNode) Child(childIdx uint32) (PageIndex, error) {
	keysNum := n.Header.KeysNum
	if childIdx > keysNum {
		return 0, fmt.Errorf("childIdx %d out of keysNum %d", childIdx, keysNum)
	}

	if childIdx == keysNum {
		return n.Header.RightChild, nil
	}

	return n.ICells[childIdx].Child, nil
}

// SetChild updates the child pointer at position idx. idx == KeysNum sets the
// right-child pointer; any value in [0, KeysNum) updates the corresponding ICell.
func (n *InternalNode) SetChild(idx uint32, childPage PageIndex) error {
	keysNum := n.Header.KeysNum
	if idx > keysNum {
		return fmt.Errorf("childIdx %d out of keysNum %d", idx, keysNum)
	}

	if idx == keysNum {
		n.Header.RightChild = childPage
		return nil
	}

	n.ICells[idx].Child = childPage
	return nil
}

// AtLeastHalfFull reports whether the node holds at least ceil(maxIcells/2) keys,
// the minimum occupancy required to avoid a merge after a deletion.
func (n *InternalNode) AtLeastHalfFull(maxIcells int) bool {
	return int(n.Header.KeysNum) >= (maxIcells+1)/2
}

// MoreThanHalfFull reports whether the node holds strictly more than ceil(maxIcells/2)
// keys, meaning it can donate a key to an under-full sibling during rebalancing.
func (n *InternalNode) MoreThanHalfFull(maxIcells int) bool {
	return int(n.Header.KeysNum) > (maxIcells+1)/2
}

// GetRightChildByIndex returns the right child of the ICell at idx: the
// right-child header field for the last key, or the next ICell's child otherwise.
func (n *InternalNode) GetRightChildByIndex(idx uint32) PageIndex {
	if idx == n.Header.KeysNum-1 {
		return n.Header.RightChild
	}

	return n.ICells[idx+1].Child
}

// DeleteKeyAndRightChild removes the key at idx plus the right child pointer from the internal node.
func (n *InternalNode) DeleteKeyAndRightChild(idx uint32) error {
	if n.Header.KeysNum == 0 {
		return nil
	}

	if idx >= n.Header.KeysNum {
		return fmt.Errorf("index %d out of range for keys %d", idx, n.Header.KeysNum)
	}

	if idx == n.Header.KeysNum-1 {
		n.Header.RightChild = n.ICells[idx].Child
	} else {
		n.ICells[idx+1].Child = n.ICells[idx].Child
		for i := int(idx); i < int(n.Header.KeysNum-1); i++ {
			n.ICells[i] = n.ICells[i+1]
		}
	}

	n.ICells[int(n.Header.KeysNum)-1] = ICell{}
	n.Header.KeysNum -= 1

	return nil
}

// FirstCell returns the leftmost ICell in the node.
func (n *InternalNode) FirstCell() ICell {
	return n.ICells[0]
}

// LastCell returns the rightmost ICell currently stored in the node.
func (n *InternalNode) LastCell() ICell {
	return n.ICells[n.Header.KeysNum-1]
}

// RemoveFirstCell shifts all ICells left by one and decrements the key count,
// effectively removing the leftmost key and its child pointer.
func (n *InternalNode) RemoveFirstCell() {
	for i := 0; i < int(n.Header.KeysNum)-1; i++ {
		n.ICells[i] = n.ICells[i+1]
	}
	n.ICells[n.Header.KeysNum-1] = ICell{}
	n.Header.KeysNum -= 1
}

// RemoveLastCell demotes the current right-child pointer into the removed cell's
// child slot and decrements the key count, effectively removing the rightmost key.
func (n *InternalNode) RemoveLastCell() {
	idx := n.Header.KeysNum - 1
	n.Header.RightChild = n.ICells[idx].Child
	n.ICells[idx] = ICell{}
	n.Header.KeysNum -= 1
}

// PrependCell shifts all existing ICells right by one and inserts cell at
// position 0, incrementing the key count.
func (n *InternalNode) PrependCell(cell ICell) {
	for i := int(n.Header.KeysNum); i > 0; i-- {
		n.ICells[i] = n.ICells[i-1]
	}
	n.ICells[0] = cell
	n.Header.KeysNum += 1
}

// AppendCells appends one or more ICells to the end of the node's cell array,
// incrementing the key count for each.
func (n *InternalNode) AppendCells(cells ...ICell) {
	for _, cell := range cells {
		n.ICells[n.Header.KeysNum] = cell
		n.Header.KeysNum += 1
	}
}

// Keys returns a slice of all separator keys stored in the node, in order.
func (n *InternalNode) Keys() []RowID {
	keys := make([]RowID, 0, n.Header.KeysNum)
	for idx := range n.Header.KeysNum {
		keys = append(keys, n.ICells[idx].Key)
	}
	return keys
}

// Children returns all child page indices of the node: the left child of each
// ICell followed by the right-child pointer from the header (when set).
func (n *InternalNode) Children() []PageIndex {
	children := make([]PageIndex, 0, n.Header.KeysNum)
	for idx := range n.Header.KeysNum {
		children = append(children, n.ICells[idx].Child)
	}
	if n.Header.RightChild != RightChildNotSet {
		children = append(children, n.Header.RightChild)
	}
	return children
}
