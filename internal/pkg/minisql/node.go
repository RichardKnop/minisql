package minisql

import (
	"fmt"
)

// IndexOfChild returns the index of the child which should contain the given key.
// For example, if node has 2 keys, this could return 0 for the leftmost child,
// 1 for the middle child or 2 for the rightmost child.
// The returned value is not a node index!
func (n *InternalNode) IndexOfChild(key uint64) uint32 {
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

// IndexOfKey returns index of cell with key and a boolean flag
// indicating whether key was found in the node or not.
func (n *InternalNode) IndexOfKey(key uint64) (uint32, bool) {
	for idx, aCell := range n.ICells {
		if aCell.Key == key {
			return uint32(idx), true
		}
	}

	return 0, false
}

// IndexOfPage returns index of child which contains page number
func (n *InternalNode) IndexOfPage(pageIdx uint32) (uint32, error) {
	for idx, aCell := range n.ICells {
		if aCell.Child == pageIdx {
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
func (n *InternalNode) Child(childIdx uint32) (uint32, error) {
	keysNum := n.Header.KeysNum
	if childIdx > keysNum {
		return 0, fmt.Errorf("childIdx %d out of keysNum %d", childIdx, keysNum)
	}

	if childIdx == keysNum {
		return n.Header.RightChild, nil
	}

	return n.ICells[childIdx].Child, nil
}

func (n *InternalNode) SetChildIdx(idx, newIdx uint32) error {
	keysNum := n.Header.KeysNum
	if idx > keysNum {
		return fmt.Errorf("childIdx %d out of keysNum %d", idx, keysNum)
	}

	if idx == keysNum {
		n.Header.RightChild = newIdx
		return nil
	}

	n.ICells[idx].Child = newIdx
	return nil
}

func (n *InternalNode) AtLeastHalfFull(maxIcells int) bool {
	return int(n.Header.KeysNum) >= (maxIcells+1)/2
}

func (n *InternalNode) MoreThanHalfFull(maxIcells int) bool {
	return int(n.Header.KeysNum) > (maxIcells+1)/2
}

func (n *InternalNode) GetRightChildByIndex(idx uint32) uint32 {
	if idx == n.Header.KeysNum-1 {
		return n.Header.RightChild
	}

	return n.ICells[idx+1].Child
}

func (n *InternalNode) DeleteKeyByIndex(idx uint32) {
	if n.Header.KeysNum == 0 {
		return
	}

	if idx == n.Header.KeysNum {
		idx -= 1
	}

	if idx == n.Header.KeysNum-1 {
		n.Header.RightChild = n.ICells[idx].Child
	} else {
		n.ICells[idx+1].Child = n.ICells[idx].Child
		for i := int(idx); i < int(n.Header.KeysNum); i++ {
			n.ICells[i] = n.ICells[i+1]
		}
	}

	n.ICells[int(n.Header.KeysNum)-1] = ICell{}
	n.Header.KeysNum -= 1
}

func (n *InternalNode) FirstCell() ICell {
	return n.ICells[0]
}

func (n *InternalNode) LastCell() ICell {
	return n.ICells[n.Header.KeysNum-1]
}

func (n *InternalNode) RemoveFirstCell() {
	for i := 0; i < int(n.Header.KeysNum)-1; i++ {
		n.ICells[i] = n.ICells[i+1]
	}
	n.ICells[n.Header.KeysNum-1] = ICell{}
	n.Header.KeysNum -= 1
}

func (n *InternalNode) RemoveLastCell() {
	idx := n.Header.KeysNum - 1
	n.Header.RightChild = n.ICells[idx].Child
	n.ICells[idx] = ICell{}
	n.Header.KeysNum -= 1
}

func (n *InternalNode) PrependCell(aCell ICell) {
	for i := int(n.Header.KeysNum) - 1; i > 0; i-- {
		n.ICells[i] = n.ICells[i-1]
	}
	n.ICells[0] = aCell
	n.Header.KeysNum += 1
}

func (n *InternalNode) AppendCells(cells ...ICell) {
	for _, aCell := range cells {
		n.ICells[n.Header.KeysNum] = aCell
		n.Header.KeysNum += 1
	}
}

func (n *InternalNode) Keys() []uint64 {
	keys := make([]uint64, 0, n.Header.KeysNum)
	for idx := range n.Header.KeysNum {
		keys = append(keys, n.ICells[idx].Key)
	}
	return keys
}

func (n *InternalNode) Children() []uint32 {
	children := make([]uint32, 0, n.Header.KeysNum)
	for idx := range n.Header.KeysNum {
		children = append(children, n.ICells[idx].Child)
	}
	if n.Header.RightChild != RIGHT_CHILD_NOT_SET {
		children = append(children, n.Header.RightChild)
	}
	return children
}
