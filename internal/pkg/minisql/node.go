package minisql

import (
	"fmt"
)

// FindChildByKey returns the index of the child which should contain the given key.
// For example, if node has 2 keys, this could return 0 for the leftmost child,
// 1 for the middle child or 2 for the rightmost child.
// The returned value is not a node index!
func (n *InternalNode) FindChildByKey(key uint32) uint32 {
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
