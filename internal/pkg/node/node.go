package node

import (
	"fmt"
)

// FindChildByKey returns the index of the child which should contain the given key
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
