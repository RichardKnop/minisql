package minisql

import (
	"fmt"
)

// Free page structure - reuse the existing page structure
type FreePage struct {
	NextFreePage PageIndex // Points to next free page, 0 if last
	// Rest of page is unused
}

func (n *FreePage) Marshal(buf []byte) error {
	i := uint64(0)

	buf[i] = PageTypeFree
	i += 1

	marshalUint32(buf, uint32(n.NextFreePage), i)

	return nil
}

func (n *FreePage) Unmarshal(buf []byte) error {
	i := uint64(0)

	if buf[i] != PageTypeFree {
		return fmt.Errorf("invalid free page type byte %d", buf[i])
	}
	i += 1

	n.NextFreePage = PageIndex(unmarshalUint32(buf, i))
	return nil
}
