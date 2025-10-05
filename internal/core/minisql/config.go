package minisql

// TODO - implement page recycling using a free list
// See PAGE_RECYCLING.md for design notes
type DatabaseHeader struct {
	FirstFreePage uint32 // Points to first free page, 0 if none
	FreePageCount uint32 // Number of free pages available
}

// Free page structure - reuse the existing page structure
type FreePage struct {
	NextFreePage uint32 // Points to next free page, 0 if last
	// Rest of page is unused
}

func (h *DatabaseHeader) Size() uint64 {
	return 100
}

func (h *DatabaseHeader) Marshal() ([]byte, error) {
	buf := make([]byte, h.Size())
	marshalUint32(buf, h.FirstFreePage, 0)
	marshalUint32(buf, h.FreePageCount, 4)
	return buf, nil
}

func UnmarshalDatabaseHeader(buf []byte, dbHeader *DatabaseHeader) error {
	dbHeader.FirstFreePage = unmarshalUint32(buf, 0)
	dbHeader.FreePageCount = unmarshalUint32(buf, 4)
	return nil
}
