package minisql

// DatabaseHeader stores the global database state persisted at the start of the first page.
type DatabaseHeader struct {
	FirstFreePage PageIndex // Points to first free page, 0 if none
	FreePageCount uint32    // Number of free pages available
}

// Size returns the fixed serialised byte size of the database header.
func (h *DatabaseHeader) Size() uint64 {
	return RootPageConfigSize
}

// Marshal serialises the database header to a byte slice.
func (h *DatabaseHeader) Marshal() ([]byte, error) {
	buf := make([]byte, h.Size())
	marshalUint32(buf, uint32(h.FirstFreePage), 0)
	marshalUint32(buf, h.FreePageCount, 4)
	return buf, nil
}

// UnmarshalDatabaseHeader deserialises a database header from the given byte slice.
func UnmarshalDatabaseHeader(buf []byte, dbHeader *DatabaseHeader) error {
	dbHeader.FirstFreePage = PageIndex(unmarshalUint32(buf, 0))
	dbHeader.FreePageCount = unmarshalUint32(buf, 4)
	return nil
}
