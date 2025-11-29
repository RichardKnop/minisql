package minisql

type DatabaseHeader struct {
	FirstFreePage PageIndex // Points to first free page, 0 if none
	FreePageCount uint32    // Number of free pages available
}

func (h *DatabaseHeader) Size() uint64 {
	return 100
}

func (h *DatabaseHeader) Marshal() ([]byte, error) {
	buf := make([]byte, h.Size())
	marshalUint32(buf, uint32(h.FirstFreePage), 0)
	marshalUint32(buf, h.FreePageCount, 4)
	return buf, nil
}

func UnmarshalDatabaseHeader(buf []byte, dbHeader *DatabaseHeader) error {
	dbHeader.FirstFreePage = PageIndex(unmarshalUint32(buf, 0))
	dbHeader.FreePageCount = unmarshalUint32(buf, 4)
	return nil
}
