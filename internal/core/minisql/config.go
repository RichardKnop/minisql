package minisql

type DatabaseHeader struct {
	FirstFreePage uint32 // Points to first free page, 0 if none
	FreePageCount uint32 // Number of free pages available
}

// Free page structure - reuse the existing page structure
type FreePage struct {
	NextFreePage uint32 // Points to next free page, 0 if last
	// Rest of page is unused
}
