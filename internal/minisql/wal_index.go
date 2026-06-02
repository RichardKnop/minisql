package minisql

import (
	"sync"
)

// WALIndex is an in-memory map from page index to the latest raw page bytes
// that have been committed to the WAL but not yet checkpointed to the main DB
// file.  It is the authoritative source for the current page content during the
// window between a WAL commit and the next checkpoint.
//
// All methods are safe for concurrent use.
//
// Lifecycle:
//
//  1. Created empty at database open via NewWALIndex.
//  2. Populated from the WAL file via Rebuild when a non-empty WAL is found.
//  3. Updated after each successful AppendTransaction via Update.
//  4. Reset after a successful Checkpoint + Truncate via Reset.
//
// The pager (Phase 4) calls Lookup before reading the DB file so readers
// always see the latest committed version of every page.
type WALIndex struct {
	pages map[PageIndex][]byte
	mu    sync.RWMutex
}

// NewWALIndex creates an empty WALIndex.
func NewWALIndex() *WALIndex {
	return &WALIndex{
		pages: make(map[PageIndex][]byte),
	}
}

// Update records the latest raw page bytes for pageIdx and returns the
// previous buffer (if any) so the caller can recycle it.
// Update takes ownership of data — the caller must not read or write the slice
// after this call.  If a prior entry exists for pageIdx it is overwritten
// (later write wins).
func (wi *WALIndex) Update(pageIdx PageIndex, data []byte) []byte {
	wi.mu.Lock()
	old := wi.pages[pageIdx]
	wi.pages[pageIdx] = data
	wi.mu.Unlock()
	return old
}

// Lookup returns the raw page bytes for pageIdx if the page has a committed
// WAL entry that has not yet been checkpointed.  The returned slice is a direct
// reference into the index — callers must treat it as read-only and must not
// hold it across a Reset call (which replaces the map) if mutations are
// possible.  In practice the pager unmarshals the bytes immediately and does
// not retain the slice, making this safe.
func (wi *WALIndex) Lookup(pageIdx PageIndex) ([]byte, bool) {
	wi.mu.RLock()
	data, ok := wi.pages[pageIdx]
	wi.mu.RUnlock()
	return data, ok
}

// Has reports whether pageIdx has a committed WAL entry in the index.
func (wi *WALIndex) Has(pageIdx PageIndex) bool {
	wi.mu.RLock()
	_, ok := wi.pages[pageIdx]
	wi.mu.RUnlock()
	return ok
}

// Size returns the number of unique page indices currently in the index.
func (wi *WALIndex) Size() int {
	wi.mu.RLock()
	n := len(wi.pages)
	wi.mu.RUnlock()
	return n
}

// MaxPageIndex returns the largest page index stored in the WAL index, or 0
// if the index is empty.  Used by the pager to initialise totalPages when the
// DB file is empty (WAL-only mode) so that new-page allocation never clobbers
// existing WAL pages.
func (wi *WALIndex) MaxPageIndex() PageIndex {
	wi.mu.RLock()
	defer wi.mu.RUnlock()
	var maxIdx PageIndex
	for idx := range wi.pages {
		if idx > maxIdx {
			maxIdx = idx
		}
	}
	return maxIdx
}

// Reset discards all entries and recycles their page buffers.  Called after a
// successful checkpoint + WAL truncation to reflect that the WAL no longer
// contains any data that is not already in the main DB file.
func (wi *WALIndex) Reset() {
	wi.mu.Lock()
	pages := wi.pages
	wi.pages = make(map[PageIndex][]byte)
	wi.mu.Unlock()

	for _, data := range pages {
		pageDataPool.Put(data)
	}
}

// Rebuild replaces the index contents with the latest committed frame for each
// page found in frames.  frames is expected to be the result of
// WAL.ReadAllFrames: all entries are already validated and belong to committed
// transactions.  When the same page appears multiple times, the last occurrence
// wins (matching WAL semantics — later frames are more recent).
//
// Rebuild takes ownership of each f.Data slice — callers must not use the
// slices after this call returns.
func (wi *WALIndex) Rebuild(frames []WALReadFrame) {
	pages := make(map[PageIndex][]byte, len(frames))
	for _, f := range frames {
		pages[f.PageIndex] = f.Data // take ownership
	}

	wi.mu.Lock()
	wi.pages = pages
	wi.mu.Unlock()
}
