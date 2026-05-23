package minisql

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"
)

// TransactionalPager wraps a base Pager and routes reads and writes through the current transaction.
type TransactionalPager struct {
	Pager
	txManager    *TransactionManager
	table, index string
}

// NewTransactionalPager creates a TransactionalPager for the given table and index names.
func NewTransactionalPager(basePager Pager, txManager *TransactionManager, table, index string) *TransactionalPager {
	return &TransactionalPager{
		Pager:     basePager,
		txManager: txManager,
		table:     table,
		index:     index,
	}
}

// ReadPage returns the page at pageIdx, returning the in-progress write copy if it exists.
//
// For read-only (snapshot) transactions the method enforces snapshot isolation:
//   - If the page was last committed at or before tx.SnapshotSeq, the shared
//     page cache is safe to use — no concurrent write has touched it since the
//     snapshot started.
//   - Otherwise the method retrieves the historical version from the
//     TransactionManager's page-version history (saved at commit time by the
//     writer that superseded the page).
//   - If no historical version is found (the page was newly allocated after
//     the snapshot), the cached version is returned as a safe fallback; this
//     should not occur during normal B+ tree traversal because pre-snapshot
//     pointer chains never reference post-snapshot pages.
func (tp *TransactionalPager) ReadPage(ctx context.Context, pageIdx PageIndex) (*Page, error) {
	tx := TxFromContext(ctx)
	if tx == nil {
		// No transaction context, use base pager directly
		return tp.GetPage(ctx, pageIdx)
	}

	// Check if we have a modified version in our write set (always empty for read-only txns)
	if modifiedPage, exists := tx.GetModifiedPage(pageIdx); exists {
		return modifiedPage, nil
	}

	page, err := tp.GetPage(ctx, pageIdx)
	if err != nil {
		return nil, err
	}

	// Write transactions: return the cached page directly.
	// Single-writer enforcement (activeWriters) guarantees no concurrent writer
	// can have modified this page since we started, so no conflict check needed.
	if !tx.ReadOnly {
		return page, nil
	}

	// Read-only snapshot path: check whether the cached version is safe for
	// this snapshot.
	lastCommitted := tp.txManager.PageLastCommittedSeq(pageIdx)
	if lastCommitted <= tx.SnapshotSeq {
		// The cache was last updated at or before our snapshot — safe to use.
		return page, nil
	}

	// The cache is newer than our snapshot.  Retrieve the historical version.
	if historical, ok := tp.txManager.PageVersionAtSnapshot(pageIdx, tx.SnapshotSeq); ok {
		return historical, nil
	}

	// No historical version found.  This can only happen for pages that were
	// newly allocated (by a split, overflow, or free-list add) after our snapshot
	// started.  Under correct snapshot pointer-following the caller should never
	// reach a post-snapshot page, but if it does we return the cache version as a
	// best-effort fallback and log a warning so it can be investigated.
	tp.txManager.logger.Warn("snapshot gap: no historical version for page, using cache",
		zap.Uint64("page_idx", uint64(pageIdx)),
		zap.Uint64("snapshot_seq", tx.SnapshotSeq),
		zap.Uint64("last_committed_seq", lastCommitted),
	)
	return page, nil
}

// ModifyPage returns a writable copy of the page at pageIdx, creating one if it doesn't exist in the write set.
func (tp *TransactionalPager) ModifyPage(ctx context.Context, pageIdx PageIndex) (*Page, error) {
	tx := TxFromContext(ctx)
	if tx == nil {
		return nil, errors.New("cannot modify page outside transaction")
	}

	// Check if we already have a copy in write set
	modifiedPage, exists := tx.GetModifiedPage(pageIdx)
	if exists {
		return modifiedPage, nil
	}

	// Get current page and create a copy for modification
	originalPage, err := tp.GetPage(ctx, pageIdx)
	if err != nil {
		return nil, err
	}

	// Create a deep copy for modification.  Keep a reference to originalPage so
	// the transaction manager can store it in the version history at commit time
	// for any concurrent snapshot readers.
	modifiedPage = originalPage.Clone()
	tx.TrackWrite(pageIdx, modifiedPage, originalPage, tp.table, tp.index)

	return modifiedPage, nil
}

// GetFreePage returns a free page from the free list, or allocates a new one.
func (tp *TransactionalPager) GetFreePage(ctx context.Context) (*Page, error) {
	tx := TxFromContext(ctx)
	if tx == nil {
		return nil, errors.New("cannot get free page outside transaction")
	}

	dbHeader := tp.readDBHeader(ctx)

	// Check if there are any free pages
	if dbHeader.FirstFreePage == 0 {
		// No free pages, allocate new one
		freePage, err := tp.ModifyPage(ctx, PageIndex(tp.TotalPages()))
		if err != nil {
			return nil, fmt.Errorf("allocate new free page: %w", err)
		}
		// Clear the page for reuse
		freePage.Clear()

		return freePage, nil
	}

	// Get the first free page
	freePage, err := tp.ModifyPage(ctx, dbHeader.FirstFreePage)
	if err != nil {
		return nil, fmt.Errorf("get free page: %w", err)
	}

	// Update header to point to next free page
	dbHeader.FirstFreePage = freePage.FreePage.NextFreePage
	dbHeader.FreePageCount -= 1
	tx.TrackDBHeaderWrite(dbHeader)

	// Clear the page for reuse
	freePage.Clear()

	return freePage, nil
}

// Clear resets all node pointers on the page, preparing it for reuse.
func (p *Page) Clear() {
	p.OverflowPage = nil
	p.FreePage = nil
	p.LeafNode = nil
	p.InternalNode = nil
	p.IndexNode = nil
	p.IndexOverflowNode = nil
	p.InvertedEntryPage = nil
	p.InvertedPostPage = nil
}

// AddFreePage marks pageIdx as a free page and prepends it to the free list.
func (tp *TransactionalPager) AddFreePage(ctx context.Context, pageIdx PageIndex) error {
	tx := TxFromContext(ctx)
	if tx == nil {
		return errors.New("cannot add free page outside transaction")
	}

	if pageIdx == 0 {
		return errors.New("cannot free page 0 (header page)")
	}

	// Get the page to mark as free
	freePage, err := tp.ModifyPage(ctx, pageIdx)
	if err != nil {
		return fmt.Errorf("add free page: %w", err)
	}

	dbHeader := tp.readDBHeader(ctx)

	// Initialize as free page
	freePage.FreePage = &FreePage{
		NextFreePage: dbHeader.FirstFreePage,
	}

	// Clear other node types
	freePage.LeafNode = nil
	freePage.InternalNode = nil
	freePage.IndexNode = nil
	freePage.OverflowPage = nil
	freePage.IndexOverflowNode = nil
	freePage.InvertedEntryPage = nil
	freePage.InvertedPostPage = nil

	// Update header
	dbHeader.FirstFreePage = pageIdx
	dbHeader.FreePageCount += 1
	tx.TrackDBHeaderWrite(dbHeader)

	return nil
}

func (tp *TransactionalPager) readDBHeader(ctx context.Context) DatabaseHeader {
	tx := TxFromContext(ctx)
	if header, exists := tx.GetModifiedDBHeader(); exists {
		return *header
	}
	return tp.GetHeader(ctx)
}

// GetOverflowPage returns a writable copy of the overflow page at pageIdx.
func (tp *TransactionalPager) GetOverflowPage(ctx context.Context, pageIdx PageIndex) (*Page, error) {
	tx := TxFromContext(ctx)
	if tx == nil {
		return nil, errors.New("cannot get overflow page outside transaction")
	}

	overflowPage, err := tp.ModifyPage(ctx, pageIdx)
	if err != nil {
		return nil, fmt.Errorf("get overflow page: %w", err)
	}

	return overflowPage, nil
}
