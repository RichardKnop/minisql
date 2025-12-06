package minisql

import (
	"context"
	"fmt"
	"sync"
)

type TransactionalPager struct {
	Pager
	txManager *TransactionManager
	mu        sync.RWMutex
}

func NewTransactionalPager(basePager Pager, txManager *TransactionManager) *TransactionalPager {
	return &TransactionalPager{
		Pager:     basePager,
		txManager: txManager,
	}
}

func (tp *TransactionalPager) ReadPage(ctx context.Context, pageIdx PageIndex) (*Page, error) {
	tx := TxFromContext(ctx)
	if tx == nil {
		// No transaction context, use base pager directly
		return tp.GetPage(ctx, pageIdx)
	}

	// Check if we have a modified version in our write set
	if modifiedPage, exists := tx.WriteSet[pageIdx]; exists {
		return modifiedPage, nil
	}

	// Read from base pager and track in read set
	var currentVersion uint64
	tp.mu.RLock()
	if _, ok := tp.txManager.globalPageVersions[pageIdx]; ok {
		currentVersion = tp.txManager.globalPageVersions[pageIdx]
	}
	tp.mu.RUnlock()

	page, err := tp.GetPage(ctx, pageIdx)
	if err != nil {
		return nil, err
	}

	// Track this read in our read set
	tx.ReadSet[pageIdx] = currentVersion

	return page, nil
}

func (tp *TransactionalPager) ModifyPage(ctx context.Context, pageIdx PageIndex) (*Page, error) {
	tx := TxFromContext(ctx)
	if tx == nil {
		return nil, fmt.Errorf("cannot modify page outside transaction")
	}

	// Check if we already have a copy in write set
	if modifiedPage, exists := tx.WriteSet[pageIdx]; exists {
		return modifiedPage, nil
	}

	// Get current page and create a copy for modification
	originalPage, err := tp.GetPage(ctx, pageIdx)
	if err != nil {
		return nil, err
	}

	// Create a deep copy for modification
	modifiedPage := originalPage.Clone()
	tx.WriteSet[pageIdx] = modifiedPage

	return modifiedPage, nil
}

func (tp *TransactionalPager) GetFreePage(ctx context.Context) (*Page, error) {
	tx := TxFromContext(ctx)
	if tx == nil {
		return nil, fmt.Errorf("cannot get free page outside transaction")
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
	dbHeader.FreePageCount--
	tx.DbHeaderWrite = &dbHeader

	// Clear the page for reuse
	freePage.Clear()

	return freePage, nil
}

func (p *Page) Clear() {
	p.OverflowPage = nil
	p.FreePage = nil
	p.LeafNode = nil
	p.InternalNode = nil
	p.IndexNode = nil
}

func (tp *TransactionalPager) AddFreePage(ctx context.Context, pageIdx PageIndex) error {
	tx := TxFromContext(ctx)
	if tx == nil {
		return fmt.Errorf("cannot add free page outside transaction")
	}

	if pageIdx == 0 {
		return fmt.Errorf("cannot free page 0 (header page)")
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

	// Update header
	dbHeader.FirstFreePage = pageIdx
	dbHeader.FreePageCount++
	tx.DbHeaderWrite = &dbHeader

	return nil
}

func (tp *TransactionalPager) readDBHeader(ctx context.Context) DatabaseHeader {
	tx := TxFromContext(ctx)

	// Check if we already have a copy of database header in the transaction
	var dbHeader DatabaseHeader
	if tx.DbHeaderWrite != nil {
		dbHeader = *tx.DbHeaderWrite
	} else {
		// Read header version and track it
		tp.mu.RLock()
		currentVersion := tp.txManager.globalDbHeaderVersion
		tp.mu.RUnlock()

		dbHeader = tp.GetHeader(ctx)

		tx.DbHeaderRead = &currentVersion
	}

	return dbHeader
}

func (tp *TransactionalPager) GetOverflowPage(ctx context.Context, pageIdx PageIndex) (*Page, error) {
	tx := TxFromContext(ctx)
	if tx == nil {
		return nil, fmt.Errorf("cannot get overflow page outside transaction")
	}

	overflowPage, err := tp.ModifyPage(ctx, pageIdx)
	if err != nil {
		return nil, fmt.Errorf("get overflow page: %w", err)
	}

	return overflowPage, nil
}
