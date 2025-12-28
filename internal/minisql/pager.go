package minisql

import (
	"context"
	"fmt"
	"io"
	"sync"
)

type DBFile interface {
	io.ReadSeeker
	io.ReaderAt
	io.WriterAt
	io.Closer
}

type PageUnmarshaler func(pageIdx PageIndex, buf []byte) (*Page, error)

type pagerImpl struct {
	pageSize       int
	totalPages     uint32 // total number of pages
	maxCachedPages int    // maximum number of pages to keep in cache

	dbHeader DatabaseHeader
	// pages is a sparse array where index = PageIndex
	// nil entries indicate evicted/unloaded pages
	// Memory overhead: ~8 bytes per total page (e.g., 76MB for 10M pages)
	pages []*Page

	// LRU tracking: most recently used at the end
	lruList []PageIndex

	file     DBFile
	fileSize int64

	mu sync.RWMutex
}

// New opens the database file and tries to read the root page
// maxCachedPages: maximum number of pages to keep in cache (0 = unlimited)
func NewPager(file DBFile, pageSize int, maxCachedPages int) (*pagerImpl, error) {
	if maxCachedPages <= 0 {
		maxCachedPages = 1000 // default limit
	}
	aPager := &pagerImpl{
		pageSize:       pageSize,
		maxCachedPages: maxCachedPages,
		file:           file,
		pages:          make([]*Page, 0, maxCachedPages),
		lruList:        make([]PageIndex, 0, maxCachedPages),
	}

	fileSize, err := aPager.file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	aPager.fileSize = fileSize

	// Basic check to verify file size is a multiple of page size (4096B)
	if fileSize%int64(pageSize) != 0 {
		return nil, fmt.Errorf("db file size is not divisible by page size: %d", fileSize)
	}

	totalPages := fileSize / int64(pageSize)
	aPager.totalPages = uint32(totalPages)

	// If file is not empty, read the DB header from the first page
	// DB header is always located at the start of the first page
	// Rest of the first page is used as a normal page
	if aPager.totalPages > 0 {
		buf := make([]byte, RootPageConfigSize)
		_, err := aPager.file.ReadAt(buf, 0)
		if err != nil {
			return nil, err
		}

		if err := UnmarshalDatabaseHeader(buf, &aPager.dbHeader); err != nil {
			return nil, err
		}
	}

	return aPager, nil
}

func (p *pagerImpl) Close() error {
	return p.file.Close()
}

func (p *pagerImpl) TotalPages() uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.totalPages
}

func (p *pagerImpl) GetPage(ctx context.Context, pageIdx PageIndex, unmarshaler PageUnmarshaler) (*Page, error) {
	// Check if page already exists in cache
	p.mu.RLock()
	if len(p.pages) > int(pageIdx) && p.pages[pageIdx] != nil {
		page := p.pages[pageIdx]
		p.mu.RUnlock()
		// Update LRU tracking
		p.trackPageAccess(pageIdx)
		return page, nil
	}
	totalPages := p.totalPages
	p.mu.RUnlock()

	if int(pageIdx) > int(totalPages) {
		return nil, fmt.Errorf("cannot skip index when getting page, index: %d, number of pages: %d", pageIdx, totalPages)
	}

	// Acquire write lock before any modifications
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check page doesn't exist (in case another goroutine created it)
	if len(p.pages) > int(pageIdx) && p.pages[pageIdx] != nil {
		p.updateLRU(pageIdx)
		return p.pages[pageIdx], nil
	}

	// Evict pages if cache is full BEFORE loading new page
	p.evictIfNeeded()

	buf := make([]byte, p.pageSize)

	if int(pageIdx) != int(p.totalPages) {
		// If we are not requesting a new page, read the page from file
		offset := int64(pageIdx) * int64(p.pageSize)
		_, err := p.file.ReadAt(buf, offset)
		if err != nil {
			return nil, err
		}

		if len(p.pages) < int(pageIdx)+1 {
			// Extend sparse array with nil entries to accommodate pageIdx
			// Maintains invariant: slice index = page index
			for i := len(p.pages); i < int(pageIdx)+1; i++ {
				p.pages = append(p.pages, nil)
			}
		}
	}

	_, err := unmarshaler(pageIdx, buf)
	if err != nil {
		return nil, err
	}

	if pageIdx == 0 {
		if p.pages[pageIdx].LeafNode != nil {
			p.pages[pageIdx].LeafNode.Header.IsRoot = true
		}
		if p.pages[pageIdx].InternalNode != nil {
			p.pages[pageIdx].InternalNode.Header.IsRoot = true
			p.pages[pageIdx].InternalNode.Header.IsInternal = true
		}
	}

	// Track this page access
	p.updateLRU(pageIdx)

	return p.pages[pageIdx], nil
}

func (p *pagerImpl) GetHeader(ctx context.Context) DatabaseHeader {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.dbHeader
}

func (p *pagerImpl) SaveHeader(ctx context.Context, header DatabaseHeader) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.dbHeader = header
}

func (p *pagerImpl) SavePage(ctx context.Context, pageIdx PageIndex, page *Page) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Extend slice if needed
	if len(p.pages) < int(pageIdx)+1 {
		for i := len(p.pages); i < int(pageIdx)+1; i++ {
			p.pages = append(p.pages, nil)
		}
	}

	p.pages[pageIdx] = page
}

func (p *pagerImpl) Flush(ctx context.Context, pageIdx PageIndex) error {
	p.mu.RLock()
	if int(pageIdx) >= len(p.pages) || p.pages[pageIdx] == nil {
		p.mu.RUnlock()
		return nil
	}

	aPage := p.pages[pageIdx]
	dbHeader := p.dbHeader
	p.mu.RUnlock()

	buf := make([]byte, p.pageSize)
	_, err := marshalPage(aPage, buf)
	if err != nil {
		return fmt.Errorf("error flushing page %d: %w", aPage.Index, err)
	}

	if pageIdx != 0 {
		_, err = p.file.WriteAt(buf, int64(pageIdx)*int64(p.pageSize))
		return err
	}

	headerBytes, err := dbHeader.Marshal()
	if err != nil {
		return err
	}

	_, err = p.file.WriteAt(headerBytes[0:RootPageConfigSize], 0)
	if err != nil {
		return err
	}
	_, err = p.file.WriteAt(buf[0:p.pageSize-RootPageConfigSize], int64(RootPageConfigSize))
	return err
}

// FlushBatch writes multiple pages to disk in a single operation.
// This reduces the number of syscalls and allows the OS to optimize I/O.
// Pages are marshaled in parallel outside of locks, then written sequentially.
func (p *pagerImpl) FlushBatch(ctx context.Context, pageIndices []PageIndex) error {
	if len(pageIndices) == 0 {
		return nil
	}

	// Single page optimization - use regular Flush
	if len(pageIndices) == 1 {
		return p.Flush(ctx, pageIndices[0])
	}

	// Phase 1: Collect pages and marshal them (can be done in parallel)
	type marshaledPage struct {
		pageIdx PageIndex
		buf     []byte
		isRoot  bool
		header  *DatabaseHeader
	}

	marshaled := make([]marshaledPage, 0, len(pageIndices))

	// Read lock once to get all pages and header
	p.mu.RLock()
	dbHeader := p.dbHeader
	pagesToMarshal := make([]*Page, 0, len(pageIndices))
	indices := make([]PageIndex, 0, len(pageIndices))

	for _, pageIdx := range pageIndices {
		if int(pageIdx) >= len(p.pages) || p.pages[pageIdx] == nil {
			continue
		}
		pagesToMarshal = append(pagesToMarshal, p.pages[pageIdx])
		indices = append(indices, pageIdx)
	}
	p.mu.RUnlock()

	// Marshal pages outside of lock
	for i, aPage := range pagesToMarshal {
		pageIdx := indices[i]
		buf := make([]byte, p.pageSize)

		_, err := marshalPage(aPage, buf)
		if err != nil {
			return fmt.Errorf("error marshaling page %d: %w", pageIdx, err)
		}

		mp := marshaledPage{
			pageIdx: pageIdx,
			buf:     buf,
			isRoot:  pageIdx == 0,
		}

		if mp.isRoot {
			mp.header = &dbHeader
		}

		marshaled = append(marshaled, mp)
	}

	// Phase 2: Write all pages to disk sequentially
	for _, mp := range marshaled {
		if !mp.isRoot {
			// Regular page write
			_, err := p.file.WriteAt(mp.buf, int64(mp.pageIdx)*int64(p.pageSize))
			if err != nil {
				return fmt.Errorf("error writing page %d: %w", mp.pageIdx, err)
			}
		} else {
			// Root page with header
			headerBytes, err := mp.header.Marshal()
			if err != nil {
				return fmt.Errorf("error marshaling header: %w", err)
			}

			_, err = p.file.WriteAt(headerBytes[0:RootPageConfigSize], 0)
			if err != nil {
				return fmt.Errorf("error writing header: %w", err)
			}

			_, err = p.file.WriteAt(mp.buf[0:p.pageSize-RootPageConfigSize], int64(RootPageConfigSize))
			if err != nil {
				return fmt.Errorf("error writing root page data: %w", err)
			}
		}
	}

	return nil
}

func marshalPage(aPage *Page, buf []byte) ([]byte, error) {
	if aPage.OverflowPage != nil {
		data, err := aPage.OverflowPage.Marshal(buf)
		if err != nil {
			return nil, fmt.Errorf("error marshaling overflow node: %w", err)
		}
		return data, nil
	} else if aPage.FreePage != nil {
		data, err := aPage.FreePage.Marshal(buf)
		if err != nil {
			return nil, fmt.Errorf("error marshaling freepage node: %w", err)
		}
		return data, nil
	} else if aPage.LeafNode != nil {
		data, err := aPage.LeafNode.Marshal(buf)
		if err != nil {
			return nil, fmt.Errorf("error marshaling leaf node: %w", err)
		}
		return data, nil
	} else if aPage.InternalNode != nil {
		data, err := aPage.InternalNode.Marshal(buf)
		if err != nil {
			return nil, fmt.Errorf("error marshaling internal node: %w", err)
		}
		return data, nil
	} else if aPage.IndexNode != nil {
		data, err := marshalIndexNode(aPage.IndexNode, buf)
		if err != nil {
			return nil, fmt.Errorf("error marshaling index node: %w", err)
		}
		return data, nil
	} else if aPage.IndexOverflowNode != nil {
		data, err := aPage.IndexOverflowNode.Marshal(buf)
		if err != nil {
			return nil, fmt.Errorf("error marshaling index overflow node: %w", err)
		}
		return data, nil
	}
	return nil, fmt.Errorf("no known node type found")
}

// updateLRU updates the LRU list for the given page (must be called with lock held)
func (p *pagerImpl) updateLRU(pageIdx PageIndex) {
	// Remove pageIdx from current position in LRU list
	for i, idx := range p.lruList {
		if idx == pageIdx {
			p.lruList = append(p.lruList[:i], p.lruList[i+1:]...)
			break
		}
	}
	// Add to end (most recently used)
	p.lruList = append(p.lruList, pageIdx)
}

// trackPageAccess updates LRU tracking (thread-safe version for fast path)
func (p *pagerImpl) trackPageAccess(pageIdx PageIndex) {
	p.mu.Lock()
	p.updateLRU(pageIdx)
	p.mu.Unlock()
}

// evictIfNeeded evicts pages if cache is full (must be called with lock held)
func (p *pagerImpl) evictIfNeeded() {
	// Count actual cached pages (non-nil entries)
	cachedCount := 0
	for _, page := range p.pages {
		if page != nil {
			cachedCount += 1
		}
	}

	if cachedCount < p.maxCachedPages {
		return
	}

	// Find candidate pages to evict from least recently used
	for _, pageIdx := range p.lruList {
		// Never evict page 0 (root/header page)
		if pageIdx == 0 {
			continue
		}

		// Only evict if page exists in cache
		if int(pageIdx) < len(p.pages) && p.pages[pageIdx] != nil {
			// Evict this page by setting to nil (preserves sparse array structure)
			// This maintains the invariant: p.pages[i] is always page i (or nil)
			p.pages[pageIdx] = nil
			cachedCount -= 1

			// Remove from LRU list
			for i, idx := range p.lruList {
				if idx == pageIdx {
					p.lruList = append(p.lruList[:i], p.lruList[i+1:]...)
					break
				}
			}

			if cachedCount < p.maxCachedPages {
				return
			}
		}
	}
}
