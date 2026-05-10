package minisql

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/RichardKnop/minisql/pkg/lrucache"
)

// PageCacheSize is the default maximum number of pages to keep in memory.
const PageCacheSize = 2000

// DBFile ...
type DBFile interface {
	io.ReadSeeker
	io.ReaderAt
	io.WriterAt
	io.Closer
	Sync() error
}

// PageUnmarshaler ...
type PageUnmarshaler func(totalPages uint32, pageIdx PageIndex, buf []byte) (*Page, error)

type pagerImpl struct {
	lruCache       LRUCache[PageIndex]
	file           DBFile
	bufferPool     *sync.Pool
	walIndex       *WALIndex
	pages          []*Page
	pageSize       int
	maxCachedPages int
	fileSize       int64
	mu             sync.RWMutex
	dbHeader       DatabaseHeader
	totalPages     uint32
}

// NewPager opens the database file and initialises the pager.
// maxCachedPages sets the maximum number of pages to keep in cache (0 = use default).
func NewPager(file DBFile, pageSize, maxCachedPages int) (*pagerImpl, error) {
	if maxCachedPages <= 0 {
		maxCachedPages = PageCacheSize
	}
	pager := &pagerImpl{
		pageSize:       pageSize,
		maxCachedPages: maxCachedPages,
		file:           file,
		pages:          make([]*Page, 0, maxCachedPages),
		// lruList:        make([]PageIndex, 0, maxCachedPages),
		lruCache: lrucache.New[PageIndex](maxCachedPages),
		bufferPool: &sync.Pool{
			New: func() any {
				return make([]byte, pageSize)
			},
		},
	}

	fileSize, err := pager.file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	pager.fileSize = fileSize

	// Basic check to verify file size is a multiple of page size (4096B)
	if fileSize%int64(pageSize) != 0 {
		return nil, fmt.Errorf("db file size is not divisible by page size: %d", fileSize)
	}

	totalPages := fileSize / int64(pageSize)
	pager.totalPages = uint32(totalPages)

	// If file is not empty, read the DB header from the first page
	// DB header is always located at the start of the first page
	// Rest of the first page is used as a normal page
	if pager.totalPages > 0 {
		buf := make([]byte, RootPageConfigSize)
		_, err := pager.file.ReadAt(buf, 0)
		if err != nil {
			return nil, err
		}

		if err := UnmarshalDatabaseHeader(buf, &pager.dbHeader); err != nil {
			return nil, err
		}
	}

	return pager, nil
}

// SetWALIndex wires a WAL index into the pager.  Once set, cache misses in
// GetPage check the index before falling back to a DB-file read.
//
// When the DB file is empty (WAL-only mode, totalPages == 0), totalPages is
// initialised to max(WAL page index) + 1 so that new-page allocation via
// TotalPages() never clobbers existing WAL pages.
func (p *pagerImpl) SetWALIndex(walIndex *WALIndex) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.walIndex = walIndex
	if p.totalPages == 0 && walIndex != nil && walIndex.Size() > 0 {
		p.totalPages = uint32(walIndex.MaxPageIndex()) + 1
	}
}

// Close ...
func (p *pagerImpl) Close() error {
	if err := fastSync(p.file); err != nil {
		return fmt.Errorf("failed to sync file before close: %w", err)
	}
	return p.file.Close()
}

// TotalPages ...
func (p *pagerImpl) TotalPages() uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.totalPages
}

// GetPage ...
func (p *pagerImpl) GetPage(ctx context.Context, pageIdx PageIndex, unmarshaler PageUnmarshaler) (*Page, error) {
	// Fast path: Check if page already exists in cache (read lock only)
	p.mu.RLock()
	if len(p.pages) > int(pageIdx) && p.pages[pageIdx] != nil {
		page := p.pages[pageIdx]
		p.mu.RUnlock()

		// For page 0 (root/header), always promote to keep it cached
		// For other pages, lazy LRU tracking via Get() is sufficient
		if pageIdx == 0 {
			p.lruCache.GetAndPromote(pageIdx)
		}
		// No LRU update needed for cache hits - Get() already incremented access count
		return page, nil
	}
	totalPages := p.totalPages
	wi := p.walIndex
	p.mu.RUnlock()

	// WAL check: on a cache miss, look up the WAL index before touching the DB
	// file.  WAL pages may not yet be on disk (checkpoint pending), so this check
	// must happen before the totalPages boundary guard.  The index is populated on
	// every WAL commit and always contains the most-recently-committed version of
	// a page.
	if wi != nil {
		if walData, ok := wi.Lookup(pageIdx); ok {
			newPage, err := unmarshaler(totalPages, pageIdx, walData)
			if err != nil {
				return nil, fmt.Errorf("unmarshal page %d from WAL index: %w", pageIdx, err)
			}

			if pageIdx == 0 {
				if newPage.LeafNode != nil {
					newPage.LeafNode.Header.IsRoot = true
				}
				if newPage.InternalNode != nil {
					newPage.InternalNode.Header.IsRoot = true
					newPage.InternalNode.Header.IsInternal = true
				}
			}

			p.mu.Lock()
			defer p.mu.Unlock()

			// Another goroutine may have populated the cache while we read the WAL.
			if len(p.pages) > int(pageIdx) && p.pages[pageIdx] != nil {
				return p.pages[pageIdx], nil
			}

			// For page 0 WAL hits, sync the in-memory DB header so GetHeader()
			// returns the latest value even before a checkpoint.
			if pageIdx == 0 {
				var hdr DatabaseHeader
				if err := UnmarshalDatabaseHeader(walData[0:RootPageConfigSize], &hdr); err == nil {
					p.dbHeader = hdr
				}
			}

			evictedIdx, evicted := p.lruCache.EvictIfNeeded()
			if evicted {
				p.pages[evictedIdx] = nil
			}

			if len(p.pages) < int(pageIdx)+1 {
				for i := len(p.pages); i < int(pageIdx)+1; i++ {
					p.pages = append(p.pages, nil)
				}
			}

			p.pages[pageIdx] = newPage

			if int(pageIdx) == int(p.totalPages) {
				p.totalPages += 1
			}

			p.lruCache.Put(pageIdx, struct{}{}, false)
			return p.pages[pageIdx], nil
		}
	}

	// Not in WAL: enforce the on-disk boundary before attempting a file read.
	if int(pageIdx) > int(totalPages) {
		return nil, fmt.Errorf("cannot skip index when getting page, index: %d, number of pages: %d", pageIdx, totalPages)
	}

	// Perform I/O and unmarshaling OUTSIDE the lock to avoid blocking other page accesses
	buf := make([]byte, p.pageSize)

	if int(pageIdx) != int(p.totalPages) {
		// If we are not requesting a new page, read the page from file (no lock held)
		offset := int64(pageIdx) * int64(p.pageSize)
		_, err := p.file.ReadAt(buf, offset)
		if err != nil {
			return nil, err
		}
	}

	// Unmarshal the page (CPU-intensive work, no lock held)
	newPage, err := unmarshaler(totalPages, pageIdx, buf)
	if err != nil {
		return nil, err
	}

	// Set root flags for page 0
	if pageIdx == 0 {
		if newPage.LeafNode != nil {
			newPage.LeafNode.Header.IsRoot = true
		}
		if newPage.InternalNode != nil {
			newPage.InternalNode.Header.IsRoot = true
			newPage.InternalNode.Header.IsInternal = true
		}
	}

	// Now acquire write lock only for cache update (fast operation)
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check page doesn't exist (in case another goroutine created it while we were unmarshaling)
	if len(p.pages) > int(pageIdx) && p.pages[pageIdx] != nil {
		// Page already exists, no need to update LRU - just return it
		return p.pages[pageIdx], nil
	}

	// Evict pages if cache is full BEFORE adding new page
	evickedIdx, evicted := p.lruCache.EvictIfNeeded()
	if evicted {
		p.pages[evickedIdx] = nil
	}

	// Extend sparse array if needed
	if len(p.pages) < int(pageIdx)+1 {
		for i := len(p.pages); i < int(pageIdx)+1; i++ {
			p.pages = append(p.pages, nil)
		}
	}

	// Store the unmarshaled page in the cache
	p.pages[pageIdx] = newPage

	// Update total pages count for new pages
	if int(pageIdx) == int(p.totalPages) {
		p.totalPages += 1
	}

	// Track this page access
	p.lruCache.Put(pageIdx, struct{}{}, false)

	return p.pages[pageIdx], nil
}

// GetHeader ...
func (p *pagerImpl) GetHeader(ctx context.Context) DatabaseHeader {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.dbHeader
}

// SaveHeader ...
func (p *pagerImpl) SaveHeader(ctx context.Context, header DatabaseHeader) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.dbHeader = header
}

// SavePage ...
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

// Flush ...
func (p *pagerImpl) Flush(ctx context.Context, pageIdx PageIndex) error {
	p.mu.RLock()
	if int(pageIdx) >= len(p.pages) || p.pages[pageIdx] == nil {
		p.mu.RUnlock()
		return nil
	}

	page := p.pages[pageIdx]
	dbHeader := p.dbHeader
	p.mu.RUnlock()

	buf := p.bufferPool.Get().([]byte)
	defer p.bufferPool.Put(buf)
	clear(buf)

	if err := marshalPage(page, buf); err != nil {
		return fmt.Errorf("error flushing page %d: %w", page.Index, err)
	}

	if pageIdx != 0 {
		_, err := p.file.WriteAt(buf, int64(pageIdx)*int64(p.pageSize))
		return err
	}

	var headerBuf [RootPageConfigSize]byte
	if err := dbHeader.MarshalTo(headerBuf[:]); err != nil {
		return err
	}

	_, err := p.file.WriteAt(headerBuf[:], 0)
	if err != nil {
		return err
	}
	_, err = p.file.WriteAt(buf[0:p.pageSize-RootPageConfigSize], int64(RootPageConfigSize))
	if err != nil {
		return err
	}

	return fastSync(p.file)
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
		header  *DatabaseHeader
		buf     []byte
		pageIdx PageIndex
		isRoot  bool
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
	for i, page := range pagesToMarshal {
		pageIdx := indices[i]
		buf := p.bufferPool.Get().([]byte)
		clear(buf)

		if err := marshalPage(page, buf); err != nil {
			p.bufferPool.Put(buf)
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
	// Return all buffers to pool after we're completely done
	defer func() {
		for _, mp := range marshaled {
			p.bufferPool.Put(mp.buf)
		}
	}()

	for _, mp := range marshaled {
		if !mp.isRoot {
			// Regular page write
			_, err := p.file.WriteAt(mp.buf, int64(mp.pageIdx)*int64(p.pageSize))
			if err != nil {
				return fmt.Errorf("error writing page %d: %w", mp.pageIdx, err)
			}
		} else {
			// Root page with header
			var headerBuf [RootPageConfigSize]byte
			if err := mp.header.MarshalTo(headerBuf[:]); err != nil {
				return fmt.Errorf("error marshaling header: %w", err)
			}

			_, err := p.file.WriteAt(headerBuf[:], 0)
			if err != nil {
				return fmt.Errorf("error writing header: %w", err)
			}

			_, err = p.file.WriteAt(mp.buf[0:p.pageSize-RootPageConfigSize], int64(RootPageConfigSize))
			if err != nil {
				return fmt.Errorf("error writing root page data: %w", err)
			}
		}
	}

	return fastSync(p.file)
}

func marshalPage(page *Page, buf []byte) error {
	switch {
	case page.OverflowPage != nil:
		if err := page.OverflowPage.Marshal(buf); err != nil {
			return fmt.Errorf("error marshaling overflow node: %w", err)
		}
	case page.FreePage != nil:
		if err := page.FreePage.Marshal(buf); err != nil {
			return fmt.Errorf("error marshaling freepage node: %w", err)
		}
	case page.LeafNode != nil:
		if err := page.LeafNode.Marshal(buf); err != nil {
			return fmt.Errorf("error marshaling leaf node: %w", err)
		}
	case page.InternalNode != nil:
		if err := page.InternalNode.Marshal(buf); err != nil {
			return fmt.Errorf("error marshaling internal node: %w", err)
		}
	case page.IndexNode != nil:
		if err := marshalIndexNode(page.IndexNode, buf); err != nil {
			return fmt.Errorf("error marshaling index node: %w", err)
		}
	case page.IndexOverflowNode != nil:
		if err := page.IndexOverflowNode.Marshal(buf); err != nil {
			return fmt.Errorf("error marshaling index overflow node: %w", err)
		}
	default:
		return errors.New("no known node type found")
	}
	return nil
}
