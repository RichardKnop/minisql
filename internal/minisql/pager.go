package minisql

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/RichardKnop/minisql/pkg/lrucache"
)

const PageCacheSize = 2000 // default maximum number of pages to keep in memory

type DBFile interface {
	io.ReadSeeker
	io.ReaderAt
	io.WriterAt
	io.Closer
	Sync() error
}

type PageUnmarshaler func(totalPages uint32, pageIdx PageIndex, buf []byte) (*Page, error)

type pagerImpl struct {
	pageSize       int
	totalPages     uint32 // total number of pages
	maxCachedPages int    // maximum number of pages to keep in cache

	dbHeader DatabaseHeader
	// pages is a sparse array where index = PageIndex
	// nil entries indicate evicted/unloaded pages
	// Memory overhead: ~8 bytes per total page (e.g., 76MB for 10M pages)
	pages []*Page

	lruCache LRUCache[PageIndex]

	// LRU tracking: most recently used at the end
	// lruList []PageIndex

	file     DBFile
	fileSize int64

	// bufferPool reuses page-sized byte slices to reduce allocations
	bufferPool *sync.Pool

	mu sync.RWMutex
}

// New opens the database file and tries to read the root page
// maxCachedPages: maximum number of pages to keep in cache (0 = unlimited)
func NewPager(file DBFile, pageSize int, maxCachedPages int) (*pagerImpl, error) {
	if maxCachedPages <= 0 {
		maxCachedPages = PageCacheSize
	}
	aPager := &pagerImpl{
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
	// Sync file to ensure all buffered writes are persisted to disk
	// This is critical for data durability - without it, writes may be lost on crash/close
	if err := p.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file before close: %w", err)
	}
	return p.file.Close()
}

func (p *pagerImpl) TotalPages() uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.totalPages
}

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
	p.mu.RUnlock()

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
		p.totalPages++
	}

	// Track this page access
	p.lruCache.Put(pageIdx, struct{}{}, false)

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

	buf := p.bufferPool.Get().([]byte)
	defer p.bufferPool.Put(buf)
	for j := range buf {
		buf[j] = 0
	}

	if err := marshalPage(aPage, buf); err != nil {
		return fmt.Errorf("error flushing page %d: %w", aPage.Index, err)
	}

	if pageIdx != 0 {
		_, err := p.file.WriteAt(buf, int64(pageIdx)*int64(p.pageSize))
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
	if err != nil {
		return err
	}

	return p.file.Sync()
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
		buf := p.bufferPool.Get().([]byte)
		for j := range buf {
			buf[j] = 0
		}

		if err := marshalPage(aPage, buf); err != nil {
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

	return p.file.Sync()
}

func marshalPage(aPage *Page, buf []byte) error {
	if aPage.OverflowPage != nil {
		if err := aPage.OverflowPage.Marshal(buf); err != nil {
			return fmt.Errorf("error marshaling overflow node: %w", err)
		}
		return nil
	} else if aPage.FreePage != nil {
		if err := aPage.FreePage.Marshal(buf); err != nil {
			return fmt.Errorf("error marshaling freepage node: %w", err)
		}
		return nil
	} else if aPage.LeafNode != nil {
		if err := aPage.LeafNode.Marshal(buf); err != nil {
			return fmt.Errorf("error marshaling leaf node: %w", err)
		}
		return nil
	} else if aPage.InternalNode != nil {
		if err := aPage.InternalNode.Marshal(buf); err != nil {
			return fmt.Errorf("error marshaling internal node: %w", err)
		}
		return nil
	} else if aPage.IndexNode != nil {
		if err := marshalIndexNode(aPage.IndexNode, buf); err != nil {
			return fmt.Errorf("error marshaling index node: %w", err)
		}
		return nil
	} else if aPage.IndexOverflowNode != nil {
		if err := aPage.IndexOverflowNode.Marshal(buf); err != nil {
			return fmt.Errorf("error marshaling index overflow node: %w", err)
		}
		return nil
	}
	return fmt.Errorf("no known node type found")
}
