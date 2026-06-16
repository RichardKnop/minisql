package minisql

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sync"

	pkgcrypto "github.com/RichardKnop/minisql/pkg/crypto"
	minisqlErrors "github.com/RichardKnop/minisql/pkg/errors"
	"github.com/RichardKnop/minisql/pkg/lrucache"
)

// PageCacheSize is the default maximum number of pages to keep in memory.
const PageCacheSize = 2000

// DBFile is the set of I/O operations the pager requires from the underlying
// database file. *os.File satisfies this interface.
type DBFile interface {
	io.ReadSeeker
	io.ReaderAt
	io.WriterAt
	io.Closer
	Sync() error
}

// PageUnmarshaler is a function that deserialises a raw page buffer into a typed Page.
// Different implementations are used for table pages, index pages, and overflow pages.
type PageUnmarshaler func(totalPages uint32, pageIdx PageIndex, buf []byte) (*Page, error)

type pagerImpl struct {
	lruCache       LRUCache[PageIndex]
	file           DBFile
	bufferPool     *sync.Pool
	walIndex       *WALIndex
	cipher         *pkgcrypto.PageCipher // nil when encryption is disabled
	metrics        *engineMetrics        // nil when metrics are not wired
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

// SetMetrics wires an engineMetrics counter store into the pager so that
// cache hits, misses, evictions, and current size are tracked automatically.
func (p *pagerImpl) SetMetrics(m *engineMetrics) {
	p.metrics = m
	if m != nil {
		m.pageCacheCapacity = int64(p.maxCachedPages)
	}
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

// SetCipher wires an AES-256-CTR cipher into the pager.  Once set, GetPage
// decrypts pages after reading them, and Flush/FlushBatch encrypt pages before
// writing them.  Must be called before any concurrent page access begins.
func (p *pagerImpl) SetCipher(c *pkgcrypto.PageCipher) {
	p.cipher = c
}

// CloseNoSync closes the underlying file handle without syncing. Used when the
// database file is being discarded (e.g. during VACUUM of the live database).
func (p *pagerImpl) CloseNoSync() error {
	return p.file.Close()
}

// Close syncs the file to ensure pending writes are flushed, then closes it.
func (p *pagerImpl) Close() error {
	if err := fastSync(p.file); err != nil {
		return fmt.Errorf("failed to sync file before close: %w", err)
	}
	return p.file.Close()
}

// TotalPages returns the current total number of pages tracked by the pager,
// including pages that exist only in the WAL and have not yet been checkpointed.
func (p *pagerImpl) TotalPages() uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.totalPages
}

// GetPage returns the page at pageIdx, serving it from the LRU cache when
// available. On a cache miss it checks the WAL index first (for uncommitted or
// not-yet-checkpointed writes), then falls back to reading from the DB file.
// Passing pageIdx == TotalPages() allocates a new blank page and increments the
// page count. The supplied unmarshaler deserialises the raw buffer into a typed Page.
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
		if m := p.metrics; m != nil {
			m.pageCacheHits.Add(1)
		}
		return page, nil
	}
	totalPages := p.totalPages
	wi := p.walIndex
	p.mu.RUnlock()

	if m := p.metrics; m != nil {
		m.pageCacheMisses.Add(1)
	}

	// WAL check: on a cache miss, look up the WAL index before touching the DB
	// file.  WAL pages may not yet be on disk (checkpoint pending), so this check
	// must happen before the totalPages boundary guard.  The index is populated on
	// every WAL commit and always contains the most-recently-committed version of
	// a page.
	if wi != nil {
		if walData, ok := wi.Lookup(pageIdx); ok {
			// Decrypt a copy so we never mutate the shared WAL index buffer.
			if p.cipher != nil {
				decrypted := make([]byte, len(walData))
				copy(decrypted, walData)
				if pageIdx == 0 {
					p.cipher.XORKeyStream(decrypted[RootPageConfigSize:], 0)
				} else {
					p.cipher.XORKeyStream(decrypted, uint32(pageIdx))
				}
				walData = decrypted
			}
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
				if m := p.metrics; m != nil {
					m.pageCacheEvictions.Add(1)
					m.pageCacheSize.Add(-1)
				}
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
			if m := p.metrics; m != nil {
				m.pageCacheSize.Add(1)
			}
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
		// Decrypt before checksum verification: the on-page CRC32 was computed over
		// plaintext and then included inside the encrypted region.
		if p.cipher != nil {
			if pageIdx == 0 {
				p.cipher.XORKeyStream(buf[RootPageConfigSize:], 0)
			} else {
				p.cipher.XORKeyStream(buf, uint32(pageIdx))
			}
		}
		if err := verifyPageChecksum(buf, pageIdx); err != nil {
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
		if m := p.metrics; m != nil {
			m.pageCacheEvictions.Add(1)
			m.pageCacheSize.Add(-1)
		}
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
	if m := p.metrics; m != nil {
		m.pageCacheSize.Add(1)
	}

	return p.pages[pageIdx], nil
}

// GetHeader returns the in-memory copy of the database header, which is kept
// in sync with page 0 on every read and write of that page.
func (p *pagerImpl) GetHeader(ctx context.Context) DatabaseHeader {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.dbHeader
}

// SaveHeader updates the in-memory database header. The change is not written
// to disk until the next Flush or FlushBatch call for page 0.
func (p *pagerImpl) SaveHeader(ctx context.Context, header DatabaseHeader) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.dbHeader = header
}

// InvalidatePage removes the page at pageIdx from the LRU cache without
// persisting it.  Called during transaction rollback when a page was modified
// in-place and the dirty copy must be discarded; the next read will reload
// the committed version from the WAL index.
func (p *pagerImpl) InvalidatePage(pageIdx PageIndex) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if int(pageIdx) < len(p.pages) {
		p.pages[pageIdx] = nil
	}
}

// SavePage stores a page in the in-memory cache at pageIdx without writing it
// to disk. The page is persisted only when Flush or FlushBatch is called.
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

// Flush marshals the page at pageIdx and writes it to the DB file. For page 0
// it writes the database header first, then the remainder of the page data, and
// calls fsync to ensure durability. No-ops if the page is not in cache.
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
		writePageChecksum(buf)
		if p.cipher != nil {
			p.cipher.XORKeyStream(buf, uint32(pageIdx))
		}
		_, err := p.file.WriteAt(buf, int64(pageIdx)*int64(p.pageSize))
		return err
	}

	var headerBuf [RootPageConfigSize]byte
	if err := dbHeader.MarshalTo(headerBuf[:]); err != nil {
		return err
	}

	// Checksum for page 0 covers the full assembled page:
	// file[0:4092] = headerBuf[0:100] + buf[0:3992].
	// Store the 4-byte result at buf[3992:3996] (= file[4092:4096]).
	writeRootPageChecksum(headerBuf[:], buf, p.pageSize)

	// Encrypt the B-tree + checksum portion (file[100:4096]) in place.
	// The plaintext header (file[0:100]) is never encrypted so that the
	// encryption salt can be read on startup before the cipher is ready.
	if p.cipher != nil {
		p.cipher.XORKeyStream(buf[:p.pageSize-RootPageConfigSize], 0)
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
			writePageChecksum(mp.buf)
			if p.cipher != nil {
				p.cipher.XORKeyStream(mp.buf, uint32(mp.pageIdx))
			}
			_, err := p.file.WriteAt(mp.buf, int64(mp.pageIdx)*int64(p.pageSize))
			if err != nil {
				return fmt.Errorf("error writing page %d: %w", mp.pageIdx, err)
			}
		} else {
			var headerBuf [RootPageConfigSize]byte
			if err := mp.header.MarshalTo(headerBuf[:]); err != nil {
				return fmt.Errorf("error marshaling header: %w", err)
			}

			writeRootPageChecksum(headerBuf[:], mp.buf, p.pageSize)

			if p.cipher != nil {
				p.cipher.XORKeyStream(mp.buf[:p.pageSize-RootPageConfigSize], 0)
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

// writePageChecksum computes CRC32-IEEE over the first PageSize-4 bytes of buf
// and stores the result in the last 4 bytes.  Called after marshalPage for all
// non-root pages.
func writePageChecksum(buf []byte) {
	checksum := crc32.ChecksumIEEE(buf[:len(buf)-pageChecksumSize])
	binary.LittleEndian.PutUint32(buf[len(buf)-pageChecksumSize:], checksum)
}

// writeRootPageChecksum computes the CRC32-IEEE checksum for the assembled
// page-0 layout (headerBuf + first (pageSize-RootPageConfigSize-4) bytes of
// buf) and stores it at buf[pageSize-RootPageConfigSize-4 :
// pageSize-RootPageConfigSize].  This is the last 4 bytes of the on-disk page.
func writeRootPageChecksum(headerBuf, buf []byte, pageSize int) {
	dataEnd := pageSize - RootPageConfigSize - pageChecksumSize // = 3992
	h := crc32.NewIEEE()
	h.Write(headerBuf)
	h.Write(buf[:dataEnd])
	binary.LittleEndian.PutUint32(buf[dataEnd:dataEnd+pageChecksumSize], h.Sum32())
}

// verifyPageChecksum reads the CRC32-IEEE stored in the last 4 bytes of buf and
// compares it against the computed value.  Returns ErrPageChecksumMismatch on
// mismatch so the caller can surface a corruption error.
func verifyPageChecksum(buf []byte, pageIdx PageIndex) error {
	stored := binary.LittleEndian.Uint32(buf[len(buf)-pageChecksumSize:])
	computed := crc32.ChecksumIEEE(buf[:len(buf)-pageChecksumSize])
	if stored != computed {
		return minisqlErrors.PageChecksumError{PageIndex: uint32(pageIdx)}
	}
	return nil
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
	case page.InvertedEntryPage != nil:
		// Reserve pageChecksumSize bytes at the end for the CRC32 checksum.
		// For page 0 also strip the DB-header prefix (RootPageConfigSize bytes).
		if page.Index == 0 {
			buf = buf[:PageSize-RootPageConfigSize-pageChecksumSize]
		} else {
			buf = buf[:PageSize-pageChecksumSize]
		}
		if err := page.InvertedEntryPage.Marshal(buf); err != nil {
			return fmt.Errorf("error marshaling inverted entry page: %w", err)
		}
	case page.InvertedPostPage != nil:
		if page.Index == 0 {
			buf = buf[:PageSize-RootPageConfigSize-pageChecksumSize]
		} else {
			buf = buf[:PageSize-pageChecksumSize]
		}
		if err := page.InvertedPostPage.Marshal(buf); err != nil {
			return fmt.Errorf("error marshaling inverted posting page: %w", err)
		}
	case page.InvertedMetaPage != nil:
		if page.Index == 0 {
			buf = buf[:PageSize-RootPageConfigSize-pageChecksumSize]
		} else {
			buf = buf[:PageSize-pageChecksumSize]
		}
		if err := page.InvertedMetaPage.Marshal(buf); err != nil {
			return fmt.Errorf("error marshaling inverted meta page: %w", err)
		}
	case page.InvertedSegmentPage != nil:
		if page.Index == 0 {
			buf = buf[:PageSize-RootPageConfigSize-pageChecksumSize]
		} else {
			buf = buf[:PageSize-pageChecksumSize]
		}
		if err := page.InvertedSegmentPage.Marshal(buf); err != nil {
			return fmt.Errorf("error marshaling inverted segment page: %w", err)
		}
	case page.HNSWMetaPage != nil:
		if page.Index == 0 {
			buf = buf[:PageSize-RootPageConfigSize-pageChecksumSize]
		} else {
			buf = buf[:PageSize-pageChecksumSize]
		}
		if err := page.HNSWMetaPage.Marshal(buf); err != nil {
			return fmt.Errorf("error marshaling HNSW meta page: %w", err)
		}
	case page.HNSWDataPage != nil:
		if page.Index == 0 {
			buf = buf[:PageSize-RootPageConfigSize-pageChecksumSize]
		} else {
			buf = buf[:PageSize-pageChecksumSize]
		}
		if err := page.HNSWDataPage.Marshal(buf); err != nil {
			return fmt.Errorf("error marshaling HNSW data page: %w", err)
		}
	default:
		return errors.New("no known node type found")
	}
	return nil
}
