package minisql

import (
	"context"
	"fmt"
	"io"
)

type DBFile interface {
	io.ReadSeeker
	io.ReaderAt
	io.WriterAt
}

type PageUnmarshaler func(pageIdx uint32, buf []byte) (*Page, error)

type pagerImpl struct {
	pageSize   int
	totalPages uint32 // total number of pages

	dbHeader DatabaseHeader
	pages    []*Page

	file     DBFile
	fileSize int64
}

// New opens the database file and tries to read the root page
func NewPager(file DBFile, pageSize int) (*pagerImpl, error) {
	aPager := &pagerImpl{
		pageSize: pageSize,
		file:     file,
		pages:    make([]*Page, 0, 1000),
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

func (p *pagerImpl) TotalPages() uint32 {
	return p.totalPages
}

func (p *pagerImpl) GetPage(ctx context.Context, pageIdx uint32, unmarshaler PageUnmarshaler) (*Page, error) {
	if len(p.pages) > int(pageIdx) && p.pages[pageIdx] != nil {
		return p.pages[pageIdx], nil
	}

	if int(pageIdx) > int(p.totalPages) {
		return nil, fmt.Errorf("cannot skip index when getting page, index: %d, number of pages: %d", pageIdx, len(p.pages))
	}

	buf := make([]byte, p.pageSize)

	if int(pageIdx) != int(p.totalPages) {
		// If we are not requesting a new page, read the page from file
		offset := int64(pageIdx) * int64(p.pageSize)
		_, err := p.file.ReadAt(buf, offset)
		if err != nil {
			return nil, err
		}

		if len(p.pages) < int(pageIdx)+1 {
			// Extend pages slice
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

	return p.pages[pageIdx], nil
}

func (p *pagerImpl) GetFreePage(ctx context.Context, unmarshaler PageUnmarshaler) (*Page, error) {
	// Check if there are any free pages
	if p.dbHeader.FirstFreePage == 0 {
		// No free pages, allocate new one
		return p.GetPage(ctx, p.TotalPages(), unmarshaler)
	}

	// Get the first free page
	freePage, err := p.GetPage(ctx, p.dbHeader.FirstFreePage, unmarshaler)
	if err != nil {
		return nil, fmt.Errorf("get free page: %w", err)
	}

	// Update header to point to next free page
	p.dbHeader.FirstFreePage = freePage.FreePage.NextFreePage
	p.dbHeader.FreePageCount--

	// Clear the page for reuse
	freePage.FreePage = nil
	freePage.LeafNode = nil
	freePage.InternalNode = nil
	freePage.IndexNode = nil

	return freePage, nil
}

func (p *pagerImpl) AddFreePage(ctx context.Context, pageIdx uint32, unmarshaler PageUnmarshaler) error {
	if pageIdx == 0 {
		return fmt.Errorf("cannot free page 0 (header page)")
	}

	// Get the page to mark as free
	freePage, err := p.GetPage(ctx, pageIdx, unmarshaler)
	if err != nil {
		return fmt.Errorf("add free page: %w", err)
	}

	// Initialize as free page
	freePage.FreePage = &FreePage{
		NextFreePage: p.dbHeader.FirstFreePage,
	}

	// Clear other node types
	freePage.LeafNode = nil
	freePage.InternalNode = nil
	freePage.IndexNode = nil

	// Update header
	p.dbHeader.FirstFreePage = pageIdx
	p.dbHeader.FreePageCount++

	return nil
}

func (p *pagerImpl) Flush(ctx context.Context, pageIdx uint32) error {
	if int(pageIdx) >= len(p.pages) || p.pages[pageIdx] == nil {
		return nil
	}

	aPage := p.pages[pageIdx]

	buf := make([]byte, p.pageSize)
	_, err := marshalPage(aPage, buf)
	if err != nil {
		return err
	}

	if pageIdx != 0 {
		_, err = p.file.WriteAt(buf, int64(pageIdx)*int64(p.pageSize))
		return err
	}

	headerBytes, err := p.dbHeader.Marshal()
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

func marshalPage(aPage *Page, buf []byte) ([]byte, error) {
	if aPage.FreePage != nil {
		data, err := aPage.FreePage.Marshal()
		if err != nil {
			return nil, err
		}
		copy(buf, data)
		return buf[:len(data)], nil
	} else if aPage.LeafNode != nil {
		marshaler := aPage.LeafNode.Marshal
		if aPage.Index == 0 {
			marshaler = aPage.LeafNode.MarshalRoot
		}
		data, err := marshaler(buf)
		if err != nil {
			return nil, err
		}
		return data, nil
	} else if aPage.InternalNode != nil {
		marshaler := aPage.InternalNode.Marshal
		if aPage.Index == 0 {
			marshaler = aPage.InternalNode.MarshalRoot
		}
		data, err := marshaler(buf)
		if err != nil {
			return nil, err
		}
		return data, nil
	} else if aPage.IndexNode != nil {
		switch node := aPage.IndexNode.(type) {
		case *IndexNode[int8]:
			return node.Marshal(buf)
		case *IndexNode[int32]:
			return node.Marshal(buf)
		case *IndexNode[int64]:
			return node.Marshal(buf)
		case *IndexNode[float32]:
			return node.Marshal(buf)
		case *IndexNode[float64]:
			return node.Marshal(buf)
		case *IndexNode[string]:
			return node.Marshal(buf)
		default:
			return nil, fmt.Errorf("error flushing, unknown index node type for page %d", aPage.Index)
		}
	}
	return nil, fmt.Errorf("error flushing, page %d is neither internal nor leaf node nor free page", aPage.Index)
}
