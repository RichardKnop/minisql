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
}

type PageUnmarshaler func(pageIdx uint32, buf []byte) (*Page, error)

type pagerImpl struct {
	pageSize   int
	totalPages uint32 // total number of pages

	dbHeader DatabaseHeader
	pages    []*Page

	file     DBFile
	fileSize int64

	mu sync.RWMutex
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

func (p *pagerImpl) GetHeader(ctx context.Context) DatabaseHeader {
	return p.dbHeader
}

func (p *pagerImpl) SaveHeader(ctx context.Context, header DatabaseHeader) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.dbHeader = header
}

func (p *pagerImpl) SavePage(ctx context.Context, pageIdx uint32, page *Page) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.pages[pageIdx] = page
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
			return nil, fmt.Errorf("error flushing page %d: %w", aPage.Index, err)
		}
		copy(buf, data)
		return buf[:len(data)], nil
	} else if aPage.LeafNode != nil {
		data, err := aPage.LeafNode.Marshal(buf)
		if err != nil {
			return nil, fmt.Errorf("error flushing page %d: %w", aPage.Index, err)
		}
		return data, nil
	} else if aPage.InternalNode != nil {
		data, err := aPage.InternalNode.Marshal(buf)
		if err != nil {
			return nil, fmt.Errorf("error flushing page %d: %w", aPage.Index, err)
		}
		return data, nil
	} else if aPage.IndexNode != nil {
		data, err := marshalIndexNode(aPage, buf)
		if err != nil {
			return nil, fmt.Errorf("error flushing page %d: %w", aPage.Index, err)
		}
		return data, nil
	}
	return nil, fmt.Errorf("error flushing page %d, neither internal nor leaf node nor free page", aPage.Index)
}
