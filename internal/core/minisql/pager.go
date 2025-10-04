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

type pagerImpl struct {
	pageSize   int
	totalPages uint32 // total number of pages

	pages []*Page

	file     DBFile
	fileSize int64
}

// New opens the database file and tries to read the root page
func NewPager(file DBFile, pageSize int, schemaTableName string) (*pagerImpl, error) {
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

	// TODO - init root page?

	return aPager, nil
}

func (p *pagerImpl) TotalPages() uint32 {
	return p.totalPages
}

func (p *pagerImpl) GetPage(ctx context.Context, aTable *Table, pageIdx uint32) (*Page, error) {
	if len(p.pages) > int(pageIdx) && p.pages[pageIdx] != nil {
		return p.pages[pageIdx], nil
	}

	if int(pageIdx) > int(p.totalPages) {
		return nil, fmt.Errorf("cannot skip index when getting page, index: %d, number of pages: %d", pageIdx, len(p.pages))
	}

	buf := make([]byte, p.pageSize)

	// Requesting a new page
	if int(pageIdx) == int(p.totalPages) {
		// Leaf node
		leaf := NewLeafNode(uint64(aTable.RowSize))
		_, err := leaf.Unmarshal(buf)
		if err != nil {
			return nil, err
		}
		p.pages = append(p.pages, &Page{Index: pageIdx, LeafNode: leaf})
		p.totalPages = pageIdx + 1
	} else {
		// Page should exist, load the page from file
		_, err := p.file.ReadAt(buf, int64(pageIdx)*int64(p.pageSize))
		if err != nil {
			return nil, err
		}

		if len(p.pages) < int(pageIdx)+1 {
			// Extend pages slice
			for i := len(p.pages); i < int(pageIdx)+1; i++ {
				p.pages = append(p.pages, nil)
			}
		}

		// First byte is Internal flag, this condition is also true if page does not exist
		if buf[0] == 0 {
			// Leaf node
			leaf := NewLeafNode(uint64(aTable.RowSize))
			_, err := leaf.Unmarshal(buf)
			if err != nil {
				return nil, err
			}
			p.pages[pageIdx] = &Page{Index: pageIdx, LeafNode: leaf}
		} else {
			// Internal node
			internal := new(InternalNode)
			_, err := internal.Unmarshal(buf)
			if err != nil {
				return nil, err
			}
			p.pages[pageIdx] = &Page{Index: pageIdx, InternalNode: internal}
		}
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

func (p *pagerImpl) Flush(ctx context.Context, pageIdx uint32, size int64) error {
	if int(pageIdx) >= len(p.pages) || p.pages[pageIdx] == nil {
		return nil
	}

	aPage := p.pages[pageIdx]

	buf := make([]byte, size)
	if aPage.LeafNode != nil {
		_, err := aPage.LeafNode.Marshal(buf)
		if err != nil {
			return err
		}
	} else if aPage.InternalNode != nil {
		_, err := aPage.InternalNode.Marshal(buf)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("error flushing, page %d is neither internal nor leaf node", pageIdx)
	}
	_, err := p.file.WriteAt(buf, int64(pageIdx)*size)
	return err
}
