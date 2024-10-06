package pager

import (
	"context"
	"fmt"
	"io"

	"github.com/RichardKnop/minisql/internal/pkg/minisql"
)

const (
	MaxPages = 1024 // temporary limit, TODO - remove later
)

type DBFile interface {
	io.ReadSeeker
	io.ReaderAt
	io.WriterAt
}

type Pager struct {
	pageSize   int
	totalPages uint32 // total number of pages

	pages []*minisql.Page

	file     DBFile
	fileSize int64
}

// New opens the database file and tries to read the root page
func New(file DBFile, pageSize int, schemaTableName string) (*Pager, error) {
	aPager := &Pager{
		pageSize: pageSize,
		file:     file,
		pages:    make([]*minisql.Page, MaxPages),
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

	// Check we are not exceeding max page limit
	totalPages := fileSize / int64(pageSize)
	if totalPages >= MaxPages {
		return nil, fmt.Errorf(("file size exceeds max pages limit"))
	}
	aPager.totalPages = uint32(totalPages)

	// TODO - init root page?

	return aPager, nil
}

func (p *Pager) TotalPages(aTable *minisql.Table) uint32 {
	return p.totalPages
}

func (p *Pager) GetPage(ctx context.Context, aTable *minisql.Table, pageIdx uint32) (*minisql.Page, error) {
	if pageIdx >= MaxPages {
		return nil, fmt.Errorf("page index %d reached limit of max pages %d", pageIdx, MaxPages)
	}

	if p.pages[pageIdx] != nil {
		return p.pages[pageIdx], nil
	}

	// Cache miss, try to load the page from file first
	// Load page from file
	buf := make([]byte, p.pageSize)
	_, err := p.file.ReadAt(buf, int64(pageIdx)*int64(p.pageSize))
	if err != nil && err != io.EOF {
		return nil, err
	}

	// First byte is Internal flag
	if buf[0] == 0 {
		// Leaf node
		leaf := minisql.NewLeafNode(uint64(aTable.RowSize))
		_, err := leaf.Unmarshal(buf)
		if err != nil {
			return nil, err
		}
		p.pages[pageIdx] = &minisql.Page{LeafNode: leaf}
	} else {
		// Internal node
		internal := new(minisql.InternalNode)
		_, err := internal.Unmarshal(buf)
		if err != nil {
			return nil, err
		}
		p.pages[pageIdx] = &minisql.Page{InternalNode: internal}
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
	if pageIdx >= p.totalPages {
		p.totalPages = pageIdx + 1
	}

	return p.pages[pageIdx], nil
}

func (p *Pager) Flush(ctx context.Context, pageIdx uint32, size int64) error {
	aPage := p.pages[pageIdx]
	if aPage == nil {
		return fmt.Errorf("flushing nil page")
	}

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
