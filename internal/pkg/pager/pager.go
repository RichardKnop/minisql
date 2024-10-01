package pager

import (
	"context"
	"fmt"
	"io"

	"github.com/RichardKnop/minisql/internal/pkg/minisql"
	"github.com/RichardKnop/minisql/internal/pkg/node"
)

const (
	PageSize = 4096 // 4 kilobytes
	MaxPages = 1024 // temporary limit, TODO - remove later
)

type DBFile interface {
	io.ReadSeeker
	io.ReaderAt
	io.WriterAt
}

type Pager struct {
	totalPages uint32 // total number of pages

	// TODO - store pages per different tables
	pages []*minisql.Page

	file     DBFile
	fileSize int64
}

// New opens the database file and tries to read the root page
func New(file DBFile, schemaTableName string) (*Pager, error) {
	aPager := &Pager{
		file:  file,
		pages: make([]*minisql.Page, MaxPages),
	}

	fileSize, err := aPager.file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	aPager.fileSize = fileSize

	// Basic check to verify file size is a multiple of page size (4096B)
	if fileSize%PageSize != 0 {
		return nil, fmt.Errorf("db file size is not divisible by page size: %d", fileSize)
	}

	// Check we are not exceeding max page limit
	totalPages := fileSize / PageSize
	if totalPages >= MaxPages {
		return nil, fmt.Errorf(("file size exceeds max pages limit"))
	}
	aPager.totalPages = uint32(totalPages)

	// TODO - init root page

	return aPager, nil
}

func (p *Pager) TotalPages(aTable *minisql.Table) uint32 {
	return p.totalPages
}

func (p *Pager) GetPage(ctx context.Context, aTable *minisql.Table, pageIdx uint32) (*minisql.Page, error) {
	if pageIdx >= MaxPages {
		return nil, fmt.Errorf("page index %d reached limit of max pages %d", pageIdx, MaxPages)
	}

	if aPage := p.pages[pageIdx]; aPage == nil {
		// Cache miss, try to load the page from file first
		if pageIdx <= uint32(p.totalPages) {
			// Load page from file
			buf := make([]byte, PageSize)
			_, err := p.file.ReadAt(buf, int64(pageIdx*PageSize))
			if err != nil && err != io.EOF {
				return nil, err
			}

			// Empty new page will be leaf node
			if buf[0] == 0 {
				// Leaf node
				rowSize := aTable.RowSize
				numCells := PageSize / rowSize
				leaf := node.NewLeafNode(numCells, uint64(rowSize))
				_, err := leaf.Unmarshal(buf)
				if err != nil {
					return nil, err
				}
				p.pages[pageIdx] = &minisql.Page{LeafNode: leaf}
			} else {
				// Internal node
				internal := new(node.InternalNode)
				_, err := internal.Unmarshal(buf)
				if err != nil {
					return nil, err
				}
				p.pages[pageIdx] = &minisql.Page{InternalNode: internal}
			}
			if pageIdx == 0 {
				p.pages[pageIdx].LeafNode.Header.IsRoot = true
			}
			if pageIdx >= p.totalPages {
				p.totalPages = pageIdx + 1
			}
		}
	}

	return p.pages[pageIdx], nil
}

func (p *Pager) Flush(ctx context.Context, pageIdx uint32, size int64) error {
	aPage := p.pages[pageIdx]
	if aPage == nil {
		return fmt.Errorf("flushing nil page")
	}

	buf := make([]byte, PageSize)
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
	_, err := p.file.WriteAt(buf, int64(pageIdx*PageSize))

	return err
}
