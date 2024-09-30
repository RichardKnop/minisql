package pager

import (
	"context"
	"fmt"
	"io"

	"github.com/RichardKnop/minisql/internal/pkg/minisql"
)

const (
	PageSize = 4096 // 4 kilobytes
	MaxPages = 1000 // temporary limit, TODO - remove later
)

type DBFile interface {
	io.ReadSeeker
	io.ReaderAt
	io.WriterAt
}

type Pager struct {
	totalPages int64 // total number of pages

	// TODO - temporary we store all pages in a slice, this will be replaced by a B-tree
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
	// Uncomment once we switch to B-tree
	// if fileSize%PageSize != 0 {
	// 	return nil, fmt.Errorf("db file size is not divisible by page size: %d", fileSize)
	// }

	// Check we are not exceeding max page limit
	totalPages := fileSize / PageSize
	if totalPages >= MaxPages {
		return nil, fmt.Errorf(("file size exceeds max pages limit"))
	}
	aPager.totalPages = totalPages

	var aRootPage *minisql.Page
	if aPager.fileSize == 0 {
		// If the file is empty, this is a fresh database, create root page
		aRootPage = minisql.NewPage(0)
	} else {
		buf := make([]byte, PageSize)
		rootPageOffset := int64(0)
		_, err = aPager.file.ReadAt(buf, rootPageOffset)
		if err != nil && err != io.EOF {
			return nil, err
		}
		aRootPage, err = minisql.NewPageWithData(0, buf)
		if err != nil {
			return nil, err
		}
	}
	aPager.pages[aRootPage.Index] = aRootPage

	return aPager, nil
}

func (p *Pager) FileSize() int64 {
	return p.fileSize
}

func (p *Pager) GetPage(ctx context.Context, tableName string, pageIdx uint32) (*minisql.Page, error) {
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

			if buf[0] == 0 {
				// Empty new page
				p.pages[pageIdx] = minisql.NewPage(pageIdx)
			} else {
				p.pages[pageIdx], err = minisql.NewPageWithData(pageIdx, buf)
				if err != nil {
					return nil, err
				}
			}
		}

		if int64(pageIdx) >= p.totalPages {
			p.totalPages += 1
		}
	}

	return p.pages[pageIdx], nil
}

func (p *Pager) Flush(ctx context.Context, pageIdx uint32, size int64) error {
	aPage := p.pages[pageIdx]
	if aPage == nil {
		return fmt.Errorf("flushing nil page")
	}

	buf := aPage.Data(size)
	_, err := p.file.WriteAt(buf[:], int64(pageIdx*PageSize))

	return err
}
