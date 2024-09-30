package pager

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/RichardKnop/minisql/internal/pkg/minisql"
)

const (
	PageSize = 4096 // 4 kilobytes
	MaxPages = 1000 // temporary limit, TODO - remove later
)

type Pager struct {
	PageNum int64
	Pages   map[string][]*minisql.Page

	fileName string
	file     *os.File
	fileSize int64
}

func New(fileName string) *Pager {
	return &Pager{fileName: fileName}
}

func (p *Pager) Open(ctx context.Context) error {
	dbFile, err := os.OpenFile(p.fileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	p.file = dbFile

	fileSize, err := dbFile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	p.fileSize = fileSize

	// Basic check to verify file size is a multiple of page size (4096B)
	if fileSize%PageSize != 0 {
		return fmt.Errorf("db file size is not divisible by page size: %d", fileSize)
	}

	// Check we are not exceeding max page limit
	pageNum := fileSize / PageSize
	if pageNum >= MaxPages {
		return fmt.Errorf(("file size exceeds max pages limit"))
	}
	p.PageNum = pageNum

	p.Pages = make(map[string][]*minisql.Page)

	return nil
}

func (p *Pager) GetPage(ctx context.Context, tableName string, pageNumber uint32) (*minisql.Page, error) {
	return nil, fmt.Errorf("not implemented")
}

func (p *Pager) Close() {
	if p.file != nil {
		p.file.Close()
	}
}
