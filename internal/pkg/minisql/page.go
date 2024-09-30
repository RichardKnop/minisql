package minisql

import (
	"context"
	"fmt"
)

const (
	PageSize = 4096 // 4 kilobytes
	MaxPages = 100  // temporary limit, TODO - remove later
)

type Page struct {
	Number     uint32
	buf        [PageSize]byte
	nextOffset uint32
}

// NewPage returns a new page with a number (page numbers begin with 0 for the first page)
func NewPage(number uint32) *Page {
	return &Page{
		Number: number,
		buf:    [PageSize]byte{},
	}
}

// Insert inserts a row into the page
func (p *Page) Insert(ctx context.Context, offset uint32, aRow Row) error {
	data, err := aRow.Marshal()
	if err != nil {
		return err
	}
	if int(offset)+len(data) > len(p.buf) {
		return fmt.Errorf("error inserting %d bytes into page at offset %d, not enough space", len(data), offset)
	}
	for i, dataByte := range data {
		p.buf[int(offset)+i] = dataByte
	}
	p.nextOffset = offset + uint32(len(data))
	return nil
}
