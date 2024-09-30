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
	Index      uint32
	buf        [PageSize]byte
	nextOffset uint32
}

// NewPage returns a new page with an index and empty buffer
func NewPage(idx uint32) *Page {
	return &Page{
		Index: idx,
		buf:   [PageSize]byte{},
	}
}

// NewPageWithData returns a new page witn and index and input data
func NewPageWithData(idx uint32, data []byte) (*Page, error) {
	if len(data) > PageSize {
		return nil, fmt.Errorf("cannot create page with %d bytes which is more than page size %d bytes", len(data), PageSize)
	}
	aPage := NewPage(idx)

	copy(aPage.buf[:], data)

	aPage.nextOffset = uint32(len(data))

	return aPage, nil
}

func (p *Page) Data(size int64) []byte {
	return p.buf[0:size]
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
