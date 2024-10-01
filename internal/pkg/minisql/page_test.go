package minisql

// import (
// 	"context"
// 	"testing"

// 	"github.com/stretchr/testify/assert"
// 	"github.com/stretchr/testify/require"
// )

// func TestPage_Insert(t *testing.T) {
// 	t.Parallel()

// 	aPage := NewPage(0)

// 	// Row size is 267 bytes
// 	// 4096B page can fit 15 rows

// 	offset := uint32(0)
// 	for i := 0; i < 15; i++ {
// 		aRow := gen.Row()
// 		err := aPage.Insert(context.Background(), offset, aRow)
// 		require.NoError(t, err)
// 		offset += aRow.Size()
// 	}
// 	assert.Equal(t, uint32(267*15), aPage.nextOffset)

// 	// When trying to insert 16th row, we should receive an error
// 	// explaining that there is not enough space left in the page
// 	err := aPage.Insert(context.Background(), offset, gen.Row())
// 	require.Error(t, err)
// 	assert.Equal(t, "error inserting 267 bytes into page at offset 4005, not enough space", err.Error())
// }
