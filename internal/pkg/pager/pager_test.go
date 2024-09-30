package pager

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/pkg/minisql/minisqltest"
)

var (
	gen = minisqltest.NewDataGen(time.Now().Unix())
)

func TestNew(t *testing.T) {
	t.Parallel()

	dbFile, err := os.CreateTemp(".", "testdb")
	require.NoError(t, err)
	defer dbFile.Close()
	defer os.Remove(dbFile.Name())

	aPager, err := New(dbFile, "minisql_main")
	require.NoError(t, err)

	assert.Equal(t, int64(0), aPager.fileSize)
	assert.Equal(t, int64(0), aPager.totalPages)
	assert.Len(t, aPager.pages, MaxPages)
}

func TestPager_GetPage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	dbFile, err := os.CreateTemp(".", "testdb")
	require.NoError(t, err)
	defer dbFile.Close()
	defer os.Remove(dbFile.Name())

	aPager, err := New(dbFile, "minisql_main")
	require.NoError(t, err)

	// Cache hit
	aPage, err := aPager.GetPage(ctx, "foo", uint32(0))
	require.NoError(t, err)
	assert.Equal(t, uint32(0), aPage.Index)

	// Cache miss, allocates new empty page
	aPage, err = aPager.GetPage(ctx, "foo", uint32(1))
	require.NoError(t, err)
	assert.Nil(t, aPage)
}
