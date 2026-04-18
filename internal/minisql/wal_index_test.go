package minisql

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewWALIndex(t *testing.T) {
	t.Parallel()

	wi := NewWALIndex()
	assert.Equal(t, 0, wi.Size())
}

func TestWALIndex_Update_And_Lookup(t *testing.T) {
	t.Parallel()

	wi := NewWALIndex()

	data := makeTestPage(0xAB)
	wi.Update(PageIndex(3), data)

	got, ok := wi.Lookup(PageIndex(3))
	require.True(t, ok)
	assert.Equal(t, data, got)
}

func TestWALIndex_Lookup_NotFound(t *testing.T) {
	t.Parallel()

	wi := NewWALIndex()

	got, ok := wi.Lookup(PageIndex(99))
	assert.False(t, ok)
	assert.Nil(t, got)
}

func TestWALIndex_Update_ReturnsCopy(t *testing.T) {
	t.Parallel()

	wi := NewWALIndex()

	original := makeTestPage(0x01)
	wi.Update(PageIndex(0), original)

	// Mutate the original slice after storing.
	original[0] = 0xFF

	got, ok := wi.Lookup(PageIndex(0))
	require.True(t, ok)
	// The stored copy must be unaffected by the mutation.
	assert.Equal(t, byte(0x01), got[0])
}

func TestWALIndex_Lookup_ReturnsCopy(t *testing.T) {
	t.Parallel()

	wi := NewWALIndex()

	wi.Update(PageIndex(1), makeTestPage(0x02))

	got, ok := wi.Lookup(PageIndex(1))
	require.True(t, ok)

	// Mutate the returned slice.
	got[0] = 0xFF

	// A second lookup must be unaffected.
	got2, ok := wi.Lookup(PageIndex(1))
	require.True(t, ok)
	assert.Equal(t, byte(0x02), got2[0])
}

func TestWALIndex_Update_OverwriteExisting(t *testing.T) {
	t.Parallel()

	wi := NewWALIndex()

	wi.Update(PageIndex(5), makeTestPage(0xAA))
	wi.Update(PageIndex(5), makeTestPage(0xBB)) // later write wins

	got, ok := wi.Lookup(PageIndex(5))
	require.True(t, ok)
	assert.Equal(t, byte(0xBB), got[0])
}

func TestWALIndex_Has(t *testing.T) {
	t.Parallel()

	wi := NewWALIndex()

	assert.False(t, wi.Has(PageIndex(0)))

	wi.Update(PageIndex(0), makeTestPage(0x01))

	assert.True(t, wi.Has(PageIndex(0)))
	assert.False(t, wi.Has(PageIndex(1)))
}

func TestWALIndex_Size(t *testing.T) {
	t.Parallel()

	wi := NewWALIndex()
	assert.Equal(t, 0, wi.Size())

	wi.Update(PageIndex(0), makeTestPage(0x01))
	assert.Equal(t, 1, wi.Size())

	wi.Update(PageIndex(1), makeTestPage(0x02))
	assert.Equal(t, 2, wi.Size())

	// Updating the same page must not increase size.
	wi.Update(PageIndex(0), makeTestPage(0x03))
	assert.Equal(t, 2, wi.Size())
}

func TestWALIndex_Reset(t *testing.T) {
	t.Parallel()

	wi := NewWALIndex()
	wi.Update(PageIndex(0), makeTestPage(0x01))
	wi.Update(PageIndex(1), makeTestPage(0x02))
	require.Equal(t, 2, wi.Size())

	wi.Reset()

	assert.Equal(t, 0, wi.Size())
	assert.False(t, wi.Has(PageIndex(0)))
	assert.False(t, wi.Has(PageIndex(1)))
}

func TestWALIndex_Rebuild(t *testing.T) {
	t.Parallel()

	wi := NewWALIndex()

	frames := []WALReadFrame{
		{PageIndex: 0, Data: makeTestPage(0xAA)},
		{PageIndex: 1, Data: makeTestPage(0xBB)},
		{PageIndex: 0, Data: makeTestPage(0xCC)}, // overrides first entry for page 0
	}
	wi.Rebuild(frames)

	assert.Equal(t, 2, wi.Size())

	got0, ok := wi.Lookup(PageIndex(0))
	require.True(t, ok)
	assert.Equal(t, byte(0xCC), got0[0], "page 0 should reflect the last frame")

	got1, ok := wi.Lookup(PageIndex(1))
	require.True(t, ok)
	assert.Equal(t, byte(0xBB), got1[0])
}

func TestWALIndex_Rebuild_Empty(t *testing.T) {
	t.Parallel()

	wi := NewWALIndex()
	wi.Update(PageIndex(0), makeTestPage(0x01)) // pre-existing entry

	wi.Rebuild(nil) // rebuild with empty slice clears everything

	assert.Equal(t, 0, wi.Size())
}

func TestWALIndex_Rebuild_ReturnsCopy(t *testing.T) {
	t.Parallel()

	wi := NewWALIndex()

	data := makeTestPage(0x42)
	frames := []WALReadFrame{{PageIndex: 7, Data: data}}
	wi.Rebuild(frames)

	// Mutate source data after rebuild.
	data[0] = 0xFF
	frames[0].Data[1] = 0xFE

	got, ok := wi.Lookup(PageIndex(7))
	require.True(t, ok)
	assert.Equal(t, byte(0x42), got[0], "stored copy must be independent of source slice")
	assert.Equal(t, byte(0x42), got[1])
}

func TestWALIndex_Concurrent(t *testing.T) {
	t.Parallel()

	wi := NewWALIndex()
	const workers = 8
	const pages = 64

	var wg sync.WaitGroup

	// Writers: each worker updates a disjoint set of pages.
	for w := range workers {
		wg.Go(func() {
			for p := range pages {
				wi.Update(PageIndex(w*pages+p), makeTestPage(byte(w)))
			}
		})
	}

	// Concurrent readers: mix of Has + Lookup during writes.
	for range workers {
		wg.Go(func() {
			for p := range pages {
				wi.Has(PageIndex(p))
				wi.Lookup(PageIndex(p))
				wi.Size()
			}
		})
	}

	wg.Wait()

	// After all writes complete, every written page must be present.
	for w := range workers {
		for p := range pages {
			assert.True(t, wi.Has(PageIndex(w*pages+p)))
		}
	}
}
