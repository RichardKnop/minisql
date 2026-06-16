package minisql

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockTxPager is a minimal in-memory TxPager implementation for unit tests.
type mockTxPager struct {
	pages      map[PageIndex]*Page
	freedPages []PageIndex
	nextIdx    PageIndex
}

func newMockTxPager() *mockTxPager {
	return &mockTxPager{pages: make(map[PageIndex]*Page), nextIdx: 1}
}

func (m *mockTxPager) ReadPage(_ context.Context, idx PageIndex) (*Page, error) {
	p, ok := m.pages[idx]
	if !ok {
		return nil, fmt.Errorf("page %d not found", idx)
	}
	return p, nil
}
func (m *mockTxPager) ModifyPage(ctx context.Context, idx PageIndex) (*Page, error) {
	return m.ReadPage(ctx, idx)
}
func (m *mockTxPager) GetFreePage(_ context.Context) (*Page, error) {
	idx := m.nextIdx
	m.nextIdx += 1
	p := &Page{Index: idx}
	m.pages[idx] = p
	return p, nil
}
func (m *mockTxPager) AddFreePage(_ context.Context, idx PageIndex) error {
	m.freedPages = append(m.freedPages, idx)
	return nil
}
func (m *mockTxPager) GetOverflowPage(ctx context.Context, idx PageIndex) (*Page, error) {
	return m.ReadPage(ctx, idx)
}

func TestParseVectorLiteral(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    VectorPointer
		wantErr bool
	}{
		{
			name:  "basic 3d vector",
			input: "[1.0, 2.0, 3.0]",
			want:  VectorPointer{Dims: 3, Data: []float32{1.0, 2.0, 3.0}},
		},
		{
			name:  "single dimension",
			input: "[0.5]",
			want:  VectorPointer{Dims: 1, Data: []float32{0.5}},
		},
		{
			name:  "negative values",
			input: "[-1.5, 2.5, -3.0]",
			want:  VectorPointer{Dims: 3, Data: []float32{-1.5, 2.5, -3.0}},
		},
		{
			name:  "whitespace handling",
			input: "[ 1.0 , 2.0 , 3.0 ]",
			want:  VectorPointer{Dims: 3, Data: []float32{1.0, 2.0, 3.0}},
		},
		{
			name:    "missing brackets",
			input:   "1.0, 2.0, 3.0",
			wantErr: true,
		},
		{
			name:    "invalid float",
			input:   "[1.0, abc, 3.0]",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseVectorLiteral(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want.Dims, got.Dims)
			assert.Equal(t, tt.want.Data, got.Data)
		})
	}
}

func TestFormatVector(t *testing.T) {
	vp := VectorPointer{Dims: 3, Data: []float32{1.0, 2.5, -3.0}}
	got := FormatVector(vp)
	assert.Equal(t, "[1, 2.5, -3]", got)

	empty := VectorPointer{}
	assert.Equal(t, "[]", FormatVector(empty))
}

func TestL2Distance(t *testing.T) {
	a := VectorPointer{Dims: 3, Data: []float32{1, 2, 3}}
	b := VectorPointer{Dims: 3, Data: []float32{4, 6, 3}}
	dist, err := L2Distance(a, b)
	require.NoError(t, err)
	assert.InDelta(t, 5.0, dist, 0.0001)

	same := VectorPointer{Dims: 3, Data: []float32{1, 2, 3}}
	dist, err = L2Distance(a, same)
	require.NoError(t, err)
	assert.InDelta(t, 0.0, dist, 0.0001)

	// dimension mismatch
	c := VectorPointer{Dims: 2, Data: []float32{1, 2}}
	_, err = L2Distance(a, c)
	require.Error(t, err)
}

func TestCosineDistance(t *testing.T) {
	// identical vectors → distance 0
	a := VectorPointer{Dims: 3, Data: []float32{1, 2, 3}}
	same := VectorPointer{Dims: 3, Data: []float32{1, 2, 3}}
	dist, err := CosineDistance(a, same)
	require.NoError(t, err)
	assert.InDelta(t, 0.0, dist, 0.0001)

	// orthogonal vectors → distance 1
	x := VectorPointer{Dims: 2, Data: []float32{1, 0}}
	y := VectorPointer{Dims: 2, Data: []float32{0, 1}}
	dist, err = CosineDistance(x, y)
	require.NoError(t, err)
	assert.InDelta(t, 1.0, dist, 0.0001)

	// dimension mismatch
	c := VectorPointer{Dims: 2, Data: []float32{1, 2}}
	_, err = CosineDistance(a, c)
	require.Error(t, err)

	// zero vector
	zero := VectorPointer{Dims: 2, Data: []float32{0, 0}}
	_, err = CosineDistance(x, zero)
	require.Error(t, err)
}

func TestVectorPointerMarshalUnmarshal(t *testing.T) {
	original := VectorPointer{Dims: 1536, FirstPage: PageIndex(42)}
	buf := make([]byte, 8)
	original.Marshal(buf, 0)

	var decoded VectorPointer
	decoded.Unmarshal(buf, 0)
	assert.Equal(t, original.Dims, decoded.Dims)
	assert.Equal(t, original.FirstPage, decoded.FirstPage)
}

func TestVectorPointerSize(t *testing.T) {
	vp := VectorPointer{Dims: 1536}
	assert.Equal(t, uint64(8), vp.Size())
}

func TestL2DistanceKnownValues(t *testing.T) {
	// distance between (0,0) and (3,4) = 5
	a := VectorPointer{Dims: 2, Data: []float32{0, 0}}
	b := VectorPointer{Dims: 2, Data: []float32{3, 4}}
	dist, err := L2Distance(a, b)
	require.NoError(t, err)
	assert.InDelta(t, 5.0, dist, 0.0001)
}

func TestCosineDistanceAntiParallel(t *testing.T) {
	// opposite directions → distance 2
	a := VectorPointer{Dims: 1, Data: []float32{1}}
	b := VectorPointer{Dims: 1, Data: []float32{-1}}
	dist, err := CosineDistance(a, b)
	require.NoError(t, err)
	assert.InDelta(t, 2.0, dist, 0.0001)
}

func TestToVectorPointer(t *testing.T) {
	// already a VectorPointer
	vp := VectorPointer{Dims: 2, Data: []float32{1.0, 2.0}}
	got, err := toVectorPointer(vp)
	require.NoError(t, err)
	assert.Equal(t, vp, got)

	// from string
	got, err = toVectorPointer("[1.0, 2.0]")
	require.NoError(t, err)
	assert.Equal(t, uint32(2), got.Dims)

	// from TextPointer
	got, err = toVectorPointer(NewTextPointer([]byte("[1.0, 2.0]")))
	require.NoError(t, err)
	assert.Equal(t, uint32(2), got.Dims)

	// unsupported type
	_, err = toVectorPointer(42)
	require.Error(t, err)
}

func TestRowSizeVector(t *testing.T) {
	col := Column{Name: "embedding", Kind: Vector, Size: 3}
	vp := VectorPointer{Dims: 3, Data: []float32{1, 2, 3}}
	row := Row{
		Columns: []Column{col},
		Values:  []OptionalValue{{Valid: true, Value: vp}},
	}
	assert.Equal(t, uint64(8), row.Size(), "vector always contributes 8 bytes inline")
}

func TestRowMarshalVector(t *testing.T) {
	col := Column{Name: "embedding", Kind: Vector, Size: 3}
	vp := VectorPointer{Dims: 3, FirstPage: PageIndex(7)}
	row := Row{
		Columns: []Column{col},
		Values:  []OptionalValue{{Valid: true, Value: vp}},
	}
	buf, err := row.Marshal()
	require.NoError(t, err)
	require.Len(t, buf, 8)
	// Dims = 3 stored at offset 0
	assert.Equal(t, uint32(3), unmarshalUint32(buf, 0))
	// FirstPage = 7 stored at offset 4
	assert.Equal(t, uint32(7), unmarshalUint32(buf, 4))
}

func TestTypeCodeVector(t *testing.T) {
	assert.Equal(t, TypeCodeVector, kindToTypeCode(Vector))
	assert.Equal(t, 8, typeCodeFixedSize(TypeCodeVector))
}

func TestCosineDistanceParallel(t *testing.T) {
	// parallel unit vectors → distance 0
	a := VectorPointer{Dims: 3, Data: []float32{1, 0, 0}}
	b := VectorPointer{Dims: 3, Data: []float32{1, 0, 0}}
	dist, err := CosineDistance(a, b)
	require.NoError(t, err)
	assert.InDelta(t, 0.0, dist, 0.0001)
}

// roundTripDist ensures L2Distance computation is within float32 precision.
func TestL2DistancePrecision(t *testing.T) {
	a := VectorPointer{Dims: 4, Data: []float32{0.1, 0.2, 0.3, 0.4}}
	b := VectorPointer{Dims: 4, Data: []float32{0.5, 0.6, 0.7, 0.8}}
	dist, err := L2Distance(a, b)
	require.NoError(t, err)
	// expected = sqrt(0.16 * 4) = sqrt(0.64) = 0.8
	assert.InDelta(t, 0.8, dist, 0.0001)
}

func TestL2DistanceIsSymmetric(t *testing.T) {
	a := VectorPointer{Dims: 3, Data: []float32{1, 2, 3}}
	b := VectorPointer{Dims: 3, Data: []float32{4, 5, 6}}
	d1, err := L2Distance(a, b)
	require.NoError(t, err)
	d2, err := L2Distance(b, a)
	require.NoError(t, err)
	assert.InDelta(t, d1, d2, 0.0001)
}

func TestCosineDistanceIsSymmetric(t *testing.T) {
	a := VectorPointer{Dims: 3, Data: []float32{1, 2, 3}}
	b := VectorPointer{Dims: 3, Data: []float32{4, 5, 6}}
	d1, err := CosineDistance(a, b)
	require.NoError(t, err)
	d2, err := CosineDistance(b, a)
	require.NoError(t, err)
	assert.InDelta(t, d1, d2, 0.0001)
}

func TestCosineDistanceRange(t *testing.T) {
	// cosine distance must be in [0, 2]
	pairs := [][2]VectorPointer{
		{{Dims: 2, Data: []float32{1, 0}}, {Dims: 2, Data: []float32{0, 1}}},
		{{Dims: 2, Data: []float32{1, 1}}, {Dims: 2, Data: []float32{-1, 1}}},
		{{Dims: 2, Data: []float32{3, 4}}, {Dims: 2, Data: []float32{-3, -4}}},
	}
	for _, pair := range pairs {
		dist, err := CosineDistance(pair[0], pair[1])
		require.NoError(t, err)
		assert.True(t, dist >= 0 && dist <= 2+1e-6, "cosine distance %v out of [0,2]", dist)
	}
}

// Ensure float32 overflow pages round-trip correctly for a small vector.
func TestVectorOverflowRoundTrip(t *testing.T) {
	pager := newMockTxPager()
	ctx := context.Background()
	col := Column{Name: "emb", Kind: Vector, Size: 4}

	vp := VectorPointer{Dims: 4, Data: []float32{1.1, 2.2, 3.3, 4.4}}
	row := Row{
		Columns: []Column{col},
		Values:  []OptionalValue{{Valid: true, Value: vp}},
	}

	var err error
	row, err = row.storeOverflowVectors(ctx, pager)
	require.NoError(t, err)

	stored := row.Values[0].Value.(VectorPointer)
	assert.NotEqual(t, PageIndex(0), stored.FirstPage, "FirstPage must be set after storing")

	// Simulate a fresh read from disk by clearing in-memory data.
	stored.Data = nil
	row.Values[0] = OptionalValue{Valid: true, Value: stored}

	row, err = row.readOverflowVectors(ctx, pager)
	require.NoError(t, err)

	loaded := row.Values[0].Value.(VectorPointer)
	assert.Equal(t, uint32(4), loaded.Dims)
	for i, f := range []float32{1.1, 2.2, 3.3, 4.4} {
		assert.InDelta(t, f, loaded.Data[i], 0.0001, "component %d mismatch", i)
	}
}

func TestVectorOverflowMultiPage(t *testing.T) {
	pager := newMockTxPager()
	ctx := context.Background()

	// MaxOverflowPageData / 4 = ~1021 floats per page; use 2500 to span 3 pages.
	dims := uint32(2500)
	data := make([]float32, dims)
	for i := range data {
		data[i] = float32(i) * 0.001
	}
	vp := VectorPointer{Dims: dims, Data: data}

	col := Column{Name: "big", Kind: Vector, Size: dims}
	row := Row{
		Columns: []Column{col},
		Values:  []OptionalValue{{Valid: true, Value: vp}},
	}

	var err error
	row, err = row.storeOverflowVectors(ctx, pager)
	require.NoError(t, err)

	stored := row.Values[0].Value.(VectorPointer)
	stored.Data = nil
	row.Values[0] = OptionalValue{Valid: true, Value: stored}

	row, err = row.readOverflowVectors(ctx, pager)
	require.NoError(t, err)

	loaded := row.Values[0].Value.(VectorPointer)
	assert.Equal(t, dims, loaded.Dims)
	assert.Len(t, loaded.Data, int(dims))
	for i, f := range data {
		assert.InDelta(t, f, loaded.Data[i], 0.0001, "component %d mismatch", i)
	}
}

// TestL2DistanceDimensionMismatch checks the correct error is returned.
func TestL2DistanceDimensionMismatch(t *testing.T) {
	a := VectorPointer{Dims: 3, Data: []float32{1, 2, 3}}
	b := VectorPointer{Dims: 2, Data: []float32{1, 2}}
	_, err := L2Distance(a, b)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dimension mismatch")
}

func TestCosineDistanceDimensionMismatch(t *testing.T) {
	a := VectorPointer{Dims: 3, Data: []float32{1, 2, 3}}
	b := VectorPointer{Dims: 2, Data: []float32{1, 2}}
	_, err := CosineDistance(a, b)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dimension mismatch")
}

// TestVectorUpdateOverflow_ReusesPagesInPlace verifies that updateOverflow rewrites
// data into the same overflow pages without allocating new pages or freeing old ones.
func TestVectorUpdateOverflow_ReusesPagesInPlace(t *testing.T) {
	pager := newMockTxPager()
	ctx := context.Background()

	orig := VectorPointer{Dims: 4, Data: []float32{1.0, 2.0, 3.0, 4.0}}
	require.NoError(t, orig.storeOverflow(ctx, pager))

	allocatedAfterStore := pager.nextIdx
	oldFirstPage := orig.FirstPage
	require.NotEqual(t, PageIndex(0), oldFirstPage)

	updated := VectorPointer{Dims: 4, Data: []float32{5.0, 6.0, 7.0, 8.0}}
	require.NoError(t, updated.updateOverflow(ctx, pager, oldFirstPage))

	// Same page index reused — no new allocations, no pages freed.
	assert.Equal(t, allocatedAfterStore, pager.nextIdx, "no new pages should be allocated")
	assert.Empty(t, pager.freedPages, "no pages should be freed")
	assert.Equal(t, oldFirstPage, updated.FirstPage, "FirstPage must point to the reused page")

	// Read back and verify new data.
	updated.Data = nil
	updated, err := updated.readOverflow(ctx, pager)
	require.NoError(t, err)
	for i, want := range []float32{5.0, 6.0, 7.0, 8.0} {
		assert.InDelta(t, want, updated.Data[i], 0.0001, "component %d", i)
	}
}

// TestVectorUpdateOverflow_FallbackWhenNoOldChain verifies that updateOverflow
// falls through to storeOverflow when oldFirstPage == 0.
func TestVectorUpdateOverflow_FallbackWhenNoOldChain(t *testing.T) {
	pager := newMockTxPager()
	ctx := context.Background()

	vp := VectorPointer{Dims: 3, Data: []float32{1.0, 2.0, 3.0}}
	require.NoError(t, vp.updateOverflow(ctx, pager, 0))

	assert.NotEqual(t, PageIndex(0), vp.FirstPage, "new pages must be allocated when no old chain")
	assert.Empty(t, pager.freedPages)
}

// TestVectorUpdateOverflow_MultiPage verifies page reuse across a multi-page chain.
func TestVectorUpdateOverflow_MultiPage(t *testing.T) {
	pager := newMockTxPager()
	ctx := context.Background()

	// 2500 floats spans 3 overflow pages.
	dims := uint32(2500)
	origData := make([]float32, dims)
	for i := range origData {
		origData[i] = float32(i)
	}
	orig := VectorPointer{Dims: dims, Data: origData}
	require.NoError(t, orig.storeOverflow(ctx, pager))

	allocatedAfterStore := pager.nextIdx
	oldFirstPage := orig.FirstPage

	newData := make([]float32, dims)
	for i := range newData {
		newData[i] = float32(dims - uint32(i))
	}
	updated := VectorPointer{Dims: dims, Data: newData}
	require.NoError(t, updated.updateOverflow(ctx, pager, oldFirstPage))

	assert.Equal(t, allocatedAfterStore, pager.nextIdx, "no new pages allocated for same-size update")
	assert.Empty(t, pager.freedPages)
	assert.Equal(t, oldFirstPage, updated.FirstPage)

	updated.Data = nil
	updated, err := updated.readOverflow(ctx, pager)
	require.NoError(t, err)
	for i, want := range newData {
		assert.InDelta(t, want, updated.Data[i], 0.0001, "component %d", i)
	}
}

// TestUpdateOverflowVectors_OnlyChangedColumns verifies that updateOverflowVectors
// only touches columns listed in changedCols, leaving unchanged columns alone.
func TestUpdateOverflowVectors_OnlyChangedColumns(t *testing.T) {
	pager := newMockTxPager()
	ctx := context.Background()

	col1 := Column{Name: "v1", Kind: Vector, Size: 2}
	col2 := Column{Name: "v2", Kind: Vector, Size: 2}

	vp1 := VectorPointer{Dims: 2, Data: []float32{1.0, 2.0}}
	vp2 := VectorPointer{Dims: 2, Data: []float32{3.0, 4.0}}

	oldRow := Row{
		Columns: []Column{col1, col2},
		Values: []OptionalValue{
			{Valid: true, Value: vp1},
			{Valid: true, Value: vp2},
		},
	}
	var err error
	oldRow, err = oldRow.storeOverflowVectors(ctx, pager)
	require.NoError(t, err)

	allocatedAfterStore := pager.nextIdx

	oldVP1 := oldRow.Values[0].Value.(VectorPointer)
	oldVP2 := oldRow.Values[1].Value.(VectorPointer)

	// New row: v1 changes, v2 is unchanged.
	newVP1 := VectorPointer{Dims: 2, Data: []float32{9.0, 10.0}}
	newRow := Row{
		Columns: []Column{col1, col2},
		Values: []OptionalValue{
			{Valid: true, Value: newVP1},
			{Valid: true, Value: oldVP2},
		},
	}
	changedCols := map[string]Column{"v1": col1}

	newRow, err = newRow.updateOverflowVectors(ctx, pager, oldRow, changedCols)
	require.NoError(t, err)

	// Only v1 was updated — no new pages, v2's FirstPage untouched.
	assert.Equal(t, allocatedAfterStore, pager.nextIdx, "no new pages for same-size update")
	assert.Empty(t, pager.freedPages)

	updatedVP1 := newRow.Values[0].Value.(VectorPointer)
	assert.Equal(t, oldVP1.FirstPage, updatedVP1.FirstPage, "v1 must reuse old pages")

	updatedVP2 := newRow.Values[1].Value.(VectorPointer)
	assert.Equal(t, oldVP2.FirstPage, updatedVP2.FirstPage, "v2 FirstPage unchanged")
}

// TestFormatVectorRoundTrip verifies that FormatVector then ParseVectorLiteral is lossless
// within float32 precision.
func TestFormatVectorRoundTrip(t *testing.T) {
	original := []float32{-1.5, 0, 3.14159, math.MaxFloat32 / 2}
	vp := VectorPointer{Dims: uint32(len(original)), Data: original}
	s := FormatVector(vp)
	parsed, err := ParseVectorLiteral(s)
	require.NoError(t, err)
	assert.Equal(t, vp.Dims, parsed.Dims)
	for i, f := range original {
		assert.InDelta(t, f, parsed.Data[i], math.Abs(float64(f))*1e-5+1e-10, "component %d", i)
	}
}
