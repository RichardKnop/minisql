package bloom

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_OptimalSizing(t *testing.T) {
	t.Parallel()
	f := New(1000, 0.01)
	require.NotNil(t, f)
	// m must be a multiple of 64.
	assert.Equal(t, uint64(0), f.M()%64)
	// k must be at least 1.
	assert.GreaterOrEqual(t, f.K(), uint(1))
}

func TestNew_ZeroN_Clamped(t *testing.T) {
	t.Parallel()
	// n=0 should be clamped to 1, not panic.
	f := New(0, 0.01)
	require.NotNil(t, f)
	assert.Greater(t, f.M(), uint64(0))
}

func TestNew_BadFPRate_Clamped(t *testing.T) {
	t.Parallel()
	// Invalid p values fall back to 0.01.
	f1 := New(100, 0)
	f2 := New(100, 1.0)
	f3 := New(100, -0.5)
	fRef := New(100, 0.01)
	assert.Equal(t, fRef.M(), f1.M())
	assert.Equal(t, fRef.M(), f2.M())
	assert.Equal(t, fRef.M(), f3.M())
}

func TestMayContain_EmptyFilter(t *testing.T) {
	t.Parallel()
	f := New(100, 0.01)
	assert.False(t, f.MayContain([]byte("hello")))
	assert.False(t, f.MayContain([]byte("world")))
	assert.False(t, f.MayContain([]byte("")))
}

func TestAdd_MayContain_NoFalseNegatives(t *testing.T) {
	t.Parallel()
	f := New(200, 0.01)
	keys := make([]string, 200)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
		f.Add([]byte(keys[i]))
	}
	for _, k := range keys {
		assert.True(t, f.MayContain([]byte(k)), "key %q must be present", k)
	}
}

func TestAdd_EmptyKey(t *testing.T) {
	t.Parallel()
	f := New(10, 0.01)
	f.Add([]byte(""))
	assert.True(t, f.MayContain([]byte("")))
}

func TestFalsePositiveRate_WithinBounds(t *testing.T) {
	t.Parallel()
	const n = 10_000
	const targetFP = 0.01
	f := New(n, targetFP)

	for i := range n {
		f.Add(fmt.Appendf(nil, "item-%d", i))
	}

	// Probe with keys that were never added.
	fp := 0
	const probeCount = 100_000
	for i := n; i < n+probeCount; i++ {
		if f.MayContain(fmt.Appendf(nil, "item-%d", i)) {
			fp++
		}
	}
	fpRate := float64(fp) / probeCount
	// Allow 5× the target rate to account for double-hashing overhead vs.
	// the theoretical k-independent-function bound.
	assert.LessOrEqual(t, fpRate, targetFP*5,
		"false positive rate %.4f exceeds 5× target %.4f", fpRate, targetFP)
}

func TestOptimalM(t *testing.T) {
	t.Parallel()
	tests := []struct {
		n    uint
		p    float64
		wantAtLeast uint64
	}{
		{1000, 0.01, 9000},   // theoretical ~9585 bits
		{1000, 0.001, 14000}, // theoretical ~14378 bits
		{100, 0.05, 600},     // theoretical ~623 bits
	}
	for _, tt := range tests {
		m := optimalM(tt.n, tt.p)
		assert.Equal(t, uint64(0), m%64, "m must be multiple of 64")
		assert.GreaterOrEqual(t, m, tt.wantAtLeast,
			"n=%d p=%.3f: m=%d too small (want >= %d)", tt.n, tt.p, m, tt.wantAtLeast)
	}
}

func TestOptimalK(t *testing.T) {
	t.Parallel()
	// k should be approximately (m/n)*ln(2).
	n := uint(1000)
	m := optimalM(n, 0.01)
	k := optimalK(m, n)
	expected := math.Round(float64(m) / float64(n) * math.Ln2)
	assert.Equal(t, uint(expected), k)
	assert.GreaterOrEqual(t, k, uint(1))
}

func TestOptimalK_MinimumOne(t *testing.T) {
	t.Parallel()
	// Even for a tiny m/n ratio, k must be >= 1.
	k := optimalK(64, 1_000_000)
	assert.GreaterOrEqual(t, k, uint(1))
}

func TestMayContain_DistinctKeys(t *testing.T) {
	t.Parallel()
	f := New(3, 0.001)
	f.Add([]byte("alpha"))
	f.Add([]byte("beta"))

	assert.True(t, f.MayContain([]byte("alpha")))
	assert.True(t, f.MayContain([]byte("beta")))
	// "gamma" was never added — very unlikely to be a false positive at 0.1% FP.
	// We can't assert false with certainty but with 0.1% FP and only 2 elements
	// it's overwhelmingly likely to be false.
	_ = f.MayContain([]byte("gamma")) // result is non-deterministic; just must not panic
}

func TestFilter_LargeN(t *testing.T) {
	t.Parallel()
	// Should not panic or OOM for a million-element filter.
	f := New(1_000_000, 0.01)
	require.NotNil(t, f)
	f.Add([]byte("stress-test"))
	assert.True(t, f.MayContain([]byte("stress-test")))
}
