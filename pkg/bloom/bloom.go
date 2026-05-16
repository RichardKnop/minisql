// Package bloom provides a space-efficient probabilistic set-membership filter.
// MayContain returns false only when an element is definitely absent; it may
// return true for elements never added (false positives). No false negatives
// are possible.
//
// The filter uses the enhanced double-hashing scheme from Kirsch & Mitzenmacher
// (2006): two base FNV hashes produce k independent bit positions per element
// without needing k separate hash functions.
package bloom

import (
	"hash/fnv"
	"math"
)

// Filter is a Bloom filter backed by a compact bit array.
type Filter struct {
	bits []uint64 // bit array, len = ceil(m/64)
	m    uint64   // total number of bits
	k    uint     // number of hash functions
}

// New returns a Filter sized for n expected elements at false-positive
// probability p. p must be in (0,1); n must be > 0.
func New(n uint, p float64) *Filter {
	if n == 0 {
		n = 1
	}
	if p <= 0 || p >= 1 {
		p = 0.01
	}
	m := optimalM(n, p)
	return &Filter{
		bits: make([]uint64, m/64),
		m:    m,
		k:    optimalK(m, n),
	}
}

// Add inserts data into the filter.
func (f *Filter) Add(data []byte) {
	h1, h2 := baseHashes(data)
	for i := uint(0); i < f.k; i++ {
		bit := (h1 + uint64(i)*h2) % f.m
		f.bits[bit>>6] |= 1 << (bit & 63)
	}
}

// MayContain reports whether data might be in the set. Returns false only when
// data is definitely absent.
func (f *Filter) MayContain(data []byte) bool {
	h1, h2 := baseHashes(data)
	for i := uint(0); i < f.k; i++ {
		bit := (h1 + uint64(i)*h2) % f.m
		if f.bits[bit>>6]&(1<<(bit&63)) == 0 {
			return false
		}
	}
	return true
}

// M returns the total number of bits in the filter.
func (f *Filter) M() uint64 { return f.m }

// K returns the number of hash functions used.
func (f *Filter) K() uint { return f.k }

// baseHashes returns two independent 64-bit hashes of data. h1 is FNV-1a;
// h2 is derived via the Murmur3 64-bit finalizer to ensure good bit diffusion
// and avoid the h2≈0 collapse that degrades double-hashing to a single function.
func baseHashes(data []byte) (uint64, uint64) {
	h := fnv.New64a()
	h.Write(data)
	h1 := h.Sum64()
	h2 := mix64(h1)
	if h2 == 0 {
		h2 = 1 // prevent all k positions from collapsing to h1%m
	}
	return h1, h2
}

// mix64 applies the Murmur3 64-bit finalizer to diffuse all bits of h.
func mix64(h uint64) uint64 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

// optimalM computes the optimal bit-array size for n elements at false-positive
// probability p, rounded up to the nearest multiple of 64.
func optimalM(n uint, p float64) uint64 {
	// Optimal bit count: -n·ln(p) / ln(2)².
	raw := math.Ceil(-float64(n) * math.Log(p) / (math.Ln2 * math.Ln2))
	// Round up to a multiple of 64 so the []uint64 slice has no wasted slots.
	words := uint64(math.Ceil(raw / 64))
	if words == 0 {
		words = 1
	}
	return words * 64
}

// optimalK computes the optimal number of hash functions for a filter of m bits
// and n expected elements.
func optimalK(m uint64, n uint) uint {
	// Optimal hash-function count: (m/n)·ln(2).
	k := math.Round(float64(m) / float64(n) * math.Ln2)
	if k < 1 {
		return 1
	}
	return uint(k)
}
