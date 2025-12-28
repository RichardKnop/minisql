package minisql

import (
	"context"
	"os"
	"testing"
)

// BenchmarkFlushSequential benchmarks flushing pages one at a time (old approach)
func BenchmarkFlushSequential(b *testing.B) {
	file, err := os.CreateTemp("", "bench_sequential_*.db")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(file.Name())
	defer file.Close()

	pager, err := NewPager(file, PageSize, 1000)
	if err != nil {
		b.Fatal(err)
	}

	// Create some test pages
	pages := make([]*Page, 20)
	for i := range pages {
		pages[i] = &Page{
			Index:    PageIndex(i),
			LeafNode: NewLeafNode(),
		}
		pager.SavePage(context.Background(), PageIndex(i), pages[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Flush pages one at a time (old approach)
		for j := 0; j < len(pages); j++ {
			if err := pager.Flush(context.Background(), PageIndex(j)); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkFlushBatch benchmarks batch flushing (new approach)
func BenchmarkFlushBatch(b *testing.B) {
	file, err := os.CreateTemp("", "bench_batch_*.db")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(file.Name())
	defer file.Close()

	pager, err := NewPager(file, PageSize, 1000)
	if err != nil {
		b.Fatal(err)
	}

	// Create some test pages
	pages := make([]*Page, 20)
	pageIndices := make([]PageIndex, 20)
	for i := range pages {
		pages[i] = &Page{
			Index:    PageIndex(i),
			LeafNode: NewLeafNode(),
		}
		pageIndices[i] = PageIndex(i)
		pager.SavePage(context.Background(), PageIndex(i), pages[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Flush all pages in batch (new approach)
		if err := pager.FlushBatch(context.Background(), pageIndices); err != nil {
			b.Fatal(err)
		}
	}
}
