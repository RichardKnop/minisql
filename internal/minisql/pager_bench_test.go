package minisql

import (
	"context"
	"fmt"
	"os"
	"testing"
)

// BenchmarkPageAccess measures page access performance with different cache sizes
func BenchmarkPageAccess(b *testing.B) {
	cacheSizes := []int{10, 50, 100, 500, 1000}

	for _, cacheSize := range cacheSizes {
		b.Run(fmt.Sprint(cacheSize), func(b *testing.B) {
			// Create temp file
			dbFile, err := os.CreateTemp(".", "bench_*.db")
			if err != nil {
				b.Fatal(err)
			}
			defer os.Remove(dbFile.Name())
			defer dbFile.Close()

			// Create pager
			aPager, err := NewPager(dbFile, PageSize, cacheSize)
			if err != nil {
				b.Fatal(err)
			}

			ctx := context.Background()
			columns := []Column{
				{Kind: Int8, Size: 8},
				{Kind: Varchar, Size: 100},
			}

			// Create 200 pages
			numPages := 200
			for i := range numPages {
				leafNode := NewLeafNode()
				if i == 0 {
					leafNode.Header.Header.IsRoot = true
				}
				aPager.pages = append(aPager.pages, &Page{
					Index:    PageIndex(i),
					LeafNode: leafNode,
				})
			}
			aPager.totalPages = uint32(numPages)

			// Flush all pages to disk
			for i := range numPages {
				if err := aPager.Flush(ctx, PageIndex(i)); err != nil {
					b.Fatal(err)
				}
			}

			// Reset pager with specific cache size
			dbFile.Seek(0, 0)
			aPager, err = NewPager(dbFile, PageSize, cacheSize)
			if err != nil {
				b.Fatal(err)
			}

			tablePager := aPager.ForTable(columns)

			b.ResetTimer()

			// Benchmark: access pages in sequence multiple times
			// This simulates table scans
			for i := 0; i < b.N; i++ {
				pageIdx := PageIndex(i % numPages)
				_, err := tablePager.GetPage(ctx, pageIdx)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSequentialScan simulates a full table scan
func BenchmarkSequentialScan(b *testing.B) {
	cacheSizes := []int{50, 1000}

	for _, cacheSize := range cacheSizes {
		b.Run(fmt.Sprint(cacheSize), func(b *testing.B) {
			dbFile, err := os.CreateTemp(".", "bench_*.db")
			if err != nil {
				b.Fatal(err)
			}
			defer os.Remove(dbFile.Name())
			defer dbFile.Close()

			aPager, err := NewPager(dbFile, PageSize, cacheSize)
			if err != nil {
				b.Fatal(err)
			}

			ctx := context.Background()
			columns := []Column{
				{Kind: Int8, Size: 8},
				{Kind: Varchar, Size: 100},
			}

			// Create 100 pages
			numPages := 100
			for i := range numPages {
				leafNode := NewLeafNode()
				if i == 0 {
					leafNode.Header.Header.IsRoot = true
				}
				aPager.pages = append(aPager.pages, &Page{
					Index:    PageIndex(i),
					LeafNode: leafNode,
				})
			}
			aPager.totalPages = uint32(numPages)

			for i := range numPages {
				if err := aPager.Flush(ctx, PageIndex(i)); err != nil {
					b.Fatal(err)
				}
			}

			dbFile.Seek(0, 0)
			aPager, err = NewPager(dbFile, PageSize, cacheSize)
			if err != nil {
				b.Fatal(err)
			}

			tablePager := aPager.ForTable(columns)

			b.ResetTimer()

			// Benchmark: full sequential scans
			for i := 0; i < b.N; i++ {
				for pageIdx := 0; pageIdx < numPages; pageIdx++ {
					_, err := tablePager.GetPage(ctx, PageIndex(pageIdx))
					if err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
