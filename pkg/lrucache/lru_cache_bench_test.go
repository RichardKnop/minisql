package lrucache

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
)

// BenchmarkLRU_SequentialGet benchmarks sequential Get operations
func BenchmarkLRU_SequentialGet(b *testing.B) {
	cache := New[int](1000)

	// Populate cache
	for i := 0; i < 1000; i++ {
		cache.Put(i, mockValue{fmt.Sprintf("value%d", i)}, true)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cache.Get(i % 1000)
	}
}

// BenchmarkLRU_RandomGet benchmarks random Get operations
func BenchmarkLRU_RandomGet(b *testing.B) {
	cache := New[int](1000)

	// Populate cache
	for i := 0; i < 1000; i++ {
		cache.Put(i, mockValue{fmt.Sprintf("value%d", i)}, true)
	}

	// Pre-generate random keys
	keys := make([]int, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = rand.Intn(1000)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cache.Get(keys[i])
	}
}

// BenchmarkLRU_Put benchmarks Put operations
func BenchmarkLRU_Put(b *testing.B) {
	cache := New[int](1000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cache.Put(i%1000, mockValue{fmt.Sprintf("value%d", i)}, true)
	}
}

// BenchmarkLRU_ConcurrentGet benchmarks concurrent Get operations
func BenchmarkLRU_ConcurrentGet(b *testing.B) {
	for _, goroutines := range []int{2, 4, 8, 16} {
		b.Run(fmt.Sprintf("goroutines=%d", goroutines), func(b *testing.B) {
			cache := New[int](1000)

			// Populate cache
			for i := 0; i < 1000; i++ {
				cache.Put(i, mockValue{fmt.Sprintf("value%d", i)}, true)
			}

			b.ResetTimer()
			b.ReportAllocs()

			var wg sync.WaitGroup
			perGoroutine := b.N / goroutines

			for g := 0; g < goroutines; g++ {
				wg.Add(1)
				go func(start int) {
					defer wg.Done()
					for i := 0; i < perGoroutine; i++ {
						cache.Get((start + i) % 1000)
					}
				}(g * perGoroutine)
			}

			wg.Wait()
		})
	}
}

// BenchmarkLRU_ConcurrentMixed benchmarks mixed read/write operations
func BenchmarkLRU_ConcurrentMixed(b *testing.B) {
	for _, goroutines := range []int{2, 4, 8, 16} {
		b.Run(fmt.Sprintf("goroutines=%d", goroutines), func(b *testing.B) {
			cache := New[int](1000)

			// Populate cache
			for i := 0; i < 1000; i++ {
				cache.Put(i, mockValue{fmt.Sprintf("value%d", i)}, true)
			}

			b.ResetTimer()
			b.ReportAllocs()

			var wg sync.WaitGroup
			perGoroutine := b.N / goroutines

			for g := 0; g < goroutines; g++ {
				wg.Add(1)
				go func(start int) {
					defer wg.Done()
					for i := 0; i < perGoroutine; i++ {
						if i%10 == 0 {
							// 10% writes
							cache.Put((start+i)%1000, mockValue{fmt.Sprintf("value%d", i)}, true)
						} else {
							// 90% reads
							cache.Get((start + i) % 1000)
						}
					}
				}(g * perGoroutine)
			}

			wg.Wait()
		})
	}
}

// BenchmarkLRU_HighContention benchmarks with high read contention on hot keys
func BenchmarkLRU_HighContention(b *testing.B) {
	for _, goroutines := range []int{2, 4, 8, 16} {
		b.Run(fmt.Sprintf("goroutines=%d", goroutines), func(b *testing.B) {
			cache := New[int](1000)

			// Populate cache
			for i := 0; i < 1000; i++ {
				cache.Put(i, mockValue{fmt.Sprintf("value%d", i)}, true)
			}

			// Hot keys: first 10 keys get 80% of traffic
			hotKeys := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

			b.ResetTimer()
			b.ReportAllocs()

			var wg sync.WaitGroup
			perGoroutine := b.N / goroutines

			for g := 0; g < goroutines; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < perGoroutine; i++ {
						if i%10 < 8 {
							// 80% access hot keys
							cache.Get(hotKeys[i%len(hotKeys)])
						} else {
							// 20% access cold keys
							cache.Get(10 + (i % 990))
						}
					}
				}()
			}

			wg.Wait()
		})
	}
}

// BenchmarkLRU_Eviction benchmarks eviction behavior
func BenchmarkLRU_Eviction(b *testing.B) {
	cache := New[int](100) // Small cache to force evictions

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cache.Put(i, mockValue{fmt.Sprintf("value%d", i)}, true)
	}
}

// BenchmarkLRU_GetAndPromote benchmarks GetAndPromote for critical pages
func BenchmarkLRU_GetAndPromote(b *testing.B) {
	cache := New[int](1000)

	// Populate cache
	for i := 0; i < 1000; i++ {
		cache.Put(i, mockValue{fmt.Sprintf("value%d", i)}, true)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cache.GetAndPromote(0) // Always access page 0 (like database header)
	}
}

// BenchmarkLRU_Memory benchmarks memory usage
func BenchmarkLRU_Memory(b *testing.B) {
	b.ReportAllocs()

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	cache := New[int](10000)
	for i := 0; i < 10000; i++ {
		cache.Put(i, mockValue{fmt.Sprintf("value%d", i)}, true)
	}

	runtime.GC()
	runtime.ReadMemStats(&m2)

	b.ReportMetric(float64(m2.Alloc-m1.Alloc)/10000, "bytes/entry")
}
