---
name: Pager Cache and Locking Strategy
description: Sparse page array + LRU cache; minimise lock hold time by doing I/O outside the lock
type: standard
---

# Pager Cache and Locking Strategy

`pagerImpl` maintains an in-memory page cache with LRU eviction.

## Data structures

- `pages []*Page` — sparse array indexed by `PageIndex`. `nil` = evicted or not yet loaded.
- `lruCache LRUCache[PageIndex]` — tracks access order; default capacity = 2000 pages.
- `bufferPool *sync.Pool` — reuses page-sized `[]byte` slices to reduce GC pressure.

## Locking strategy

`GetPage` minimises lock hold time:
1. Read lock (fast path): if page exists in `pages[]`, return it.
2. No lock: read file + unmarshal (I/O and CPU work outside lock).
3. Write lock (slow path): double-check, evict if full, insert page, update LRU.

`FlushBatch` follows the same pattern: read lock to snapshot pages, marshal outside lock, then write sequentially.

## Rules

- Never hold a lock while doing file I/O or unmarshaling.
- Page 0 always gets `GetAndPromote` to stay cached (it is the root and accessed on every operation).
- `bufferPool` buffers are zeroed before marshal and returned to the pool with `defer`.
