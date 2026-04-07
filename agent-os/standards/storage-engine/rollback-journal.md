---
name: Rollback Journal
description: Write-before protocol, finalized-header completeness signaling, crash recovery, and context.Background() rule for VACUUM
type: standard
---

# Rollback Journal

A rollback journal (`{dbpath}-journal`) is written before any page modification to enable crash recovery.

## Protocol

1. **Before modifying** any page, write the **original** bytes to the journal via `WritePageBefore`.
2. Finalize the journal header with page count and `Sync()` it to disk before flushing modified database pages.
3. On crash recovery, only replay a journal whose finalized header is valid and whose file length exactly matches the finalized contents.
4. Delete the journal file after successful recovery or after a clean commit.

## Integrity

- Journal header contains: magic string `"minisql\n"`, version, page size, number of pages, CRC32 checksum.
- The finalized journal header is the current completeness signal. MiniSQL does not currently use a separate footer or commit-magic marker.
- Recovery validates header fields, header CRC32, page size compatibility, and exact end-of-file after the finalized journal body.
- If the journal is truncated, corrupt, or has trailing bytes beyond the finalized contents, recovery must fail closed and leave the journal file in place for inspection.

## Rules

- Always call `WritePageBefore` / `WriteDBHeaderBefore` **before** the page is mutated, not after.
- The journal file path is always `{dbpath}-journal` — do not change this convention.
- If recovery semantics change, update README + standards + tests in the same change.
- `context.Background()` (not the request context) must be used when creating temporary databases for operations like VACUUM, to avoid OCC contamination from the calling transaction.
