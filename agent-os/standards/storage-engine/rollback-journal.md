---
name: Rollback Journal
description: Write-before protocol, journal-header CRC32 integrity, crash recovery, and context.Background() rule for VACUUM
type: standard
---

# Rollback Journal

A rollback journal (`{dbpath}-journal`) is written before any page modification to enable crash recovery.

## Protocol

1. **Before modifying** any page, write the **original** bytes to the journal via `WritePageBefore`.
2. Finalize the journal header with page count and `Sync()` it to disk before flushing modified database pages.
3. On crash recovery, if the journal file exists and has a valid header/checksum, replay it to restore original page content.
4. Delete the journal file after successful recovery or after a clean commit.

## Integrity

- Journal header contains: magic string `"minisql\n"`, version, page size, number of pages, CRC32 checksum.
- The current implementation validates the journal header fields and header CRC32 only.
- The current implementation does not yet persist a separate commit magic marker; keep standards in sync with code until that changes.

## Rules

- Always call `WritePageBefore` / `WriteDBHeaderBefore` **before** the page is mutated, not after.
- The journal file path is always `{dbpath}-journal` — do not change this convention.
- `context.Background()` (not the request context) must be used when creating temporary databases for operations like VACUUM, to avoid OCC contamination from the calling transaction.
