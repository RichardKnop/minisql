# AGENTS.md — `internal/minisql`

## Scope

This directory contains the database engine: storage, transactions, query planning/execution, row encoding, indexes, JSON/full-text/HNSW, constraints, and integrity checks. Correctness matters more than performance wins.

## High-Risk Areas

- WAL, checkpointing, rollback, page cache invalidation, and MVCC snapshot isolation are correctness-critical. Preserve transaction boundaries and rollback behaviour.
- `TransactionalPager.ModifyPage` has in-place and clone paths. In-place writes are valid only when no snapshot reader can observe the old version.
- B+ tree page layout, row/cell encoding, `RowView`, overflow pages, and free-page handling are on-disk format concerns. Update storage standards when the format changes.
- Index DML hooks must stay consistent across INSERT, UPDATE, DELETE, constraint handling, rollback, and persistence after reopen.
- Full-text, JSON inverted, and HNSW indexes have dedicated storage/update paths; test build, insert, update, delete, search, and drop/recreate when touching them.

## Local Standards

- Read the relevant files in `agent-os/standards/storage-engine/`, `agent-os/standards/query-execution/`, or `agent-os/standards/testing/` before changing those subsystems.
- Prefer `RowView`/lazy decoding on read paths unless materialisation is required for sorting, deduplication, mutation, or API return values.
- Keep context as the first argument and pass transaction-bearing contexts through the stack.
- Preserve sentinel errors and `%w` wrapping so callers can use `errors.Is` and `errors.As`.
- Do not hand-edit `mocks_test.go`; update interfaces in `ports.go`, then regenerate mocks.

## Validation

- Targeted engine tests: `LOG_LEVEL=warn go test ./internal/minisql/... -run '<TestName>' -count=1`
- Broader engine pass: `LOG_LEVEL=warn go test ./internal/minisql/... -count=1`
- Relevant e2e suite for user-visible SQL behaviour: `LOG_LEVEL=warn go test ./e2e_tests/... -run 'TestTestSuite/Test<Name>' -count=1 -v`
- Storage/WAL/transaction changes should include relevant `tx`, `wal`, `concurrency`, `vacuum`, and integrity-check tests.
- Always run `make lint` before committing.
