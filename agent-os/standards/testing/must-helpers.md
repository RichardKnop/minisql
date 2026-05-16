---
name: mustXxx Test Helpers
description: Pattern for wrapping repeated DML calls in test helpers; naming convention and signature
type: standard
---

# mustXxx Test Helpers

Any DML operation used more than twice in test bodies should be extracted into a
`mustXxx` helper that calls `t.Fatal` on error.

## Pattern

```go
func mustInsert(ctx context.Context, t *testing.T, table *Table, txManager *TransactionManager, stmt Statement) {
    t.Helper()
    err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
        _, err := table.Insert(ctx, stmt)
        return err
    })
    require.NoError(t, err)
}
```

## Rules

- Always call `t.Helper()` as the first line — error lines point to the caller, not the helper.
- Always use `require.NoError` (fatal), not `assert.NoError` (non-fatal).
- Prefix with `must` to signal: "this must succeed or the test stops".
- Wrap `ExecuteInTransaction` inside the helper — callers should not manage transactions manually.
- If the operation returns a useful result (e.g. `StatementResult`), return it from the helper.
