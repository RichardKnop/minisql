---
name: Parser Test Structure
description: testCase struct pattern, table-driven loop, error assertion via ErrorIs not string comparison
type: standard
---

# Parser Test Structure

Parser tests (`internal/parser/*_test.go`) use a shared `testCase` struct:

```go
type testCase struct {
    Name     string
    SQL      string
    Expected []minisql.Statement
    Err      error
}
```

## Standard loop

```go
testCases := []testCase{
    {Name: "valid SELECT", SQL: "SELECT id FROM t;", Expected: [...]},
    {Name: "missing table", SQL: "SELECT FROM;", Err: errEmptyTableName},
}
for _, tc := range testCases {
    t.Run(tc.Name, func(t *testing.T) {
        stmts, err := New().Parse(context.Background(), tc.SQL)
        assert.ErrorIs(t, err, tc.Err)
        if tc.Err == nil {
            assert.Equal(t, tc.Expected, stmts)
        }
    })
}
```

## Error assertion rules

- Always use `assert.ErrorIs` — never compare error strings.
- `Err` field must be a package-level sentinel (e.g. `errEmptyTableName`).
- `ParseError` wraps the sentinel via `Unwrap()` so `errors.Is` resolves correctly.
- For success cases set `Err: nil`; for error cases leave `Expected` empty.

## New syntax: add at least one positive and one negative testCase

One case that parses successfully, one that exercises each new error path.
