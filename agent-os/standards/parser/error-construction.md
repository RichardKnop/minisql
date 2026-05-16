---
name: Parser Error Construction
description: errorf vs wrapErr — when to use each; why raw errors.New is forbidden inside parser methods
type: standard
---

# Parser Error Construction

Two helpers produce `ParseError` (position + near-snippet + message):

| Helper | When to use |
|---|---|
| `p.errorf(format, args...)` | One-off inline error — no sentinel needed |
| `p.wrapErr(sentinel)` | Existing package-level sentinel var — preserves `errors.Is` |

## Rule

- If you're adding a new error condition that **external callers check with `errors.Is`**:
  define a `var errXxx = errors.New(...)` sentinel, then use `p.wrapErr(errXxx)`.
- All other errors: use `p.errorf(...)` directly.

## Never use `fmt.Errorf` or `errors.New` directly inside parser methods

Raw errors lose the position and near-snippet context. Always go through
`p.errorf` or `p.wrapErr`.

## Example

```go
// New sentinel (callers need errors.Is):
var errFooRequired = errors.New("at FOO: bar is required")
// ...
return p.wrapErr(errFooRequired)

// Inline error (no external check needed):
return p.errorf("at FOO: expected %q, got %q", "BAR", p.peek())
```
