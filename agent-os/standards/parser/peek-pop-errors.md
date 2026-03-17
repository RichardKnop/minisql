---
name: peek/pop Cursor and Error Conventions
description: How the parser cursor advances, which peek helper to use, and error message format rules
type: standard
---

# peek/pop Cursor and Error Conventions

## Cursor model

`parserItem.i` is the current byte offset into the normalised SQL string. All token reading goes through two methods:

- `peek()` — look-ahead; does NOT advance `p.i`.
- `pop()` — consume the current token and skip trailing whitespace.

`peekWithLength()` resolves tokens in priority order:
1. Reserved words (longest-prefix wins)
2. Quoted string literals (`'...'`)
3. Numbers (int or float)
4. Identifiers

## Helper choice

| Use | When |
|-----|------|
| `peekValue()` | Expecting a literal: bool, int, float, quoted string |
| `peekIdentifierWithLength()` | Expecting a column or table name |
| `peek()` + compare | Expecting a known reserved word |

Using `peekValue()` for an identifier (or vice-versa) silently fails to match and returns a misleading error.

## Error conventions

- All error messages are prefixed with `"at STATEMENT_TYPE: "` (e.g. `"at SELECT: "`, `"at WHERE: "`).
- Package-level `var err... = fmt.Errorf(...)` for known sentinel conditions (e.g. `errEmptyWhereClause`).
- Inline `fmt.Errorf(...)` for context-specific errors that carry dynamic state.
- `validate()` runs **once** at statement end, not inline during parsing.
