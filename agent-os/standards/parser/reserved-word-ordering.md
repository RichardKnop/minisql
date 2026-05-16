---
name: Reserved Word Ordering (Longest-Match)
description: reservedWords slice must order multi-word/longer tokens before shorter prefix matches to avoid silent mis-parses
type: standard
---

# Reserved Word Ordering (Longest-Match)

`reservedWords` in `parser.go` is scanned linearly — first match wins.
Multi-word and longer tokens MUST appear before any prefix they share with a shorter token.

## Rule

When adding a new reserved word, place it BEFORE any existing word it starts with:

| New word | Must precede |
|---|---|
| `"UNION ALL"` | `"UNION"` |
| `"->>"` | `"->"`, `"-"` |
| `"IS NOT NULL"` | `"IS NULL"` |
| `"DO UPDATE"` | `"DO NOTHING"` |
| `"EXPLAIN ANALYZE"` | `"EXPLAIN"` |

## Failure mode

Wrong order causes a **silent mis-parse** — no error is raised. The shorter token
matches first, and the remaining characters become a spurious identifier or
trigger an unrelated step. The test suite will catch it only if there is a test
for that exact syntax.

## Adding a new reserved word

1. Identify every existing word the new token starts with.
2. Insert the new word **above** all of them in the slice.
3. Add a parser test that uses the new keyword in a full SQL statement.
