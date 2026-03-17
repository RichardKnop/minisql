---
name: Reserved Words Tokenizer
description: How the SQL tokenizer recognises keywords and rules for adding new ones
type: standard
---

# Reserved Words Tokenizer

`reservedWords` in `parser.go` is the single source of truth for all SQL keywords. `peekWithLength()` scans this list on every token.

## Rules

- **Add new keywords here first** — the parser cannot recognise a token it cannot peek.
- **Order matters when one token is a strict prefix of another.**
  - `">="` before `">"`, `"PRIMARY KEY AUTOINCREMENT"` before `"PRIMARY KEY"`.
  - Wrong order → shorter token wins and the longer form is never matched.
- Multi-word tokens include their trailing space/paren (e.g. `"IN ("`, `"INSERT INTO"`).
- A boundary check prevents identifier-like tokens (e.g. `DESC`) matching mid-word; single-char tokens are exempt.

## Adding a new keyword

1. Add the string to `reservedWords` — if it shares a prefix with an existing shorter token, place it before that token.
2. Add the matching `case` in the relevant `doParseXxx` switch.
3. Add a parser test covering the new syntax.
