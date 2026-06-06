# AGENTS.md — `internal/parser`

## Scope

This directory contains the hand-written SQL parser. Parser changes usually require coordinated changes in `internal/minisql/stmt.go`, validation, execution, schema DDL round-tripping, and e2e tests.

## Parser Rules

- Preserve the state-machine plus recursive-descent split. Statement structure is parsed by step functions; WHERE conditions and scalar expressions use recursive descent.
- When adding reserved words, keep longest phrases before shorter prefixes, for example `"DO UPDATE"` before `"DO"` and `"UNION ALL"` before `"UNION"`.
- Avoid adding parser step constants for expression internals. Extend `parseCondExpr`, `parseExpr`, or `parseFuncCall` instead.
- Error messages should remain lower-case, specific, and compatible with existing parser tests.
- After parsing new fields into `Statement`, update `Clone`, validation, preparation, execution, and schema DDL helpers as needed.

## Standards To Read

- `agent-os/standards/parser/reserved-words.md`
- `agent-os/standards/parser/reserved-word-ordering.md`
- `agent-os/standards/parser/state-machine.md`
- `agent-os/standards/parser/step-machine.md`
- `agent-os/standards/parser/where-recursive-descent.md`
- `agent-os/standards/testing/parser-test-structure.md`

## Validation

- Parser package: `LOG_LEVEL=warn go test ./internal/parser/... -count=1`
- Specific parser test: `LOG_LEVEL=warn go test ./internal/parser/... -run '<TestName>' -count=1`
- Add or update e2e tests for any syntax that reaches execution.
- Run `make lint` before committing.
