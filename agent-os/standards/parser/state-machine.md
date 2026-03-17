---
name: Two-Level State Machine Dispatch
description: How the parser delegates statement-specific steps to per-file sub-parsers
type: standard
---

# Two-Level State Machine Dispatch

The parser uses a two-level dispatch pattern:

1. `doParse()` in `parser.go` loops over the SQL string and switches on `p.step`, grouping steps by statement type and delegating to a sub-parser.
2. Each sub-parser (`doParseSelect`, `doParseInsert`, `doParseUpdate`, `doParseDelete`, `doParseCreateTable`, etc.) lives in its own file and switches on `p.step` again to handle its individual steps.

This split keeps all steps for a given statement type co-located in one file.

## Rules

- **New statement type** → new file `parser/doparse_xxx.go`, new `stepXxx` iota constants, new case group in `doParse()`.
- **New step within a statement** → add the `stepXxx` iota constant, add the case to the sub-parser's switch, and update the previous step to transition to it.
- Steps are `iota` constants in `parser.go`; they must be grouped together (all `stepCreate*`, then all `stepDrop*`, etc.) for readability.
- Sub-parsers must always set `p.step` before returning `nil` to advance the state machine; forgetting this causes an infinite loop.
