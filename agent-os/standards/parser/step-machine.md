---
name: Parser Step Machine
description: Step iota constants + doParse*() dispatch pattern; three required changes when adding new SQL syntax
type: standard
---

# Parser Step Machine

The parser is a hand-written state machine. `step` iota constants (in `parser.go`)
represent parse positions; the main `doParse()` loop dispatches to `doParse*()` helpers.

## Adding new syntax — 3 required changes

1. **Define step constants** — add new `stepXxx` values in the `step` iota block,
   grouped with related steps (e.g. all `stepInsert*` together).

2. **Wire into the main switch** — list all new step constants in the correct
   `case` block inside `doParse()`. Steps missing here are silently skipped.

3. **Implement transitions** — inside the relevant `doParse*()` function, set
   `p.step = stepNext` at every exit path. A missing transition leaves `p.step`
   unchanged and the loop will re-enter the same step forever.

## Step naming convention

```
stepVerbNoun          // e.g. stepInsertFields
stepVerbNounModifier  // e.g. stepInsertFieldsCommaOrClosingParens
```

## Common mistakes (both happen equally)

- **Dispatch omission**: new step defined + implemented, but not listed in the
  `case` in `doParse()` — the step is silently never reached.
- **Wrong transition**: `p.step` set to a plausible-but-incorrect next step,
  causing a mis-parse with no error at the step boundary.

Always add a parser-level test that exercises the full new statement end-to-end.
