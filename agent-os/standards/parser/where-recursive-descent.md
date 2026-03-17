---
name: WHERE Clause Recursive-Descent + DNF
description: Why WHERE uses recursive-descent instead of the state machine, and how to extend it
type: standard
---

# WHERE Clause: Recursive-Descent + DNF

The WHERE clause is **not** parsed by the state machine. It uses a recursive-descent parser instead.

## Why

A linear state machine cannot handle arbitrary parenthesis nesting like `(a AND (b OR c))`. Recursion is required to build the condition tree. DNF normalisation then flattens the tree into `[][]Condition` so all downstream engine code (query planner, row evaluation) is unchanged.

## Call graph

```
doParseWhere
  parseCondExpr        → OR  (lowest precedence)
    parseAndExpr       → AND
      parsePrimaryCondExpr  → parenthesised group or leaf
        parseLeafCondition  → field op value
```

## Rules

- `doParseWhere` consumes `WHERE`, calls `parseCondExpr`, then calls `node.ToDNF()` to produce `[][]Condition`.
- `parseCondBetweenValues` consumes the syntactic `AND` between BETWEEN bounds before `parseAndExpr` sees it — do not add extra `AND` handling.
- Adding a new WHERE operator: add the keyword to `reservedWords`, add a `case` in `parseLeafCondition`, add a value-parsing helper if needed.
- The `ConditionNode` tree lives only during parsing; after `ToDNF()` it is discarded.
