---
name: WHERE / Expression Parsing (Recursive Descent)
description: Entry points, mandatory DNF normalisation, BETWEEN AND handling, and expression operator precedence
type: standard
---

# WHERE / Expression Parsing (Recursive Descent)

WHERE conditions and scalar expressions use a **recursive-descent parser** that
runs inside the step machine — it is NOT driven by step constants.

## Entry points

| Function | Parses |
|---|---|
| `parseCondExpr()` | Full WHERE clause (OR level, lowest precedence) |
| `parseAndExpr()` | AND-chained conditions |
| `parsePrimaryCondExpr()` | Parenthesised group or single leaf |
| `parseLeafCondition()` | Single `field op value` condition |
| `parseExpr()` | Scalar arithmetic / JSON / function expression |

## DNF normalisation — mandatory

After `parseCondExpr()` the result MUST be normalised to DNF via `node.ToDNF()`:

```go
node, err := p.parseCondExpr()
// ...
p.Conditions = node.ToDNF()
```

All downstream code (evaluation in `row.go`, index scan in `query_plan.go`,
predicate push-down) iterates over `stmt.Conditions` as `[][]Condition` (DNF).
A raw `*ConditionNode` is never consumed downstream — passing it unwrapped causes
panics or silent mis-evaluation.

## BETWEEN's syntactic AND

`parseCondBetweenValues` consumes the `AND` keyword between BETWEEN bounds.
This prevents `parseAndExpr` from treating it as a logical AND and splitting
the condition in two. Never remove this consume step.

## Expression precedence (highest to lowest)

1. Unary minus, literals, function calls, parenthesised sub-expressions (`parseFactor`)
2. JSON path `->` / `->>` (`parseJSONExpr`)
3. `*` `/` (`parseTerm`)
4. `+` `-` (`parseExpr`)
