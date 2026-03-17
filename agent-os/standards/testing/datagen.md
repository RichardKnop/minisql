---
name: dataGen Test Data Factory
description: Row generator helpers, uniqueness guarantees, and naming convention for new entity types
type: standard
---

# dataGen Test Data Factory

Both the unit and e2e test layers use a `dataGen` wrapper around `gofakeit.Faker`.

```go
var gen = newDataGen(uint64(time.Now().Unix()))
```

## Row generators

| Method | Columns used | Uniqueness guaranteed |
|---|---|---|
| `gen.Row()` | `testColumns` | unique `id` + unique `email` |
| `gen.MediumRow()` | `testMediumColumns` | unique `id` + unique `email` |
| `gen.BigRow()` | `testBigColumns` | unique `id` + unique `email` |
| `gen.Rows(n)` / `MediumRows(n)` / `BigRows(n)` | — | batch; uniqueness enforced with retry loop |

## Rules

- Use `gen.Rows(n)` to get a batch with guaranteed unique IDs and emails rather than generating rows one-by-one.
- When adding a new test entity type, follow the same `Entity()` / `Entities(n)` pattern with a uniqueness guard.
- The seed is time-based so tests are non-deterministic across runs — if a test needs a fixed seed, pass a constant to `newDataGen`.
