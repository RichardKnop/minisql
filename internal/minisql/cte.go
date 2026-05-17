package minisql

import (
	"context"
	"fmt"
)

type cteRegistryKey struct{}

func ctxWithCTERegistry(ctx context.Context, registry map[string]*Table) context.Context {
	return context.WithValue(ctx, cteRegistryKey{}, registry)
}

func cteFromContext(ctx context.Context, name string) (*Table, bool) {
	reg, ok := ctx.Value(cteRegistryKey{}).(map[string]*Table)
	if !ok {
		return nil, false
	}
	t, ok := reg[name]
	return t, ok
}

// executeCTESelect handles WITH … SELECT statements.
// Each CTE body is executed in declaration order; results are materialised
// into virtual tables and registered in the context so subsequent CTEs and
// the main query can reference them by name.
func (d *Database) executeCTESelect(ctx context.Context, stmt Statement) (StatementResult, error) {
	registry := make(map[string]*Table, len(stmt.CTEs))

	for _, cte := range stmt.CTEs {
		// Each CTE body executes with the current registry in context,
		// enabling CTE-to-CTE references (cte2 can SELECT FROM cte1).
		cteCtx := ctxWithCTERegistry(ctx, registry)
		result, err := d.ExecuteStatement(cteCtx, *cte.Body)
		if err != nil {
			return StatementResult{}, fmt.Errorf("CTE %q: %w", cte.Name, err)
		}
		// Fast path: steal the pre-projected slice directly when the result was
		// produced by selectStreamingDirect — avoids a second heap allocation of
		// the same rows when draining the iterator.
		var rows []Row
		if result.rawRows != nil {
			rows = result.rawRows
		} else {
			for result.Rows.Next(cteCtx) {
				rows = append(rows, result.Rows.Row())
			}
			if err := result.Rows.Err(); err != nil {
				return StatementResult{}, fmt.Errorf("CTE %q: reading rows: %w", cte.Name, err)
			}
		}
		vt := newVirtualTable(d.logger, cte.Name, result.Columns, rows)
		// Use the database's locked provider so JOINs inside the main query can
		// resolve both real tables and CTE virtual tables (via context registry).
		vt.provider = d.lockedProvider
		registry[cte.Name] = vt
	}

	mainCtx := ctxWithCTERegistry(ctx, registry)
	mainStmt := stmt
	mainStmt.CTEs = nil

	// Resolve subqueries in the main WHERE with the CTE-aware context so that
	// conditions like "col IN (SELECT id FROM some_cte)" can be evaluated now
	// that all CTE virtual tables are in the registry.
	if len(mainStmt.Conditions) > 0 {
		resolved, err := d.resolveSubqueries(mainCtx, mainStmt.Conditions)
		if err != nil {
			return StatementResult{}, err
		}
		mainStmt.Conditions = resolved
	}

	// If the main FROM table is a CTE virtual table, strip its alias prefix
	// from all field references (same as executeSelectFromDerivedTable does for
	// derived tables) so that OnlyFields can match plain column names in the
	// virtual table rows.
	if vt, ok := registry[mainStmt.TableName]; ok {
		// Attempt predicate pushdown: if the CTE is the sole FROM source (no
		// JOINs that also reference it), push eligible outer WHERE conditions
		// back into the CTE body and re-materialise with fewer rows.
		if len(mainStmt.Joins) == 0 {
			outerAlias := mainStmt.TableAlias
			if outerAlias == "" {
				outerAlias = mainStmt.TableName
			}
			// Find the CTE body in stmt.CTEs so we can re-execute it.
			for _, cte := range stmt.CTEs {
				if cte.Name != mainStmt.TableName {
					continue
				}
				newBody, remaining := pushIntoInner(mainStmt.Conditions, *cte.Body, outerAlias)
				if len(remaining) < len(mainStmt.Conditions) {
					// At least one group was pushed — re-materialise the CTE.
					cteCtx := ctxWithCTERegistry(ctx, registry)
					result, err := d.ExecuteStatement(cteCtx, newBody)
					if err != nil {
						return StatementResult{}, fmt.Errorf("CTE %q (pushdown): %w", cte.Name, err)
					}
					var rows []Row
					if result.rawRows != nil {
						rows = result.rawRows
					} else {
						for result.Rows.Next(cteCtx) {
							rows = append(rows, result.Rows.Row())
						}
						if err := result.Rows.Err(); err != nil {
							return StatementResult{}, fmt.Errorf("CTE %q (pushdown): reading rows: %w", cte.Name, err)
						}
					}
					vt = newVirtualTable(d.logger, cte.Name, result.Columns, rows)
					vt.provider = d.lockedProvider
					// Update the registry so that plan.Execute (which re-fetches the
					// table by name via provider.GetTable) sees the filtered table,
					// not the original fully-materialised one.
					registry[cte.Name] = vt
					mainStmt.Conditions = remaining
				}
				break
			}
		}

		outer := stripDerivedTableAliasPrefix(mainStmt, mainStmt.TableName)
		outer.Conditions = stripConditionsAlias(mainStmt.Conditions, mainStmt.TableName)
		return vt.Select(mainCtx, outer)
	}

	// Main FROM is a real table. JOINs to CTE virtual tables are resolved
	// by singleTableProvider.GetTable checking the CTE context (see table.go).
	return d.ExecuteStatement(mainCtx, mainStmt)
}
