package minisql

// returningColumns resolves the output Column slice for a RETURNING clause.
// Fields containing "*" expand to all table columns.
func returningColumns(fields []Field, tableColumns []Column) []Column {
	out := make([]Column, 0, len(fields))
	for _, f := range fields {
		if f.Name == "*" {
			out = append(out, tableColumns...)
			continue
		}
		for _, col := range tableColumns {
			if col.Name == f.Name {
				out = append(out, col)
				break
			}
		}
	}
	return out
}

// projectReturning projects a row down to the columns requested by a RETURNING
// clause. The returned Row has one Value per field (in field order).
func projectReturning(row Row, fields []Field) (Row, error) {
	cols := make([]Column, 0, len(fields))
	vals := make([]OptionalValue, 0, len(fields))
	for _, f := range fields {
		if f.Name == "*" {
			cols = append(cols, row.Columns...)
			vals = append(vals, row.Values...)
			continue
		}
		val, ok := row.GetValue(f.Name)
		if !ok {
			// Column not in row — return NULL rather than an error so that callers
			// composing rows from partial fetches still work.
			val = OptionalValue{}
		}
		// Find the Column definition for type metadata.
		var col Column
		for _, c := range row.Columns {
			if c.Name == f.Name {
				col = c
				break
			}
		}
		cols = append(cols, col)
		vals = append(vals, val)
	}
	return NewRowWithValues(cols, vals), nil
}
