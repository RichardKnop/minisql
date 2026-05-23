package minisql

import (
	"context"
	"fmt"
	"sort"
)

// selectWithWindowFuncs is the entry point for SELECT queries that contain at
// least one window function.  It:
//  1. Executes the statement with window-function fields replaced by "*" (and
//     LIMIT/OFFSET stripped) to materialise all base rows.
//  2. For every window-function field, computes a column of values (one per row).
//  3. Projects the final output rows from base + window values.
//  4. Applies stmt-level ORDER BY / DISTINCT / LIMIT / OFFSET.
func (t *Table) selectWithWindowFuncs(ctx context.Context, stmt Statement) (StatementResult, error) {
	// ── Step 1: materialise all base rows ─────────────────────────────────
	baseStmt := stmt
	baseStmt.Fields = []Field{{Name: "*"}}
	baseStmt.Aggregates = nil
	baseStmt.OrderBy = nil  // we will sort after window computation
	baseStmt.Limit = OptionalValue{}
	baseStmt.Offset = OptionalValue{}
	// HasWindowFuncs() on baseStmt returns false because we replaced Fields.

	baseResult, err := t.Select(ctx, baseStmt)
	if err != nil {
		return StatementResult{}, fmt.Errorf("window func base scan: %w", err)
	}
	rows, err := materializeResultRows(ctx, baseResult)
	if err != nil {
		return StatementResult{}, fmt.Errorf("window func materialise: %w", err)
	}

	// ── Step 2: compute window columns ────────────────────────────────────
	// Collect window-function fields and their output positions.
	type wfEntry struct {
		fieldIdx int
		wf       *WindowFunc
	}
	var wfEntries []wfEntry
	for i, f := range stmt.Fields {
		if f.Expr != nil && f.Expr.WindowFunc != nil {
			wfEntries = append(wfEntries, wfEntry{i, f.Expr.WindowFunc})
		}
	}

	// windowCols[wi] holds computed values for wfEntries[wi], indexed by row position.
	windowCols := make([][]OptionalValue, len(wfEntries))
	for wi, entry := range wfEntries {
		windowCols[wi], err = computeWindowColumn(rows, entry.wf)
		if err != nil {
			return StatementResult{}, fmt.Errorf("window func field %d: %w", entry.fieldIdx, err)
		}
	}

	// ── Step 3: project output rows ───────────────────────────────────────
	// Build output column schema from stmt.Fields.
	outCols := make([]Column, len(stmt.Fields))
	for i, f := range stmt.Fields {
		outCols[i] = Column{Name: f.OutputName()}
	}

	// Map window field indices → windowCols index.
	wfFieldToCol := make(map[int]int, len(wfEntries))
	for wi, entry := range wfEntries {
		wfFieldToCol[entry.fieldIdx] = wi
	}

	outRows := make([]Row, len(rows))
	for ri, baseRow := range rows {
		values := make([]OptionalValue, len(stmt.Fields))
		for fi, f := range stmt.Fields {
			if wi, isWin := wfFieldToCol[fi]; isWin {
				values[fi] = windowCols[wi][ri]
				continue
			}
			// Regular field (or expression): delegate to projectRow logic.
			if f.Expr != nil {
				result, evalErr := f.Expr.Eval(baseRow)
				if evalErr != nil {
					return StatementResult{}, fmt.Errorf("eval field %q: %w", f.OutputName(), evalErr)
				}
				if result == nil {
					values[fi] = OptionalValue{Valid: false}
				} else {
					values[fi] = OptionalValue{Value: result, Valid: true}
				}
				continue
			}
			col, idx := baseRow.getColumnQualified(f.AliasPrefix, f.Name)
			if idx >= 0 {
				values[fi] = baseRow.Values[idx]
				_ = col
			} else {
				values[fi] = OptionalValue{Valid: false}
			}
		}
		outRows[ri] = NewRowWithValues(outCols, values)
	}

	// ── Step 4: ORDER BY / DISTINCT / LIMIT / OFFSET ──────────────────────
	if len(stmt.OrderBy) > 0 {
		if err := t.sortRows(outRows, stmt.OrderBy); err != nil {
			return StatementResult{}, err
		}
	}

	if stmt.Distinct {
		requestedFields := make([]Field, len(stmt.Fields))
		copy(requestedFields, stmt.Fields)
		outRows = deduplicateRows(outRows, requestedFields)
	}

	var offset int
	if stmt.Offset.Valid {
		offset = int(stmt.Offset.Value.(int64))
	}
	if offset >= len(outRows) {
		outRows = nil
	} else {
		outRows = outRows[offset:]
	}
	if stmt.Limit.Valid {
		limit := int(stmt.Limit.Value.(int64))
		if limit < len(outRows) {
			outRows = outRows[:limit]
		}
	}

	return StatementResult{
		Columns: outCols,
		Rows:    NewSliceIterator(outRows),
	}, nil
}

// computeWindowColumn computes the output value for window function wf for
// every row.  The returned slice has exactly len(rows) entries in the same
// order as rows.
func computeWindowColumn(rows []Row, wf *WindowFunc) ([]OptionalValue, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	// Build partition groups: map from group-key → sorted list of original indices.
	partitions := buildPartitions(rows, wf.Spec.PartitionBy, wf.Spec.OrderBy)

	out := make([]OptionalValue, len(rows))
	for _, partition := range partitions {
		if err := evalWindowOverPartition(rows, partition, wf, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// partition is a list of row indices belonging to one PARTITION BY group,
// already sorted by the window ORDER BY.
type windowPartition struct {
	indices []int // original row indices, sorted by ORDER BY
}

// buildPartitions groups row indices by PARTITION BY columns and sorts each
// group by ORDER BY columns.
func buildPartitions(rows []Row, partitionBy []string, orderBy []OrderBy) []windowPartition {
	if len(partitionBy) == 0 {
		// All rows in one partition.
		indices := make([]int, len(rows))
		for i := range indices {
			indices[i] = i
		}
		sortPartition(indices, rows, orderBy)
		return []windowPartition{{indices}}
	}

	// Group by partition key.
	order := make([]string, 0, len(rows))
	groups := make(map[string][]int)
	var buf []byte
	for i, row := range rows {
		buf = buf[:0]
		for j, col := range partitionBy {
			if j > 0 {
				buf = append(buf, '\x1f')
			}
			v, _ := row.GetValue(col)
			buf = appendGroupKeyValue(buf, v)
		}
		key := string(buf)
		if _, seen := groups[key]; !seen {
			order = append(order, key)
		}
		groups[key] = append(groups[key], i)
	}

	partitions := make([]windowPartition, len(order))
	for i, key := range order {
		indices := groups[key]
		sortPartition(indices, rows, orderBy)
		partitions[i] = windowPartition{indices}
	}
	return partitions
}

func sortPartition(indices []int, rows []Row, orderBy []OrderBy) {
	if len(orderBy) == 0 {
		return
	}
	sort.SliceStable(indices, func(a, b int) bool {
		ra, rb := rows[indices[a]], rows[indices[b]]
		for _, ob := range orderBy {
			va, _ := ra.getValueQualified(ob.Field.AliasPrefix, ob.Field.Name)
			vb, _ := rb.getValueQualified(ob.Field.AliasPrefix, ob.Field.Name)
			cmp := compareValues(va, vb)
			if cmp == 0 {
				continue
			}
			if ob.Direction == Desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
}

// evalWindowOverPartition computes the window function result for every index
// in partition.indices and writes the values to out[index].
func evalWindowOverPartition(rows []Row, partition windowPartition, wf *WindowFunc, out []OptionalValue) error {
	idx := partition.indices
	n := len(idx)

	switch wf.Kind {
	// ── Ranking functions ─────────────────────────────────────────────────
	case WindowRowNumber:
		for pos, ri := range idx {
			out[ri] = intVal(int64(pos + 1))
		}

	case WindowRank:
		rank := 1
		for pos, ri := range idx {
			if pos > 0 && !peerRows(rows[idx[pos-1]], rows[ri], wf.Spec.OrderBy) {
				rank = pos + 1
			}
			out[ri] = intVal(int64(rank))
		}

	case WindowDenseRank:
		rank := 1
		for pos, ri := range idx {
			if pos > 0 && !peerRows(rows[idx[pos-1]], rows[ri], wf.Spec.OrderBy) {
				rank++
			}
			out[ri] = intVal(int64(rank))
		}

	case WindowNtile:
		buckets := int64(1)
		if wf.Arg != nil {
			if v, ok := wf.Arg.Literal.(int64); ok {
				buckets = v
			}
		}
		if buckets <= 0 {
			buckets = 1
		}
		for pos, ri := range idx {
			bucket := (int64(pos)*buckets)/int64(n) + 1
			out[ri] = intVal(bucket)
		}

	// ── Offset functions ──────────────────────────────────────────────────
	case WindowLag, WindowLead:
		offset := 1
		if wf.Arg2 != nil {
			if wf.Arg2.Literal != nil {
				if v, ok := wf.Arg2.Literal.(int64); ok {
					offset = int(v)
				}
			}
		}
		for pos, ri := range idx {
			var srcPos int
			if wf.Kind == WindowLag {
				srcPos = pos - offset
			} else {
				srcPos = pos + offset
			}
			if srcPos < 0 || srcPos >= n {
				out[ri] = OptionalValue{Valid: false}
				continue
			}
			srcRow := rows[idx[srcPos]]
			v, err := wf.Arg.Eval(srcRow)
			if err != nil {
				return err
			}
			out[ri] = toOptional(v)
		}

	// ── Positional value functions ─────────────────────────────────────────
	case WindowFirstValue:
		for _, ri := range idx {
			v, err := wf.Arg.Eval(rows[idx[0]])
			if err != nil {
				return err
			}
			out[ri] = toOptional(v)
		}

	case WindowLastValue:
		// Default frame for LAST_VALUE: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
		// (PostgreSQL/standard default; full-partition requires explicit ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING.)
		for pos, ri := range idx {
			frameEnd := frameEndPos(pos, n, wf.Spec.Frame, defaultLastValueFrame())
			v, err := wf.Arg.Eval(rows[idx[frameEnd]])
			if err != nil {
				return err
			}
			out[ri] = toOptional(v)
		}

	case WindowNthValue:
		nth := int64(1)
		if wf.Arg2 != nil && wf.Arg2.Literal != nil {
			if v, ok := wf.Arg2.Literal.(int64); ok {
				nth = v
			}
		}
		nthIdx := int(nth) - 1 // zero-based
		for _, ri := range idx {
			if nthIdx < 0 || nthIdx >= n {
				out[ri] = OptionalValue{Valid: false}
				continue
			}
			v, err := wf.Arg.Eval(rows[idx[nthIdx]])
			if err != nil {
				return err
			}
			out[ri] = toOptional(v)
		}

	// ── Aggregate window functions ─────────────────────────────────────────
	case WindowSum:
		return evalAggWindow(rows, idx, wf, out, aggWindowSum)
	case WindowAvg:
		return evalAggWindow(rows, idx, wf, out, aggWindowAvg)
	case WindowCount:
		return evalAggWindow(rows, idx, wf, out, aggWindowCount)
	case WindowMin:
		return evalAggWindow(rows, idx, wf, out, aggWindowMin)
	case WindowMax:
		return evalAggWindow(rows, idx, wf, out, aggWindowMax)

	default:
		return fmt.Errorf("unsupported window function kind %d", wf.Kind)
	}
	return nil
}

// ── Frame helpers ──────────────────────────────────────────────────────────

// defaultLastValueFrame returns the standard ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
func defaultLastValueFrame() *WindowFrame {
	return &WindowFrame{
		Mode:  FrameRows,
		Start: FrameBound{Kind: FrameUnboundedPreceding},
		End:   FrameBound{Kind: FrameCurrentRow},
	}
}

// frameStartPos returns the (inclusive) start index within the partition for
// a given pos (0-based position in partition), given an explicit frame.
// Callers must resolve nil frame to the appropriate default before calling.
func frameStartPos(pos, n int, frame *WindowFrame) int {
	if frame == nil {
		return 0 // UNBOUNDED PRECEDING fallback
	}
	switch frame.Start.Kind {
	case FrameUnboundedPreceding:
		return 0
	case FrameCurrentRow:
		return pos
	case FramePreceding:
		s := pos - frame.Start.Offset
		if s < 0 {
			s = 0
		}
		return s
	case FrameFollowing:
		s := pos + frame.Start.Offset
		if s >= n {
			s = n - 1
		}
		return s
	case FrameUnboundedFollowing:
		return n - 1
	}
	return 0
}

// frameEndPos returns the (inclusive) end index for the current position.
func frameEndPos(pos, n int, frame, def *WindowFrame) int {
	f := frame
	if f == nil {
		f = def
	}
	if f == nil {
		return pos // default: CURRENT ROW
	}
	switch f.End.Kind {
	case FrameUnboundedPreceding:
		return 0
	case FrameCurrentRow:
		return pos
	case FramePreceding:
		e := pos - f.End.Offset
		if e < 0 {
			e = 0
		}
		return e
	case FrameFollowing:
		e := pos + f.End.Offset
		if e >= n {
			e = n - 1
		}
		return e
	case FrameUnboundedFollowing:
		return n - 1
	}
	return pos
}

// ── Aggregate evaluation ───────────────────────────────────────────────────

type aggWindowFn func(rows []Row, idx []int, start, end int, wf *WindowFunc) (OptionalValue, error)

func evalAggWindow(rows []Row, idx []int, wf *WindowFunc, out []OptionalValue, fn aggWindowFn) error {
	n := len(idx)
	// Default frame depends on whether an ORDER BY is present in the window spec:
	//   - No ORDER BY → ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING (entire partition)
	//   - ORDER BY present → ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (running accumulation)
	var defaultFrame *WindowFrame
	if len(wf.Spec.OrderBy) == 0 {
		defaultFrame = &WindowFrame{
			Mode:  FrameRows,
			Start: FrameBound{Kind: FrameUnboundedPreceding},
			End:   FrameBound{Kind: FrameUnboundedFollowing},
		}
	} else {
		defaultFrame = &WindowFrame{
			Mode:  FrameRows,
			Start: FrameBound{Kind: FrameUnboundedPreceding},
			End:   FrameBound{Kind: FrameCurrentRow},
		}
	}
	frame := wf.Spec.Frame
	if frame == nil {
		frame = defaultFrame
	}
	for pos, ri := range idx {
		start := frameStartPos(pos, n, frame)
		end := frameEndPos(pos, n, frame, defaultFrame)
		v, err := fn(rows, idx, start, end, wf)
		if err != nil {
			return err
		}
		out[ri] = v
	}
	return nil
}

func aggWindowSum(rows []Row, idx []int, start, end int, wf *WindowFunc) (OptionalValue, error) {
	var sumI int64
	var sumF float64
	isFloat := false
	hasVal := false

	for pos := start; pos <= end; pos++ {
		v, err := wf.Arg.Eval(rows[idx[pos]])
		if err != nil {
			return OptionalValue{}, err
		}
		if v == nil {
			continue
		}
		hasVal = true
		switch n := v.(type) {
		case int64:
			if isFloat {
				sumF += float64(n)
			} else {
				sumI += n
			}
		case int32:
			if isFloat {
				sumF += float64(n)
			} else {
				sumI += int64(n)
			}
		case float64:
			if !isFloat {
				sumF = float64(sumI)
				isFloat = true
			}
			sumF += n
		}
	}
	if !hasVal {
		return OptionalValue{Valid: false}, nil
	}
	if isFloat {
		return OptionalValue{Value: sumF, Valid: true}, nil
	}
	return intVal(sumI), nil
}

func aggWindowAvg(rows []Row, idx []int, start, end int, wf *WindowFunc) (OptionalValue, error) {
	var sum float64
	count := 0
	for pos := start; pos <= end; pos++ {
		v, err := wf.Arg.Eval(rows[idx[pos]])
		if err != nil {
			return OptionalValue{}, err
		}
		if v == nil {
			continue
		}
		count++
		switch n := v.(type) {
		case int64:
			sum += float64(n)
		case int32:
			sum += float64(n)
		case float64:
			sum += n
		}
	}
	if count == 0 {
		return OptionalValue{Valid: false}, nil
	}
	return OptionalValue{Value: sum / float64(count), Valid: true}, nil
}

func aggWindowCount(rows []Row, idx []int, start, end int, wf *WindowFunc) (OptionalValue, error) {
	if wf.Arg == nil {
		// COUNT(*) — count all rows in frame.
		return intVal(int64(end - start + 1)), nil
	}
	count := int64(0)
	for pos := start; pos <= end; pos++ {
		v, err := wf.Arg.Eval(rows[idx[pos]])
		if err != nil {
			return OptionalValue{}, err
		}
		if v != nil {
			count++
		}
	}
	return intVal(count), nil
}

func aggWindowMin(rows []Row, idx []int, start, end int, wf *WindowFunc) (OptionalValue, error) {
	var minVal any
	for pos := start; pos <= end; pos++ {
		v, err := wf.Arg.Eval(rows[idx[pos]])
		if err != nil {
			return OptionalValue{}, err
		}
		if v == nil {
			continue
		}
		if minVal == nil || compareAny(v, minVal) < 0 {
			minVal = v
		}
	}
	return toOptional(minVal), nil
}

func aggWindowMax(rows []Row, idx []int, start, end int, wf *WindowFunc) (OptionalValue, error) {
	var maxVal any
	for pos := start; pos <= end; pos++ {
		v, err := wf.Arg.Eval(rows[idx[pos]])
		if err != nil {
			return OptionalValue{}, err
		}
		if v == nil {
			continue
		}
		if maxVal == nil || compareAny(v, maxVal) > 0 {
			maxVal = v
		}
	}
	return toOptional(maxVal), nil
}

// ── Small helpers ──────────────────────────────────────────────────────────

func intVal(v int64) OptionalValue { return OptionalValue{Value: v, Valid: true} }

func toOptional(v any) OptionalValue {
	if v == nil {
		return OptionalValue{Valid: false}
	}
	return OptionalValue{Value: v, Valid: true}
}

// peerRows returns true when rowA and rowB are peers according to the window
// ORDER BY (i.e. their ORDER BY values are all equal).  Used by RANK to detect
// group boundaries.
func peerRows(rowA, rowB Row, orderBy []OrderBy) bool {
	if len(orderBy) == 0 {
		return true
	}
	for _, ob := range orderBy {
		va, _ := rowA.getValueQualified(ob.Field.AliasPrefix, ob.Field.Name)
		vb, _ := rowB.getValueQualified(ob.Field.AliasPrefix, ob.Field.Name)
		if compareValues(va, vb) != 0 {
			return false
		}
	}
	return true
}
