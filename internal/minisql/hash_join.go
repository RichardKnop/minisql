package minisql

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/RichardKnop/minisql/pkg/bloom"
)

const (
	// bloomFPRate is the target false-positive rate for the per-bucket Bloom
	// filter. At 1%, the filter rejects ~99% of definite misses without a
	// hash-map probe, while adding negligible memory overhead.
	bloomFPRate = 0.01
	// bloomMinN is the minimum expected-element count passed to bloom.New when
	// the inner table has no row-count estimate. Sized for small tables.
	bloomMinN = 512
)

// compactCell stores the raw leaf-cell bytes for one inner build-side row.
// Keeping raw bytes instead of a materialised Row eliminates the per-row
// []OptionalValue allocation during the hash-join build phase.  On probe hit
// the bytes are decoded lazily via NewRowView + MaterializeWithOverflow, so
// only matched rows pay the decoding cost.
// value is a sub-slice of a shared arena; it is safe to read after page
// eviction because the arena holds an independent copy of the bytes.
type compactCell struct {
	value       []byte
	nullBitmask uint64
	key         RowID
}

func (cc compactCell) toCell() Cell {
	return Cell{Value: cc.value, NullBitmask: cc.nullBitmask, Key: cc.key, isOwned: true}
}

// hashJoinBucket is the in-memory hash table built from the inner (build) side
// of a single hash join.  The map key is a null-byte-delimited encoding of the
// join column values.
//
// For regular joins (INNER/LEFT/RIGHT/FULL OUTER) on a sequential inner scan,
// cells holds compact raw-byte copies of inner rows (no []OptionalValue).  The
// legacy rows map is used only when the inner scan is not sequential or its
// filters are not RowView-compatible.
//
// For semi/anti-semi joins only key existence is needed; present stores just the
// set of keys, eliminating the per-key []Row allocation and the cost of keeping
// full inner rows in memory.
//
// innerColumns is kept to construct NULL rows for LEFT JOIN misses.
// filter is a Bloom filter over the same key set used to reject probe keys
// that are definitely not present, avoiding an unnecessary map lookup.
type hashJoinBucket struct {
	rows         map[string][]Row         // non-nil for regular joins using the legacy path
	cells        map[string][]compactCell // non-nil for regular joins using the compact path
	present      map[string]struct{}      // non-nil for Semi/AntiSemi joins
	innerColumns []Column
	filter       *bloom.Filter
}

// buildHashBuckets scans the build side of every JoinAlgorithmHash join in the
// plan and returns a map from join-slice index → bucket.  All other join types
// are skipped.
func buildHashBuckets(ctx context.Context, plan QueryPlan, provider TableProvider) (map[int]*hashJoinBucket, error) {
	buckets := make(map[int]*hashJoinBucket)
	for i, join := range plan.Joins {
		if join.Algorithm != JoinAlgorithmHash {
			continue
		}
		innerScan := plan.Scans[join.RightScanIndex]
		innerTable, ok := provider.GetTable(ctx, innerScan.TableName)
		if !ok {
			return nil, fmt.Errorf("%w: %s", errTableDoesNotExist, innerScan.TableName)
		}
		// Size the Bloom filter using the inner table's row-count estimate.
		// Fall back to bloomMinN when no statistics are available.
		n := innerTable.estimatedRowCount()
		if n <= 0 {
			n = bloomMinN
		}

		isSemiJoin := join.Type == Semi || join.Type == AntiSemi
		bucket := &hashJoinBucket{
			innerColumns: innerTable.Columns,
			filter:       bloom.New(uint(n), bloomFPRate),
		}

		// Virtual tables (CTEs, derived tables) have no B+tree pager; only
		// physical sequential scans can use the RowView path.
		useRowViewPath := innerTable.virtualRows == nil &&
			innerScan.Type == ScanTypeSequential &&
			rowViewFilterSupports(innerTable.Columns, innerScan.Filters)

		switch {
		case isSemiJoin:
			bucket.present = make(map[string]struct{})
			if useRowViewPath {
				// Semi-join RowView path: key-only scan, no row materialisation.
				if err := buildSemiJoinBucketFromRowViews(ctx, innerTable, innerScan, join, bucket); err != nil {
					return nil, fmt.Errorf("hash join build phase (join %d): %w", i, err)
				}
				buckets[i] = bucket
				continue
			}
		case useRowViewPath:
			// Regular join RowView path: store compact cell bytes instead of full
			// rows, deferring []OptionalValue allocation to probe-hit time.
			bucket.cells = make(map[string][]compactCell)
			if err := buildHashBucketFromCells(ctx, innerTable, innerScan, join, bucket); err != nil {
				return nil, fmt.Errorf("hash join build phase (join %d): %w", i, err)
			}
			buckets[i] = bucket
			continue
		default:
			bucket.rows = make(map[string][]Row)
		}

		// Fallback: legacy materialising scan for index scans or non-RowView-
		// compatible filters.
		innerFields := fieldsFromColumns(innerTable.Columns...)
		// keyBuf is reused across rows in the build phase to avoid one alloc per row.
		var keyBuf []byte
		if err := runTableScan(ctx, plan, innerTable, innerScan, innerFields, func(row Row) error {
			keyBuf = appendHashKey(keyBuf[:0], join, row, "", 0, true)
			if keyBuf == nil {
				return nil // NULL join key — never matches
			}
			if isSemiJoin {
				// Existence check only: avoid storing full inner rows.
				// The Go compiler optimises map[string]struct{}[string([]byte)] to
				// skip allocation when the key already exists.
				if _, exists := bucket.present[string(keyBuf)]; !exists {
					key := string(keyBuf)
					bucket.present[key] = struct{}{}
					bucket.filter.Add(keyBuf)
				}
			} else {
				key := string(keyBuf)
				bucket.rows[key] = append(bucket.rows[key], row)
				bucket.filter.Add(keyBuf)
			}
			return nil
		}); err != nil {
			return nil, fmt.Errorf("hash join build phase (join %d): %w", i, err)
		}
		buckets[i] = bucket
	}
	return buckets, nil
}

// buildSemiJoinBucketFromRowViews fills bucket.present using a RowView
// sequential scan that avoids materialising full rows.  It applies filters and
// extracts join keys via typed accessors, eliminating per-row heap allocations.
func buildSemiJoinBucketFromRowViews(ctx context.Context, innerTable *Table, innerScan Scan, join JoinPlan, bucket *hashJoinBucket) error {
	// Pre-compute join-key column indexes and kinds so the inner loop can use
	// typed accessors without name lookups.
	colIdxs := make([]int, len(join.JoinColumnPairs))
	colKinds := make([]ColumnKind, len(join.JoinColumnPairs))
	colIndexMap := make(map[string]int, len(innerTable.Columns))
	for idx, col := range innerTable.Columns {
		colIndexMap[col.Name] = idx
	}
	for p, pair := range join.JoinColumnPairs {
		idx, ok := colIndexMap[pair.JoinTableColumn.Name]
		if !ok {
			return fmt.Errorf("join column %s not found in inner table", pair.JoinTableColumn.Name)
		}
		colIdxs[p] = idx
		colKinds[p] = innerTable.Columns[idx].Kind
	}

	innerFields := fieldsFromColumns(innerTable.Columns...)
	filter := innerTable.compileRowViewScanFilter(innerScan, innerFields)

	cursor, err := innerTable.SeekFirst(ctx)
	if err != nil {
		return err
	}
	page, err := innerTable.pager.ReadPage(ctx, cursor.PageIdx)
	if err != nil {
		return fmt.Errorf("semi-join build scan: %w", err)
	}
	cursor.EndOfTable = page.LeafNode.Header.Cells == 0

	var keyBuf []byte
	for !cursor.EndOfTable {
		if err := ctx.Err(); err != nil {
			return err
		}
		if page.Index != cursor.PageIdx {
			page, err = innerTable.pager.ReadPage(ctx, cursor.PageIdx)
			if err != nil {
				return fmt.Errorf("semi-join build scan: %w", err)
			}
		}
		if cursor.CellIdx > page.LeafNode.Header.Cells-1 || len(page.LeafNode.Cells) == 0 {
			return fmt.Errorf("cell index %d out of bounds, max %d", cursor.CellIdx, page.LeafNode.Header.Cells-1)
		}
		cell := page.LeafNode.Cells[cursor.CellIdx]
		advanceLeafCursor(cursor, page)

		view := NewRowView(innerTable.Columns, cell)
		ok, err := filter.accept(ctx, innerTable.pager, view)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		var valid bool
		keyBuf, valid, err = appendHashKeyFromView(keyBuf[:0], view, colIdxs, colKinds)
		if err != nil {
			return err
		}
		if !valid {
			continue // NULL key — never matches
		}
		if _, exists := bucket.present[string(keyBuf)]; !exists {
			key := string(keyBuf)
			bucket.present[key] = struct{}{}
			bucket.filter.Add(keyBuf)
		}
	}
	return nil
}

// buildHashBucketFromCells fills bucket.cells using a RowView sequential scan.
// Rather than materialising a full []OptionalValue row per inner row, it copies
// the raw cell bytes (value + nullBitmask + key) into a compactCell.  On probe
// hit the bytes are decoded lazily via NewRowView + MaterializeWithOverflow,
// so only matched rows pay the full decoding cost.
//
// Cell bytes are stored in a contiguous arena (one large []byte grown via
// append) rather than one make([]byte) per row.  Each compactCell.value is a
// sub-slice of the arena; the GC keeps the backing array alive through those
// slice headers, so the arena does not need to escape to the bucket.
func buildHashBucketFromCells(ctx context.Context, innerTable *Table, innerScan Scan, join JoinPlan, bucket *hashJoinBucket) error {
	colIdxs := make([]int, len(join.JoinColumnPairs))
	colKinds := make([]ColumnKind, len(join.JoinColumnPairs))
	colIndexMap := make(map[string]int, len(innerTable.Columns))
	for idx, col := range innerTable.Columns {
		colIndexMap[col.Name] = idx
	}
	for p, pair := range join.JoinColumnPairs {
		idx, ok := colIndexMap[pair.JoinTableColumn.Name]
		if !ok {
			return fmt.Errorf("join column %s not found in inner table", pair.JoinTableColumn.Name)
		}
		colIdxs[p] = idx
		colKinds[p] = innerTable.Columns[idx].Kind
	}

	innerFields := fieldsFromColumns(innerTable.Columns...)
	filter := innerTable.compileRowViewScanFilter(innerScan, innerFields)

	cursor, err := innerTable.SeekFirst(ctx)
	if err != nil {
		return err
	}
	page, err := innerTable.pager.ReadPage(ctx, cursor.PageIdx)
	if err != nil {
		return fmt.Errorf("hash join compact build scan: %w", err)
	}
	cursor.EndOfTable = page.LeafNode.Header.Cells == 0

	// Pre-size the arena so the common case needs only one allocation.
	// Sample the first page to get the average cell value size for this table;
	// the page is already in memory from the SeekFirst call above, so this adds
	// no I/O.  Multiply by the estimated row count to get the total capacity.
	// The arena grows via append if the estimate is low; sub-slices stored in
	// compactCell remain valid across reallocations because they reference the
	// old backing array.
	avgCellBytes := 64 // fallback for empty first page
	if n := len(page.LeafNode.Cells); n > 0 {
		var total int
		for _, c := range page.LeafNode.Cells {
			total += len(c.Value)
		}
		avgCellBytes = total / n
	}
	estRows := innerTable.estimatedRowCount()
	if estRows <= 0 {
		estRows = 64
	}
	arena := make([]byte, 0, int(estRows)*avgCellBytes)

	var keyBuf []byte
	for !cursor.EndOfTable {
		if err := ctx.Err(); err != nil {
			return err
		}
		if page.Index != cursor.PageIdx {
			page, err = innerTable.pager.ReadPage(ctx, cursor.PageIdx)
			if err != nil {
				return fmt.Errorf("hash join compact build scan: %w", err)
			}
		}
		if cursor.CellIdx > page.LeafNode.Header.Cells-1 || len(page.LeafNode.Cells) == 0 {
			return fmt.Errorf("cell index %d out of bounds, max %d", cursor.CellIdx, page.LeafNode.Header.Cells-1)
		}
		cell := page.LeafNode.Cells[cursor.CellIdx]
		advanceLeafCursor(cursor, page)

		view := NewRowView(innerTable.Columns, cell)
		ok, err := filter.accept(ctx, innerTable.pager, view)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		var valid bool
		keyBuf, valid, err = appendHashKeyFromView(keyBuf[:0], view, colIdxs, colKinds)
		if err != nil {
			return err
		}
		if !valid {
			continue // NULL join key — never matches
		}
		// Append cell bytes into the arena instead of allocating per row.
		// Use a three-index slice so no later append can corrupt adjacent cells.
		start := len(arena)
		arena = append(arena, cell.Value...)
		end := len(arena)
		cc := compactCell{
			value:       arena[start:end:end],
			nullBitmask: cell.NullBitmask,
			key:         cell.Key,
		}
		key := string(keyBuf)
		bucket.cells[key] = append(bucket.cells[key], cc)
		bucket.filter.Add(keyBuf)
	}
	return nil
}

// appendHashKey encodes the join column values from row into buf and returns
// the extended slice.  Returns nil when any join column is NULL (NULL never
// matches in SQL).
//
// buildSide=true  → uses JoinTableColumn (inner/build side).
// buildSide=false → uses BaseTableColumn (outer/probe side); when joinIndex>0
// the column name is qualified with fromAlias.
//
// The caller resets buf to buf[:0] between calls to reuse the backing array.
func appendHashKey(buf []byte, join JoinPlan, row Row, fromAlias string, joinIndex int, buildSide bool) []byte {
	for i, pair := range join.JoinColumnPairs {
		var colName string
		if buildSide {
			colName = pair.JoinTableColumn.Name
		} else {
			colName = pair.BaseTableColumn.Name
			if joinIndex > 0 {
				colName = fromAlias + "." + colName
			}
		}
		val, ok := row.GetValue(colName)
		if !ok || !val.Valid {
			return nil // NULL key — never matches
		}
		if i > 0 {
			buf = append(buf, '\x00')
		}
		buf = appendHashKeyPart(buf, val.Value)
	}
	return buf
}

// appendHashKeyPart encodes a single column value into buf using a distinct,
// reversible representation for each type.
func appendHashKeyPart(buf []byte, v any) []byte {
	switch x := v.(type) {
	case int64:
		return strconv.AppendInt(buf, x, 10)
	case int32:
		return strconv.AppendInt(buf, int64(x), 10)
	case int8:
		return strconv.AppendInt(buf, int64(x), 10)
	case float32:
		return strconv.AppendFloat(buf, float64(x), 'f', -1, 32)
	case float64:
		return strconv.AppendFloat(buf, x, 'f', -1, 64)
	case string:
		return append(buf, x...)
	case TextPointer:
		return append(buf, x.String()...)
	case bool:
		if x {
			return append(buf, '1')
		}
		return append(buf, '0')
	case TimestampMicros:
		return strconv.AppendInt(buf, int64(x), 10)
	default:
		return fmt.Appendf(buf, "%v", v)
	}
}

// appendHashKeyFromView encodes join key columns from a RowView into buf using
// typed accessors, avoiding boxing values into any.  Returns (buf, true, nil)
// on success, (nil, false, nil) when any join column is NULL, or an error on
// decode failure.  colIdxs and colKinds must be pre-computed and co-indexed.
func appendHashKeyFromView(buf []byte, view RowView, colIdxs []int, colKinds []ColumnKind) ([]byte, bool, error) {
	for i, colIdx := range colIdxs {
		isNull, err := view.IsNull(colIdx)
		if err != nil {
			return nil, false, err
		}
		if isNull {
			return nil, false, nil // NULL key — never matches
		}
		if i > 0 {
			buf = append(buf, '\x00')
		}
		switch colKinds[i] {
		case Int4, Int8, Timestamp:
			v, _, err := view.Int64At(colIdx)
			if err != nil {
				return nil, false, err
			}
			buf = strconv.AppendInt(buf, v, 10)
		case Real, Double:
			v, _, err := view.Float64At(colIdx)
			if err != nil {
				return nil, false, err
			}
			buf = strconv.AppendFloat(buf, v, 'f', -1, 64)
		case Boolean:
			v, _, err := view.BoolAt(colIdx)
			if err != nil {
				return nil, false, err
			}
			if v {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case Varchar, Text, JSON:
			tp, err := view.TextAt(colIdx)
			if err != nil {
				return nil, false, err
			}
			buf = append(buf, tp.String()...)
		case UUID:
			v, _, err := view.UUIDAt(colIdx)
			if err != nil {
				return nil, false, err
			}
			buf = append(buf, v.String()...)
		default:
			val, err := view.ValueAt(colIdx)
			if err != nil {
				return nil, false, err
			}
			if !val.Valid {
				return nil, false, nil
			}
			buf = appendHashKeyPart(buf, val.Value)
		}
	}
	return buf, true, nil
}

// semiJoinProbeRowViewIteratorFactory returns a factory that creates a
// RowViewIterator scanning the outer (probe) table sequentially. For each outer
// row it extracts the join key via typed accessors (no boxing), checks the
// Bloom filter and then the bucket.present map, and yields only matching outer
// RowViews. Non-matching rows are skipped without materialising a full Row.
func semiJoinProbeRowViewIteratorFactory(
	ctx context.Context,
	outerTable *Table,
	join JoinPlan,
	bucket *hashJoinBucket,
	tableFilter func(context.Context, RowView) (bool, error),
	remaining, offset int64,
	hasLimit, hasOffset bool,
) (func() RowViewIterator, error) {
	colIdxs := make([]int, len(join.JoinColumnPairs))
	colKinds := make([]ColumnKind, len(join.JoinColumnPairs))
	colIndexMap := make(map[string]int, len(outerTable.Columns))
	for i, col := range outerTable.Columns {
		colIndexMap[col.Name] = i
	}
	for p, pair := range join.JoinColumnPairs {
		idx, ok := colIndexMap[pair.BaseTableColumn.Name]
		if !ok {
			return nil, fmt.Errorf("semi-join probe: column %s not found in outer table", pair.BaseTableColumn.Name)
		}
		colIdxs[p] = idx
		colKinds[p] = outerTable.Columns[idx].Kind
	}

	cursor, err := outerTable.SeekFirst(ctx)
	if err != nil {
		return nil, err
	}
	page, err := outerTable.pager.ReadPage(ctx, cursor.PageIdx)
	if err != nil {
		return nil, fmt.Errorf("semi-join probe scan: %w", err)
	}
	cursor.EndOfTable = page.LeafNode.Header.Cells == 0

	return func() RowViewIterator {
		iterCursor := cursor
		iterPage := page
		iterRemaining := remaining
		iterOffset := offset
		var keyBuf []byte
		var valid bool
		var scanErr error
		return NewRowViewIterator(func(iterCtx context.Context) (RowView, error) {
			for !iterCursor.EndOfTable {
				if ctxErr := iterCtx.Err(); ctxErr != nil {
					return RowView{}, ctxErr
				}
				if iterPage.Index != iterCursor.PageIdx {
					iterPage, scanErr = outerTable.pager.ReadPage(iterCtx, iterCursor.PageIdx)
					if scanErr != nil {
						return RowView{}, fmt.Errorf("semi-join probe scan: %w", scanErr)
					}
				}
				cell := iterPage.LeafNode.Cells[iterCursor.CellIdx]
				advanceLeafCursor(iterCursor, iterPage)

				view := NewRowView(outerTable.Columns, cell)
				if tableFilter != nil {
					ok, filterErr := tableFilter(iterCtx, view)
					if filterErr != nil {
						return RowView{}, filterErr
					}
					if !ok {
						continue
					}
				}

				keyBuf, valid, scanErr = appendHashKeyFromView(keyBuf[:0], view, colIdxs, colKinds)
				if scanErr != nil {
					return RowView{}, scanErr
				}
				if !valid {
					continue
				}
				if !bucket.filter.MayContain(keyBuf) {
					continue
				}
				if _, present := bucket.present[string(keyBuf)]; !present {
					continue
				}
				if hasOffset && iterOffset > 0 {
					iterOffset -= 1
					continue
				}
				if hasLimit {
					if iterRemaining == 0 {
						return RowView{}, ErrNoMoreRows
					}
					iterRemaining -= 1
				}
				return view, nil
			}
			return RowView{}, ErrNoMoreRows
		})
	}, nil
}

// inljProbeRowViewIteratorFactory returns a factory that creates a
// RowViewIterator for a single INNER/LEFT index nested-loop join (INLJ) with a
// unique inner index (primary key or unique secondary index).
//
// For each outer row it extracts the join key via typed accessors (no boxing of
// the full row), probes the inner index with PointUniqueRowID, copies the inner
// cell bytes into a reusable buffer, and yields a CombinedRowView that routes
// column reads to outer page bytes or the copied inner bytes without ever
// constructing a []OptionalValue.  Unmatched outer rows in INNER JOIN are skipped
// without materialisation; LEFT JOIN emits a NULL-inner CombinedRowView instead.
//
// Restriction: the inner scan must have no post-lookup filters (innerFilters empty)
// and the join must be single-column equi-join over a unique inner index.
func inljProbeRowViewIteratorFactory(
	ctx context.Context,
	outerTable *Table,
	innerTable *Table,
	innerIndex BTreeIndex,
	join JoinPlan,
	combinedCols []Column,
	outerFilter func(context.Context, RowView) (bool, error),
	remaining, offset int64,
	hasLimit, hasOffset bool,
	isLeft bool,
) (func() RowViewIterator, error) {
	if len(join.JoinColumnPairs) == 0 {
		return nil, fmt.Errorf("inlj probe: no join column pairs")
	}
	colIndexMap := make(map[string]int, len(outerTable.Columns))
	for i, col := range outerTable.Columns {
		colIndexMap[col.Name] = i
	}
	outerColIdx, ok := colIndexMap[join.JoinColumnPairs[0].BaseTableColumn.Name]
	if !ok {
		return nil, fmt.Errorf("inlj probe: column %s not found in outer table", join.JoinColumnPairs[0].BaseTableColumn.Name)
	}

	splitIdx := len(outerTable.Columns)

	cursor, err := outerTable.SeekFirst(ctx)
	if err != nil {
		return nil, err
	}
	page, err := outerTable.pager.ReadPage(ctx, cursor.PageIdx)
	if err != nil {
		return nil, fmt.Errorf("inlj probe scan: %w", err)
	}
	cursor.EndOfTable = page.LeafNode.Header.Cells == 0

	return func() RowViewIterator {
		iterCursor := cursor
		iterPage := page
		iterRemaining := remaining
		iterOffset := offset

		innerView := new(RowView)
		// innerCellBuf is a reusable buffer for inner cell bytes; it grows to fit
		// the largest inner row seen and is reset ([:0]) for each new inner match.
		// This eliminates per-row heap allocation for the inner cell copy.
		var innerCellBuf []byte

		var scanErr error

		return NewRowViewIterator(func(iterCtx context.Context) (RowView, error) {
			for !iterCursor.EndOfTable {
				if ctxErr := iterCtx.Err(); ctxErr != nil {
					return RowView{}, ctxErr
				}
				if iterPage.Index != iterCursor.PageIdx {
					iterPage, scanErr = outerTable.pager.ReadPage(iterCtx, iterCursor.PageIdx)
					if scanErr != nil {
						return RowView{}, fmt.Errorf("inlj probe scan: %w", scanErr)
					}
				}
				cell := iterPage.LeafNode.Cells[iterCursor.CellIdx]
				advanceLeafCursor(iterCursor, iterPage)

				outerView := NewRowView(outerTable.Columns, cell)

				if outerFilter != nil {
					ok, filterErr := outerFilter(iterCtx, outerView)
					if filterErr != nil {
						return RowView{}, filterErr
					}
					if !ok {
						continue
					}
				}

				// Extract probe key value via compatibility bridge (boxes to any once).
				probeKeyOV, err := outerView.ValueAt(outerColIdx)
				if err != nil {
					return RowView{}, err
				}
				if !probeKeyOV.Valid {
					// NULL outer join key — no match possible.
					if isLeft {
						return NewCombinedRowView(combinedCols, outerView, nil, nil, splitIdx, true), nil
					}
					continue
				}

				// INLJ point lookup on the unique inner index.
				rowID, err := innerIndex.PointUniqueRowID(iterCtx, probeKeyOV.Value)
				if isNotFound(err) {
					if isLeft {
						return NewCombinedRowView(combinedCols, outerView, nil, nil, splitIdx, true), nil
					}
					continue
				}
				if err != nil {
					return RowView{}, fmt.Errorf("inlj probe: %w", err)
				}

				// Fetch inner row view by rowID and copy its cell bytes into the
				// reusable buffer so the inner RowView remains valid after the
				// inner table's pager reads subsequent pages.
				innerRowView, err := innerTable.rowViewByRowID(iterCtx, rowID)
				if err != nil {
					return RowView{}, fmt.Errorf("inlj probe fetch inner row: %w", err)
				}
				innerCellBuf = append(innerCellBuf[:0], innerRowView.value...)
				*innerView = RowView{
					columns:     innerTable.Columns,
					value:       innerCellBuf,
					nullBitmask: innerRowView.nullBitmask,
					key:         innerRowView.key,
				}

				combined := NewCombinedRowView(combinedCols, outerView, innerView, innerTable.pager, splitIdx, false)
				if hasOffset && iterOffset > 0 {
					iterOffset--
					continue
				}
				if hasLimit {
					if iterRemaining == 0 {
						return RowView{}, ErrNoMoreRows
					}
					iterRemaining--
				}
				return combined, nil
			}
			return RowView{}, ErrNoMoreRows
		})
	}, nil
}

// isNotFound reports whether err signals that a key was not found in an index.
func isNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

// hashJoinProbeRowViewIteratorFactory returns a factory that creates a
// RowViewIterator for INNER/LEFT hash join probing without materialising
// combined rows into []OptionalValue.  For each outer row it extracts the join
// key via typed accessors, checks the Bloom filter and the bucket.cells map, and
// yields CombinedRowViews that route column reads directly to the outer page
// bytes or the arena-backed inner cell bytes.  Non-matching outer rows are
// skipped without any heap allocation.
//
// For LEFT JOIN, unmatched outer rows emit a combined view with innerIsNull=true
// so the driver receives NULL for every inner column.
//
// The returned factory may be called multiple times; each call produces an
// independent iterator starting from the beginning of the outer scan.
func hashJoinProbeRowViewIteratorFactory(
	ctx context.Context,
	outerTable *Table,
	innerTable *Table,
	join JoinPlan,
	bucket *hashJoinBucket,
	combinedCols []Column,
	outerFilter func(context.Context, RowView) (bool, error),
	remaining, offset int64,
	hasLimit, hasOffset bool,
	isLeft bool,
) (func() RowViewIterator, error) {
	// Pre-compute outer join-key column indexes and kinds for appendHashKeyFromView.
	colIdxs := make([]int, len(join.JoinColumnPairs))
	colKinds := make([]ColumnKind, len(join.JoinColumnPairs))
	colIndexMap := make(map[string]int, len(outerTable.Columns))
	for i, col := range outerTable.Columns {
		colIndexMap[col.Name] = i
	}
	for p, pair := range join.JoinColumnPairs {
		idx, ok := colIndexMap[pair.BaseTableColumn.Name]
		if !ok {
			return nil, fmt.Errorf("hash join probe: column %s not found in outer table", pair.BaseTableColumn.Name)
		}
		colIdxs[p] = idx
		colKinds[p] = outerTable.Columns[idx].Kind
	}

	splitIdx := len(outerTable.Columns)

	cursor, err := outerTable.SeekFirst(ctx)
	if err != nil {
		return nil, err
	}
	page, err := outerTable.pager.ReadPage(ctx, cursor.PageIdx)
	if err != nil {
		return nil, fmt.Errorf("hash join probe scan: %w", err)
	}
	cursor.EndOfTable = page.LeafNode.Header.Cells == 0

	return func() RowViewIterator {
		iterCursor := cursor
		iterPage := page
		iterRemaining := remaining
		iterOffset := offset

		// innerView is allocated once per iterator and updated for each inner match.
		// The pointer is stored in each returned CombinedRowView; the driver reads
		// from it before the next Next() call updates it.
		innerView := new(RowView)

		var (
			outerView    RowView
			pendingCells []compactCell
			pendingIdx   int
			leftMissNext bool // emit NULL-inner combined view before next outer row
		)

		var (
			keyBuf  []byte
			scanErr error
		)

		return NewRowViewIterator(func(iterCtx context.Context) (RowView, error) {
			for {
				if ctxErr := iterCtx.Err(); ctxErr != nil {
					return RowView{}, ctxErr
				}

				// Emit pending LEFT JOIN null row before advancing to the next outer row.
				if leftMissNext {
					leftMissNext = false
					combined := NewCombinedRowView(combinedCols, outerView, nil, nil, splitIdx, true)
					if hasOffset && iterOffset > 0 {
						iterOffset -= 1
						continue
					}
					if hasLimit {
						if iterRemaining == 0 {
							return RowView{}, ErrNoMoreRows
						}
						iterRemaining -= 1
					}
					return combined, nil
				}

				// Drain remaining inner matches for the current outer row.
				if pendingIdx < len(pendingCells) {
					cc := pendingCells[pendingIdx]
					pendingIdx += 1
					*innerView = RowView{
						columns:     innerTable.Columns,
						value:       cc.value,
						nullBitmask: cc.nullBitmask,
						key:         cc.key,
					}
					combined := NewCombinedRowView(combinedCols, outerView, innerView, innerTable.pager, splitIdx, false)
					if hasOffset && iterOffset > 0 {
						iterOffset -= 1
						continue
					}
					if hasLimit {
						if iterRemaining == 0 {
							return RowView{}, ErrNoMoreRows
						}
						iterRemaining -= 1
					}
					return combined, nil
				}

				// Advance to next outer row.
				if iterCursor.EndOfTable {
					return RowView{}, ErrNoMoreRows
				}
				if iterPage.Index != iterCursor.PageIdx {
					iterPage, scanErr = outerTable.pager.ReadPage(iterCtx, iterCursor.PageIdx)
					if scanErr != nil {
						return RowView{}, fmt.Errorf("hash join probe scan: %w", scanErr)
					}
				}
				cell := iterPage.LeafNode.Cells[iterCursor.CellIdx]
				advanceLeafCursor(iterCursor, iterPage)

				outerView = NewRowView(outerTable.Columns, cell)

				if outerFilter != nil {
					ok, filterErr := outerFilter(iterCtx, outerView)
					if filterErr != nil {
						return RowView{}, filterErr
					}
					if !ok {
						continue
					}
				}

				var valid bool
				keyBuf, valid, scanErr = appendHashKeyFromView(keyBuf[:0], outerView, colIdxs, colKinds)
				if scanErr != nil {
					return RowView{}, scanErr
				}
				if !valid || !bucket.filter.MayContain(keyBuf) {
					if isLeft {
						leftMissNext = true
					}
					continue
				}

				cells := bucket.cells[string(keyBuf)]
				if len(cells) == 0 {
					if isLeft {
						leftMissNext = true
					}
					continue
				}

				pendingCells = cells
				pendingIdx = 0
				// Loop back to drain inner matches.
			}
		})
	}, nil
}
