package minisql

import (
	"context"
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

// hashJoinBucket is the in-memory hash table built from the inner (build) side
// of a single hash join.  The map key is a null-byte-delimited encoding of the
// join column values.
//
// For regular joins (INNER/LEFT/RIGHT/FULL OUTER) rows holds all inner rows per
// key so they can be combined with the outer row on a hit.
//
// For semi/anti-semi joins only key existence is needed; present stores just the
// set of keys and rows is nil, eliminating the per-key []Row allocation and the
// cost of keeping full inner rows in memory.
//
// innerColumns is kept to construct NULL rows for LEFT JOIN misses.
// filter is a Bloom filter over the same key set used to reject probe keys
// that are definitely not present, avoiding an unnecessary map lookup.
type hashJoinBucket struct {
	rows         map[string][]Row   // non-nil for INNER/LEFT/RIGHT/FULL OUTER joins
	present      map[string]struct{} // non-nil for Semi/AntiSemi joins
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
		if isSemiJoin {
			bucket.present = make(map[string]struct{})
		} else {
			bucket.rows = make(map[string][]Row)
		}

		// For semi-joins on a sequential scan with RowView-compatible filters,
		// use the RowView path to avoid materialising full rows just for key
		// extraction.  This eliminates the per-row []OptionalValue allocation.
		if isSemiJoin && innerScan.Type == ScanTypeSequential &&
			rowViewFilterSupports(innerTable.Columns, innerScan.Filters) {
			if err := buildSemiJoinBucketFromRowViews(ctx, innerTable, innerScan, join, bucket); err != nil {
				return nil, fmt.Errorf("hash join build phase (join %d): %w", i, err)
			}
			buckets[i] = bucket
			continue
		}

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

