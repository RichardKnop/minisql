package minisql

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// hashJoinBucket is the in-memory hash table built from the inner (build) side
// of a single hash join.  The map key is a null-byte-delimited encoding of the
// join column values; the value is the list of inner rows sharing that key.
// innerColumns is kept to construct NULL rows for LEFT JOIN misses.
type hashJoinBucket struct {
	rows         map[string][]Row
	innerColumns []Column
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
		bucket := &hashJoinBucket{
			rows:         make(map[string][]Row),
			innerColumns: innerTable.Columns,
		}
		innerFields := fieldsFromColumns(innerTable.Columns...)
		if err := runTableScan(ctx, plan, innerTable, innerScan, innerFields, func(row Row) error {
			key := buildSideHashKey(join, row)
			if key == "" {
				return nil // NULL join key — never matches
			}
			bucket.rows[key] = append(bucket.rows[key], row)
			return nil
		}); err != nil {
			return nil, fmt.Errorf("hash join build phase (join %d): %w", i, err)
		}
		buckets[i] = bucket
	}
	return buckets, nil
}

// buildSideHashKey encodes the inner (build-side) row's join column values
// into a string key.  Returns "" when any join column is NULL (NULL never
// matches anything in SQL).
func buildSideHashKey(join JoinPlan, innerRow Row) string {
	parts := make([]string, len(join.JoinColumnPairs))
	for i, pair := range join.JoinColumnPairs {
		val, ok := innerRow.GetValue(pair.JoinTableColumn.Name)
		if !ok || !val.Valid {
			return ""
		}
		parts[i] = formatHashKeyPart(val.Value)
	}
	return strings.Join(parts, "\x00")
}

// probeSideHashKey encodes the outer (probe-side) row's join column values
// into a string key compatible with buildSideHashKey.  Returns "" when any
// join column is NULL.
//
// joinIndex==0: currentRow has plain column names (not yet alias-prefixed).
// joinIndex >0: currentRow already carries "alias.column" prefixes.
func probeSideHashKey(join JoinPlan, currentRow Row, fromAlias string, joinIndex int) string {
	parts := make([]string, len(join.JoinColumnPairs))
	for i, pair := range join.JoinColumnPairs {
		colName := pair.BaseTableColumn.Name
		if joinIndex > 0 {
			colName = fromAlias + "." + colName
		}
		val, ok := currentRow.GetValue(colName)
		if !ok || !val.Valid {
			return ""
		}
		parts[i] = formatHashKeyPart(val.Value)
	}
	return strings.Join(parts, "\x00")
}

// formatHashKeyPart encodes a single column value as a string suitable for
// use inside a hash key.  Each type has a distinct, reversible representation.
func formatHashKeyPart(v any) string {
	switch x := v.(type) {
	case int64:
		return strconv.FormatInt(x, 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int8:
		return strconv.FormatInt(int64(x), 10)
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case string:
		return x
	case TextPointer:
		return x.String()
	case bool:
		if x {
			return "1"
		}
		return "0"
	case TimestampMicros:
		return strconv.FormatInt(int64(x), 10)
	default:
		return fmt.Sprintf("%v", v)
	}
}
