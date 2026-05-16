package minisql

import (
	"context"
	"fmt"
	"strconv"
	"strings"

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
// join column values; the value is the list of inner rows sharing that key.
// innerColumns is kept to construct NULL rows for LEFT JOIN misses.
// filter is a Bloom filter over the same key set used to reject probe keys
// that are definitely not present, avoiding an unnecessary map lookup.
type hashJoinBucket struct {
	rows         map[string][]Row
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
		bucket := &hashJoinBucket{
			rows:         make(map[string][]Row),
			innerColumns: innerTable.Columns,
			filter:       bloom.New(uint(n), bloomFPRate),
		}
		innerFields := fieldsFromColumns(innerTable.Columns...)
		if err := runTableScan(ctx, plan, innerTable, innerScan, innerFields, func(row Row) error {
			key := buildSideHashKey(join, row)
			if key == "" {
				return nil // NULL join key — never matches
			}
			bucket.rows[key] = append(bucket.rows[key], row)
			bucket.filter.Add([]byte(key))
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
