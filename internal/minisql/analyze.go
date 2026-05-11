package minisql

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

var (
	statsTableColumns = []Column{
		{
			Kind:     Varchar,
			Size:     MaxInlineVarchar,
			Name:     "tbl",
			Nullable: true,
		},
		{
			Kind:     Varchar,
			Size:     MaxInlineVarchar,
			Name:     "idx",
			Nullable: true,
		},
		{
			Kind: Text,
			Name: "stat",
		},
	}

	// StatsTableSQL is the DDL used to create the internal statistics table.
	StatsTableSQL = fmt.Sprintf(`create table "%s" (
	tbl varchar(255),
	idx varchar(255),
	stat text
);`, StatsTableName)

	statsTableFields = fieldsFromColumns(statsTableColumns...)
)

// Analyze gathers statistics for all tables (or a specific target) and stores them in the stats table.
func (d *Database) Analyze(ctx context.Context, target string) error {
	_, exists, err := d.checkSchemaExists(ctx, SchemaTable, StatsTableSQL)
	if err != nil {
		return err
	}

	var statsTable *Table
	if !exists {
		// Create stats table if it doesn't exist
		statsTable, err = d.createTable(ctx, Statement{
			Kind:      CreateTable,
			TableName: StatsTableName,
			Columns:   statsTableColumns,
		})
		if err != nil {
			return err
		}
	} else {
		statsTable, exists = d.tables[StatsTableName]
		if !exists {
			return errors.New("stats table not found")
		}
	}

	// Determine which tables/indexes to analyze
	tablesToAnalyze := make([]string, 0)
	if target == "" {
		// Analyze all user tables (skip system tables)
		for tableName := range d.tables {
			if !isSystemTable(tableName) {
				tablesToAnalyze = append(tablesToAnalyze, tableName)
			}
		}
	} else {
		// Analyze specific table
		if _, exists := d.tables[target]; !exists {
			return fmt.Errorf("table %s does not exist", target)
		}
		tablesToAnalyze = append(tablesToAnalyze, target)
	}

	// Delete old statistics for tables we're analyzing
	for _, tableName := range tablesToAnalyze {
		if err := d.deleteOldStats(ctx, statsTable, tableName); err != nil {
			return fmt.Errorf("delete old stats for %s: %w", tableName, err)
		}
		d.tables[tableName].indexStats = make(map[string]IndexStats) // Clear cached stats
	}

	// Gather and store new statistics
	for _, tableName := range tablesToAnalyze {
		table := d.tables[tableName]

		// 1. Analyze the table itself
		if err := d.analyzeTable(ctx, statsTable, table); err != nil {
			return fmt.Errorf("analyze table %s: %w", tableName, err)
		}

		// 2. Analyze primary key if exists
		if table.PrimaryKey.Index != nil {
			if err := d.analyzeIndex(ctx, statsTable, tableName, table.PrimaryKey.Name, table.PrimaryKey.Index, true); err != nil {
				return fmt.Errorf("analyze primary key %s: %w", table.PrimaryKey.Name, err)
			}
		}

		// 3. Analyze unique indexes
		for _, uniqueIdx := range table.UniqueIndexes {
			if err := d.analyzeIndex(ctx, statsTable, tableName, uniqueIdx.Name, uniqueIdx.Index, true); err != nil {
				return fmt.Errorf("analyze unique index %s: %w", uniqueIdx.Name, err)
			}
		}

		// 4. Analyze secondary indexes
		for _, secondaryIdx := range table.SecondaryIndexes {
			if !secondaryIdx.IsBTree() {
				continue
			}
			if err := d.analyzeIndex(ctx, statsTable, tableName, secondaryIdx.Name, secondaryIdx.Index, false); err != nil {
				return fmt.Errorf("analyze secondary index %s: %w", secondaryIdx.Name, err)
			}
		}
	}

	// Load stats into memory
	stats, err := d.listStats(ctx, target)
	if err != nil {
		return err
	}
	for _, s := range stats {
		if s.IndexName == "" {
			continue
		}
		indexStats, err := parseIndexStats(s.StatValue)
		if err != nil {
			return err
		}
		d.tables[s.TableName].indexStats[s.IndexName] = indexStats
	}

	return nil
}

func (d *Database) deleteOldStats(ctx context.Context, statsTable *Table, tableName string) error {
	// Delete all stats for this table
	_, err := statsTable.Delete(ctx, Statement{
		Kind:      Delete,
		TableName: StatsTableName,
		Conditions: OneOrMore{
			{
				FieldIsEqual(Field{Name: "tbl"}, OperandQuotedString, NewTextPointer([]byte(tableName))),
			},
		},
	})
	return err
}

func (d *Database) analyzeTable(ctx context.Context, statsTable, table *Table) error {
	// Count total rows by scanning the table
	rowCount := int64(0)
	cursor, err := table.SeekFirst(ctx)
	if err != nil {
		return err
	}

	for !cursor.EndOfTable {
		_, err := cursor.fetchRow(ctx, true)
		if err != nil {
			return err
		}
		rowCount += 1
	}

	// Store stat in format: "rowCount"
	stat := fmt.Sprintf("%d", rowCount)

	_, err = statsTable.Insert(ctx, Statement{
		Kind:      Insert,
		TableName: StatsTableName,
		Fields:    statsTableFields,
		Inserts: [][]OptionalValue{
			{
				{Value: NewTextPointer([]byte(table.Name)), Valid: true}, // tbl
				{}, // idx (NULL for table stats)
				{Value: NewTextPointer([]byte(stat)), Valid: true}, // stat
			},
		},
	})

	return err
}

func (d *Database) analyzeIndex(ctx context.Context, statsTable *Table, tableName, indexName string, index BTreeIndex, isUnique bool) error {
	var (
		indexColumns []Column
		table        = d.tables[tableName]
	)

	if table.PrimaryKey.Name == indexName {
		indexColumns = table.PrimaryKey.Columns
	} else {
		for _, idx := range table.UniqueIndexes {
			if idx.Name == indexName {
				indexColumns = idx.Columns
				break
			}
		}
		if indexColumns == nil {
			for _, idx := range table.SecondaryIndexes {
				if idx.IsBTree() && idx.Name == indexName {
					indexColumns = idx.Columns
					break
				}
			}
		}
	}

	var (
		numColumns = len(indexColumns)
		entryCount = int64(0)
	)

	// Track distinct values for each prefix level.
	prefixMaps := make([]map[string]struct{}, numColumns)
	for i := range numColumns {
		prefixMaps[i] = make(map[string]struct{})
	}

	// Collect first-column numeric values for histogram building.
	collectHist := len(indexColumns) > 0 && isNumericColumn(indexColumns[0])
	var histValues []float64

	if scanErr := index.ScanAll(ctx, false, func(key any, rowID RowID) error {
		entryCount += 1

		if ck, isComposite := key.(CompositeKey); isComposite && numColumns > 1 {
			for prefixLen := 1; prefixLen <= numColumns; prefixLen++ {
				prefixKey := buildPrefixKey(ck.Values[:prefixLen])
				prefixMaps[prefixLen-1][prefixKey] = struct{}{}
			}
			if collectHist && len(ck.Values) > 0 {
				if f, ok := anyToFloat64(ck.Values[0]); ok {
					histValues = append(histValues, f)
				}
			}
		} else {
			keyStr := fmt.Sprintf("%v", key)
			prefixMaps[0][keyStr] = struct{}{}
			if collectHist {
				if f, ok := anyToFloat64(key); ok {
					histValues = append(histValues, f)
				}
			}
		}

		return nil
	}); scanErr != nil {
		return scanErr
	}

	stat := strings.Builder{}
	fmt.Fprintf(&stat, "%d", entryCount)

	for i := range numColumns {
		distinctCount := int64(len(prefixMaps[i]))
		if isUnique && i == numColumns-1 {
			distinctCount = entryCount
		}
		fmt.Fprintf(&stat, " %d", distinctCount)
	}

	if len(histValues) > 0 {
		sort.Float64s(histValues)
		hist := buildEquiDepthHistogram(histValues, histogramBuckets)
		serializeHistogram(&stat, hist)
	}

	_, err := statsTable.Insert(ctx, Statement{
		Kind:      Insert,
		TableName: StatsTableName,
		Fields:    statsTableFields,
		Inserts: [][]OptionalValue{
			{
				{Value: NewTextPointer([]byte(tableName)), Valid: true},
				{Value: NewTextPointer([]byte(indexName)), Valid: true},
				{Value: NewTextPointer([]byte(stat.String())), Valid: true},
			},
		},
	})

	return err
}

// buildPrefixKey creates a string representation of a prefix of composite key values
// This is used to track distinct value combinations for statistics
func buildPrefixKey(values []any) string {
	if len(values) == 1 {
		return fmt.Sprintf("%v", values[0])
	}

	// For multi-column prefixes, concatenate with a separator
	// Using \x00 as separator to handle cases where values might contain common separators
	result := strings.Builder{}
	fmt.Fprintf(&result, "%v", values[0])
	for i := 1; i < len(values); i++ {
		fmt.Fprintf(&result, "\x00%v", values[i])
	}
	return result.String()
}

// Stats holds a single row from the internal statistics table.
type Stats struct {
	TableName string
	IndexName string
	StatValue string
}

// listStatsNoLock reads stats without acquiring dbLock.  The caller must
// ensure exclusive or at least read access to d.tables (e.g. by holding
// dbLock, or by being in a single-threaded init path).
func (d *Database) listStatsNoLock(ctx context.Context, tableName string) ([]Stats, error) {
	statsTable, ok := d.tables[StatsTableName]
	if !ok {
		return nil, nil
	}
	return d.scanStatsTable(ctx, statsTable, tableName)
}

func (d *Database) listStats(ctx context.Context, tableName string) ([]Stats, error) {
	d.dbLock.RLock()
	statsTable, ok := d.tables[StatsTableName]
	if !ok {
		d.dbLock.RUnlock()
		return nil, nil
	}
	d.dbLock.RUnlock()
	return d.scanStatsTable(ctx, statsTable, tableName)
}

func (d *Database) scanStatsTable(ctx context.Context, statsTable *Table, tableName string) ([]Stats, error) {
	stmt := Statement{
		Kind:   Select,
		Fields: statsTableFields,
	}
	if tableName != "" {
		stmt.Conditions = OneOrMore{
			{
				FieldIsEqual(Field{Name: "tbl"}, OperandQuotedString, NewTextPointer([]byte(tableName))),
			},
		}
	}

	results, err := statsTable.Select(ctx, stmt)
	if err != nil {
		return nil, err
	}

	var stats []Stats
	for results.Rows.Next(ctx) {
		stats = append(stats, scanStats(results.Rows.Row()))
	}
	if err := results.Rows.Err(); err != nil {
		return nil, err
	}

	return stats, nil
}

func scanStats(row Row) Stats {
	s := Stats{
		TableName: row.Values[0].Value.(TextPointer).String(),
		StatValue: row.Values[2].Value.(TextPointer).String(),
	}
	if row.Values[1].Valid {
		s.IndexName = row.Values[1].Value.(TextPointer).String()
	}
	return s
}
