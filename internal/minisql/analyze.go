package minisql

import (
	"context"
	"fmt"
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

	StatsTableSQL = fmt.Sprintf(`create table "%s" (
	tbl varchar(255),
	idx varchar(255),
	stat text
);`, StatsTableName)

	statsTableFields = fieldsFromColumns(statsTableColumns...)
)

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
			return fmt.Errorf("stats table not found")
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
				FieldIsEqual("tbl", OperandQuotedString, NewTextPointer([]byte(tableName))),
			},
		},
	})
	return err
}

func (d *Database) analyzeTable(ctx context.Context, statsTable *Table, table *Table) error {
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
		rowCount++
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

func (d *Database) analyzeIndex(ctx context.Context, statsTable *Table, tableName string, indexName string, index BTreeIndex, isUnique bool) error {
	// Get the index columns to determine if this is a composite index
	var indexColumns []Column
	table := d.tables[tableName]

	// Find the columns for this index
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
				if idx.Name == indexName {
					indexColumns = idx.Columns
					break
				}
			}
		}
	}

	numColumns := len(indexColumns)
	entryCount := int64(0)

	// Track distinct values for each prefix level
	// prefixMaps[0] = distinct values for first column only
	// prefixMaps[1] = distinct values for first 2 columns combined
	// etc.
	prefixMaps := make([]map[string]struct{}, numColumns)
	for i := range numColumns {
		prefixMaps[i] = make(map[string]struct{})
	}

	// The index is a generic type, need to handle different key types
	// For simplicity, use ScanAll which works for all index types
	if scanErr := index.ScanAll(ctx, false, func(key any, rowID RowID) error {
		entryCount++

		// Handle composite keys by tracking each prefix
		if ck, isComposite := key.(CompositeKey); isComposite && numColumns > 1 {
			// For each prefix level (1 column, 2 columns, ..., all columns)
			for prefixLen := 1; prefixLen <= numColumns; prefixLen++ {
				// Build a key string for this prefix
				prefixKey := buildPrefixKey(ck.Values[:prefixLen])
				prefixMaps[prefixLen-1][prefixKey] = struct{}{}
			}
		} else {
			// Single column index - just track the full key
			keyStr := fmt.Sprintf("%v", key)
			prefixMaps[0][keyStr] = struct{}{}
		}

		return nil
	}); scanErr != nil {
		return scanErr
	}

	// Build stat string in SQLite format: "nEntry nDistinct1 nDistinct2 ..."
	// For single-column index: "100 50" means 100 entries, 50 distinct values
	// For composite (col1, col2): "100 50 80" means 100 entries, 50 distinct col1 values, 80 distinct (col1,col2) pairs
	stat := strings.Builder{}
	fmt.Fprintf(&stat, "%d", entryCount)

	for i := range numColumns {
		distinctCount := int64(len(prefixMaps[i]))

		// For unique indexes, only the final prefix (all columns) should have distinctCount == entryCount
		// Intermediate prefixes can have fewer distinct values
		if isUnique && i == numColumns-1 {
			distinctCount = entryCount
		}

		fmt.Fprintf(&stat, " %d", distinctCount)
	}

	// Store stat in SQLite format
	// Single-column example: "100 50" means 100 entries, 50 distinct values (avg 2 entries per value)
	// Composite example: "100 10 50 100" for (col1, col2, col3) means:
	//   - 100 total entries
	//   - 10 distinct col1 values (avg 10 entries per col1 value)
	//   - 50 distinct (col1, col2) combinations (avg 2 entries per combination)
	//   - 100 distinct (col1, col2, col3) combinations (all unique)

	_, err := statsTable.Insert(ctx, Statement{
		Kind:      Insert,
		TableName: StatsTableName,
		Fields:    statsTableFields,
		Inserts: [][]OptionalValue{
			{
				{Value: NewTextPointer([]byte(tableName)), Valid: true},     // tbl
				{Value: NewTextPointer([]byte(indexName)), Valid: true},     // idx
				{Value: NewTextPointer([]byte(stat.String())), Valid: true}, // stat
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

type Stats struct {
	TableName string
	IndexName string
	StatValue string
}

func (d *Database) listStats(ctx context.Context, tableName string) ([]Stats, error) {
	d.dbLock.RLock()
	statsTable, ok := d.tables[StatsTableName]
	if !ok {
		d.dbLock.RUnlock()
		return nil, nil
	}
	d.dbLock.RUnlock()

	stmt := Statement{
		Kind:   Select,
		Fields: statsTableFields,
	}
	if tableName != "" {
		stmt.Conditions = OneOrMore{
			{
				FieldIsEqual("tbl", OperandQuotedString, NewTextPointer([]byte(tableName))),
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

func scanStats(aRow Row) Stats {
	s := Stats{
		TableName: aRow.Values[0].Value.(TextPointer).String(),
		StatValue: aRow.Values[2].Value.(TextPointer).String(),
	}
	if aRow.Values[1].Valid {
		s.IndexName = aRow.Values[1].Value.(TextPointer).String()
	}
	return s
}
