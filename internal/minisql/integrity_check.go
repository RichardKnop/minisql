package minisql

import (
	"context"
	"fmt"
	"maps"
	"slices"
)

// IntegrityIssue represents a single integrity problem discovered by a check.
type IntegrityIssue struct {
	Code    string
	Message string
	Page    *PageIndex
	Object  string
}

// IntegrityReport summarises the result of an integrity check.
type IntegrityReport struct {
	TotalPages        uint32
	CheckedRootPages  int
	CheckedFreePages  int
	FreeListPageCount uint32
	Issues            []IntegrityIssue
}

// Ok returns true when the integrity check found no issues.
func (r IntegrityReport) Ok() bool {
	return len(r.Issues) == 0
}

// QuickCheck performs a cheap structural health check of the open database.
//
// It validates header-linked free-list metadata and the decodability/shape of
// table and index root pages, but it does not walk full B-tree contents or
// cross-check tables against indexes.
func (d *Database) QuickCheck(ctx context.Context) (IntegrityReport, error) {
	mainPager := d.factory.ForTable(mainTableColumns)
	totalPages := d.saver.TotalPages()
	report := IntegrityReport{
		TotalPages: totalPages,
	}

	if totalPages == 0 {
		report.Issues = append(report.Issues, IntegrityIssue{
			Code:    "empty_database",
			Message: "database has no pages",
		})
		return report, nil
	}

	dbHeader := mainPager.GetHeader(ctx)
	report, freePages := d.checkFreeList(ctx, report, mainPager, dbHeader, totalPages)
	tables := d.snapshotTables()

	if _, ok := tables[SchemaTableName]; !ok {
		report.Issues = append(report.Issues, IntegrityIssue{
			Code:    "missing_schema_table",
			Message: "system schema table is not loaded",
			Object:  SchemaTableName,
		})
		return report, nil
	}

	rootPages := make(map[PageIndex]string, len(tables)*3+1)
	for _, table := range tables {
		rootPages[table.GetRootPageIdx()] = fmt.Sprintf("table %s", table.Name)
		if table.HasPrimaryKey() && table.PrimaryKey.Index != nil {
			rootPages[table.PrimaryKey.Index.GetRootPageIdx()] = fmt.Sprintf("index %s", table.PrimaryKey.Name)
		}
		for _, index := range table.UniqueIndexes {
			if index.Index != nil {
				rootPages[index.Index.GetRootPageIdx()] = fmt.Sprintf("index %s", index.Name)
			}
		}
		for _, index := range table.SecondaryIndexes {
			if index.Index != nil {
				rootPages[index.Index.GetRootPageIdx()] = fmt.Sprintf("index %s", index.Name)
			}
		}
	}

	rootIndexes := make([]PageIndex, 0, len(rootPages))
	for pageIdx := range rootPages {
		rootIndexes = append(rootIndexes, pageIdx)
	}
	slices.Sort(rootIndexes)

	for _, pageIdx := range rootIndexes {
		objectName := rootPages[pageIdx]
		if _, isFree := freePages[pageIdx]; isFree {
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "root_page_on_free_list",
				Message: fmt.Sprintf("%s root page is also present in the free list", objectName),
				Page:    pageIndexPtr(pageIdx),
				Object:  objectName,
			})
		}
	}

	for _, table := range tables {
		report = d.checkTableRoot(ctx, report, table)

		if table.HasPrimaryKey() && table.PrimaryKey.Index != nil {
			report = d.checkIndexRoot(ctx, report, table.Name, table.PrimaryKey.Name, table.PrimaryKey.Columns, true, table.PrimaryKey.Index.GetRootPageIdx())
		}
		for _, index := range table.UniqueIndexes {
			if index.Index == nil {
				continue
			}
			report = d.checkIndexRoot(ctx, report, table.Name, index.Name, index.Columns, true, index.Index.GetRootPageIdx())
		}
		for _, index := range table.SecondaryIndexes {
			if index.Index == nil {
				continue
			}
			report = d.checkIndexRoot(ctx, report, table.Name, index.Name, index.Columns, false, index.Index.GetRootPageIdx())
		}
	}

	return report, nil
}

func (d *Database) snapshotTables() map[string]*Table {
	d.dbLock.RLock()
	defer d.dbLock.RUnlock()

	tables := make(map[string]*Table, len(d.tables))
	maps.Copy(tables, d.tables)
	return tables
}

func (d *Database) checkFreeList(ctx context.Context, report IntegrityReport, pager Pager, dbHeader DatabaseHeader, totalPages uint32) (IntegrityReport, map[PageIndex]struct{}) {
	freePages := make(map[PageIndex]struct{}, dbHeader.FreePageCount)

	if dbHeader.FirstFreePage >= PageIndex(totalPages) && dbHeader.FirstFreePage != 0 {
		report.Issues = append(report.Issues, IntegrityIssue{
			Code:    "free_list_head_out_of_range",
			Message: fmt.Sprintf("free list head page %d is outside database page range", dbHeader.FirstFreePage),
			Page:    pageIndexPtr(dbHeader.FirstFreePage),
		})
		return report, freePages
	}

	current := dbHeader.FirstFreePage
	for current != 0 {
		if current >= PageIndex(totalPages) {
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "free_list_page_out_of_range",
				Message: fmt.Sprintf("free list page %d is outside database page range", current),
				Page:    pageIndexPtr(current),
			})
			break
		}
		if _, exists := freePages[current]; exists {
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "free_list_cycle",
				Message: fmt.Sprintf("free list contains a cycle at page %d", current),
				Page:    pageIndexPtr(current),
			})
			break
		}

		page, err := pager.GetPage(ctx, current)
		if err != nil {
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "free_list_page_decode_failed",
				Message: fmt.Sprintf("failed to decode free-list page %d: %v", current, err),
				Page:    pageIndexPtr(current),
			})
			break
		}
		if page.FreePage == nil {
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "free_list_page_not_free",
				Message: fmt.Sprintf("page %d is referenced from free list but does not decode as a free page", current),
				Page:    pageIndexPtr(current),
			})
			break
		}

		freePages[current] = struct{}{}
		report.CheckedFreePages += 1
		current = page.FreePage.NextFreePage
	}

	report.FreeListPageCount = dbHeader.FreePageCount
	if uint32(len(freePages)) != dbHeader.FreePageCount {
		report.Issues = append(report.Issues, IntegrityIssue{
			Code:    "free_page_count_mismatch",
			Message: fmt.Sprintf("database header says %d free pages, but walked %d pages from the free list", dbHeader.FreePageCount, len(freePages)),
		})
	}

	return report, freePages
}

func (d *Database) checkTableRoot(ctx context.Context, report IntegrityReport, table *Table) IntegrityReport {
	pageIdx := table.GetRootPageIdx()
	if pageIdx >= PageIndex(report.TotalPages) {
		report.Issues = append(report.Issues, IntegrityIssue{
			Code:    "table_root_out_of_range",
			Message: fmt.Sprintf("table %s root page %d is outside database page range", table.Name, pageIdx),
			Page:    pageIndexPtr(pageIdx),
			Object:  table.Name,
		})
		return report
	}

	page, err := d.factory.ForTable(table.Columns).GetPage(ctx, pageIdx)
	if err != nil {
		report.Issues = append(report.Issues, IntegrityIssue{
			Code:    "table_root_decode_failed",
			Message: fmt.Sprintf("failed to decode root page for table %s: %v", table.Name, err),
			Page:    pageIndexPtr(pageIdx),
			Object:  table.Name,
		})
		return report
	}
	report.CheckedRootPages += 1

	if page.LeafNode == nil && page.InternalNode == nil {
		report.Issues = append(report.Issues, IntegrityIssue{
			Code:    "table_root_invalid_type",
			Message: fmt.Sprintf("table %s root page %d is not a table leaf/internal node", table.Name, pageIdx),
			Page:    pageIndexPtr(pageIdx),
			Object:  table.Name,
		})
	}

	return report
}

func (d *Database) checkIndexRoot(ctx context.Context, report IntegrityReport, tableName, indexName string, columns []Column, unique bool, pageIdx PageIndex) IntegrityReport {
	if pageIdx >= PageIndex(report.TotalPages) {
		report.Issues = append(report.Issues, IntegrityIssue{
			Code:    "index_root_out_of_range",
			Message: fmt.Sprintf("index %s on table %s has root page %d outside database page range", indexName, tableName, pageIdx),
			Page:    pageIndexPtr(pageIdx),
			Object:  indexName,
		})
		return report
	}

	page, err := d.factory.ForIndex(columns, unique).GetPage(ctx, pageIdx)
	if err != nil {
		report.Issues = append(report.Issues, IntegrityIssue{
			Code:    "index_root_decode_failed",
			Message: fmt.Sprintf("failed to decode root page for index %s on table %s: %v", indexName, tableName, err),
			Page:    pageIndexPtr(pageIdx),
			Object:  indexName,
		})
		return report
	}
	report.CheckedRootPages += 1

	if page.IndexNode == nil {
		report.Issues = append(report.Issues, IntegrityIssue{
			Code:    "index_root_invalid_type",
			Message: fmt.Sprintf("index %s on table %s root page %d is not an index node", indexName, tableName, pageIdx),
			Page:    pageIndexPtr(pageIdx),
			Object:  indexName,
		})
	}

	return report
}

func pageIndexPtr(pageIdx PageIndex) *PageIndex {
	value := pageIdx
	return &value
}
