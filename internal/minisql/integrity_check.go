package minisql

import (
	"context"
	"encoding/hex"
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
	Issues            []IntegrityIssue
	CheckedRootPages  int
	CheckedFreePages  int
	CheckedLivePages  int
	TotalPages        uint32
	FreeListPageCount uint32
}

// Ok returns true when the integrity check found no issues.
func (r IntegrityReport) Ok() bool {
	return len(r.Issues) == 0
}

// IntegrityCheck performs a deeper structural walk of the database file.
//
// In addition to QuickCheck, it traverses reachable table, index, and overflow
// pages from schema roots, reports orphan pages, and flags pages that appear in
// both live structures and the free list.
func (d *Database) IntegrityCheck(ctx context.Context) (IntegrityReport, error) {
	report, freePages, tables, err := d.quickCheckState(ctx)
	if err != nil {
		return report, err
	}
	if len(tables) == 0 {
		return report, nil
	}

	livePages := make(map[PageIndex]string, len(tables)*4+1)

	for _, table := range tables {
		report = d.walkTablePages(ctx, report, table, table.GetRootPageIdx(), livePages)

		if table.HasPrimaryKey() && table.PrimaryKey.Index != nil {
			report = d.walkIndexPages(ctx, report, table.Name, table.PrimaryKey.Name, table.PrimaryKey.Columns, true, table.PrimaryKey.Index.GetRootPageIdx(), livePages)
		}
		for _, index := range table.UniqueIndexes {
			if index.Index == nil {
				continue
			}
			report = d.walkIndexPages(ctx, report, table.Name, index.Name, index.Columns, true, index.Index.GetRootPageIdx(), livePages)
		}
		for _, index := range table.SecondaryIndexes {
			if secondaryIndexUsesDedicatedInvertedStorage(index.Method) && index.InvertedIndex != nil {
				report = d.walkInvertedIndexPages(ctx, report, table.Name, index.Name, index.InvertedIndex.GetRootPageIdx(), livePages)
				continue
			}
			if index.Index == nil {
				continue
			}
			report = d.walkIndexPages(ctx, report, table.Name, index.Name, secondaryIndexStorageColumns(index), false, index.Index.GetRootPageIdx(), livePages)
		}
	}

	for pageIdx, owner := range livePages {
		if _, isFree := freePages[pageIdx]; isFree {
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "live_page_on_free_list",
				Message: fmt.Sprintf("page %d is reachable from %s and also present on the free list", pageIdx, owner),
				Page:    pageIndexPtr(pageIdx),
				Object:  owner,
			})
		}
	}

	for pageIdx := PageIndex(0); pageIdx < PageIndex(report.TotalPages); pageIdx++ {
		if _, isLive := livePages[pageIdx]; isLive {
			continue
		}
		if _, isFree := freePages[pageIdx]; isFree {
			continue
		}
		report.Issues = append(report.Issues, IntegrityIssue{
			Code:    "orphan_page",
			Message: fmt.Sprintf("page %d is not reachable from any table/index root and is not on the free list", pageIdx),
			Page:    pageIndexPtr(pageIdx),
		})
	}

	for _, table := range tables {
		report = d.checkTableIndexConsistency(ctx, report, table)
	}

	return report, nil
}

// QuickCheck performs a cheap structural health check of the open database.
//
// It validates header-linked free-list metadata and the decodability/shape of
// table and index root pages, but it does not walk full B-tree contents or
// cross-check tables against indexes.
func (d *Database) QuickCheck(ctx context.Context) (IntegrityReport, error) {
	report, freePages, tables, err := d.quickCheckState(ctx)
	if err != nil {
		return report, err
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
			if secondaryIndexUsesDedicatedInvertedStorage(index.Method) && index.InvertedIndex != nil {
				rootPages[index.InvertedIndex.GetRootPageIdx()] = fmt.Sprintf("index %s", index.Name)
			} else if index.Index != nil {
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
			if secondaryIndexUsesDedicatedInvertedStorage(index.Method) && index.InvertedIndex != nil {
				report = d.checkInvertedIndexRoot(ctx, report, table.Name, index.Name, index.InvertedIndex.GetRootPageIdx())
				continue
			}
			if index.Index == nil {
				continue
			}
			report = d.checkIndexRoot(ctx, report, table.Name, index.Name, index.Columns, false, index.Index.GetRootPageIdx())
		}
	}

	return report, nil
}

func (d *Database) quickCheckState(ctx context.Context) (IntegrityReport, map[PageIndex]struct{}, map[string]*Table, error) {
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
		return report, nil, nil, nil
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
	}

	return report, freePages, tables, nil
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

func (d *Database) checkInvertedIndexRoot(ctx context.Context, report IntegrityReport, tableName, indexName string, pageIdx PageIndex) IntegrityReport {
	if pageIdx >= PageIndex(report.TotalPages) {
		report.Issues = append(report.Issues, IntegrityIssue{
			Code:    "index_root_out_of_range",
			Message: fmt.Sprintf("index %s on table %s has root page %d outside database page range", indexName, tableName, pageIdx),
			Page:    pageIndexPtr(pageIdx),
			Object:  indexName,
		})
		return report
	}

	page, err := d.factory.ForInvertedIndex().GetPage(ctx, pageIdx)
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

	if page.InvertedEntryPage == nil {
		report.Issues = append(report.Issues, IntegrityIssue{
			Code:    "index_root_invalid_type",
			Message: fmt.Sprintf("index %s on table %s root page %d is not an inverted entry node", indexName, tableName, pageIdx),
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

func (d *Database) walkTablePages(ctx context.Context, report IntegrityReport, table *Table, root PageIndex, livePages map[PageIndex]string) IntegrityReport {
	pager := d.factory.ForTable(table.Columns)
	fields := fieldsFromColumns(table.Columns...)
	visited := make(map[PageIndex]struct{})
	stack := []PageIndex{root}
	objectName := fmt.Sprintf("table %s", table.Name)

	for len(stack) > 0 {
		pageIdx := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if pageIdx >= PageIndex(report.TotalPages) {
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "table_page_out_of_range",
				Message: fmt.Sprintf("%s references page %d outside database page range", objectName, pageIdx),
				Page:    pageIndexPtr(pageIdx),
				Object:  objectName,
			})
			continue
		}
		if _, seen := visited[pageIdx]; seen {
			continue
		}
		visited[pageIdx] = struct{}{}
		report = markLivePage(report, livePages, pageIdx, objectName)

		page, err := pager.GetPage(ctx, pageIdx)
		if err != nil {
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "table_page_decode_failed",
				Message: fmt.Sprintf("failed to decode page %d for %s: %v", pageIdx, objectName, err),
				Page:    pageIndexPtr(pageIdx),
				Object:  objectName,
			})
			continue
		}

		switch {
		case page.LeafNode != nil:
			report = d.checkTableLeafPage(ctx, report, table, page, fields, livePages)
			if nextLeaf := page.LeafNode.Header.NextLeaf; nextLeaf != 0 {
				stack = append(stack, nextLeaf)
			}
		case page.InternalNode != nil:
			if page.InternalNode.Header.RightChild == RightChildNotSet {
				report.Issues = append(report.Issues, IntegrityIssue{
					Code:    "table_internal_missing_right_child",
					Message: fmt.Sprintf("%s internal page %d has RightChildNotSet", objectName, pageIdx),
					Page:    pageIndexPtr(pageIdx),
					Object:  objectName,
				})
			} else {
				stack = append(stack, page.InternalNode.Header.RightChild)
			}
			for i := 0; i < int(page.InternalNode.Header.KeysNum); i++ {
				stack = append(stack, page.InternalNode.ICells[i].Child)
			}
		default:
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "table_page_invalid_type",
				Message: fmt.Sprintf("%s page %d is neither a leaf nor an internal node", objectName, pageIdx),
				Page:    pageIndexPtr(pageIdx),
				Object:  objectName,
			})
		}
	}

	return report
}

func (d *Database) checkTableLeafPage(ctx context.Context, report IntegrityReport, table *Table, page *Page, fields []Field, livePages map[PageIndex]string) IntegrityReport {
	for _, cell := range page.LeafNode.Cells[:page.LeafNode.Header.Cells] {
		row, err := NewRow(table.Columns).Unmarshal(cell, fields...)
		if err != nil {
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "table_row_decode_failed",
				Message: fmt.Sprintf("failed to decode row on table %s leaf page %d: %v", table.Name, page.Index, err),
				Page:    pageIndexPtr(page.Index),
				Object:  table.Name,
			})
			continue
		}
		for i, col := range table.Columns {
			if !col.Kind.IsText() || !row.Values[i].Valid {
				continue
			}
			tp, ok := row.Values[i].Value.(TextPointer)
			if !ok || tp.IsInline() || tp.FirstPage == 0 {
				continue
			}
			report = d.walkOverflowPages(report, fmt.Sprintf("table %s column %s", table.Name, col.Name), tp.FirstPage, livePages)
		}
	}
	return report
}

func (d *Database) walkOverflowPages(report IntegrityReport, objectName string, start PageIndex, livePages map[PageIndex]string) IntegrityReport {
	pager := d.factory.ForTable(mainTableColumns)
	visited := make(map[PageIndex]struct{})
	current := start

	for current != 0 {
		if current >= PageIndex(report.TotalPages) {
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "overflow_page_out_of_range",
				Message: fmt.Sprintf("%s references overflow page %d outside database page range", objectName, current),
				Page:    pageIndexPtr(current),
				Object:  objectName,
			})
			return report
		}
		if _, seen := visited[current]; seen {
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "overflow_cycle",
				Message: fmt.Sprintf("%s overflow chain contains a cycle at page %d", objectName, current),
				Page:    pageIndexPtr(current),
				Object:  objectName,
			})
			return report
		}
		visited[current] = struct{}{}
		report = markLivePage(report, livePages, current, objectName)

		page, err := pager.GetPage(context.Background(), current)
		if err != nil {
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "overflow_page_decode_failed",
				Message: fmt.Sprintf("failed to decode overflow page %d for %s: %v", current, objectName, err),
				Page:    pageIndexPtr(current),
				Object:  objectName,
			})
			return report
		}
		if page.OverflowPage == nil {
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "overflow_page_invalid_type",
				Message: fmt.Sprintf("page %d referenced by %s is not an overflow page", current, objectName),
				Page:    pageIndexPtr(current),
				Object:  objectName,
			})
			return report
		}
		current = page.OverflowPage.Header.NextPage
	}

	return report
}

func (d *Database) walkIndexPages(ctx context.Context, report IntegrityReport, tableName, indexName string, columns []Column, unique bool, root PageIndex, livePages map[PageIndex]string) IntegrityReport {
	pager := d.factory.ForIndex(columns, unique)
	visited := make(map[PageIndex]struct{})
	stack := []PageIndex{root}
	objectName := fmt.Sprintf("index %s on table %s", indexName, tableName)

	for len(stack) > 0 {
		pageIdx := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if pageIdx >= PageIndex(report.TotalPages) {
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "index_page_out_of_range",
				Message: fmt.Sprintf("%s references page %d outside database page range", objectName, pageIdx),
				Page:    pageIndexPtr(pageIdx),
				Object:  indexName,
			})
			continue
		}
		if _, seen := visited[pageIdx]; seen {
			continue
		}
		visited[pageIdx] = struct{}{}
		report = markLivePage(report, livePages, pageIdx, objectName)

		page, err := pager.GetPage(ctx, pageIdx)
		if err != nil {
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "index_page_decode_failed",
				Message: fmt.Sprintf("failed to decode page %d for %s: %v", pageIdx, objectName, err),
				Page:    pageIndexPtr(pageIdx),
				Object:  indexName,
			})
			continue
		}
		if page.IndexNode == nil {
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "index_page_invalid_type",
				Message: fmt.Sprintf("page %d reachable from %s is not an index node", pageIdx, objectName),
				Page:    pageIndexPtr(pageIdx),
				Object:  indexName,
			})
			continue
		}

		children := indexNodeChildren(page.IndexNode)
		stack = append(stack, children...)
		report = d.walkIndexOverflowPages(ctx, report, pager, objectName, page.IndexNode, livePages)
	}

	return report
}

func (d *Database) walkInvertedIndexPages(ctx context.Context, report IntegrityReport, tableName, indexName string, root PageIndex, livePages map[PageIndex]string) IntegrityReport {
	pager := d.factory.ForInvertedIndex()
	visited := make(map[PageIndex]struct{})
	stack := []PageIndex{root}
	objectName := fmt.Sprintf("index %s on table %s", indexName, tableName)

	for len(stack) > 0 {
		pageIdx := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if pageIdx >= PageIndex(report.TotalPages) {
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "index_page_out_of_range",
				Message: fmt.Sprintf("%s references page %d outside database page range", objectName, pageIdx),
				Page:    pageIndexPtr(pageIdx),
				Object:  indexName,
			})
			continue
		}
		if _, seen := visited[pageIdx]; seen {
			continue
		}
		visited[pageIdx] = struct{}{}
		report = markLivePage(report, livePages, pageIdx, objectName)

		page, err := pager.GetPage(ctx, pageIdx)
		if err != nil {
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "index_page_decode_failed",
				Message: fmt.Sprintf("failed to decode page %d for %s: %v", pageIdx, objectName, err),
				Page:    pageIndexPtr(pageIdx),
				Object:  indexName,
			})
			continue
		}
		switch {
		case page.InvertedEntryPage != nil:
			entryPage := page.InvertedEntryPage
			if entryPage.Header.IsLeaf {
				for _, cell := range entryPage.Cells {
					if cell.PostingKind == invertedPostingKindTree && cell.Child != 0 {
						stack = append(stack, cell.Child)
					}
				}
				if entryPage.Header.NextLeaf != 0 {
					stack = append(stack, entryPage.Header.NextLeaf)
				}
			} else {
				stack = append(stack, invertedEntryChildren(entryPage)...)
			}
		case page.InvertedPostPage != nil:
			postingPage := page.InvertedPostPage
			if postingPage.Header.Level == 0 {
				if postingPage.Header.NextLeaf != 0 {
					stack = append(stack, postingPage.Header.NextLeaf)
				}
			} else {
				for _, block := range postingPage.Blocks {
					if block.Child != 0 {
						stack = append(stack, block.Child)
					}
				}
			}
		default:
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "index_page_invalid_type",
				Message: fmt.Sprintf("page %d reachable from %s is not an inverted index page", pageIdx, objectName),
				Page:    pageIndexPtr(pageIdx),
				Object:  indexName,
			})
		}
	}

	return report
}

func (d *Database) walkIndexOverflowPages(ctx context.Context, report IntegrityReport, pager Pager, objectName string, node any, livePages map[PageIndex]string) IntegrityReport {
	switch n := node.(type) {
	case *IndexNode[int8]:
		return walkIndexOverflowPagesTyped(ctx, report, pager, objectName, n, livePages)
	case *IndexNode[int32]:
		return walkIndexOverflowPagesTyped(ctx, report, pager, objectName, n, livePages)
	case *IndexNode[int64]:
		return walkIndexOverflowPagesTyped(ctx, report, pager, objectName, n, livePages)
	case *IndexNode[float32]:
		return walkIndexOverflowPagesTyped(ctx, report, pager, objectName, n, livePages)
	case *IndexNode[float64]:
		return walkIndexOverflowPagesTyped(ctx, report, pager, objectName, n, livePages)
	case *IndexNode[string]:
		return walkIndexOverflowPagesTyped(ctx, report, pager, objectName, n, livePages)
	case *IndexNode[CompositeKey]:
		return walkIndexOverflowPagesTyped(ctx, report, pager, objectName, n, livePages)
	case *IndexNode[UUIDValue]:
		return walkIndexOverflowPagesTyped(ctx, report, pager, objectName, n, livePages)
	default:
		report.Issues = append(report.Issues, IntegrityIssue{
			Code:    "index_node_type_unknown",
			Message: fmt.Sprintf("unsupported index node type %T during integrity walk", node),
			Object:  objectName,
		})
		return report
	}
}

func walkIndexOverflowPagesTyped[T IndexKey](ctx context.Context, report IntegrityReport, pager Pager, objectName string, node *IndexNode[T], livePages map[PageIndex]string) IntegrityReport {
	for i := 0; i < int(node.Header.Keys); i++ {
		if node.Cells[i].Overflow == 0 {
			continue
		}
		report = walkIndexOverflowChain(ctx, report, pager, objectName, node.Cells[i].Overflow, livePages)
	}
	return report
}

func walkIndexOverflowChain(ctx context.Context, report IntegrityReport, pager Pager, objectName string, start PageIndex, livePages map[PageIndex]string) IntegrityReport {
	visited := make(map[PageIndex]struct{})
	current := start

	for current != 0 {
		if current >= PageIndex(report.TotalPages) {
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "index_overflow_out_of_range",
				Message: fmt.Sprintf("%s references index-overflow page %d outside database page range", objectName, current),
				Page:    pageIndexPtr(current),
				Object:  objectName,
			})
			return report
		}
		if _, seen := visited[current]; seen {
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "index_overflow_cycle",
				Message: fmt.Sprintf("%s index-overflow chain contains a cycle at page %d", objectName, current),
				Page:    pageIndexPtr(current),
				Object:  objectName,
			})
			return report
		}
		visited[current] = struct{}{}
		report = markLivePage(report, livePages, current, objectName)

		page, err := pager.GetPage(ctx, current)
		if err != nil {
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "index_overflow_decode_failed",
				Message: fmt.Sprintf("failed to decode index-overflow page %d for %s: %v", current, objectName, err),
				Page:    pageIndexPtr(current),
				Object:  objectName,
			})
			return report
		}
		if page.IndexOverflowNode == nil {
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "index_overflow_invalid_type",
				Message: fmt.Sprintf("page %d referenced by %s is not an index-overflow page", current, objectName),
				Page:    pageIndexPtr(current),
				Object:  objectName,
			})
			return report
		}
		current = page.IndexOverflowNode.Header.NextPage
	}

	return report
}

func markLivePage(report IntegrityReport, livePages map[PageIndex]string, pageIdx PageIndex, owner string) IntegrityReport {
	if existing, seen := livePages[pageIdx]; seen {
		if existing != owner {
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "page_reachable_from_multiple_objects",
				Message: fmt.Sprintf("page %d is reachable from both %s and %s", pageIdx, existing, owner),
				Page:    pageIndexPtr(pageIdx),
				Object:  owner,
			})
		}
		return report
	}
	livePages[pageIdx] = owner
	report.CheckedLivePages += 1
	return report
}

func indexNodeChildren(node any) []PageIndex {
	switch n := node.(type) {
	case *IndexNode[int8]:
		return n.Children()
	case *IndexNode[int32]:
		return n.Children()
	case *IndexNode[int64]:
		return n.Children()
	case *IndexNode[float32]:
		return n.Children()
	case *IndexNode[float64]:
		return n.Children()
	case *IndexNode[string]:
		return n.Children()
	case *IndexNode[CompositeKey]:
		return n.Children()
	case *IndexNode[UUIDValue]:
		return n.Children()
	default:
		return nil
	}
}

func (d *Database) checkTableIndexConsistency(ctx context.Context, report IntegrityReport, table *Table) IntegrityReport {
	if table.HasPrimaryKey() && table.PrimaryKey.Index != nil {
		report = checkIndexConsistency(ctx, report, table, indexConsistencyTarget{
			name:    table.PrimaryKey.Name,
			kind:    "primary key",
			columns: table.PrimaryKey.Columns,
			index:   table.PrimaryKey.Index,
		})
	}
	for _, index := range table.UniqueIndexes {
		if index.Index == nil {
			continue
		}
		report = checkIndexConsistency(ctx, report, table, indexConsistencyTarget{
			name:    index.Name,
			kind:    "unique index",
			columns: index.Columns,
			index:   index.Index,
		})
	}
	for _, index := range table.SecondaryIndexes {
		if secondaryIndexUsesDedicatedInvertedStorage(index.Method) {
			if index.InvertedIndex != nil {
				report = checkInvertedIndexConsistency(ctx, report, table, invertedIndexConsistencyTarget{
					name:       index.Name,
					method:     index.Method,
					index:      index.InvertedIndex,
					tableIndex: index,
				})
			}
			continue
		}
		if !index.IsBTree() || index.Index == nil {
			continue
		}
		report = checkIndexConsistency(ctx, report, table, indexConsistencyTarget{
			name:       index.Name,
			kind:       "secondary index",
			columns:    index.Columns,
			index:      index.Index,
			whereCond:  index.WhereCond,
			expression: index.Expression,
		})
	}

	return report
}

type invertedIndexConsistencyTarget struct {
	index      invertedIndex
	tableIndex SecondaryIndex
	name       string
	method     IndexMethod
}

type indexConsistencyTarget struct {
	index      BTreeIndex
	whereCond  OneOrMore // partial index predicate; nil = full index
	expression *Expr     // non-nil for expression indexes
	name       string
	kind       string
	columns    []Column
}

type integrityIndexEntries map[string]map[RowID]int

func checkInvertedIndexConsistency(ctx context.Context, report IntegrityReport, table *Table, target invertedIndexConsistencyTarget) IntegrityReport {
	objectName := fmt.Sprintf("%s index %s on table %s", target.method.String(), target.name, table.Name)

	actual, duplicateEntries, err := scanInvertedIndexEntries(ctx, target.index)
	if err != nil {
		report.Issues = append(report.Issues, IntegrityIssue{
			Code:    "index_scan_failed",
			Message: fmt.Sprintf("failed to scan %s: %v", objectName, err),
			Object:  target.name,
		})
		return report
	}

	for entryID, count := range duplicateEntries {
		if count <= 1 {
			continue
		}
		report.Issues = append(report.Issues, IntegrityIssue{
			Code:    "index_duplicate_entry",
			Message: fmt.Sprintf("%s contains duplicate entry %s (%d occurrences)", objectName, entryID, count),
			Object:  target.name,
		})
	}

	report, err = streamCheckExpectedInvertedIndexEntries(ctx, report, table, target, actual)
	if err != nil {
		report.Issues = append(report.Issues, IntegrityIssue{
			Code:    "table_scan_failed",
			Message: fmt.Sprintf("failed to scan table %s for integrity verification against %s: %v", table.Name, objectName, err),
			Object:  target.name,
		})
		return report
	}

	for termID, actualRowIDs := range actual {
		for rowID, count := range actualRowIDs {
			if count <= 0 {
				continue
			}
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "index_orphan_entry",
				Message: fmt.Sprintf("%s contains row %d for unexpected term %s", objectName, rowID, termID),
				Object:  target.name,
			})
		}
	}

	return report
}

func checkIndexConsistency(ctx context.Context, report IntegrityReport, table *Table, target indexConsistencyTarget) IntegrityReport {
	objectName := fmt.Sprintf("%s %s on table %s", target.kind, target.name, table.Name)

	actual, duplicateEntries, err := scanIndexEntries(ctx, target.index)
	if err != nil {
		report.Issues = append(report.Issues, IntegrityIssue{
			Code:    "index_scan_failed",
			Message: fmt.Sprintf("failed to scan %s: %v", objectName, err),
			Object:  target.name,
		})
		return report
	}

	for entryID, count := range duplicateEntries {
		if count <= 1 {
			continue
		}
		report.Issues = append(report.Issues, IntegrityIssue{
			Code:    "index_duplicate_entry",
			Message: fmt.Sprintf("%s contains duplicate entry %s (%d occurrences)", objectName, entryID, count),
			Object:  target.name,
		})
	}

	report, err = streamCheckExpectedIndexEntries(ctx, report, table, target, actual)
	if err != nil {
		report.Issues = append(report.Issues, IntegrityIssue{
			Code:    "table_scan_failed",
			Message: fmt.Sprintf("failed to scan table %s for integrity verification against %s: %v", table.Name, objectName, err),
			Object:  target.name,
		})
		return report
	}

	for keyID, actualRowIDs := range actual {
		for rowID, count := range actualRowIDs {
			if count <= 0 {
				continue
			}
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "index_orphan_entry",
				Message: fmt.Sprintf("%s contains row %d for unexpected key %s", objectName, rowID, keyID),
				Object:  target.name,
			})
		}
	}

	return report
}

func streamCheckExpectedInvertedIndexEntries(ctx context.Context, report IntegrityReport, table *Table, target invertedIndexConsistencyTarget, actual integrityIndexEntries) (IntegrityReport, error) {
	cursor, err := table.SeekFirst(ctx)
	if err != nil {
		return report, err
	}

	fields := fieldsFromColumns(table.Columns...)
	for !cursor.EndOfTable {
		row, err := cursor.fetchRow(ctx, true, fields...)
		if err != nil {
			return report, err
		}

		if len(target.tableIndex.WhereCond) > 0 {
			ok, err := row.CheckOneOrMore(target.tableIndex.WhereCond)
			if err != nil {
				report.Issues = append(report.Issues, IntegrityIssue{
					Code:    "index_expected_entries_failed",
					Message: fmt.Sprintf("failed to evaluate partial index predicate for %s on row %d: %v", target.name, row.Key, err),
					Object:  target.name,
				})
				continue
			}
			if !ok {
				continue
			}
		}

		expected, err := expectedInvertedIndexEntriesForRow(target.tableIndex, row)
		if err != nil {
			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "index_expected_entries_failed",
				Message: fmt.Sprintf("failed to derive expected terms for %s on row %d: %v", target.name, row.Key, err),
				Object:  target.name,
			})
			continue
		}

		for termID, count := range expected {
			rowIDs := actual[termID]
			if rowIDs[row.Key] >= count {
				rowIDs[row.Key] -= count
				if rowIDs[row.Key] == 0 {
					delete(rowIDs, row.Key)
					if len(rowIDs) == 0 {
						delete(actual, termID)
					}
				}
				continue
			}

			report.Issues = append(report.Issues, IntegrityIssue{
				Code:    "index_missing_entry",
				Message: fmt.Sprintf("%s index %s on table %s is missing row %d for term %s", target.method.String(), target.name, table.Name, row.Key, termID),
				Object:  target.name,
			})
		}
	}
	return report, nil
}

func streamCheckExpectedIndexEntries(ctx context.Context, report IntegrityReport, table *Table, target indexConsistencyTarget, actual integrityIndexEntries) (IntegrityReport, error) {
	cursor, err := table.SeekFirst(ctx)
	if err != nil {
		return report, err
	}

	fields := fieldsFromColumns(table.Columns...)
	for !cursor.EndOfTable {
		row, err := cursor.fetchRow(ctx, true, fields...)
		if err != nil {
			return report, err
		}

		// Partial index: rows not satisfying the WHERE predicate are not in the index.
		if len(target.whereCond) > 0 {
			ok, err := row.CheckOneOrMore(target.whereCond)
			if err != nil {
				report.Issues = append(report.Issues, IntegrityIssue{
					Code:    "index_expected_entries_failed",
					Message: fmt.Sprintf("failed to evaluate partial index predicate for %s on row %d: %v", target.name, row.Key, err),
					Object:  target.name,
				})
				continue
			}
			if !ok {
				continue
			}
		}

		var keyID string
		var indexed bool
		if target.expression != nil {
			keyVal, ok, err := evalExprIndexKey(target.expression, target.columns[0], row)
			if err != nil {
				report.Issues = append(report.Issues, IntegrityIssue{
					Code:    "index_expected_entries_failed",
					Message: fmt.Sprintf("failed to evaluate expression for %s on row %d: %v", target.name, row.Key, err),
					Object:  target.name,
				})
				continue
			}
			if !ok {
				continue // NULL result — row not indexed
			}
			keyID = integrityKeyID(keyVal)
			indexed = true
		} else {
			var err error
			keyID, indexed, err = integrityKeyIDFromRow(row, target.columns, target.kind == "primary key")
			if err != nil {
				report.Issues = append(report.Issues, IntegrityIssue{
					Code:    "index_expected_entries_failed",
					Message: fmt.Sprintf("failed to derive expected entry for %s on row %d: %v", target.name, row.Key, err),
					Object:  target.name,
				})
				continue
			}
			if !indexed {
				continue
			}
		}

		rowIDs := actual[keyID]
		if rowIDs[row.Key] > 0 {
			rowIDs[row.Key]--
			if rowIDs[row.Key] == 0 {
				delete(rowIDs, row.Key)
				if len(rowIDs) == 0 {
					delete(actual, keyID)
				}
			}
			continue
		}

		report.Issues = append(report.Issues, IntegrityIssue{
			Code:    "index_missing_entry",
			Message: fmt.Sprintf("%s %s on table %s is missing row %d for key %s", target.kind, target.name, table.Name, row.Key, keyID),
			Object:  target.name,
		})
	}
	return report, nil
}

func expectedInvertedIndexEntriesForRow(index SecondaryIndex, row Row) (map[string]int, error) {
	expected := make(map[string]int)
	switch index.Method {
	case IndexMethodFullText:
		tokens, err := fullTextTokenPositionsForRow(index, row)
		if err != nil {
			return nil, err
		}
		for _, token := range tokens {
			expected[integrityKeyID(token.Term)]++
		}
	case IndexMethodInverted:
		terms, err := jsonInvertedTermsForRow(index, row)
		if err != nil {
			return nil, err
		}
		for _, term := range terms {
			expected[integrityKeyID(term)]++
		}
	default:
		return nil, fmt.Errorf("unsupported inverted index method %s", index.Method.String())
	}
	return expected, nil
}

func scanInvertedIndexEntries(ctx context.Context, index invertedIndex) (integrityIndexEntries, map[string]int, error) {
	dedicated, ok := index.(*dedicatedInvertedIndex)
	if !ok {
		return nil, nil, fmt.Errorf("unsupported inverted index implementation %T", index)
	}

	entries := make(integrityIndexEntries)
	entryCounts := make(map[string]int)
	if err := scanDedicatedInvertedEntryCells(ctx, dedicated, func(cell invertedEntryCell) error {
		termID := integrityKeyID(cell.Term)
		iter, err := dedicated.newPostingIterator(ctx, cell)
		if err != nil {
			return err
		}
		for {
			block, ok, err := iter.NextBlock(ctx)
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			mode, postings, err := decodeInvertedPostingList(block.Payload)
			if err != nil {
				return err
			}
			if mode != dedicated.Mode() {
				return fmt.Errorf("inverted term %q uses posting mode %d, expected %d", cell.Term, mode, dedicated.Mode())
			}
			for _, posting := range postings {
				count := integrityInvertedPostingCount(mode, posting)
				addIntegrityEntryCount(entries, termID, posting.RowID, count)
				if mode == invertedPostingModeRowIDs {
					entryCounts[fmt.Sprintf("%s|%d", termID, posting.RowID)] += count
				}
			}
		}
		return nil
	}); err != nil {
		return nil, nil, err
	}

	return entries, entryCounts, nil
}

func scanDedicatedInvertedEntryCells(ctx context.Context, index *dedicatedInvertedIndex, visit func(invertedEntryCell) error) error {
	root, err := index.readRootEntryPage(ctx)
	if err != nil {
		return err
	}

	stack := []PageIndex{root.Index}
	for len(stack) > 0 {
		pageIdx := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		page, err := index.pager.ReadPage(ctx, pageIdx)
		if err != nil {
			return fmt.Errorf("read inverted entry page %d: %w", pageIdx, err)
		}
		if page.InvertedEntryPage == nil {
			return fmt.Errorf("page %d is not an inverted entry page", pageIdx)
		}
		entryPage := page.InvertedEntryPage
		if !entryPage.Header.IsLeaf {
			stack = append(stack, invertedEntryChildren(entryPage)...)
			continue
		}
		for _, cell := range entryPage.Cells {
			if err := visit(cell); err != nil {
				return err
			}
		}
	}
	return nil
}

func integrityInvertedPostingCount(mode invertedPostingMode, posting invertedPosting) int {
	if mode == invertedPostingModePositions {
		return len(posting.Positions)
	}
	return 1
}

func scanIndexEntries(ctx context.Context, index BTreeIndex) (integrityIndexEntries, map[string]int, error) {
	entries := make(integrityIndexEntries)
	entryCounts := make(map[string]int)
	if err := index.ScanAll(ctx, false, func(key any, rowID RowID) error {
		keyID := integrityKeyID(key)
		addIntegrityEntry(entries, keyID, rowID)
		entryCounts[fmt.Sprintf("%s|%d", keyID, rowID)]++
		return nil
	}); err != nil {
		return nil, nil, err
	}
	return entries, entryCounts, nil
}

func addIntegrityEntry(entries integrityIndexEntries, keyID string, rowID RowID) {
	addIntegrityEntryCount(entries, keyID, rowID, 1)
}

func addIntegrityEntryCount(entries integrityIndexEntries, keyID string, rowID RowID, count int) {
	rowIDs, ok := entries[keyID]
	if !ok {
		rowIDs = make(map[RowID]int)
		entries[keyID] = rowIDs
	}
	rowIDs[rowID] += count
}

func integrityKeyIDFromRow(row Row, columns []Column, required bool) (string, bool, error) {
	keyParts, ok := row.GetValuesForColumns(columns)
	if !ok {
		return "", false, fmt.Errorf("failed to get values for columns %s", columnNames(columns))
	}

	if len(columns) == 1 {
		if !keyParts[0].Valid {
			if required {
				return "", false, fmt.Errorf("required key column %s is NULL", columns[0].Name)
			}
			return "", false, nil
		}
		keyValue, err := castKeyValue(columns[0], keyParts[0].Value)
		if err != nil {
			return "", false, err
		}
		return integrityKeyID(keyValue), true, nil
	}

	keyValues := make([]any, 0, len(columns))
	for i, keyPart := range keyParts {
		if !keyPart.Valid {
			if required {
				return "", false, fmt.Errorf("required key column %s is NULL", columns[i].Name)
			}
			return "", false, nil
		}
		keyValue, err := castKeyValue(columns[i], keyPart.Value)
		if err != nil {
			return "", false, err
		}
		keyValues = append(keyValues, keyValue)
	}

	return integrityKeyID(NewCompositeKey(columns, keyValues...)), true, nil
}

func integrityKeyID(key any) string {
	switch value := key.(type) {
	case CompositeKey:
		return "composite:" + hex.EncodeToString(value.Comparison)
	default:
		return fmt.Sprintf("%T:%v", key, key)
	}
}
