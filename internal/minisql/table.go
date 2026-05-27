package minisql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"go.uber.org/zap"
)

// Table is the central runtime structure for a single database table. It holds
// the schema (columns), the root page of the B+ tree that stores row data, and
// all associated indexes (primary key, unique, secondary). Method calls on Table
// execute DML statements, maintain indexes, and enforce constraints.
type Table struct {
	PrimaryKey           PrimaryKey
	provider             TableProvider
	pager                TxPager
	columnIndexInfoCache map[string]IndexInfo
	UniqueIndexes        map[string]UniqueIndex
	SecondaryIndexes     map[string]SecondaryIndex
	logger               *zap.Logger
	columnCache          map[string]int
	txManager            *TransactionManager
	indexStats           map[string]IndexStats
	getRowCount          func() int64
	// virtualRows is non-nil only for derived-table virtual tables created by
	// executeSelectFromDerivedTable. When set, sequentialScan iterates these
	// in-memory rows instead of reading from the B+ tree pager.
	virtualRows []Row
	// parallelScan enables concurrent leaf-page scanning via parallelSequentialScan.
	// Toggled by PRAGMA parallel_scan = on/off.
	parallelScan bool
	// ForeignKeys holds all outgoing FK constraints defined on this table.
	ForeignKeys []ForeignKey
	// fkColumnSet is a fast-lookup set of column names that are FK columns.
	fkColumnSet map[string]bool
	// referencedColumns is a fast-lookup set of column names in this table that
	// are referenced by FK constraints from other tables.
	referencedColumns map[string]bool
	// checkChildFK is called before INSERT/UPDATE to verify outgoing FK constraints.
	// Set by *Database when the table is created or loaded.
	checkChildFK func(context.Context, Row) error
	// checkParentFK is called before DELETE to enforce inbound FK constraints.
	// Handles RESTRICT/CASCADE/SET NULL based on each FK's OnDelete action.
	checkParentFK func(context.Context, Row) error
	// enforceParentFKOnUpdate is called before UPDATE to enforce inbound FK constraints.
	// Handles RESTRICT/CASCADE/SET NULL based on each FK's OnUpdate action.
	enforceParentFKOnUpdate func(context.Context, Row, Row) error
	// planCache is the shared plan cache from *Database. When non-nil, PlanQuery
	// checks here first and stores results after planning. Nil for system tables
	// and virtual tables created for derived-table subqueries.
	planCache LRUCache[string]
	// allFields and textOverflowCols are derived from Columns at construction time
	// and reused across calls to avoid per-call allocations.
	allFields        []Field
	textOverflowCols []Column
	// rightmostTablePage caches the last leaf page index for SeekNextRowID so that
	// sequential (autoincrement) inserts skip the O(log N) root→leaf traversal.
	// lastTxIDTablePage guards against stale hints from rolled-back transactions.
	rightmostTablePage atomic.Int64
	lastTxIDTablePage  atomic.Uint64
	Name               string
	Columns            []Column
	rootPageIdx        PageIndex
	maximumICells      uint32
}

// NewTable constructs a Table and applies the given options (primary key, unique
// and secondary indexes, FK callbacks, etc.). If provider is nil a simple
// single-table provider is created, which is sufficient for unit tests.
func NewTable(logger *zap.Logger, pager TxPager, txManager *TransactionManager, name string, columns []Column, rootPageIdx PageIndex, provider TableProvider, opts ...TableOption) *Table {
	table := &Table{
		Name:                 name,
		Columns:              columns,
		columnCache:          make(map[string]int, len(columns)),
		rootPageIdx:          rootPageIdx,
		maximumICells:        InternalNodeMaxCells,
		logger:               logger,
		pager:                pager,
		txManager:            txManager,
		UniqueIndexes:        make(map[string]UniqueIndex),
		SecondaryIndexes:     make(map[string]SecondaryIndex),
		columnIndexInfoCache: make(map[string]IndexInfo),
		indexStats:           make(map[string]IndexStats),
		provider:             provider,
	}

	table.rightmostTablePage.Store(-1)

	// If no provider is given, create a simple single-table provider for testing
	if provider == nil {
		table.provider = &singleTableProvider{table: table}
	}

	// Build column name -> column index cache; also pre-compute derived column slices.
	for i, col := range columns {
		table.columnCache[col.Name] = i
	}
	table.allFields = fieldsFromColumns(columns...)
	for _, col := range columns {
		if col.MayUseOverflowText() {
			table.textOverflowCols = append(table.textOverflowCols, col)
		}
	}

	// Apply options
	for _, opt := range opts {
		opt(table)
	}

	// Build column name -> IndexInfo cache
	if table.HasPrimaryKey() {
		table.columnIndexInfoCache[indexColumnHash(table.PrimaryKey.Columns)] = table.PrimaryKey.IndexInfo
	}
	for _, index := range table.UniqueIndexes {
		table.columnIndexInfoCache[indexColumnHash(index.Columns)] = index.IndexInfo
	}
	for _, index := range table.SecondaryIndexes {
		if !index.IsBTree() {
			continue
		}
		table.columnIndexInfoCache[indexColumnHash(index.Columns)] = index.IndexInfo
	}

	return table
}

func indexColumnHash(columns []Column) string {
	var hash strings.Builder
	for i, col := range columns {
		hash.WriteString(col.Name)
		if i < len(columns)-1 {
			hash.WriteString("|")
		}
	}
	return hash.String()
}

// singleTableProvider is a simple TableProvider for single-table scenarios (e.g., tests)
type singleTableProvider struct {
	table *Table
}

// GetTable returns the single table this provider wraps, or a CTE virtual table
// stored in the context, if the requested name matches.
func (p *singleTableProvider) GetTable(ctx context.Context, name string) (*Table, bool) {
	if vt, ok := cteFromContext(ctx, name); ok {
		return vt, true
	}
	if p.table != nil && p.table.Name == name {
		return p.table, true
	}
	return nil, false
}

// SetSecondaryIndex registers or replaces a secondary index on the table and
// updates the column-to-IndexInfo cache used by the query planner.
func (t *Table) SetSecondaryIndex(si SecondaryIndex) {
	t.SecondaryIndexes[si.Name] = si
	if !si.IsBTree() {
		return
	}
	if si.Expression != nil {
		// Expression indexes are looked up by their SQL text, not by column name.
		t.columnIndexInfoCache[si.ExpressionSQL] = si.IndexInfo
	} else {
		t.columnIndexInfoCache[indexColumnHash(si.Columns)] = si.IndexInfo
	}
}

// RemoveSecondaryIndex removes the named secondary index from the table and
// clears it from the column-to-IndexInfo cache.
func (t *Table) RemoveSecondaryIndex(name string) {
	si, ok := t.SecondaryIndexes[name]
	if !ok {
		return
	}
	if !si.IsBTree() {
		delete(t.SecondaryIndexes, name)
		return
	}
	if si.Expression != nil {
		delete(t.columnIndexInfoCache, si.ExpressionSQL)
	} else {
		delete(t.columnIndexInfoCache, indexColumnHash(si.Columns))
	}
	delete(t.SecondaryIndexes, name)
}

// FindExpressionIndex returns the secondary index whose Expression tree is
// structurally equal to expr, or (SecondaryIndex{}, false) if none exists.
func (t *Table) FindExpressionIndex(expr *Expr) (SecondaryIndex, bool) {
	for _, si := range t.SecondaryIndexes {
		if si.IsBTree() && si.Expression != nil && exprEqual(si.Expression, expr) {
			return si, true
		}
	}
	return SecondaryIndex{}, false
}

// GetRootPageIdx returns the page index of the root page for the table's row B+ tree.
func (t *Table) GetRootPageIdx() PageIndex {
	return t.rootPageIdx
}

// HasNoIndex reports whether the table has no indexes at all — no primary key,
// no unique indexes, and no secondary indexes.
func (t *Table) HasNoIndex() bool {
	if t.HasPrimaryKey() || len(t.UniqueIndexes) > 0 {
		return false
	}
	for _, idx := range t.SecondaryIndexes {
		if idx.IsBTree() || idx.Method == IndexMethodFullText || idx.Method == IndexMethodInverted {
			return false
		}
	}
	return true
}

// HasPrimaryKey reports whether a primary key index has been defined for the table.
func (t *Table) HasPrimaryKey() bool {
	return t.PrimaryKey.Name != ""
}

// ColumnByName looks up a column in the table's schema by name using the column cache.
func (t *Table) ColumnByName(name string) (Column, bool) {
	if idx, ok := t.columnCache[name]; ok {
		return t.Columns[idx], true
	}
	return Column{}, false
}

// IndexColumnsByIndexName returns the ordered column list for the named index
// (primary key, unique, or secondary), or (nil, false) if no such index exists.
func (t *Table) IndexColumnsByIndexName(name string) ([]Column, bool) {
	if t.HasPrimaryKey() && t.PrimaryKey.Name == name {
		return t.PrimaryKey.Columns, true
	}
	if index, ok := t.UniqueIndexes[name]; ok {
		return index.Columns, true
	}
	if index, ok := t.SecondaryIndexes[name]; ok {
		return index.Columns, true
	}
	return nil, false
}

// HasIndexOnColumns reports whether any index (primary, unique, or secondary)
// covers exactly the given ordered column list.
func (t *Table) HasIndexOnColumns(columns []Column) bool {
	if len(columns) == 0 {
		return false
	}
	_, ok := t.columnIndexInfoCache[indexColumnHash(columns)]
	return ok
}

// HasIndexOnColumn reports whether any single-column index covers the named column.
func (t *Table) HasIndexOnColumn(name string) bool {
	_, ok := t.columnIndexInfoCache[name]
	return ok
}

// IndexInfoByColumnName returns the IndexInfo for the index covering the named
// single column, or (IndexInfo{}, false) if no such index exists.
func (t *Table) IndexInfoByColumnName(name string) (IndexInfo, bool) {
	info, ok := t.columnIndexInfoCache[name]
	if !ok {
		return IndexInfo{}, false
	}
	return info, true
}

// IndexInfoByColumns looks up an index whose columns (in order) exactly match the given slice.
func (t *Table) IndexInfoByColumns(columns []Column) (IndexInfo, bool) {
	info, ok := t.columnIndexInfoCache[indexColumnHash(columns)]
	if !ok {
		return IndexInfo{}, false
	}
	return info, true
}

// IndexByName returns the BTreeIndex for the named index (primary, unique, or secondary).
func (t *Table) IndexByName(name string) (BTreeIndex, bool) {
	if t.HasPrimaryKey() && t.PrimaryKey.Name == name {
		return t.PrimaryKey.Index, true
	}
	if index, ok := t.UniqueIndexes[name]; ok {
		return index.Index, true
	}
	if index, ok := t.SecondaryIndexes[name]; ok {
		if !index.IsBTree() {
			return nil, false
		}
		return index.Index, true
	}
	return nil, false
}

// SeekNextRowID returns cursor pointing at the position after the last row ID
// plus a new row ID to insert
func (t *Table) SeekNextRowID(ctx context.Context, pageIdx PageIndex) (*Cursor, RowID, error) {
	// Fast path: skip the root→leaf traversal when we already know the last leaf.
	// Only applies at the root entry point (not during internal recursion) and
	// only when running inside a transaction — without a transaction context we
	// cannot guard against stale hints from rolled-back transactions.
	if pageIdx == t.rootPageIdx {
		if tx := TxFromContext(ctx); tx != nil {
			// Per-transaction guard: invalidate the hint when a new transaction starts
			// so that a stale page left behind by a rolled-back transaction is never reused.
			if uint64(tx.ID) != t.lastTxIDTablePage.Load() {
				t.rightmostTablePage.Store(-1)
				t.lastTxIDTablePage.Store(uint64(tx.ID))
			}
			if cached := t.rightmostTablePage.Load(); cached >= 0 {
				page, err := t.pager.ReadPage(ctx, PageIndex(cached))
				if err == nil && page.LeafNode != nil && page.LeafNode.Header.NextLeaf == 0 {
					maxKey, err := t.GetMaxKey(ctx, page)
					nextRowID := maxKey
					if err == nil {
						nextRowID = maxKey + 1
					}
					return &Cursor{
						Table:   t,
						PageIdx: PageIndex(cached),
						CellIdx: page.LeafNode.Header.Cells,
					}, nextRowID, nil
				}
				// Stale or no longer the last leaf — fall through.
				t.rightmostTablePage.Store(-1)
			}
		}
	}

	page, err := t.pager.ReadPage(ctx, pageIdx)
	if err != nil {
		return nil, 0, fmt.Errorf("seek next row ID: %w", err)
	}
	if page.LeafNode == nil {
		return t.SeekNextRowID(ctx, page.InternalNode.Header.RightChild)
	}
	if page.LeafNode.Header.NextLeaf != 0 {
		return t.SeekNextRowID(ctx, page.LeafNode.Header.NextLeaf)
	}
	maxKey, err := t.GetMaxKey(ctx, page)
	nextRowID := maxKey
	if err == nil {
		nextRowID = maxKey + 1
	}
	// Warm the cache: this is the confirmed last leaf.
	t.rightmostTablePage.Store(int64(pageIdx))
	return &Cursor{
		Table:   t,
		PageIdx: pageIdx,
		CellIdx: page.LeafNode.Header.Cells,
	}, nextRowID, nil
}

// SeekFirst returns a cursor pointing at the first row in the table.
func (t *Table) SeekFirst(ctx context.Context) (*Cursor, error) {
	pageIdx := t.GetRootPageIdx()
	page, err := t.pager.ReadPage(ctx, pageIdx)
	if err != nil {
		return nil, fmt.Errorf("seek first: %w", err)
	}
	for page.LeafNode == nil {
		pageIdx = page.InternalNode.ICells[0].Child
		page, err = t.pager.ReadPage(ctx, pageIdx)
		if err != nil {
			return nil, fmt.Errorf("seek first: %w", err)
		}
	}
	return &Cursor{
		Table:      t,
		PageIdx:    pageIdx,
		CellIdx:    0,
		EndOfTable: page.LeafNode.Header.Cells == 0,
	}, nil
}

// SeekLast returns cursor pointing at the last row in the table
func (t *Table) SeekLast(ctx context.Context, pageIdx PageIndex) (*Cursor, error) {
	page, err := t.pager.ReadPage(ctx, pageIdx)
	if err != nil {
		return nil, fmt.Errorf("seek next row ID: %w", err)
	}
	if page.LeafNode == nil {
		return t.SeekLast(ctx, page.InternalNode.Header.RightChild)
	}
	if page.LeafNode.Header.NextLeaf != 0 {
		return t.SeekLast(ctx, page.LeafNode.Header.NextLeaf)
	}
	return &Cursor{
		Table:   t,
		PageIdx: pageIdx,
		CellIdx: page.LeafNode.Header.Cells - 1,
	}, nil
}

// Seek the cursor for a key, if it does not exist then return the cursor
// for the page and cell where it should be inserted
func (t *Table) Seek(ctx context.Context, key RowID) (*Cursor, error) {
	rootPage, err := t.pager.ReadPage(ctx, t.GetRootPageIdx())
	if err != nil {
		return nil, fmt.Errorf("seek: %w", err)
	}
	if rootPage.LeafNode != nil {
		return t.leafNodeSeek(t.GetRootPageIdx(), rootPage, key)
	} else if rootPage.InternalNode != nil {
		return t.internalNodeSeek(ctx, rootPage, key)
	}
	return nil, errors.New("root page type")
}

func (t *Table) leafNodeSeek(pageIdx PageIndex, page *Page, key RowID) (*Cursor, error) {
	var (
		minIdx uint32
		maxIdx = page.LeafNode.Header.Cells

		cursor = Cursor{
			Table:   t,
			PageIdx: pageIdx,
		}
	)

	// Search the Btree
	for i := maxIdx; i != minIdx; {
		index := (minIdx + i) / 2
		keyIdx := page.LeafNode.Cells[index].Key
		if key == keyIdx {
			cursor.CellIdx = index
			return &cursor, nil
		}
		if key < keyIdx {
			i = index
		} else {
			minIdx = index + 1
		}
	}

	cursor.CellIdx = minIdx

	return &cursor, nil
}

func (t *Table) internalNodeSeek(ctx context.Context, page *Page, key RowID) (*Cursor, error) {
	childIdx := page.InternalNode.IndexOfChild(key)
	childPageIdx, err := page.InternalNode.Child(childIdx)
	if err != nil {
		return nil, err
	}

	childPage, err := t.pager.ReadPage(ctx, childPageIdx)
	if err != nil {
		return nil, fmt.Errorf("internal node seek: %w", err)
	}

	if childPage.InternalNode != nil {
		return t.internalNodeSeek(ctx, childPage, key)
	}
	return t.leafNodeSeek(childPageIdx, childPage, key)
}

// Handle splitting the root.
// Old root copied to new page, becomes left child.
// Address of right child passed in.
// Re-initialize root page to contain the new root node.
// New root node points to two children.
func (t *Table) createNewRoot(ctx context.Context, rightChildPageIdx PageIndex) (*Page, error) {
	oldRootPage, err := t.pager.ModifyPage(ctx, t.GetRootPageIdx())
	if err != nil {
		return nil, fmt.Errorf("get old root page: %w", err)
	}
	rightChildPage, err := t.pager.ModifyPage(ctx, rightChildPageIdx)
	if err != nil {
		return nil, fmt.Errorf("get right child page: %w", err)
	}

	// Use recycled page if available, otherwise create new one
	leftChildPage, err := t.pager.GetFreePage(ctx)
	if err != nil {
		return nil, fmt.Errorf("get left child page: %w", err)
	}

	if ce := t.logger.Check(zap.DebugLevel, "create new root"); ce != nil {
		ce.Write(zap.Int("left_child_index", int(leftChildPage.Index)), zap.Int("right_child_index", int(rightChildPageIdx)))
	}

	// Copy all node contents to left child
	if oldRootPage.LeafNode != nil {
		leftChildPage.LeafNode = NewLeafNode()
		*leftChildPage.LeafNode = *oldRootPage.LeafNode
		leftChildPage.LeafNode.Header.IsRoot = false
	} else if oldRootPage.InternalNode != nil {
		// New pages by default are leafs so we need to reset left child page
		// as an internal node here
		leftChildPage.LeafNode = nil
		leftChildPage.InternalNode = NewInternalNode()
		*leftChildPage.InternalNode = *oldRootPage.InternalNode
		leftChildPage.InternalNode.Header.IsRoot = false
		// Update parent for all child pages
		for i := 0; i < int(leftChildPage.InternalNode.Header.KeysNum); i++ {
			childPage, err := t.pager.ModifyPage(ctx, leftChildPage.InternalNode.ICells[i].Child)
			if err != nil {
				return nil, fmt.Errorf("get child page: %w", err)
			}
			childPage.setParent(leftChildPage.Index)
		}
		// Don't forget right child
		childPage, err := t.pager.ModifyPage(ctx, leftChildPage.InternalNode.Header.RightChild)
		if err != nil {
			return nil, fmt.Errorf("get right child page: %w", err)
		}
		childPage.setParent(leftChildPage.Index)
	}

	// Change root node to a new internal node
	newRootNode := NewInternalNode()
	oldRootPage.LeafNode = nil
	oldRootPage.InternalNode = newRootNode
	newRootNode.Header.IsRoot = true
	newRootNode.Header.KeysNum = 1

	// Set left and right child
	newRootNode.Header.RightChild = rightChildPageIdx
	if err := newRootNode.SetChild(0, leftChildPage.Index); err != nil {
		return nil, err
	}
	leftChildMaxKey, err := t.GetMaxKey(ctx, leftChildPage)
	if err != nil {
		return nil, fmt.Errorf("get max key: %w", err)
	}
	newRootNode.ICells[0].Key = leftChildMaxKey

	// Set parent for both left and right child
	leftChildPage.setParent(t.GetRootPageIdx())
	rightChildPage.setParent(t.GetRootPageIdx())

	return leftChildPage, nil
}

// InternalNodeInsert adds a new child/key pair to the parent internal node corresponding to the given child page.
func (t *Table) InternalNodeInsert(ctx context.Context, parentPageIdx, childPageIdx PageIndex) error {
	parentPage, err := t.pager.ModifyPage(ctx, parentPageIdx)
	if err != nil {
		return fmt.Errorf("internal node insert: %w", err)
	}

	childPage, err := t.pager.ModifyPage(ctx, childPageIdx)
	if err != nil {
		return fmt.Errorf("internal node insert: %w", err)
	}
	childPage.setParent(parentPageIdx)

	childMaxKey, err := t.GetMaxKey(ctx, childPage)
	if err != nil {
		return fmt.Errorf("internal node insert: %w", err)
	}
	var (
		index            = parentPage.InternalNode.IndexOfChild(childMaxKey)
		originalKeyCount = parentPage.InternalNode.Header.KeysNum
	)

	if parentPage.InternalNode.Header.KeysNum >= uint32(t.maxICells(parentPageIdx)) {
		return t.InternalNodeSplitInsert(ctx, parentPageIdx, childPageIdx)
	}

	/*
	  An internal node with a right child of RightChildNotSet is empty
	*/
	if parentPage.InternalNode.Header.RightChild == RightChildNotSet {
		parentPage.InternalNode.Header.RightChild = childPageIdx
		return nil
	}

	/*
	  If we are already at the max number of cells for a node, we cannot increment
	  before splitting. Incrementing without inserting a new key/child pair
	  and immediately calling internal_node_split_and_insert has the effect
	  of creating a new key at (max_cells + 1) with an uninitialized value
	*/
	parentPage.InternalNode.Header.KeysNum += 1

	rightChildPageIdx := parentPage.InternalNode.Header.RightChild
	rightChildPage, err := t.pager.ModifyPage(ctx, rightChildPageIdx)
	if err != nil {
		return fmt.Errorf("internal node insert: %w", err)
	}

	rightChildMaxKey, err := t.GetMaxKey(ctx, rightChildPage)
	if err != nil {
		return fmt.Errorf("internal node insert: %w", err)
	}
	if childMaxKey > rightChildMaxKey {
		// Replace right child
		if err := parentPage.InternalNode.SetChild(originalKeyCount, rightChildPageIdx); err != nil {
			return fmt.Errorf("internal node insert: %w", err)
		}
		parentPage.InternalNode.ICells[originalKeyCount].Key = rightChildMaxKey
		parentPage.InternalNode.Header.RightChild = childPageIdx
		return nil
	}

	// Make room for the new cell
	for i := originalKeyCount; i > index; i-- {
		parentPage.InternalNode.ICells[i] = parentPage.InternalNode.ICells[i-1]
	}
	if err := parentPage.InternalNode.SetChild(index, childPageIdx); err != nil {
		return fmt.Errorf("internal node insert: %w", err)
	}
	parentPage.InternalNode.ICells[index].Key = childMaxKey

	return nil
}

// InternalNodeSplitInsert splits an internal node and inserts the child.
// It creates a sibling to hold (n-1)/2 keys, updates the parent's max key,
// inserts the sibling into the parent (possibly triggering further splits),
// and creates a new root if the original node was the root.
func (t *Table) InternalNodeSplitInsert(ctx context.Context, pageIdx, childPageIdx PageIndex) error {
	splitPage, err := t.pager.ModifyPage(ctx, pageIdx)
	if err != nil {
		return fmt.Errorf("internal node split insert: %w", err)
	}
	splittingRoot := splitPage.InternalNode.Header.IsRoot
	oldMaxKey, err := t.GetMaxKey(ctx, splitPage)
	if err != nil {
		return fmt.Errorf("internal node split insert: %w", err)
	}

	childPage, err := t.pager.ModifyPage(ctx, childPageIdx)
	if err != nil {
		return fmt.Errorf("internal node split insert: %w", err)
	}
	childMaxKey, err := t.GetMaxKey(ctx, childPage)
	if err != nil {
		return fmt.Errorf("internal node split insert: %w", err)
	}

	// Create a new page, it will be on the same level as original node and to the right of it
	// Use recycled page if available, otherwise create new one
	newPage, err := t.pager.GetFreePage(ctx)
	if err != nil {
		return fmt.Errorf("get new page: %w", err)
	}
	// Make sure the new page is an internal node
	newPage.InternalNode = NewInternalNode()
	newPage.LeafNode = nil

	if ce := t.logger.Check(zap.DebugLevel, "internal node split insert"); ce != nil {
		ce.Write(zap.Int("page_index", int(pageIdx)), zap.Int("new_page_index", int(newPage.Index)))
	}

	if splittingRoot {
		/*
		   If we are splitting the root, we need to update old_node to point
		   to the new root's left child, new_page_num will already point to
		   the new root's right child
		*/
		splitPage, err = t.createNewRoot(ctx, newPage.Index)
		if err != nil {
			return fmt.Errorf("create new root: %w", err)
		}
	}
	newPage.InternalNode.Header.Parent = splitPage.InternalNode.Header.Parent

	maxICells := t.maxICells(pageIdx)

	// First put right child into new node and set right child of old node to invalid page number
	newPage.InternalNode.Header.RightChild = splitPage.InternalNode.Header.RightChild
	newPageRightChild, err := t.pager.ModifyPage(ctx, newPage.InternalNode.Header.RightChild)
	if err != nil {
		return fmt.Errorf("internal node split insert: %w", err)
	}
	newPageRightChild.setParent(newPage.Index)
	splitPage.InternalNode.Header.RightChild = RightChildNotSet

	// For each key until you get to the middle key, move the key and the child to the new node
	for i := maxICells - 1; i > maxICells/2; i-- {
		if err := t.InternalNodeInsert(ctx, newPage.Index, splitPage.InternalNode.ICells[i].Child); err != nil {
			return fmt.Errorf("internal node split insert: %w", err)
		}
		splitPage.InternalNode.ICells[i] = ICell{}
		splitPage.InternalNode.Header.KeysNum -= 1
	}

	// Set child before middle key, which is now the highest key, to be node's right child,
	// and decrement number of keys
	splitPage.InternalNode.Header.RightChild, err = splitPage.InternalNode.Child(splitPage.InternalNode.Header.KeysNum - 1)
	if err != nil {
		return fmt.Errorf("internal node split insert: %w", err)
	}
	splitPage.InternalNode.RemoveLastCell()

	maxAfterSplit, err := t.GetMaxKey(ctx, splitPage)
	if err != nil {
		return fmt.Errorf("internal node split insert: %w", err)
	}

	// Determine which of the two nodes after the split should contain the child to be inserted,
	// and insert the child
	if childMaxKey < maxAfterSplit {
		if err := t.InternalNodeInsert(ctx, pageIdx, childPageIdx); err != nil {
			return fmt.Errorf("internal node split insert: %w", err)
		}
		childPage.setParent(pageIdx)
	} else {
		if err := t.InternalNodeInsert(ctx, newPage.Index, childPageIdx); err != nil {
			return fmt.Errorf("internal node split insert: %w", err)
		}
		childPage.setParent(newPage.Index)
	}

	parentPage, err := t.pager.ModifyPage(ctx, splitPage.InternalNode.Header.Parent)
	if err != nil {
		return fmt.Errorf("internal node split insert: %w", err)
	}
	// Update the parent's key for the split (left) page from oldMaxKey to maxAfterSplit.
	// IndexOfChild returns KeysNum when splitPage is the parent's right child (no explicit ICell key).
	// In that case, the InternalNodeInsert call below will handle the demotion naturally:
	// newPage (with the larger keys) replaces the right child slot and splitPage gets
	// an ICell with key=maxAfterSplit.
	if idx := parentPage.InternalNode.IndexOfChild(oldMaxKey); idx < parentPage.InternalNode.Header.KeysNum {
		parentPage.InternalNode.ICells[idx].Key = maxAfterSplit
	}

	if splittingRoot {
		return nil
	}

	return t.InternalNodeInsert(ctx, splitPage.InternalNode.Header.Parent, newPage.Index)
}

// GetMaxKey returns the largest RowID in the subtree rooted at page by
// following right-child pointers down to the rightmost leaf.
func (t *Table) GetMaxKey(ctx context.Context, page *Page) (RowID, error) {
	if page.LeafNode != nil {
		if page.LeafNode.Header.Cells == 0 {
			return 0, errors.New("get max key: leaf node has no cells")
		}
		return page.LeafNode.Cells[page.LeafNode.Header.Cells-1].Key, nil
	}
	rightChild, err := t.pager.ReadPage(ctx, page.InternalNode.Header.RightChild)
	if err != nil {
		return 0, err
	}
	return t.GetMaxKey(ctx, rightChild)
}

// DeleteKey deletes a key from the table, when this is called, you should already
// have located the leaf that contains the key and pass its page and cell index here.
// The deletion process starts at the leaf and then recursively bubbles up the tree.
func (t *Table) DeleteKey(ctx context.Context, pageIdx PageIndex, key RowID) error {
	page, err := t.pager.ModifyPage(ctx, pageIdx)
	if err != nil {
		return fmt.Errorf("delete key: %w", err)
	}

	if page.LeafNode == nil {
		return errors.New("DeleteKey called on non-leaf node")
	}

	// Remove key
	cellToDelete, ok := page.LeafNode.Delete(key)

	// Remove any overflow pages
	if overflowFields := textOverflowFields(t.Columns...); len(overflowFields) > 0 && ok {
		row, err := NewRowView(t.Columns, cellToDelete).MaterializeWithOverflow(ctx, t.pager, selectedColumnsMask(t.Columns, overflowFields))
		if err != nil {
			return err
		}
		if err := t.freeOverflowPages(ctx, row); err != nil {
			return err
		}
	}

	// Check for underflow
	if page.LeafNode.AtLeastHalfFull() {
		return t.updateParentMaxKey(ctx, page)
	}

	// Rebalance leaf node
	if err := t.rebalanceLeaf(ctx, page); err != nil {
		return err
	}

	return nil
}

func (t *Table) updateParentMaxKey(ctx context.Context, page *Page) error {
	if page == nil {
		return nil
	}

	var parentIdx PageIndex
	switch {
	case page.LeafNode != nil:
		if page.LeafNode.Header.IsRoot {
			return nil
		}
		parentIdx = page.LeafNode.Header.Parent
	case page.InternalNode != nil:
		if page.InternalNode.Header.IsRoot {
			return nil
		}
		parentIdx = page.InternalNode.Header.Parent
	default:
		return nil
	}

	parentPage, err := t.pager.ModifyPage(ctx, parentIdx)
	if err != nil {
		return fmt.Errorf("update parent max key: %w", err)
	}

	position, err := parentPage.InternalNode.IndexOfPage(page.Index)
	if err != nil {
		return fmt.Errorf("update parent max key: %w", err)
	}
	if position < parentPage.InternalNode.Header.KeysNum {
		maxKey, err := t.GetMaxKey(ctx, page)
		if err != nil {
			return fmt.Errorf("update parent max key: %w", err)
		}
		if parentPage.InternalNode.ICells[position].Key == maxKey {
			return nil
		}
		parentPage.InternalNode.ICells[position].Key = maxKey
		return t.updateParentMaxKey(ctx, parentPage)
	}

	return t.updateParentMaxKey(ctx, parentPage)
}

func (t *Table) refreshInternalNodeKeys(ctx context.Context, page *Page) error {
	if page == nil || page.InternalNode == nil {
		return nil
	}

	for i := uint32(0); i < page.InternalNode.Header.KeysNum; i++ {
		childIdx, err := page.InternalNode.Child(i)
		if err != nil {
			return fmt.Errorf("refresh internal node keys: %w", err)
		}

		childPage, err := t.pager.ReadPage(ctx, childIdx)
		if err != nil {
			return fmt.Errorf("refresh internal node keys: %w", err)
		}

		maxKey, err := t.GetMaxKey(ctx, childPage)
		if err != nil {
			return fmt.Errorf("refresh internal node keys: %w", err)
		}
		page.InternalNode.ICells[i].Key = maxKey
	}

	return nil
}

func (t *Table) freeOverflowPages(ctx context.Context, row Row, onlyForColumns ...Column) error {
	columns := row.Columns
	if onlyForColumns != nil {
		columns = onlyForColumns
	}
	for _, col := range columns {
		if !col.Kind.IsText() {
			continue
		}
		value, ok := row.GetValue(col.Name)
		if !ok || !value.Valid {
			continue
		}
		textPointer, ok := value.Value.(TextPointer)
		if !ok {
			return fmt.Errorf("expected TextPointer value for text column %s", col.Name)
		}
		if textPointer.IsInline() {
			continue
		}
		pagesToFree := make([]PageIndex, 0, 1)
		overflowPage, err := t.pager.GetOverflowPage(ctx, textPointer.FirstPage)
		if err != nil {
			return err
		}
		pagesToFree = append(pagesToFree, overflowPage.Index)
		for overflowPage.OverflowPage.Header.NextPage > 0 {
			overflowPage, err = t.pager.GetOverflowPage(ctx, overflowPage.OverflowPage.Header.NextPage)
			if err != nil {
				return err
			}
			pagesToFree = append(pagesToFree, overflowPage.Index)
		}
		for _, pageIdx := range pagesToFree {
			if err := t.pager.AddFreePage(ctx, pageIdx); err != nil {
				return err
			}
		}
	}

	return nil
}

func (t *Table) rebalanceLeaf(ctx context.Context, page *Page) error {
	leafNode := page.LeafNode

	if leafNode.Header.IsRoot {
		return nil
	}

	parentPage, err := t.pager.ModifyPage(ctx, leafNode.Header.Parent)
	if err != nil {
		return fmt.Errorf("rebalance leaf: %w", err)
	}
	myPositionInParent, err := parentPage.InternalNode.IndexOfPage(page.Index)
	if err != nil {
		return fmt.Errorf("rebalance leaf: %w", err)
	}

	var (
		left  *Page
		right *Page
	)
	if myPositionInParent > 0 {
		left, err = t.pager.ModifyPage(ctx, parentPage.InternalNode.ICells[myPositionInParent-1].Child)
		if err != nil {
			return fmt.Errorf("rebalance leaf: %w", err)
		}
	} else {
		right, err = t.pager.ModifyPage(ctx, parentPage.InternalNode.GetRightChildByIndex(myPositionInParent))
		if err != nil {
			return fmt.Errorf("rebalance leaf: %w", err)
		}
	}

	if right != nil && right.LeafNode.CanBorrowFirst() {
		if err := t.borrowFromRightLeaf(
			parentPage.InternalNode,
			leafNode,
			right.LeafNode,
			myPositionInParent,
		); err != nil {
			return err
		}
		if err := t.refreshInternalNodeKeys(ctx, parentPage); err != nil {
			return err
		}
		return t.updateParentMaxKey(ctx, parentPage)
	}

	if left != nil && left.LeafNode.CanBorrowLast() {
		if err := t.borrowFromLeftLeaf(
			parentPage.InternalNode,
			leafNode,
			left.LeafNode,
			myPositionInParent-1,
		); err != nil {
			return err
		}
		if err := t.refreshInternalNodeKeys(ctx, parentPage); err != nil {
			return err
		}
		return t.updateParentMaxKey(ctx, parentPage)
	}

	if right != nil && leafNode.CanMergeWith(right.LeafNode) {
		if err := t.mergeLeaves(
			ctx,
			parentPage,
			page,
			right,
			myPositionInParent,
		); err != nil {
			return err
		}

		if err := t.pager.AddFreePage(ctx, right.Index); err != nil {
			return err
		}
		if err := t.refreshInternalNodeKeys(ctx, parentPage); err != nil {
			return err
		}
		return t.updateParentMaxKey(ctx, parentPage)
	}

	if left != nil && leafNode.CanMergeWith(left.LeafNode) {
		if err := t.mergeLeaves(
			ctx,
			parentPage,
			left,
			page,
			myPositionInParent-1,
		); err != nil {
			return err
		}

		if err := t.pager.AddFreePage(ctx, page.Index); err != nil {
			return err
		}
		if err := t.refreshInternalNodeKeys(ctx, parentPage); err != nil {
			return err
		}
		return t.updateParentMaxKey(ctx, parentPage)
	}

	return nil
}

// borrowFromLeftLeaf borrows a key from the left neighbor of the given leaf node.
// It inserts the last key and value from the left neighbor into the given node,
// and removes the key and value from the left neighbor.
// It also updates the key in the parent node.
func (t *Table) borrowFromLeftLeaf(parent *InternalNode, node, left *LeafNode, idx uint32) error {
	left.PrepareModifyCell(left.Header.Cells - 1)
	cellToRotate := left.LastCell()
	left.RemoveLastCell()

	node.PrependCell(cellToRotate)

	parent.ICells[idx].Key = left.LastCell().Key

	return nil
}

// borrowFromRightLeaf borrows a key from the right neighbor of the given leaf node.
// It inserts the first key and value from the right neighbor into the given node,
// and removes the key and value from the right neighbor.
// It also updates the key in the parent node.
func (t *Table) borrowFromRightLeaf(parent *InternalNode, node, right *LeafNode, idx uint32) error {
	right.PrepareModifyCell(0)
	cellToRotate := right.FirstCell()
	right.RemoveFirstCell()

	node.AppendCells(cellToRotate)

	parent.ICells[idx].Key = node.LastCell().Key

	return nil
}

// mergeLeaves merges two leaf nodes and deletes the key from the parent node.
func (t *Table) mergeLeaves(ctx context.Context, parent, left, right *Page, idx uint32) error {
	for i := range right.LeafNode.Header.Cells {
		right.LeafNode.PrepareModifyCell(i)
	}
	left.LeafNode.AppendCells(right.LeafNode.Cells[0:right.LeafNode.Header.Cells]...)
	left.LeafNode.Header.NextLeaf = right.LeafNode.Header.NextLeaf

	// Remove key from parent plus the right child pointer
	if err := parent.InternalNode.DeleteKeyAndRightChild(idx); err != nil {
		return err
	}
	if err := t.refreshInternalNodeKeys(ctx, parent); err != nil {
		return fmt.Errorf("merge leaves: %w", err)
	}

	if parent.InternalNode.Header.IsRoot && parent.InternalNode.Header.KeysNum == 0 {
		rootPage, err := t.pager.ModifyPage(ctx, t.GetRootPageIdx())
		if err != nil {
			return fmt.Errorf("get root page: %w", err)
		}
		rootPage.InternalNode = nil
		rootPage.LeafNode = left.LeafNode.DeepClone()
		rootPage.LeafNode.Header.IsRoot = true
		rootPage.LeafNode.Header.Parent = 0
		rootPage.LeafNode.Header.NextLeaf = 0
		return t.pager.AddFreePage(ctx, left.Index)
	}

	// Check for underflow
	if parent.InternalNode.AtLeastHalfFull(t.maxICells(parent.Index)) {
		return t.updateParentMaxKey(ctx, parent)
	}

	return t.rebalanceInternal(ctx, parent)
}

func (t *Table) rebalanceInternal(ctx context.Context, page *Page) error {
	node := page.InternalNode
	if node.Header.IsRoot {
		if node.Header.KeysNum == 0 {
			rootPage, err := t.pager.ModifyPage(ctx, t.GetRootPageIdx())
			if err != nil {
				return fmt.Errorf("rebalance internal: %w", err)
			}
			firstChildPage, err := t.pager.ModifyPage(ctx, node.Header.RightChild)
			if err != nil {
				return fmt.Errorf("rebalance internal: %w", err)
			}
			switch {
			case firstChildPage.InternalNode != nil:
				rootPage.InternalNode = firstChildPage.InternalNode.Clone()
				rootPage.InternalNode.Header.IsRoot = true
				rootPage.InternalNode.Header.Parent = 0
				rootPage.LeafNode = nil
				for _, childIdx := range rootPage.InternalNode.Children() {
					childPage, err := t.pager.ModifyPage(ctx, childIdx)
					if err != nil {
						return fmt.Errorf("rebalance internal: %w", err)
					}
					childPage.setParent(t.GetRootPageIdx())
				}
			case firstChildPage.LeafNode != nil:
				rootPage.InternalNode = nil
				rootPage.LeafNode = firstChildPage.LeafNode.DeepClone()
				rootPage.LeafNode.Header.IsRoot = true
				rootPage.LeafNode.Header.Parent = 0
			default:
				return fmt.Errorf("rebalance internal: invalid child page type %d", firstChildPage.Index)
			}
			return t.pager.AddFreePage(ctx, firstChildPage.Index)
		}
		return nil
	}

	parentPage, err := t.pager.ModifyPage(ctx, node.Header.Parent)
	if err != nil {
		return fmt.Errorf("rebalance internal: %w", err)
	}

	myPositionInParent, err := parentPage.InternalNode.IndexOfPage(page.Index)
	if err != nil {
		return fmt.Errorf("rebalance internal: %w", err)
	}

	var (
		left  *Page
		right *Page
	)
	if myPositionInParent > 0 {
		left, err = t.pager.ModifyPage(ctx, parentPage.InternalNode.ICells[myPositionInParent-1].Child)
		if err != nil {
			return fmt.Errorf("get left internal page: %w", err)
		}
	} else {
		right, err = t.pager.ModifyPage(ctx, parentPage.InternalNode.GetRightChildByIndex(myPositionInParent))
		if err != nil {
			return fmt.Errorf("get right internal page: %w", err)
		}
	}

	if right != nil && right.InternalNode.MoreThanHalfFull(t.maxICells(right.Index)) {
		if err := t.borrowFromRightInternal(
			ctx,
			parentPage,
			page,
			right,
			myPositionInParent,
		); err != nil {
			return fmt.Errorf("borrow from right internal: %w", err)
		}
		if err := t.refreshInternalNodeKeys(ctx, parentPage); err != nil {
			return err
		}
		return t.updateParentMaxKey(ctx, parentPage)
	}

	if left != nil && left.InternalNode.MoreThanHalfFull(t.maxICells(left.Index)) {
		if err := t.borrowFromLeftInternal(
			ctx,
			parentPage,
			page,
			left,
			myPositionInParent-1,
		); err != nil {
			return fmt.Errorf("borrow from left internal: %w", err)
		}
		if err := t.refreshInternalNodeKeys(ctx, parentPage); err != nil {
			return err
		}
		return t.updateParentMaxKey(ctx, parentPage)
	}

	if right != nil && int(right.InternalNode.Header.KeysNum+node.Header.KeysNum+1) <= t.maxICells(right.Index) {
		if err := t.mergeInternalNodes(
			ctx,
			parentPage,
			page,
			right,
			myPositionInParent,
		); err != nil {
			return fmt.Errorf("merge internal node with right: %w", err)
		}

		if err := t.pager.AddFreePage(ctx, right.Index); err != nil {
			return err
		}
		if err := t.refreshInternalNodeKeys(ctx, parentPage); err != nil {
			return err
		}
		return t.updateParentMaxKey(ctx, parentPage)
	}

	if left != nil && int(left.InternalNode.Header.KeysNum+node.Header.KeysNum+1) <= t.maxICells(left.Index) {
		if err := t.mergeInternalNodes(
			ctx,
			parentPage,
			left,
			page,
			myPositionInParent-1,
		); err != nil {
			return fmt.Errorf("merge internal node with left: %w", err)
		}

		if err := t.pager.AddFreePage(ctx, page.Index); err != nil {
			return err
		}
		if err := t.refreshInternalNodeKeys(ctx, parentPage); err != nil {
			return err
		}
		return t.updateParentMaxKey(ctx, parentPage)
	}

	return nil
}

// borrowFromLeftInternal borrows a key from the left neighbor of the given internal node.
// It inserts the last key and value from the left neighbor into the given node,
// and removes the key and value from the left neighbor.
// It also updates the key in the parent node.
func (t *Table) borrowFromLeftInternal(ctx context.Context, parent, page, left *Page, idx uint32) error {
	page.InternalNode.PrependCell(ICell{
		Key:   parent.InternalNode.ICells[idx].Key,
		Child: left.InternalNode.Header.RightChild,
	})

	childPage, err := t.pager.ModifyPage(ctx, left.InternalNode.Header.RightChild)
	if err != nil {
		return fmt.Errorf("get child page: %w", err)
	}
	childPage.setParent(page.Index)

	left.InternalNode.RemoveLastCell()

	leftMax, err := t.GetMaxKey(ctx, left)
	if err != nil {
		return fmt.Errorf("borrow from left internal: %w", err)
	}
	parent.InternalNode.ICells[idx].Key = leftMax
	if err := t.refreshInternalNodeKeys(ctx, page); err != nil {
		return fmt.Errorf("borrow from left internal: %w", err)
	}
	if err := t.refreshInternalNodeKeys(ctx, left); err != nil {
		return fmt.Errorf("borrow from left internal: %w", err)
	}

	return nil
}

// borrowFromRightInternal borrows a key from the right neighbor of the given internal node.
// It inserts the first key and value from the right neighbor into the given node,
// and removes the key and value from the right neighbor.
// It also updates the key in the parent node.
func (t *Table) borrowFromRightInternal(ctx context.Context, parent, page, right *Page, idx uint32) error {
	page.InternalNode.AppendCells(ICell{
		Child: page.InternalNode.Header.RightChild,
		Key:   parent.InternalNode.ICells[idx].Key,
	})
	page.InternalNode.Header.RightChild = right.InternalNode.FirstCell().Child

	childPage, err := t.pager.ModifyPage(ctx, page.InternalNode.Header.RightChild)
	if err != nil {
		return fmt.Errorf("get child page: %w", err)
	}
	childPage.setParent(page.Index)

	right.InternalNode.RemoveFirstCell()

	pageMax, err := t.GetMaxKey(ctx, page)
	if err != nil {
		return fmt.Errorf("borrow from right internal: %w", err)
	}
	parent.InternalNode.ICells[idx].Key = pageMax
	if err := t.refreshInternalNodeKeys(ctx, page); err != nil {
		return fmt.Errorf("borrow from right internal: %w", err)
	}
	if err := t.refreshInternalNodeKeys(ctx, right); err != nil {
		return fmt.Errorf("borrow from right internal: %w", err)
	}

	return nil
}

// mergeInternalNodes merges two internal nodes and deletes the key from the parent node.
func (t *Table) mergeInternalNodes(ctx context.Context, parent, left, right *Page, idx uint32) error {
	leftIndex := left.Index
	if parent.InternalNode.Header.IsRoot && parent.InternalNode.Header.KeysNum == 1 {
		leftIndex = t.GetRootPageIdx()
	}

	// Update parent of all cells we are moving to the left node
	cellsToMoveLeft := right.InternalNode.ICells[0:right.InternalNode.Header.KeysNum]
	for _, iCell := range cellsToMoveLeft {
		movedPage, err := t.pager.ModifyPage(ctx, iCell.Child)
		if err != nil {
			return fmt.Errorf("get moved page: %w", err)
		}
		movedPage.setParent(leftIndex)
	}
	newRightChildPage, err := t.pager.ModifyPage(ctx, right.InternalNode.Header.RightChild)
	if err != nil {
		return fmt.Errorf("get new right child page: %w", err)
	}
	newRightChildPage.setParent(leftIndex)

	// Do not lose right most child of the left node in the process
	oldRightChildPage, err := t.pager.ModifyPage(ctx, left.InternalNode.Header.RightChild)
	if err != nil {
		return fmt.Errorf("get old right child page: %w", err)
	}
	maxKey, err := t.GetMaxKey(ctx, oldRightChildPage)
	if err != nil {
		return fmt.Errorf("get max key: %w", err)
	}
	iCell := ICell{
		Child: left.InternalNode.Header.RightChild,
		Key:   maxKey,
	}
	left.InternalNode.AppendCells(append([]ICell{iCell}, cellsToMoveLeft...)...)
	left.InternalNode.Header.RightChild = right.InternalNode.Header.RightChild

	// Remove key from parent plus the right child pointer
	if err := parent.InternalNode.DeleteKeyAndRightChild(idx); err != nil {
		return err
	}
	if err := t.refreshInternalNodeKeys(ctx, left); err != nil {
		return fmt.Errorf("merge internal nodes: %w", err)
	}
	if err := t.refreshInternalNodeKeys(ctx, parent); err != nil {
		return fmt.Errorf("merge internal nodes: %w", err)
	}

	// If root has no keys, make left the new root
	if parent.InternalNode.Header.IsRoot && parent.InternalNode.Header.KeysNum == 0 {
		rootPage, err := t.pager.ModifyPage(ctx, t.GetRootPageIdx())
		if err != nil {
			return fmt.Errorf("get root page: %w", err)
		}
		rootPage.InternalNode = left.InternalNode.Clone()
		rootPage.LeafNode = nil
		rootPage.InternalNode.Header.IsRoot = true
		rootPage.InternalNode.Header.Parent = 0
		for _, childIdx := range rootPage.InternalNode.Children() {
			childPage, err := t.pager.ModifyPage(ctx, childIdx)
			if err != nil {
				return fmt.Errorf("get child page: %w", err)
			}
			childPage.setParent(t.GetRootPageIdx())
		}
		return t.pager.AddFreePage(ctx, left.Index)
	}

	// Check for underflow
	if parent.InternalNode.AtLeastHalfFull(t.maxICells(parent.Index)) {
		return t.updateParentMaxKey(ctx, parent)
	}

	return t.rebalanceInternal(ctx, parent)
}

func (t *Table) maxICells(pageIdx PageIndex) int {
	maxICells := t.maximumICells
	if maxICells == InternalNodeMaxCells && pageIdx == 0 {
		maxICells = maxICells - uint32(RootPageConfigSize/ICellSize) - 1 // root page has less space
	}
	return int(maxICells)
}

type callback func(page *Page)

// BFS performs a breadth-first traversal of the table's row B+ tree, calling f
// for every page visited. Used by tests and the integrity checker.
func (t *Table) BFS(ctx context.Context, f callback) error {
	rootPage, err := t.pager.ReadPage(ctx, t.GetRootPageIdx())
	if err != nil {
		return err
	}

	// Create a queue and enqueue the root node
	queue := make([]*Page, 0, 1)
	queue = append(queue, rootPage)

	// Repeat until queue is empty
	for len(queue) > 0 {
		// Get the first node in the queue
		current := queue[0]

		// Dequeue
		queue = queue[1:]

		f(current)

		if current.InternalNode != nil {
			for i := range current.InternalNode.Header.KeysNum {
				iCell := current.InternalNode.ICells[i]
				page, err := t.pager.ReadPage(ctx, iCell.Child)
				if err != nil {
					return err
				}
				queue = append(queue, page)
			}
			if current.InternalNode.Header.RightChild != RightChildNotSet {
				page, err := t.pager.ReadPage(ctx, current.InternalNode.Header.RightChild)
				if err != nil {
					return err
				}
				queue = append(queue, page)
			}
		}
	}

	return nil
}

func (t *Table) createBTreeIndex(pager *TransactionalPager, freePage *Page, columns []Column, indexName string, unique bool) (BTreeIndex, error) {
	if len(columns) > 1 {
		freePage.IndexNode = NewRootIndexNode[CompositeKey](unique)
	} else {
		switch columns[0].Kind {
		case Boolean:
			freePage.IndexNode = NewRootIndexNode[int8](unique)
		case Int4:
			freePage.IndexNode = NewRootIndexNode[int32](unique)
		case Int8, Timestamp:
			freePage.IndexNode = NewRootIndexNode[int64](unique)
		case Real:
			freePage.IndexNode = NewRootIndexNode[float32](unique)
		case Double:
			freePage.IndexNode = NewRootIndexNode[float64](unique)
		case Varchar:
			freePage.IndexNode = NewRootIndexNode[string](unique)
		case UUID:
			freePage.IndexNode = NewRootIndexNode[UUIDValue](unique)
		default:
			return nil, fmt.Errorf("unsupported BTree index column type %v for index %s", columns[0].Kind, indexName)
		}
	}
	return t.newBTreeIndex(pager, freePage.Index, columns, indexName, unique)
}

func (t *Table) newBTreeIndex(pager *TransactionalPager, rootPageIdx PageIndex, columns []Column, indexName string, unique bool) (BTreeIndex, error) {
	if len(columns) > 1 {
		if unique {
			return NewUniqueIndex[CompositeKey](t.logger, t.txManager, indexName, columns, pager, rootPageIdx)
		}
		return NewNonUniqueIndex[CompositeKey](t.logger, t.txManager, indexName, columns, pager, rootPageIdx)
	}
	switch columns[0].Kind {
	case Boolean:
		if unique {
			return NewUniqueIndex[int8](t.logger, t.txManager, indexName, columns, pager, rootPageIdx)
		}
		return NewNonUniqueIndex[int8](t.logger, t.txManager, indexName, columns, pager, rootPageIdx)
	case Int4:
		if unique {
			return NewUniqueIndex[int32](t.logger, t.txManager, indexName, columns, pager, rootPageIdx)
		}
		return NewNonUniqueIndex[int32](t.logger, t.txManager, indexName, columns, pager, rootPageIdx)
	case Int8, Timestamp:
		if unique {
			return NewUniqueIndex[int64](t.logger, t.txManager, indexName, columns, pager, rootPageIdx)
		}
		return NewNonUniqueIndex[int64](t.logger, t.txManager, indexName, columns, pager, rootPageIdx)
	case Real:
		if unique {
			return NewUniqueIndex[float32](t.logger, t.txManager, indexName, columns, pager, rootPageIdx)
		}
		return NewNonUniqueIndex[float32](t.logger, t.txManager, indexName, columns, pager, rootPageIdx)
	case Double:
		if unique {
			return NewUniqueIndex[float64](t.logger, t.txManager, indexName, columns, pager, rootPageIdx)
		}
		return NewNonUniqueIndex[float64](t.logger, t.txManager, indexName, columns, pager, rootPageIdx)
	case Varchar:
		if unique {
			return NewUniqueIndex[string](t.logger, t.txManager, indexName, columns, pager, rootPageIdx)
		}
		return NewNonUniqueIndex[string](t.logger, t.txManager, indexName, columns, pager, rootPageIdx)
	case UUID:
		if unique {
			return NewUniqueIndex[UUIDValue](t.logger, t.txManager, indexName, columns, pager, rootPageIdx)
		}
		return NewNonUniqueIndex[UUIDValue](t.logger, t.txManager, indexName, columns, pager, rootPageIdx)
	default:
		return nil, fmt.Errorf("unsupported BTree index column type %v for index %s", columns[0].Kind, indexName)
	}
}

// estimatedRowCount returns the tracked row count, or -1 if no row-count
// accessor has been wired up (e.g. in unit tests that build tables directly).
func (t *Table) estimatedRowCount() int64 {
	if t.getRowCount == nil {
		return -1
	}
	return t.getRowCount()
}
