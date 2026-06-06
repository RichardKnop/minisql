package minisql

import (
	"context"
	"sync"
	"time"
)

type txKeyType struct{}

var txKey = txKeyType{}

// WithTransaction stores a transaction in the context and returns the new context.
func WithTransaction(ctx context.Context, tx *Transaction) context.Context {
	return context.WithValue(ctx, txKey, tx)
}

// TxFromContext retrieves the current transaction from the context, or nil if none.
func TxFromContext(ctx context.Context) *Transaction {
	if tx, ok := ctx.Value(txKey).(*Transaction); ok {
		return tx
	}
	return nil
}

// MustTxFromContext retrieves the current transaction from the context and panics if none is present.
func MustTxFromContext(ctx context.Context) *Transaction {
	if tx := TxFromContext(ctx); tx != nil {
		return tx
	}
	panic("no transaction in context")
}

// TransactionID is the unique identifier for a transaction.
type TransactionID uint64

// WriteInfo holds a modified page together with the table and index names it belongs to.
type WriteInfo struct {
	*Page
	OriginalPage *Page
	Table        string
	Index        string
	// InPlace is true when the page was modified directly in the LRU cache
	// rather than via a deep clone.  Set only by ModifyPage when no snapshot
	// readers are active.  RollbackTransaction uses this flag to evict the
	// dirty LRU slot so the next read reloads from the WAL.
	InPlace bool
}

// Transaction tracks the write set for the current transaction.
type Transaction struct {
	DDLChanges     DDLChanges
	StartTime      time.Time
	WriteSet       map[PageIndex]WriteInfo
	DBHeaderWrite  *DatabaseHeader
	firstWrite     WriteInfo
	rowCountDeltas map[string]int64
	rowCountTable  string
	rowCountDelta  int64
	firstWritePage PageIndex
	ID             TransactionID
	Status         TransactionStatus
	SnapshotSeq    uint64
	mu             sync.RWMutex
	ReadOnly       bool
	hasRowCount    bool
	hasFirstWrite  bool
}

// TransactionStatus represents the lifecycle state of a transaction.
type TransactionStatus int

// TransactionStatus constants describe the lifecycle state of a transaction.
const (
	// TxActive means the transaction is in progress and has not yet been committed or aborted.
	TxActive TransactionStatus = iota + 1
	// TxCommitted means the transaction has been successfully committed.
	TxCommitted
	// TxAborted means the transaction has been rolled back or aborted.
	TxAborted
)

// Commit marks the transaction as committed.
func (tx *Transaction) Commit() {
	tx.Status = TxCommitted
}

// Abort sets status to TxAborted and discards all in-memory changes.
func (tx *Transaction) Abort() {
	tx.Status = TxAborted
	tx.WriteSet = nil
	tx.DBHeaderWrite = nil
	tx.firstWrite = WriteInfo{}
	tx.rowCountDeltas = nil
	tx.rowCountTable = ""
	tx.rowCountDelta = 0
	tx.firstWritePage = 0
	tx.hasRowCount = false
	tx.hasFirstWrite = false
}

func (tx *Transaction) resetForReuse() {
	tx.DDLChanges = DDLChanges{}
	tx.StartTime = time.Time{}
	tx.WriteSet = nil
	tx.DBHeaderWrite = nil
	tx.firstWrite = WriteInfo{}
	tx.rowCountDeltas = nil
	tx.rowCountTable = ""
	tx.rowCountDelta = 0
	tx.firstWritePage = 0
	tx.ID = 0
	tx.Status = 0
	tx.SnapshotSeq = 0
	tx.ReadOnly = false
	tx.hasRowCount = false
	tx.hasFirstWrite = false
}

// AddRowCountDelta records a net row-count change for the named table.
// It is a no-op for read-only transactions.
func (tx *Transaction) AddRowCountDelta(table string, delta int64) {
	if tx.ReadOnly || delta == 0 {
		return
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.rowCountDeltas != nil {
		tx.rowCountDeltas[table] += delta
		return
	}
	if !tx.hasRowCount {
		tx.rowCountTable = table
		tx.rowCountDelta = delta
		tx.hasRowCount = true
		return
	}
	if tx.rowCountTable == table {
		tx.rowCountDelta += delta
		return
	}
	tx.rowCountDeltas = make(map[string]int64, 2)
	tx.rowCountDeltas[tx.rowCountTable] = tx.rowCountDelta
	tx.rowCountDeltas[table] += delta
	tx.rowCountTable = ""
	tx.rowCountDelta = 0
	tx.hasRowCount = false
}

// RowCountDeltas returns the accumulated row-count deltas.  The returned map
// must not be modified by the caller.
func (tx *Transaction) RowCountDeltas() map[string]int64 {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	if tx.rowCountDeltas == nil && tx.hasRowCount {
		return map[string]int64{tx.rowCountTable: tx.rowCountDelta}
	}
	return tx.rowCountDeltas
}

// ForEachRowCountDelta calls fn for each accumulated row-count delta without
// materialising a map for the common single-table transaction case.
func (tx *Transaction) ForEachRowCountDelta(fn func(string, int64)) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	if tx.rowCountDeltas != nil {
		for table, delta := range tx.rowCountDeltas {
			fn(table, delta)
		}
		return
	}
	if tx.hasRowCount {
		fn(tx.rowCountTable, tx.rowCountDelta)
	}
}

// TrackWrite records a modified page in the transaction write set.
// originalPage is the page as it was before modification; it is stored for
// MVCC snapshot reads and must not be nil for pages that existed prior to
// this transaction (use nil for newly-allocated pages).
// inPlace must be true when the page was modified directly in the LRU cache
// (no clone); RollbackTransaction will then evict the slot via InvalidatePage.
func (tx *Transaction) TrackWrite(pageIdx PageIndex, page, originalPage *Page, table, index string, inPlace bool) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.WriteSet == nil {
		info := WriteInfo{
			Page:         page,
			Table:        table,
			Index:        index,
			OriginalPage: originalPage,
			InPlace:      inPlace,
		}
		if !tx.hasFirstWrite || tx.firstWritePage == pageIdx {
			tx.firstWritePage = pageIdx
			tx.firstWrite = info
			tx.hasFirstWrite = true
			return
		}
		tx.WriteSet = make(map[PageIndex]WriteInfo, 2)
		tx.WriteSet[tx.firstWritePage] = tx.firstWrite
		tx.hasFirstWrite = false
	}
	tx.WriteSet[pageIdx] = WriteInfo{
		Page:         page,
		Table:        table,
		Index:        index,
		OriginalPage: originalPage,
		InPlace:      inPlace,
	}
}

// TrackDBHeaderWrite records a modified database header in the transaction write set.
func (tx *Transaction) TrackDBHeaderWrite(header DatabaseHeader) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	tx.DBHeaderWrite = &header
}

// WritePage associates a page with the table it belongs to.
type WritePage struct {
	Page  *Page
	Table string
}

// GetWriteVersions returns the transaction's write set.
func (tx *Transaction) GetWriteVersions() map[PageIndex]WriteInfo {
	if tx.WriteSet == nil && tx.hasFirstWrite {
		return map[PageIndex]WriteInfo{tx.firstWritePage: tx.firstWrite}
	}
	return tx.WriteSet
}

// WriteCount returns the number of pages modified by the transaction.
func (tx *Transaction) WriteCount() int {
	if tx.WriteSet != nil {
		return len(tx.WriteSet)
	}
	if tx.hasFirstWrite {
		return 1
	}
	return 0
}

// GetWriteInfo returns the modified page metadata for pageIdx, if present.
func (tx *Transaction) GetWriteInfo(pageIdx PageIndex) (WriteInfo, bool) {
	if tx.WriteSet != nil {
		info, exists := tx.WriteSet[pageIdx]
		return info, exists
	}
	if tx.hasFirstWrite && tx.firstWritePage == pageIdx {
		return tx.firstWrite, true
	}
	return WriteInfo{}, false
}

// ForEachWrite calls fn for each modified page in the transaction.
func (tx *Transaction) ForEachWrite(fn func(PageIndex, WriteInfo)) {
	if tx.WriteSet != nil {
		for pageIdx, info := range tx.WriteSet {
			fn(pageIdx, info)
		}
		return
	}
	if tx.hasFirstWrite {
		fn(tx.firstWritePage, tx.firstWrite)
	}
}

// GetModifiedPage returns the in-memory modified copy of the page at pageIdx, if any.
func (tx *Transaction) GetModifiedPage(pageIdx PageIndex) (*Page, bool) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	modifiedPage, exists := tx.GetWriteInfo(pageIdx)
	if !exists {
		return nil, false
	}
	return modifiedPage.Page, true
}

// GetModifiedDBHeader returns the in-memory modified database header, if any.
func (tx *Transaction) GetModifiedDBHeader() (*DatabaseHeader, bool) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	return tx.DBHeaderWrite, tx.DBHeaderWrite != nil
}

// DDLChanges accumulates schema modifications made within a single transaction.
type DDLChanges struct {
	CreateIndexes map[string]SecondaryIndex
	DropIndexes   map[string]SecondaryIndex
	CreateTables  []*Table
	DropTables    []string
}

// CreatedTable records a table creation in the DDL change set.
func (d DDLChanges) CreatedTable(t *Table) DDLChanges {
	d.CreateTables = append(d.CreateTables, t)
	return d
}

// DroppedTable records a table drop in the DDL change set.
func (d DDLChanges) DroppedTable(tableName string) DDLChanges {
	d.DropTables = append(d.DropTables, tableName)
	return d
}

// CreatedIndex records an index creation in the DDL change set.
func (d DDLChanges) CreatedIndex(tableName string, index SecondaryIndex) DDLChanges {
	if d.CreateIndexes == nil {
		d.CreateIndexes = make(map[string]SecondaryIndex)
	}
	d.CreateIndexes[tableName] = index
	return d
}

// DroppedIndex records an index drop in the DDL change set.
func (d DDLChanges) DroppedIndex(tableName string, index SecondaryIndex) DDLChanges {
	if d.DropIndexes == nil {
		d.DropIndexes = make(map[string]SecondaryIndex)
	}
	d.DropIndexes[tableName] = index
	return d
}

// HasChanges reports whether there are any uncommitted DDL changes.
func (d DDLChanges) HasChanges() bool {
	return len(d.CreateTables) > 0 ||
		len(d.DropTables) > 0 ||
		len(d.CreateIndexes) > 0 ||
		len(d.DropIndexes) > 0
}
