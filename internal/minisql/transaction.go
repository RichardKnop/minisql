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
	rowCountDeltas map[string]int64
	ID             TransactionID
	Status         TransactionStatus
	SnapshotSeq    uint64
	mu             sync.RWMutex
	ReadOnly       bool
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
	tx.rowCountDeltas = nil
}

// AddRowCountDelta records a net row-count change for the named table.
// It is a no-op for read-only transactions.
func (tx *Transaction) AddRowCountDelta(table string, delta int64) {
	if tx.ReadOnly || delta == 0 {
		return
	}
	tx.mu.Lock()
	if tx.rowCountDeltas == nil {
		tx.rowCountDeltas = make(map[string]int64, 1)
	}
	tx.rowCountDeltas[table] += delta
	tx.mu.Unlock()
}

// RowCountDeltas returns the accumulated row-count deltas.  The returned map
// must not be modified by the caller.
func (tx *Transaction) RowCountDeltas() map[string]int64 {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.rowCountDeltas
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
	return tx.WriteSet
}

// GetModifiedPage returns the in-memory modified copy of the page at pageIdx, if any.
func (tx *Transaction) GetModifiedPage(pageIdx PageIndex) (*Page, bool) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	modifiedPage, exists := tx.WriteSet[pageIdx]
	return modifiedPage.Page, exists
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
