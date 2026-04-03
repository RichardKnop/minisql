package minisql

import (
	"context"
	"maps"
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
	Table string
	Index string
}

// Transaction tracks the read and write sets for optimistic concurrency control.
type Transaction struct {
	ID            TransactionID
	StartTime     time.Time
	ReadSet       map[PageIndex]uint64    // pageIdx -> version when read
	WriteSet      map[PageIndex]WriteInfo // pageIdx -> modified page copy (+ table name, index name)
	DBHeaderRead  *uint64                 // version of DB header when read
	DBHeaderWrite *DatabaseHeader         // modified DB header
	DDLChanges    DDLChanges
	Status        TransactionStatus
	mu            sync.RWMutex
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
}

// TrackRead records that the given page was read at the given version.
func (tx *Transaction) TrackRead(pageIdx PageIndex, version uint64) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	tx.ReadSet[pageIdx] = version
}

// TrackWrite records a modified page in the transaction write set.
func (tx *Transaction) TrackWrite(pageIdx PageIndex, page *Page, table, index string) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	tx.WriteSet[pageIdx] = WriteInfo{
		Page:  page,
		Table: table,
		Index: index,
	}
}

// TrackDBHeaderRead records the version of the database header when it was read.
func (tx *Transaction) TrackDBHeaderRead(version uint64) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	tx.DBHeaderRead = &version
}

// TrackDBHeaderWrite records a modified database header in the transaction write set.
func (tx *Transaction) TrackDBHeaderWrite(header DatabaseHeader) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	tx.DBHeaderWrite = &header
}

// GetReadVersions returns a copy of the page read-version map.
func (tx *Transaction) GetReadVersions() map[PageIndex]uint64 {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	// Return a copy to avoid concurrent map access
	readSetCopy := make(map[PageIndex]uint64, len(tx.ReadSet))
	maps.Copy(readSetCopy, tx.ReadSet)
	return readSetCopy
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

// GetDBHeaderReadVersion returns the version of the database header as it was read, if any.
func (tx *Transaction) GetDBHeaderReadVersion() (uint64, bool) {
	if tx.DBHeaderRead == nil {
		return 0, false
	}
	return *tx.DBHeaderRead, true
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
	CreateTables  []*Table
	DropTables    []string
	CreateIndexes map[string]SecondaryIndex // table name -> index
	DropIndexes   map[string]SecondaryIndex // table name -> index
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
