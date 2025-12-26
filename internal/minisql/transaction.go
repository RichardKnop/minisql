package minisql

import (
	"context"
	"sync"
	"time"
)

type txKeyType struct{}

var txKey = txKeyType{}

func WithTransaction(ctx context.Context, tx *Transaction) context.Context {
	return context.WithValue(ctx, txKey, tx)
}

func TxFromContext(ctx context.Context) *Transaction {
	if tx, ok := ctx.Value(txKey).(*Transaction); ok {
		return tx
	}
	return nil
}

func MustTxFromContext(ctx context.Context) *Transaction {
	if tx := TxFromContext(ctx); tx != nil {
		return tx
	}
	panic("no transaction in context")
}

type TransactionID uint64

type DDLChanges struct {
	CreateTables  []*Table
	DropTables    []string
	CreateIndexes map[string]SecondaryIndex // table name -> index
	DropIndexes   map[string]SecondaryIndex // table name -> index
}

func (d DDLChanges) CreatedTable(t *Table) DDLChanges {
	d.CreateTables = append(d.CreateTables, t)
	return d
}

func (d DDLChanges) DroppedTable(tableName string) DDLChanges {
	d.DropTables = append(d.DropTables, tableName)
	return d
}

func (d DDLChanges) CreatedIndex(tableName string, index SecondaryIndex) DDLChanges {
	if d.CreateIndexes == nil {
		d.CreateIndexes = make(map[string]SecondaryIndex)
	}
	d.CreateIndexes[tableName] = index
	return d
}

func (d DDLChanges) DroppedIndex(tableName string, index SecondaryIndex) DDLChanges {
	if d.DropIndexes == nil {
		d.DropIndexes = make(map[string]SecondaryIndex)
	}
	d.DropIndexes[tableName] = index
	return d
}

func (d DDLChanges) HasChanges() bool {
	return len(d.CreateTables) > 0 ||
		len(d.DropTables) > 0 ||
		len(d.CreateIndexes) > 0 ||
		len(d.DropIndexes) > 0
}

type Transaction struct {
	ID            TransactionID
	StartTime     time.Time
	ReadSet       map[PageIndex]uint64 // pageIdx -> version when read
	WriteSet      map[PageIndex]*Page  // pageIdx -> modified page copy
	DbHeaderRead  *uint64              // version of DB header when read
	DbHeaderWrite *DatabaseHeader      // modified DB header
	DDLChanges    DDLChanges
	Status        TransactionStatus
	mu            sync.RWMutex
}

type TransactionStatus int

const (
	TxActive TransactionStatus = iota + 1
	TxCommitted
	TxAborted
)

func (tx *Transaction) Commit() {
	tx.Status = TxCommitted
}

func (tx *Transaction) Abort() {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	tx.Status = TxAborted
	// Simply discard all changes - they're only in memory
	tx.WriteSet = make(map[PageIndex]*Page)
	tx.DbHeaderWrite = nil
}

func (tx *Transaction) TrackRead(pageIdx PageIndex, version uint64) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.ReadSet[pageIdx] = version
}

func (tx *Transaction) TrackWrite(pageIdx PageIndex, page *Page) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.WriteSet[pageIdx] = page
}

func (tx *Transaction) TrackDBHeaderRead(version uint64) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.DbHeaderRead = &version
}

func (tx *Transaction) TrackDBHeaderWrite(header DatabaseHeader) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.DbHeaderWrite = &header
}

func (tx *Transaction) GetReadVersions() map[PageIndex]uint64 {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	// Return a copy to avoid concurrent map access
	readSetCopy := make(map[PageIndex]uint64, len(tx.ReadSet))
	for k, v := range tx.ReadSet {
		readSetCopy[k] = v
	}
	return readSetCopy
}

func (tx *Transaction) GetWriteVersions() map[PageIndex]*Page {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	// Return a copy to avoid concurrent map access
	writeSetCopy := make(map[PageIndex]*Page, len(tx.WriteSet))
	for k, v := range tx.WriteSet {
		writeSetCopy[k] = v
	}
	return writeSetCopy
}

func (tx *Transaction) GetDBHeaderReadVersion() (uint64, bool) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	if tx.DbHeaderRead == nil {
		return 0, false
	}
	return *tx.DbHeaderRead, true
}

func (tx *Transaction) GetModifiedPage(pageIdx PageIndex) (*Page, bool) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	modifiedPage, exists := tx.WriteSet[pageIdx]
	return modifiedPage, exists
}

func (tx *Transaction) GetModifiedDBHeader() (*DatabaseHeader, bool) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.DbHeaderWrite, tx.DbHeaderWrite != nil
}
