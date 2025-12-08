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

type TransactionID uint64

type Transaction struct {
	ID            TransactionID
	StartTime     time.Time
	ReadSet       map[PageIndex]uint64 // pageIdx -> version when read
	WriteSet      map[PageIndex]*Page  // pageIdx -> modified page copy
	DbHeaderRead  *uint64              // version of DB header when read
	DbHeaderWrite *DatabaseHeader      // modified DB header
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
	return tx.ReadSet
}

func (tx *Transaction) GetWriteVersions() map[PageIndex]*Page {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.WriteSet
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
