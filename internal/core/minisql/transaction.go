package minisql

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type TransactionID uint64

type Transaction struct {
	ID            TransactionID
	StartTime     time.Time
	ReadSet       map[PageIndex]uint64 // pageIdx -> version when read
	WriteSet      map[PageIndex]*Page  // pageIdx -> modified page copy
	DbHeaderRead  *uint64              // version of DB header when read
	DbHeaderWrite *DatabaseHeader      // modified DB header
	Status        TransactionStatus
}

type TransactionStatus int

const (
	TxActive TransactionStatus = iota
	TxCommitted
	TxAborted
)

type TransactionManager struct {
	mu                    sync.RWMutex
	nextTxID              TransactionID
	transactions          map[TransactionID]*Transaction
	globalPageVersions    map[PageIndex]uint64 // pageIdx -> current version
	globalDbHeaderVersion uint64
}

func NewTransactionManager() *TransactionManager {
	return &TransactionManager{
		nextTxID:           1,
		transactions:       make(map[TransactionID]*Transaction),
		globalPageVersions: make(map[PageIndex]uint64),
	}
}

func (tm *TransactionManager) BeginTransaction(ctx context.Context) *Transaction {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tx := &Transaction{
		ID:        tm.nextTxID,
		StartTime: time.Now(),
		ReadSet:   make(map[PageIndex]uint64),
		WriteSet:  make(map[PageIndex]*Page),
		Status:    TxActive,
	}

	tm.nextTxID++
	tm.transactions[tx.ID] = tx

	return tx
}

func (tm *TransactionManager) CommitTransaction(ctx context.Context, tx *Transaction, saver PageSaver) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tx.Status != TxActive {
		return fmt.Errorf("transaction %d is not active", tx.ID)
	}

	// Check for conflicts (simplified optimistic concurrency control)
	for pageIdx, readVersion := range tx.ReadSet {
		currentVersion := tm.globalPageVersions[pageIdx]
		if currentVersion > readVersion {
			// Page was modified by another transaction
			tx.Status = TxAborted
			return fmt.Errorf("transaction %d aborted due to conflict on page %d", tx.ID, pageIdx)
		}
	}
	if tx.DbHeaderRead != nil && tm.globalDbHeaderVersion > *tx.DbHeaderRead {
		// DB header was modified by another transaction
		tx.Status = TxAborted
		return fmt.Errorf("transaction %d aborted due to conflict on DB header", tx.ID)
	}

	// No conflicts, apply all writes
	// First update DB header if modified
	if tx.DbHeaderWrite != nil {
		saver.SaveHeader(ctx, *tx.DbHeaderWrite)
		tm.globalDbHeaderVersion += 1
	}
	// Then update modified pages
	for pageIdx, modifiedPage := range tx.WriteSet {
		// Write the modified page to base storage
		saver.SavePage(ctx, pageIdx, modifiedPage)

		// Increment page version
		tm.globalPageVersions[pageIdx] += 1
	}

	tx.Status = TxCommitted

	// Clean up transaction
	delete(tm.transactions, tx.ID)

	return nil
}

func (tm *TransactionManager) RollbackTransaction(ctx context.Context, tx *Transaction) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tx.Status = TxAborted

	// Simply discard all changes - they're only in memory
	tx.WriteSet = make(map[PageIndex]*Page)
	tx.DbHeaderWrite = nil

	// Clean up transaction
	delete(tm.transactions, tx.ID)
}

func (tm *TransactionManager) ExecuteInTransaction(ctx context.Context, fn func(ctx context.Context) error, saver PageSaver) error {
	tx := tm.BeginTransaction(ctx)
	ctx = WithTransaction(ctx, tx)

	if err := fn(ctx); err != nil {
		tm.RollbackTransaction(ctx, tx)
		return err
	}

	if err := tm.CommitTransaction(ctx, tx, saver); err != nil {
		tm.RollbackTransaction(ctx, tx)
		return err
	}

	return nil
}
