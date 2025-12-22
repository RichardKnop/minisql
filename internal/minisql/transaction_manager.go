package minisql

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

type TransactionManager struct {
	mu                    sync.RWMutex
	nextTxID              TransactionID
	transactions          map[TransactionID]*Transaction
	globalPageVersions    map[PageIndex]uint64 // pageIdx -> current version
	globalDbHeaderVersion uint64
	logger                *zap.Logger
}

func NewTransactionManager(logger *zap.Logger) *TransactionManager {
	return &TransactionManager{
		nextTxID:           1,
		transactions:       make(map[TransactionID]*Transaction),
		globalPageVersions: make(map[PageIndex]uint64),
		logger:             logger,
	}
}

func (tm *TransactionManager) ExecuteInTransaction(ctx context.Context, fn func(ctx context.Context) error, saver PageSaver) error {
	// If there is a transaction already in context, use it.
	// This means tx was manually started with BEGIN
	// and will stay open until COMMIT/ROLLBACK.
	if TxFromContext(ctx) != nil {
		return fn(ctx)
	}

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

	tm.logger.Debug("begin transaction", zap.Uint64("tx_id", uint64(tx.ID)))

	return tx
}

var ErrTxConflict = errors.New("transaction conflict detected")

func (tm *TransactionManager) CommitTransaction(ctx context.Context, tx *Transaction, saver PageSaver) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tx.Status != TxActive {
		return fmt.Errorf("transaction %d is not active", tx.ID)
	}

	// Check for conflicts (simplified optimistic concurrency control)
	for pageIdx, readVersion := range tx.GetReadVersions() {
		currentVersion := tm.globalPageVersions[pageIdx]
		if currentVersion > readVersion {
			// Page was modified by another transaction
			tx.Abort()
			return fmt.Errorf("%w: tx %d aborted due to conflict on page %d", ErrTxConflict, tx.ID, pageIdx)
		}
	}
	readDBHeaderVersion, exists := tx.GetDBHeaderReadVersion()
	if exists && tm.globalDbHeaderVersion > readDBHeaderVersion {
		// DB header was modified by another transaction
		tx.Abort()
		return fmt.Errorf("%w: tx %d aborted due to conflict on DB header", ErrTxConflict, tx.ID)
	}

	pagesToFlush := make([]PageIndex, 0, len(tx.WriteSet))

	// No conflicts, apply all writes
	// First update DB header if modified
	if header, modified := tx.GetModifiedDBHeader(); modified {
		saver.SaveHeader(ctx, *header)
		tm.globalDbHeaderVersion += 1

		pagesToFlush = append(pagesToFlush, 0) // header is first 100 bytes of page 0
	}
	// Then update modified pages
	for pageIdx, modifiedPage := range tx.GetWriteVersions() {
		// Write the modified page to base storage
		saver.SavePage(ctx, pageIdx, modifiedPage)

		// Increment page version
		tm.globalPageVersions[pageIdx] += 1

		pagesToFlush = append(pagesToFlush, pageIdx)
	}

	tx.Commit()

	// Clean up transaction
	delete(tm.transactions, tx.ID)

	// TODO - implement rollback journal file
	// https://sqlite.org/atomiccommit.html

	// Flush modified pages to disk
	for _, pageIdx := range pagesToFlush {
		if err := saver.Flush(ctx, pageIdx); err != nil {
			return fmt.Errorf("failed to flush page %d: %w", pageIdx, err)
		}
	}

	tm.logger.Debug("commit transaction", zap.Uint64("tx_id", uint64(tx.ID)))

	return nil
}

func (tm *TransactionManager) RollbackTransaction(ctx context.Context, tx *Transaction) {
	tx.Abort()

	// Clean up transaction
	tm.mu.Lock()
	delete(tm.transactions, tx.ID)
	tm.mu.Unlock()

	tm.logger.Debug("rollback transaction", zap.Uint64("tx_id", uint64(tx.ID)))
}

func (tm *TransactionManager) GlobalDBHeaderVersion(ctx context.Context) uint64 {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	return tm.globalDbHeaderVersion
}

func (tm *TransactionManager) GlobalPageVersion(ctx context.Context, pageIdx PageIndex) uint64 {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	return tm.globalPageVersions[pageIdx]
}
