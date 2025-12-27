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
	dbFilePath            string
	journalEnabled        bool // TODO - remove once journaling is always on
	factory               TxPagerFactory
	saver                 PageSaver
	ddlSaver              DDLSaver
}

func NewTransactionManager(logger *zap.Logger, dbFilePath string, factory TxPagerFactory, saver PageSaver, ddlSaver DDLSaver) *TransactionManager {
	return &TransactionManager{
		nextTxID:           1,
		transactions:       make(map[TransactionID]*Transaction),
		globalPageVersions: make(map[PageIndex]uint64),
		logger:             logger,
		factory:            factory,
		dbFilePath:         dbFilePath,
		saver:              saver,
		ddlSaver:           ddlSaver,
	}
}

func (tm *TransactionManager) ExecuteInTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
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

	if err := tm.CommitTransaction(ctx, tx); err != nil {
		tm.RollbackTransaction(ctx, tx)
		return err
	}

	return nil
}

func (tm *TransactionManager) BeginTransaction(ctx context.Context) *Transaction {
	tm.mu.Lock()
	tx := &Transaction{
		ID:           tm.nextTxID,
		StartTime:    time.Now(),
		ReadSet:      make(map[PageIndex]uint64),
		WriteSet:     make(map[PageIndex]*Page),
		WriteInfoSet: make(map[PageIndex]WriteInfo),
		Status:       TxActive,
	}
	tm.nextTxID++
	tm.transactions[tx.ID] = tx
	tm.mu.Unlock()

	tm.logger.Debug("begin transaction", zap.Uint64("tx_id", uint64(tx.ID)))

	return tx
}

var ErrTxConflict = errors.New("transaction conflict detected")

func (tm *TransactionManager) CommitTransaction(ctx context.Context, tx *Transaction) error {
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

	// === PHASE 1: Create Rollback Journal ===
	// Write original page contents to journal before modifying database
	// This enables crash recovery by restoring original pages
	var journal *RollbackJournal
	if tm.journalEnabled && tm.dbFilePath != "" {
		var err error
		journal, err = CreateJournal(tm.dbFilePath, PageSize)
		if err != nil {
			tx.Abort()
			return fmt.Errorf("create rollback journal: %w", err)
		}
		defer func() {
			if journal != nil {
				journal.Close()
			}
		}()

		// Write original pages to journal
		numJournaledPages := 0
		writePages, writeInfo := tx.GetWriteVersions()
		if len(writePages) != len(writeInfo) {
			tx.Abort()
			return fmt.Errorf("internal error: mismatched write pages and info")
		}
		for pageIdx := range writePages {
			aPager, err := tm.factory(ctx, writeInfo[pageIdx].Table, writeInfo[pageIdx].Index)
			if err != nil {
				tx.Abort()
				return fmt.Errorf("get pager for journaling page %d: %w", pageIdx, err)
			}
			// Read original page from disk
			originalPage, err := aPager.GetPage(ctx, pageIdx)
			if err != nil {
				return fmt.Errorf("read original page %d for journal: %w", pageIdx, err)
			}

			// Write original page to journal
			if err := journal.WritePageBefore(ctx, pageIdx, originalPage, aPager); err != nil {
				return fmt.Errorf("journal page %d: %w", pageIdx, err)
			}
			numJournaledPages++
		}

		// Finalize journal header with page count and sync to disk
		if err := journal.Finalize(numJournaledPages); err != nil {
			return fmt.Errorf("finalize journal: %w", err)
		}
	}

	// === PHASE 2: Apply Writes to In-Memory Pages ===
	// No conflicts, apply all writes
	// First update DB header if modified
	if header, modified := tx.GetModifiedDBHeader(); modified {
		tm.saver.SaveHeader(ctx, *header)
		tm.globalDbHeaderVersion += 1

		pagesToFlush = append(pagesToFlush, 0) // header is first 100 bytes of page 0
	}
	// Then update modified pages
	writePages, _ := tx.GetWriteVersions()
	for pageIdx, writePage := range writePages {
		// Write the modified page to base storage
		tm.saver.SavePage(ctx, pageIdx, writePage)

		// Increment page version
		tm.globalPageVersions[pageIdx] += 1

		pagesToFlush = append(pagesToFlush, pageIdx)
	}

	// === PHASE 3: Flush Modified Pages to Disk ===
	// If we crash during this phase, journal will restore original pages on recovery
	for _, pageIdx := range pagesToFlush {
		if err := tm.saver.Flush(ctx, pageIdx); err != nil {
			return fmt.Errorf("failed to flush page %d: %w", pageIdx, err)
		}
	}

	// === PHASE 4: Delete Journal (Atomic Commit Point) ===
	// Once all pages are safely on disk, delete the journal
	// This is the atomic commit point - after this, the transaction is committed
	if journal != nil {
		if err := journal.Delete(); err != nil {
			// Database is consistent, journal deletion is non-critical
			tm.logger.Warn("failed to delete journal after commit",
				zap.Uint64("tx_id", uint64(tx.ID)),
				zap.Error(err))
		}
		journal = nil // Prevent defer from closing again
	}

	// Save DDL changes first (CREATE / DROP TABLE)
	if tx.DDLChanges.HasChanges() {
		tm.ddlSaver.SaveDDLChanges(ctx, tx.DDLChanges)
	}

	tx.Commit()

	// Clean up transaction
	delete(tm.transactions, tx.ID)

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
