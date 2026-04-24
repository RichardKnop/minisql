package minisql

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"go.uber.org/zap"
)

// pageVersion is one historical snapshot of a page stored in the version
// history for MVCC snapshot isolation.  It is valid for read transactions
// whose SnapshotSeq <= validUntilSeq.
type pageVersion struct {
	validUntilSeq uint64
	page          *Page
}

// pageDataPool is a package-level pool for PageSize-byte slices used during WAL
// serialization. A single pool is shared across all TransactionManagers (there
// is normally only one per database). Buffers returned to the pool via
// WALIndex.Update are safe to reuse: walWriteMu serialises all writers, so by
// the time a buffer is recycled the pager has finished unmarshalling it.
var pageDataPool = sync.Pool{
	New: func() any { return make([]byte, PageSize) },
}

// TransactionManager coordinates optimistic concurrency control for the database.
type TransactionManager struct {
	mu                    sync.RWMutex
	nextTxID              TransactionID
	transactions          map[TransactionID]*Transaction
	globalPageVersions    map[PageIndex]uint64 // pageIdx -> current version
	globalDBHeaderVersion uint64
	logger                *zap.Logger
	dbFilePath            string

	// MVCC snapshot isolation fields.
	//
	// commitSeq is a monotonically increasing counter incremented on every write
	// commit.  Read-only transactions record their SnapshotSeq = commitSeq at
	// Begin time; writes committed after that point are invisible to them.
	//
	// pageLastCommittedSeq tracks the commitSeq at which each page was last
	// written to the shared page cache (via SavePage).  ReadPage compares this
	// against tx.SnapshotSeq to decide whether the cached version is safe to use.
	//
	// pageVersionHistory stores old *Page versions for active snapshot readers.
	// Each entry is valid for reads where snapshotSeq <= entry.validUntilSeq.
	// Entries are GC'd when no active reader needs them any more.
	commitSeq            uint64
	pageLastCommittedSeq map[PageIndex]uint64
	pageVersionHistory   map[PageIndex][]pageVersion

	// WAL fields (non-nil when a WAL is configured)
	walWriteMu          sync.Mutex // serialises WAL appends — only one writer at a time
	wal                 *WAL
	walIndex            *WALIndex
	checkpointThreshold int          // auto-checkpoint after this many WAL frames (0 = disabled)
	checkpointFn        func() error // called by runAutoCheckpoint; set via SetCheckpointFunc
	factory             TxPagerFactory
	saver               PageSaver
	ddlSaver            DDLSaver
	commitHook          func(commitPhase)
}

type commitPhase string

const (
	// WAL commit phases
	commitPhaseBeforeWALAppend commitPhase = "before_wal_append"
	commitPhaseAfterWALAppend  commitPhase = "after_wal_append"
)

// NewTransactionManager creates and returns a new TransactionManager.
func NewTransactionManager(logger *zap.Logger, dbFilePath string, factory TxPagerFactory, saver PageSaver, ddlSaver DDLSaver) *TransactionManager {
	return &TransactionManager{
		nextTxID:             1,
		transactions:         make(map[TransactionID]*Transaction),
		globalPageVersions:   make(map[PageIndex]uint64),
		pageLastCommittedSeq: make(map[PageIndex]uint64),
		pageVersionHistory:   make(map[PageIndex][]pageVersion),
		logger:               logger,
		factory:              factory,
		dbFilePath:           dbFilePath,
		saver:                saver,
		ddlSaver:             ddlSaver,
	}
}

// SetCheckpointFunc registers a callback that is invoked by runAutoCheckpoint
// when the WAL frame count exceeds checkpointThreshold.  Typically set to
// Database.Checkpoint so the auto-checkpoint path mirrors the manual one.
func (tm *TransactionManager) SetCheckpointFunc(fn func() error) {
	tm.checkpointFn = fn
}

// CheckpointWAL checkpoints the WAL into dbFile, then truncates it.
//
// Checkpoint sequence (all under walWriteMu so no concurrent writers can
// interleave):
//  1. Write every unique page in the WAL to its correct offset in dbFile.
//  2. fsync dbFile.
//  3. Truncate the WAL file and write fresh salts (invalidates stale frames).
//  4. Reset the in-memory WAL index (under tm.mu).
//
// Returns ErrNotWALMode if no WAL is configured.
// Returns ErrCheckpointBlockedByReaders if any read-only snapshot transaction
// is currently active.  Callers should retry after the readers have finished.
func (tm *TransactionManager) CheckpointWAL(dbFile DBFile) error {
	if tm.wal == nil {
		return ErrNotWALMode
	}

	// Block the checkpoint while snapshot readers are active.  After a successful
	// checkpoint the WAL index is reset and the DB file holds the latest version,
	// so any reader that started before this checkpoint could see wrong data if
	// we proceeded.
	tm.mu.RLock()
	blocked := tm.hasActiveSnapshotReadersLocked()
	tm.mu.RUnlock()
	if blocked {
		return ErrCheckpointBlockedByReaders
	}

	tm.walWriteMu.Lock()
	defer tm.walWriteMu.Unlock()

	framesBefore := tm.wal.FrameCount()

	if err := tm.wal.Checkpoint(dbFile); err != nil {
		return fmt.Errorf("WAL checkpoint: %w", err)
	}

	if err := tm.wal.Truncate(); err != nil {
		return fmt.Errorf("WAL truncate after checkpoint: %w", err)
	}

	// Reset the WAL index so subsequent cache misses read from the (now
	// up-to-date) DB file rather than stale WAL entries.
	tm.mu.Lock()
	if tm.walIndex != nil {
		tm.walIndex.Reset()
	}
	tm.mu.Unlock()

	tm.logger.Info("WAL checkpoint completed",
		zap.Int64("frames_checkpointed", framesBefore))

	return nil
}

// ErrNotWALMode is returned when a WAL-specific operation is called on a
// transaction manager that is not in WAL mode.
var ErrNotWALMode = errors.New("WAL mode is not enabled")

// ExecuteReadOnlyTransaction runs fn within a read-only transaction.  Read
// tracking is disabled so no ReadSet is built and conflict validation is
// skipped at commit time.  fn must not perform any writes.
func (tm *TransactionManager) ExecuteReadOnlyTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	if TxFromContext(ctx) != nil {
		return fn(ctx)
	}

	tx := tm.BeginReadOnlyTransaction(ctx)
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

// ExecuteInTransaction runs fn within a transaction, committing on success or rolling back on failure.
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

// BeginTransaction starts a new transaction and registers it with the manager.
func (tm *TransactionManager) BeginTransaction(ctx context.Context) *Transaction {
	tm.mu.Lock()
	tx := &Transaction{
		ID:        tm.nextTxID,
		StartTime: time.Now(),
		WriteSet:  make(map[PageIndex]WriteInfo),
		Status:    TxActive,
		// ReadSet is lazily allocated in TrackRead to avoid the map allocation
		// for read-only transactions.
	}
	tm.nextTxID += 1
	tm.transactions[tx.ID] = tx
	tm.mu.Unlock()

	tm.logger.Debug("begin transaction", zap.Uint64("tx_id", uint64(tx.ID)))

	return tx
}

// BeginReadOnlyTransaction starts a read-only transaction with snapshot
// isolation.  The transaction sees a consistent snapshot of the database as
// of the moment it begins: any write committed after this call is invisible
// to it.  TrackRead calls are no-ops; conflict validation is skipped at
// commit time.  Read-only transactions never return ErrTxConflict.
//
// SnapshotSeq is captured inside tm.mu so it is atomic with the transaction
// registration — preventing a race where a write commits between the map
// insert and the seq capture.
func (tm *TransactionManager) BeginReadOnlyTransaction(ctx context.Context) *Transaction {
	tm.mu.Lock()
	tx := &Transaction{
		ID:          tm.nextTxID,
		StartTime:   time.Now(),
		WriteSet:    make(map[PageIndex]WriteInfo),
		Status:      TxActive,
		ReadOnly:    true,
		SnapshotSeq: tm.commitSeq,
	}
	tm.nextTxID++
	tm.transactions[tx.ID] = tx
	tm.mu.Unlock()

	tm.logger.Debug("begin read-only transaction",
		zap.Uint64("tx_id", uint64(tx.ID)),
		zap.Uint64("snapshot_seq", tx.SnapshotSeq),
	)
	return tx
}

// ErrTxConflict is returned when an optimistic concurrency check fails at commit time.
var ErrTxConflict = errors.New("transaction conflict detected")

// ErrCheckpointBlockedByReaders is returned when a checkpoint cannot proceed
// because one or more read-only transactions hold snapshots that predate the
// current commitSeq.  Callers should retry after the readers have finished.
var ErrCheckpointBlockedByReaders = errors.New("checkpoint blocked: active snapshot readers")

// ---- MVCC helpers ----

// minActiveSnapshotSeqLocked returns the minimum SnapshotSeq across all active
// read-only transactions, or math.MaxUint64 when none exist.
// Caller must hold tm.mu (read or write lock).
func (tm *TransactionManager) minActiveSnapshotSeqLocked() uint64 {
	minSeq := uint64(math.MaxUint64)
	for _, tx := range tm.transactions {
		if tx.ReadOnly && tx.Status == TxActive && tx.SnapshotSeq < minSeq {
			minSeq = tx.SnapshotSeq
		}
	}
	return minSeq
}

// hasActiveSnapshotReadersLocked reports whether any read-only transaction is
// currently active.  Caller must hold tm.mu (read or write lock).
func (tm *TransactionManager) hasActiveSnapshotReadersLocked() bool {
	for _, tx := range tm.transactions {
		if tx.ReadOnly && tx.Status == TxActive {
			return true
		}
	}
	return false
}

// appendPageVersionLocked adds an old page snapshot to the version history for
// pageIdx.  validUntilSeq is the highest SnapshotSeq for which this version
// should be served (i.e., the commitSeq of the write that replaced it minus 1).
// Entries are appended in monotonically increasing validUntilSeq order because
// commits are serialised.  Caller must hold tm.mu write lock.
func (tm *TransactionManager) appendPageVersionLocked(pageIdx PageIndex, validUntilSeq uint64, page *Page) {
	tm.pageVersionHistory[pageIdx] = append(tm.pageVersionHistory[pageIdx], pageVersion{
		validUntilSeq: validUntilSeq,
		page:          page,
	})
}

// trimPageVersionHistoryLocked removes version-history entries that are no
// longer needed by any active snapshot reader.  Should be called after a
// transaction is removed from tm.transactions.  Caller must hold tm.mu write lock.
func (tm *TransactionManager) trimPageVersionHistoryLocked() {
	minSnap := tm.minActiveSnapshotSeqLocked()
	if minSnap == math.MaxUint64 {
		// No active readers — discard all history immediately.
		tm.pageVersionHistory = make(map[PageIndex][]pageVersion)
		return
	}
	// Remove entries whose validUntilSeq is strictly less than minSnap: every
	// active reader has SnapshotSeq >= minSnap, so they will never need a version
	// that was superseded before minSnap.
	for pageIdx, versions := range tm.pageVersionHistory {
		lo := 0
		for lo < len(versions) && versions[lo].validUntilSeq < minSnap {
			lo++
		}
		if lo > 0 {
			tm.pageVersionHistory[pageIdx] = versions[lo:]
		}
	}
}

// PageLastCommittedSeq returns the commitSeq at which pageIdx was last updated
// in the shared page cache.  Used by ReadPage to decide whether the cached
// version is safe to serve to a snapshot reader.
func (tm *TransactionManager) PageLastCommittedSeq(pageIdx PageIndex) uint64 {
	tm.mu.RLock()
	seq := tm.pageLastCommittedSeq[pageIdx]
	tm.mu.RUnlock()
	return seq
}

// PageVersionAtSnapshot returns the *Page that was current for the given
// snapshotSeq by binary-searching the version history.  ok is false when no
// suitable entry exists (page either predates any WAL write or was newly
// allocated after the snapshot).
func (tm *TransactionManager) PageVersionAtSnapshot(pageIdx PageIndex, snapshotSeq uint64) (*Page, bool) {
	tm.mu.RLock()
	versions := tm.pageVersionHistory[pageIdx]
	tm.mu.RUnlock()

	if len(versions) == 0 {
		return nil, false
	}

	// Versions are ordered by validUntilSeq ascending.  We want the entry with
	// the smallest validUntilSeq that is still >= snapshotSeq — that is the
	// version which was current at snapshotSeq.
	lo, hi := 0, len(versions)
	for lo < hi {
		mid := (lo + hi) / 2
		if versions[mid].validUntilSeq < snapshotSeq {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(versions) {
		return versions[lo].page, true
	}
	return nil, false
}

// CommitTransaction validates the write set for conflicts and, if clean, persists all changes.
// When a WAL is configured it uses the WAL commit path; otherwise it writes directly to the pager
// (used by unit tests that do not set up a WAL file).
func (tm *TransactionManager) CommitTransaction(ctx context.Context, tx *Transaction) error {
	if tm.wal != nil {
		return tm.commitWithWAL(ctx, tx)
	}
	return tm.commitDirect(ctx, tx)
}

// commitDirect is the non-WAL commit path: OCC check then write pages straight to the pager.
// Used only when WAL mode has not been enabled (e.g. in unit tests that do not set up a WAL).
func (tm *TransactionManager) commitDirect(ctx context.Context, tx *Transaction) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tx.Status != TxActive {
		return fmt.Errorf("transaction %d is not active", tx.ID)
	}

	// OCC conflict check (skip for ReadOnly transactions — ReadSet is never populated).
	if !tx.ReadOnly {
		for pageIdx, readVersion := range tx.GetReadVersions() {
			if tm.globalPageVersions[pageIdx] > readVersion {
				tx.Abort()
				return fmt.Errorf("%w: tx %d aborted due to conflict on page %d", ErrTxConflict, tx.ID, pageIdx)
			}
		}
		if readDBHeaderVersion, exists := tx.GetDBHeaderReadVersion(); exists {
			if tm.globalDBHeaderVersion > readDBHeaderVersion {
				tx.Abort()
				return fmt.Errorf("%w: tx %d aborted due to conflict on DB header", ErrTxConflict, tx.ID)
			}
		}
	}

	writeInfos := tx.GetWriteVersions()
	isReadOnly := len(writeInfos) == 0 && !tx.DDLChanges.HasChanges()
	if isReadOnly {
		tx.Commit()
		delete(tm.transactions, tx.ID)
		tm.trimPageVersionHistoryLocked()
		tm.logger.Debug("commit read-only transaction", zap.Uint64("tx_id", uint64(tx.ID)))
		return nil
	}

	// Advance the commit sequence for MVCC snapshot isolation.
	tm.commitSeq++
	newSeq := tm.commitSeq
	minSnap := tm.minActiveSnapshotSeqLocked()

	pagesToFlush := make([]PageIndex, 0, len(tx.WriteSet)+1)

	// Apply DB header write first.
	if header, modified := tx.GetModifiedDBHeader(); modified {
		tm.saver.SaveHeader(ctx, *header)
		tm.globalDBHeaderVersion += 1
		pagesToFlush = append(pagesToFlush, 0)
	}
	// Apply page writes.  For each page, save the old version into the history
	// before overwriting the cache so active snapshot readers can still see the
	// pre-commit state.
	for pageIdx, info := range writeInfos {
		if minSnap < newSeq && info.OriginalPage != nil {
			tm.appendPageVersionLocked(pageIdx, newSeq-1, info.OriginalPage)
		}
		tm.saver.SavePage(ctx, pageIdx, info.Page)
		tm.pageLastCommittedSeq[pageIdx] = newSeq
		tm.globalPageVersions[pageIdx] += 1
		pagesToFlush = append(pagesToFlush, pageIdx)
	}

	// Flush to disk.  Panic on failure: in-memory state is already ahead of disk.
	if err := tm.saver.FlushBatch(ctx, pagesToFlush); err != nil {
		tm.logger.Sugar().Panicf(
			"PANIC: batch flush failed during commit, tx_id %d, pages_to_flush %d, error %v",
			uint64(tx.ID), len(pagesToFlush), err,
		)
	}

	if tx.DDLChanges.HasChanges() {
		tm.ddlSaver.SaveDDLChanges(ctx, tx.DDLChanges)
	}

	tx.Commit()
	delete(tm.transactions, tx.ID)
	tm.logger.Debug("commit transaction", zap.Uint64("tx_id", uint64(tx.ID)))
	return nil
}

func (tm *TransactionManager) runCommitHook(phase commitPhase) {
	if tm.commitHook != nil {
		tm.commitHook(phase)
	}
}

// commitWithWAL is the WAL-based commit path.
//
// Concurrency design:
//   - walWriteMu serialises writers so only one transaction appends to the WAL
//     at a time.  This prevents interleaved frames and ensures the OCC check
//     remains valid until the WAL append completes.
//   - tm.mu is held only briefly: once to perform the OCC check (after
//     acquiring walWriteMu) and once to update globalPageVersions and the
//     in-memory pager cache after a successful WAL append.
//   - WAL I/O happens outside tm.mu so readers are never blocked by write I/O.
func (tm *TransactionManager) commitWithWAL(ctx context.Context, tx *Transaction) error {
	// === FAST PATH: read-only transactions ===
	// Validate under tm.mu and return immediately — no WAL involvement needed.
	tm.mu.Lock()
	if tx.Status != TxActive {
		tm.mu.Unlock()
		return fmt.Errorf("transaction %d is not active", tx.ID)
	}
	writeInfos := tx.GetWriteVersions()
	isReadOnly := len(writeInfos) == 0 && !tx.DDLChanges.HasChanges()
	if isReadOnly {
		// For read-only transactions (ReadOnly flag set), the ReadSet was never
		// populated, so there is nothing to validate — skip the conflict check.
		if !tx.ReadOnly {
			for pageIdx, readVersion := range tx.GetReadVersions() {
				if tm.globalPageVersions[pageIdx] > readVersion {
					tx.Abort()
					tm.mu.Unlock()
					return fmt.Errorf("%w: tx %d aborted due to conflict on page %d", ErrTxConflict, tx.ID, pageIdx)
				}
			}
			if readDBHeaderVersion, exists := tx.GetDBHeaderReadVersion(); exists {
				if tm.globalDBHeaderVersion > readDBHeaderVersion {
					tx.Abort()
					tm.mu.Unlock()
					return fmt.Errorf("%w: tx %d aborted due to conflict on DB header", ErrTxConflict, tx.ID)
				}
			}
		}
		tx.Commit()
		delete(tm.transactions, tx.ID)
		tm.trimPageVersionHistoryLocked()
		tm.mu.Unlock()
		tm.logger.Debug("commit read-only transaction (WAL)", zap.Uint64("tx_id", uint64(tx.ID)))
		return nil
	}
	tm.mu.Unlock()

	// === WRITE PATH ===

	// Step 1: Acquire WAL write mutex to serialise writers.
	// Steps 2–5 are protected by this mutex so the OCC check remains valid
	// until the WAL append completes.  We release it explicitly before steps
	// 6–7 so that a triggered auto-checkpoint can re-acquire it without
	// deadlocking.
	tm.walWriteMu.Lock()

	// Step 2: OCC check (under tm.mu, after acquiring walWriteMu).
	tm.mu.Lock()
	for pageIdx, readVersion := range tx.GetReadVersions() {
		if tm.globalPageVersions[pageIdx] > readVersion {
			tx.Abort()
			tm.mu.Unlock()
			tm.walWriteMu.Unlock()
			return fmt.Errorf("%w: tx %d aborted due to conflict on page %d", ErrTxConflict, tx.ID, pageIdx)
		}
	}
	if readDBHeaderVersion, exists := tx.GetDBHeaderReadVersion(); exists {
		if tm.globalDBHeaderVersion > readDBHeaderVersion {
			tx.Abort()
			tm.mu.Unlock()
			tm.walWriteMu.Unlock()
			return fmt.Errorf("%w: tx %d aborted due to conflict on DB header", ErrTxConflict, tx.ID)
		}
	}
	tm.mu.Unlock()

	// Step 3: Serialise modified pages into WAL frames (outside both locks).
	walPages, err := tm.serializeWritesForWAL(ctx, tx)
	if err != nil {
		tx.Abort()
		tm.walWriteMu.Unlock()
		return fmt.Errorf("serialize writes for WAL tx %d: %w", tx.ID, err)
	}

	// Step 4: Append to WAL (I/O outside tm.mu; walWriteMu held).
	if len(walPages) > 0 {
		tm.runCommitHook(commitPhaseBeforeWALAppend)
		if err := tm.wal.AppendTransaction(walPages); err != nil {
			tx.Abort()
			tm.walWriteMu.Unlock()
			return fmt.Errorf("WAL append tx %d: %w", tx.ID, err)
		}
		tm.runCommitHook(commitPhaseAfterWALAppend)
	}

	// Step 5: Update in-memory state (under tm.mu).
	tm.mu.Lock()
	if header, modified := tx.GetModifiedDBHeader(); modified {
		tm.saver.SaveHeader(ctx, *header)
		tm.globalDBHeaderVersion++
	}
	// Advance commit sequence and save old page versions for snapshot readers.
	tm.commitSeq++
	newSeq := tm.commitSeq
	minSnap := tm.minActiveSnapshotSeqLocked()
	for pageIdx, info := range writeInfos {
		if minSnap < newSeq && info.OriginalPage != nil {
			tm.appendPageVersionLocked(pageIdx, newSeq-1, info.OriginalPage)
		}
		tm.saver.SavePage(ctx, pageIdx, info.Page)
		tm.pageLastCommittedSeq[pageIdx] = newSeq
		tm.globalPageVersions[pageIdx] += 1
	}
	if tx.DDLChanges.HasChanges() {
		tm.ddlSaver.SaveDDLChanges(ctx, tx.DDLChanges)
	}
	tx.Commit()
	delete(tm.transactions, tx.ID)
	tm.mu.Unlock()

	// Release walWriteMu before steps 6–7 so a triggered auto-checkpoint can
	// acquire it without deadlocking.
	tm.walWriteMu.Unlock()

	// Step 6: Update WAL index so subsequent reads see the latest committed data.
	// Recycle old page buffers that are displaced by newer commits.
	for _, wp := range walPages {
		if old := tm.walIndex.Update(wp.Index, wp.Data); old != nil {
			pageDataPool.Put(old)
		}
	}

	tm.logger.Debug("commit transaction (WAL)", zap.Uint64("tx_id", uint64(tx.ID)))

	// Step 7: Trigger auto-checkpoint if the WAL has grown past the threshold.
	tm.runAutoCheckpoint(ctx)

	return nil
}

// serializeWritesForWAL converts the transaction's write set into WAL frames.
// Page 0 receives special treatment: its WAL frame must combine the DB-header
// bytes (first RootPageConfigSize bytes) with the B-tree content of page 0.
func (tm *TransactionManager) serializeWritesForWAL(ctx context.Context, tx *Transaction) ([]WALPage, error) {
	writeInfos := tx.GetWriteVersions()
	header, dbHeaderModified := tx.GetModifiedDBHeader()

	page0Info, page0Modified := writeInfos[0]
	needPage0Frame := dbHeaderModified || page0Modified

	pages := make([]WALPage, 0, len(writeInfos)+1)

	if needPage0Frame {
		var p0 *Page
		if page0Modified {
			p0 = page0Info.Page
		}
		frame, err := tm.serializePage0ForWAL(ctx, p0, header, dbHeaderModified)
		if err != nil {
			return nil, fmt.Errorf("serialize page 0 for WAL: %w", err)
		}
		pages = append(pages, WALPage{Index: 0, Data: frame})
	}

	for pageIdx, info := range writeInfos {
		if pageIdx == 0 {
			continue // already included above
		}
		buf := pageDataPool.Get().([]byte)
		if err := marshalPage(info.Page, buf); err != nil {
			pageDataPool.Put(buf)
			return nil, fmt.Errorf("marshal page %d for WAL: %w", pageIdx, err)
		}
		pages = append(pages, WALPage{Index: pageIdx, Data: buf})
	}

	return pages, nil
}

// serializePage0ForWAL builds the full-page (PageSize bytes) WAL frame for
// page 0 by merging the DB-header portion with the B-tree portion.
//
//   - page0 is the modified B-tree page (nil if only the header changed).
//   - header is the modified DB header (nil / dbHeaderModified=false if only
//     the B-tree changed).
func (tm *TransactionManager) serializePage0ForWAL(ctx context.Context, page0 *Page, header *DatabaseHeader, dbHeaderModified bool) ([]byte, error) {
	frame := make([]byte, PageSize)

	// --- Header portion (bytes 0 .. RootPageConfigSize-1) ---
	var hdr DatabaseHeader
	if dbHeaderModified {
		hdr = *header
	} else {
		// Reads the current in-memory header from the pager cache.
		pager, err := tm.factory(ctx, SchemaTableName, "")
		if err != nil {
			return nil, fmt.Errorf("get pager for page 0 header: %w", err)
		}
		hdr = pager.GetHeader(ctx)
	}
	headerBytes, err := hdr.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal DB header for WAL page 0: %w", err)
	}
	copy(frame[0:RootPageConfigSize], headerBytes[0:RootPageConfigSize])

	// --- B-tree portion (bytes RootPageConfigSize .. PageSize-1) ---
	var btreePage *Page
	if page0 != nil {
		btreePage = page0
	} else {
		// Only the header changed; read the current page 0 B-tree from cache.
		pager, err := tm.factory(ctx, SchemaTableName, "")
		if err != nil {
			return nil, fmt.Errorf("get pager for page 0 B-tree: %w", err)
		}
		btreePage, err = pager.GetPage(ctx, 0)
		if err != nil {
			return nil, fmt.Errorf("read page 0 B-tree for WAL: %w", err)
		}
	}
	pageBuf := pageDataPool.Get().([]byte)
	if err := marshalPage(btreePage, pageBuf); err != nil {
		pageDataPool.Put(pageBuf)
		return nil, fmt.Errorf("marshal page 0 B-tree for WAL: %w", err)
	}
	copy(frame[RootPageConfigSize:], pageBuf[0:PageSize-RootPageConfigSize])
	pageDataPool.Put(pageBuf)

	return frame, nil
}

// runAutoCheckpoint triggers a WAL checkpoint when the frame count exceeds the
// configured threshold.  The checkpoint is performed via checkpointFn (set by
// SetCheckpointFunc).  If no function is registered, or if snapshot readers
// are active, the call is a no-op (the checkpoint will be attempted on the
// next commit once the readers have finished).  Failures are logged but do not
// propagate — the commit already succeeded.
func (tm *TransactionManager) runAutoCheckpoint(_ context.Context) {
	if tm.checkpointThreshold <= 0 || tm.wal == nil || tm.checkpointFn == nil {
		return
	}
	if tm.wal.FrameCount() < int64(tm.checkpointThreshold) {
		return
	}
	// Skip if snapshot readers are active; they will be unblocked by the next
	// commit that reduces minActiveSnapshotSeq.
	tm.mu.RLock()
	blocked := tm.hasActiveSnapshotReadersLocked()
	tm.mu.RUnlock()
	if blocked {
		tm.logger.Debug("auto-checkpoint deferred: active snapshot readers")
		return
	}
	if err := tm.checkpointFn(); err != nil {
		if !errors.Is(err, ErrCheckpointBlockedByReaders) {
			tm.logger.Warn("WAL auto-checkpoint failed",
				zap.Int64("frame_count", tm.wal.FrameCount()),
				zap.Int("threshold", tm.checkpointThreshold),
				zap.Error(err))
		}
	}
}

// RollbackTransaction aborts the transaction and discards all in-memory changes.
func (tm *TransactionManager) RollbackTransaction(ctx context.Context, tx *Transaction) {
	tx.Abort()

	// Clean up transaction and GC any version history that is no longer needed.
	tm.mu.Lock()
	delete(tm.transactions, tx.ID)
	tm.trimPageVersionHistoryLocked()
	tm.mu.Unlock()

	tm.logger.Debug("rollback transaction", zap.Uint64("tx_id", uint64(tx.ID)))
}

// GlobalDBHeaderVersion returns the current committed version of the database header.
func (tm *TransactionManager) GlobalDBHeaderVersion(ctx context.Context) uint64 {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	return tm.globalDBHeaderVersion
}

// GlobalPageVersion returns the current committed version of the given page.
func (tm *TransactionManager) GlobalPageVersion(ctx context.Context, pageIdx PageIndex) uint64 {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	return tm.globalPageVersions[pageIdx]
}
