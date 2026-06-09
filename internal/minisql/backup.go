package minisql

import (
	"context"
	"fmt"
	"os"
)

// Backup writes a consistent, point-in-time copy of the database to destPath.
//
// The algorithm mirrors SQLite's WAL-mode online backup:
//
//  1. Begin a read-only snapshot transaction.  CheckpointWAL checks
//     hasActiveSnapshotReadersLocked before acquiring walWriteMu, so any
//     checkpoint that starts after this point will be turned away with
//     ErrCheckpointBlockedByReaders.  This freezes the main DB file from
//     checkpoint's perspective for the lifetime of the backup.
//
//  2. Hold walWriteMu briefly to deep-copy the WAL index (raw page bytes) and
//     record the current page count.  The deep-copy is necessary because
//     WALIndex.Reset (called during checkpoint) returns those slices to
//     pageDataPool; any read after walWriteMu is released would race with reuse.
//
//  3. Release walWriteMu — writers proceed immediately.
//
//  4. For every page index 0..N-1:
//     - If the page has an entry in the WAL snapshot use those bytes
//       (committed but not yet checkpointed at snapshot time).
//     - Otherwise read directly from the main DB file.
//     Because checkpoints are blocked by step 1, the DB file cannot be
//     modified by post-snapshot transactions during this phase.
//
//  5. Release the read-only snapshot, sync, and close the destination.
//
// The destination is a standalone database file that can be opened directly
// with sql.Open("minisql", destPath).  It carries no WAL file.  If the source
// is encrypted the backup is encrypted with the same key.
//
// Backup must not be called from inside an explicit user transaction.
func (d *Database) Backup(_ context.Context, destPath string) (retErr error) {
	if d.walDBFile == nil || d.walIndex == nil {
		return fmt.Errorf("backup: database is not in WAL mode")
	}

	// --- PHASE 1: Begin a read-only snapshot to block checkpoints. ---
	// A checkpoint that starts after this point will see an active snapshot
	// reader and return ErrCheckpointBlockedByReaders without modifying the DB
	// file.  This guarantees that all ReadAt calls in phase 4 see DB file pages
	// at their pre-snapshot versions, even if writers commit and trigger
	// auto-checkpoint attempts during the copy.
	snapCtx := context.Background()
	snapTx := d.txManager.BeginReadOnlyTransaction(snapCtx)
	snapCtx = WithTransaction(snapCtx, snapTx)
	defer func() {
		d.txManager.RollbackTransaction(snapCtx, snapTx)
	}()

	// --- PHASE 2: Snapshot WAL index and page count under walWriteMu. ---
	// If a checkpoint is already in progress (holding walWriteMu) we block here
	// until it completes.  Subsequent checkpoint attempts are blocked by phase 1.
	d.txManager.walWriteMu.Lock()
	rawSnapshot := d.walIndex.SnapshotSorted()
	walMap := make(map[PageIndex][]byte, len(rawSnapshot))
	for _, p := range rawSnapshot {
		buf := make([]byte, len(p.Data))
		copy(buf, p.Data)
		walMap[p.Index] = buf
	}
	totalPages := d.saver.TotalPages()
	d.txManager.walWriteMu.Unlock()

	if totalPages == 0 {
		return fmt.Errorf("backup: database is empty")
	}

	// --- PHASE 3: Open destination file. ---
	destFile, err := os.OpenFile(destPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("backup: create destination: %w", err)
	}
	defer func() {
		if cerr := destFile.Close(); cerr != nil && retErr == nil {
			retErr = fmt.Errorf("backup: close destination: %w", cerr)
		}
	}()

	// backupHook is nil in production; tests inject concurrent operations here
	// to verify the checkpoint-blocking invariant.
	if d.backupHook != nil {
		d.backupHook()
	}

	// --- PHASE 4: Copy pages (writers unblocked, checkpoints blocked). ---
	// WAL snapshot bytes take priority.  Pages absent from the snapshot are read
	// from the DB file — safe because no checkpoint can flush post-snapshot WAL
	// frames to the DB file while the snapshot transaction is active.
	readBuf := make([]byte, PageSize)
	for idx := PageIndex(0); idx < PageIndex(totalPages); idx++ {
		offset := int64(idx) * int64(PageSize)
		if data, ok := walMap[idx]; ok {
			if _, err = destFile.WriteAt(data, offset); err != nil {
				return fmt.Errorf("backup: write WAL page %d: %w", idx, err)
			}
		} else {
			if _, err = d.walDBFile.ReadAt(readBuf, offset); err != nil {
				return fmt.Errorf("backup: read DB page %d: %w", idx, err)
			}
			if _, err = destFile.WriteAt(readBuf, offset); err != nil {
				return fmt.Errorf("backup: write DB page %d: %w", idx, err)
			}
		}
	}

	// --- PHASE 5: Sync destination and release snapshot (deferred above). ---
	if err = destFile.Sync(); err != nil {
		return fmt.Errorf("backup: sync destination: %w", err)
	}

	return nil
}
