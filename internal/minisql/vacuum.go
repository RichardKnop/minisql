package minisql

import (
	"context"
	"fmt"
	"os"
)

// Vacuum compacts the database file by copying all live data into a fresh file,
// then atomically replacing the original.  The algorithm is:
//
//  1. Create a temporary database file with its own pager.
//  2. Acquire the exclusive database write lock, blocking all concurrent reads
//     and writes for the duration.
//  3. Recreate every table schema (tables first, indexes second) in the temp DB.
//  4. Copy every row from the live DB into the temp DB.
//  5. Flush and close both the temp DB and the live DB.
//  6. Safe atomic file swap:
//     a. Rename live → live.bak
//     b. Rename temp → live  (on failure, restore live from live.bak)
//     c. Remove live.bak
//  7. Reopen the database with a fresh pager and transaction manager.
//
// Crash-safety: if the process crashes between steps 6a and 6b the original
// data is intact in live.bak.  On restart the caller should check for a
// live.bak file and rename it back if the expected database file is missing.
//
// VACUUM must not be called from inside an explicit user transaction; doing so
// returns an error.
func (d *Database) Vacuum(ctx context.Context) error {
	tempFile := d.GetFileName() + ".tmp"
	backupFile := d.GetFileName() + ".bak"

	// Ensure the temp file is removed on any early error.
	vacuumDone := false
	defer func() {
		if !vacuumDone {
			os.Remove(tempFile)
		}
	}()

	// --- PHASE 1: Create temp database with its own file and pager. ---
	// This must happen before acquiring the lock so we don't hold the lock
	// while doing file I/O that can't fail gracefully.
	//
	// Use context.Background() so the temp DB's own initialisation runs in a
	// fresh transaction on its own TransactionManager, completely independent
	// of any outer transaction that the caller may have placed on ctx.
	f, err := os.OpenFile(tempFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("vacuum: create temp file: %w", err)
	}

	tempPager, err := NewPager(f, PageSize, PageCacheSize)
	if err != nil {
		f.Close()
		return fmt.Errorf("vacuum: create temp pager: %w", err)
	}

	tempDB, err := NewDatabase(context.Background(), d.logger, tempFile, d.parser, tempPager, tempPager, nil)
	if err != nil {
		return fmt.Errorf("vacuum: init temp database: %w", err)
	}

	// --- PHASE 2: Acquire exclusive lock — blocks all concurrent operations. ---
	d.dbLock.Lock()
	defer d.dbLock.Unlock()

	// --- PHASE 3: Read all schema records from the live DB. ---
	// listSchemas accesses d.tables directly and calls mainTable.Select(),
	// both of which bypass the dbLock, so no deadlock occurs here.
	var schemas []Schema
	if err := d.txManager.ExecuteInTransaction(ctx, func(txCtx context.Context) error {
		var err error
		schemas, err = d.listSchemas(txCtx)
		return err
	}); err != nil {
		return fmt.Errorf("vacuum: list schemas: %w", err)
	}

	// --- PHASE 4: Recreate schema in temp DB — tables first, then indexes. ---
	// Using a fresh context keeps temp DB transactions independent of the live
	// DB's transaction manager.
	tempCtx := context.Background()

	for _, schema := range schemas {
		if schema.Type != SchemaTable {
			continue
		}
		stmts, err := d.parser.Parse(tempCtx, schema.DDL)
		if err != nil {
			return fmt.Errorf("vacuum: parse table DDL for %q: %w", schema.Name, err)
		}
		if err := tempDB.txManager.ExecuteInTransaction(tempCtx, func(txCtx context.Context) error {
			_, err := tempDB.ExecuteStatement(txCtx, stmts[0])
			return err
		}); err != nil {
			return fmt.Errorf("vacuum: recreate table %q: %w", schema.Name, err)
		}
	}

	// Non-HNSW indexes are recreated before rows so that INSERT maintenance
	// keeps them in sync as rows are copied.  HNSW indexes use batch-build-only
	// maintenance, so they must be created AFTER the rows are copied.
	for _, schema := range schemas {
		if schema.Type != SchemaSecondaryIndex {
			continue
		}
		stmts, err := d.parser.Parse(tempCtx, schema.DDL)
		if err != nil {
			return fmt.Errorf("vacuum: parse index DDL for table %q: %w", schema.TableName, err)
		}
		if stmts[0].IndexMethod == IndexMethodHNSW {
			continue // deferred until after row copy
		}
		if err := tempDB.txManager.ExecuteInTransaction(tempCtx, func(txCtx context.Context) error {
			_, err := tempDB.ExecuteStatement(txCtx, stmts[0])
			return err
		}); err != nil {
			return fmt.Errorf("vacuum: recreate index for table %q: %w", schema.TableName, err)
		}
	}

	// --- PHASE 5: Copy all rows from each live table into the temp DB. ---
	for _, schema := range schemas {
		if schema.Type != SchemaTable {
			continue
		}

		liveTable, ok := d.tables[schema.Name]
		if !ok {
			return fmt.Errorf("vacuum: live table %q not found", schema.Name)
		}
		tempTable, ok := tempDB.tables[schema.Name]
		if !ok {
			return fmt.Errorf("vacuum: temp table %q not found", schema.Name)
		}

		// Read all rows from the live table in a single read transaction.
		var rows []Row
		if err := d.txManager.ExecuteInTransaction(ctx, func(txCtx context.Context) error {
			result, err := liveTable.Select(txCtx, Statement{
				Kind:   Select,
				Fields: fieldsFromColumns(liveTable.Columns...),
			})
			if err != nil {
				return err
			}
			for result.Rows.Next(txCtx) {
				rows = append(rows, result.Rows.Row())
			}
			return result.Rows.Err()
		}); err != nil {
			return fmt.Errorf("vacuum: read table %q: %w", schema.Name, err)
		}

		// Insert each row into the temp table.  Each insert is its own
		// transaction so a failure on one row doesn't silently roll back others.
		for _, row := range rows {
			if err := tempDB.txManager.ExecuteInTransaction(tempCtx, func(txCtx context.Context) error {
				_, err := tempTable.Insert(txCtx, Statement{
					Kind:    Insert,
					Fields:  fieldsFromColumns(tempTable.Columns...),
					Inserts: [][]OptionalValue{row.Values},
				})
				return err
			}); err != nil {
				return fmt.Errorf("vacuum: insert row into temp table %q: %w", schema.Name, err)
			}
		}
	}

	// --- PHASE 6: Recreate HNSW indexes now that all rows are in the temp DB. ---
	// HNSW uses batch-build-only maintenance, so the index must be built after
	// the table is fully populated.
	for _, schema := range schemas {
		if schema.Type != SchemaSecondaryIndex {
			continue
		}
		stmts, err := d.parser.Parse(tempCtx, schema.DDL)
		if err != nil {
			return fmt.Errorf("vacuum: parse HNSW index DDL for table %q: %w", schema.TableName, err)
		}
		if stmts[0].IndexMethod != IndexMethodHNSW {
			continue
		}
		if err := tempDB.txManager.ExecuteInTransaction(tempCtx, func(txCtx context.Context) error {
			_, err := tempDB.ExecuteStatement(txCtx, stmts[0])
			return err
		}); err != nil {
			return fmt.Errorf("vacuum: recreate HNSW index for table %q: %w", schema.TableName, err)
		}
	}

	// --- PHASE 8: Flush and close both databases. ---
	if err := tempDB.Close(); err != nil {
		return fmt.Errorf("vacuum: close temp database: %w", err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("vacuum: close live database: %w", err)
	}

	// --- PHASE 9: Safe atomic file swap. ---
	// Remove any stale backup from a previous failed vacuum.
	os.Remove(backupFile)

	// Move the live file to the backup path.  If this fails, the live file is
	// untouched and no data is lost.
	if err := os.Rename(d.GetFileName(), backupFile); err != nil {
		return fmt.Errorf("vacuum: rename live to backup: %w", err)
	}

	// Move the temp file into the live path.  On failure, restore from backup.
	if err := os.Rename(tempFile, d.GetFileName()); err != nil {
		if restoreErr := os.Rename(backupFile, d.GetFileName()); restoreErr != nil {
			return fmt.Errorf(
				"vacuum: swap failed (%w) and restore also failed (%v): "+
					"original database is in %s", err, restoreErr, backupFile)
		}
		return fmt.Errorf("vacuum: swap failed, original database restored from backup: %w", err)
	}

	// Swap succeeded — remove the backup.
	os.Remove(backupFile)
	vacuumDone = true

	// --- PHASE 10: Reopen the database with the compacted file. ---
	newFile, err := os.OpenFile(d.GetFileName(), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("vacuum: reopen database file: %w", err)
	}

	newPager, err := NewPager(newFile, PageSize, PageCacheSize)
	if err != nil {
		newFile.Close()
		return fmt.Errorf("vacuum: create new pager: %w", err)
	}

	// Reopen replaces d.txManager with a fresh instance so stale page version
	// numbers from the old file cannot cause spurious OCC conflicts.
	if err := d.Reopen(tempCtx, newPager, newPager); err != nil {
		return fmt.Errorf("vacuum: reopen database: %w", err)
	}

	return nil
}
