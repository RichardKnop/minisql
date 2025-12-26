package minisql

import (
	"context"
	"fmt"
	"os"
)

// Vacuum is an experimental function to compact the database file by removing fragmentation.
// TODO - test and optimize
func (db *Database) Vacuum(ctx context.Context) error {
	// 1. Create a temporary database file
	tempFile := db.FileName + ".tmp"

	tempDB, err := NewDatabase(ctx, db.logger, tempFile, db.parser, db.factory, db.saver)
	if err != nil {
		return err
	}

	// Use lock to block any write operations on the live database during vacuum
	db.dbLock.Lock()
	defer db.dbLock.Unlock()

	// Recreate all tables and indexes in the temporary database
	schemas, err := db.listSchemas(ctx)
	if err != nil {
		return err
	}
	for _, aSchema := range schemas {
		switch aSchema.Type {
		case SchemaTable, SchemaSecondaryIndex:
			stmts, err := db.parser.Parse(ctx, aSchema.DDL)
			if err != nil {
				return err
			}
			stmt := stmts[0]
			_, err = tempDB.ExecuteStatement(ctx, stmt)
			if err != nil {
				return err
			}
		}
	}

	// 3. Copy all data from the live database into temporary database table by table
	for _, aSchema := range schemas {
		if aSchema.Type != SchemaTable {
			continue
		}
		stmt := Statement{
			Kind:      Select,
			TableName: aSchema.TableName,
		}
		aResult, err := db.ExecuteStatement(ctx, stmt)
		if err != nil {
			return err
		}
		for aResult.Rows.Next(ctx) {
			aRow := aResult.Rows.Row()
			stmt := Statement{
				Kind:      Insert,
				TableName: aSchema.TableName,
				Inserts:   [][]OptionalValue{aRow.Values},
			}
			_, err = db.ExecuteStatement(ctx, stmt)
			if err != nil {
				return err
			}
		}
	}

	// 4. Close both databases
	if err := tempDB.Close(); err != nil {
		return fmt.Errorf("failed to close temporary database: %w", err)
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("failed to close live database: %w", err)
	}

	// 4. Replace old file with new compacted file
	os.Remove(db.FileName + ".bak")
	os.Rename(db.FileName, db.FileName+".bak")
	os.Rename(tempFile, db.FileName)

	// 5. Reopen the database
	dbFile, err := os.OpenFile(db.FileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return fmt.Errorf("failed to open database file: %w", err)
	}
	newPager, err := NewPager(dbFile, PageSize)
	if err != nil {
		return fmt.Errorf("failed to create pager: %w", err)
	}

	if err := db.Reopen(ctx, newPager, newPager); err != nil {
		return fmt.Errorf("failed to reopen database: %w", err)
	}

	return nil
}
