package minisql

import (
	"context"
	"database/sql"
	"fmt"
)

// Backup creates a consistent, point-in-time copy of db at destPath.
//
// The database remains fully readable and writable during the backup.
// Write transactions are blocked only for the brief moment needed to snapshot
// the WAL index (typically microseconds); the page-copy phase runs concurrently
// with normal database activity.
//
// The destination is a standalone file that can be opened directly:
//
//	if err := minisql.Backup(ctx, db, "/path/to/backup.db"); err != nil {
//	    log.Fatal(err)
//	}
//	backup, err := sql.Open("minisql", "/path/to/backup.db")
//
// If the source database uses transparent encryption the backup is encrypted
// with the same key.  The backup carries no WAL file.
//
// Backup must not be called from inside an explicit user transaction.
func Backup(ctx context.Context, db *sql.DB, destPath string) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("minisql: Backup: acquire connection: %w", err)
	}
	defer conn.Close()

	return conn.Raw(func(c any) error {
		mc, ok := c.(*Conn)
		if !ok {
			return fmt.Errorf("minisql: Backup: unexpected connection type %T", c)
		}
		return mc.db.Backup(ctx, destPath)
	})
}
