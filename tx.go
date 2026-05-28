package minisql

import (
	"context"

	"github.com/RichardKnop/minisql/internal/minisql"
)

// Tx is a database/sql/driver.Tx implementation representing an explicit
// BEGIN/COMMIT/ROLLBACK transaction. Write transactions use Optimistic
// Concurrency Control (OCC); conflicts return pkg/errors.ErrTxConflict.
type Tx struct {
	conn *Conn
	tx   *minisql.Transaction
	ctx  context.Context
}

// Commit flushes the transaction's write set to the WAL and clears the active
// transaction on the connection. Returns pkg/errors.ErrTxConflict if a
// concurrent write transaction modified a page read by this transaction.
func (tx Tx) Commit() error {
	if err := tx.conn.db.GetTransactionManager().CommitTransaction(tx.ctx, tx.tx); err != nil {
		return err
	}
	tx.conn.SetTransaction(nil)
	return nil
}

// Rollback discards all writes made in this transaction and clears the active
// transaction on the connection. It never returns an error.
func (tx Tx) Rollback() error {
	tx.conn.db.GetTransactionManager().RollbackTransaction(tx.ctx, tx.tx)
	tx.conn.SetTransaction(nil)
	return nil
}
