package minisql

import (
	"context"

	"github.com/RichardKnop/minisql/internal/minisql"
)

type Tx struct {
	conn *Conn
	tx   *minisql.Transaction
	ctx  context.Context
}

func (tx Tx) Commit() error {
	if err := tx.conn.db.GetTransactionManager().CommitTransaction(tx.ctx, tx.tx, tx.conn.db.GetSaver()); err != nil {
		return err
	}
	tx.conn.SetTransaction(nil)
	return nil
}

func (tx Tx) Rollback() error {
	tx.conn.db.GetTransactionManager().RollbackTransaction(tx.ctx, tx.tx)
	tx.conn.SetTransaction(nil)
	return nil
}
