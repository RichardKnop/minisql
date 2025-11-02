package minisql

import (
	"context"
)

type txKeyType struct{}

var txKey = txKeyType{}

func WithTransaction(ctx context.Context, tx *Transaction) context.Context {
	return context.WithValue(ctx, txKey, tx)
}

func TxFromContext(ctx context.Context) *Transaction {
	if tx, ok := ctx.Value(txKey).(*Transaction); ok {
		return tx
	}
	return nil
}
