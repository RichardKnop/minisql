package minisql

import (
	"context"
	"fmt"
)

func (d *Database) executeDelete(ctx context.Context, stmt Statement) (StatementResult, error) {
	return StatementResult{}, fmt.Errorf("not implemented")
}
