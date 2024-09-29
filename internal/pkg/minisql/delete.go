package minisql

import (
	"context"
	"fmt"
)

func (d *Database) executeDelete(ctx context.Context, stmt Statement) (StatementResult, error) {
	fmt.Println("TODO - implement DELETE")
	return StatementResult{}, nil
}
