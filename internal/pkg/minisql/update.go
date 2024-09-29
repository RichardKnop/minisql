package minisql

import (
	"context"
	"fmt"
)

func (d *Database) executeUpdate(ctx context.Context, stmt Statement) (StatementResult, error) {
	fmt.Println("TODO - implement UPDATE")
	return StatementResult{}, nil
}
