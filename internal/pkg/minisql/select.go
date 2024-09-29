package minisql

import (
	"context"
	"fmt"
)

func (d Database) executeSelect(ctx context.Context, stmt Statement) (StatementResult, error) {
	fmt.Println("TODO - implement SELECT")
	return StatementResult{}, nil
}
