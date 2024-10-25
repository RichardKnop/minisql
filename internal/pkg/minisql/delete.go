package minisql

import (
	"context"
	"fmt"
)

func (t *Table) Delete(ctx context.Context, stmt Statement) (StatementResult, error) {
	fmt.Println("TODO - implement DELETE")
	return StatementResult{}, fmt.Errorf("not implemented")
}
