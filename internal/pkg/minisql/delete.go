package minisql

import (
	"context"
	"fmt"
)

func (t *Table) Delete(ctx context.Context, stmt Statement) error {
	fmt.Println("TODO - implement DELETE")
	return fmt.Errorf("not implemented")
}
