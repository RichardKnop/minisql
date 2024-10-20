package minisql

import (
	"context"
	"fmt"
)

func (t *Table) Update(ctx context.Context, stmt Statement) error {
	fmt.Println("TODO - implement UPDATE")
	return fmt.Errorf("not implemented")
}
