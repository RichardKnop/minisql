package minisql

import (
	"context"
	"fmt"
)

// var (
// 	ErrUnrecognizedStatementType = errors.New("Unrecognised statement type")
// )

// type StatementType int

// const (
// 	Insert StatementType = iota + 1
// 	Select
// 	Update
// 	Delete
// )

// type Statement struct {
// 	Kind StatementType
// }

// // PrepareStatement parses input into a statement
// func PrepareStatement(ctx context.Context, inputBuffer string) (*Statement, bool) {
// 	if len(inputBuffer) >= 6 {
// 		var kind StatementType
// 		switch inputBuffer[:6] {
// 		case "insert":
// 			kind = Insert
// 		case "select":
// 			kind = Select
// 		default:
// 			return nil, false
// 		}
// 		return &Statement{Kind: kind}, true
// 	}
// 	return nil, false
// }

var (
	errUnrecognizedStatementType = fmt.Errorf("Unrecognised statement type")
)

// Execute will eventually become virtual machine
func (s *Statement) Execute(ctx context.Context) error {
	switch s.Kind {
	case Insert:
		return s.executeInsert(ctx)
	case Select:
		return s.executeSelect(ctx)
	}
	return errUnrecognizedStatementType
}

func (stmt *Statement) executeInsert(ctx context.Context) error {
	fmt.Println("This is where we would do insert")
	return nil
}

func (stmt *Statement) executeSelect(ctx context.Context) error {
	fmt.Println("This is where we would do select")
	return nil
}
