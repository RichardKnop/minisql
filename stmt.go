package minisql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/RichardKnop/minisql/internal/minisql"
)

type Stmt struct {
	conn      *Conn
	statement minisql.Statement
}

// Close closes the statement.
//
// As of Go 1.1, a Stmt will not be closed if it's in use
// by any queries.
//
// Drivers must ensure all network calls made by Close
// do not block indefinitely (e.g. apply a timeout).
func (s Stmt) Close() error {
	return nil
}

// NumInput returns the number of placeholder parameters.
//
// If NumInput returns >= 0, the sql package will sanity check
// argument counts from callers and return errors to the caller
// before the statement's Exec or Query methods are called.
//
// NumInput may also return -1, if the driver doesn't know
// its number of placeholders. In that case, the sql package
// will not sanity check Exec or Query argument counts.
func (s Stmt) NumInput() int {
	return s.statement.NumPlaceholders()
}

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// Deprecated: Drivers should implement StmtExecContext instead (or additionally).
func (s Stmt) Exec(args []driver.Value) (driver.Result, error) {
	internalArgs, err := toInternalArgs(args)
	if err != nil {
		return nil, err
	}

	s.statement, err = s.statement.BindArguments(internalArgs...)
	if err != nil {
		return nil, err
	}

	result, err := s.conn.executeStatement(context.Background(), s.statement)
	if err != nil {
		return nil, err
	}

	return Result{rowsAffected: int64(result.RowsAffected)}, nil
}

// Query executes a query that may return rows, such as a
// SELECT.
//
// Deprecated: Drivers should implement StmtQueryContext instead (or additionally).
func (s Stmt) Query(args []driver.Value) (driver.Rows, error) {
	internalArgs, err := toInternalArgs(args)
	if err != nil {
		return nil, err
	}

	s.statement, err = s.statement.BindArguments(internalArgs...)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	result, err := s.conn.executeStatement(ctx, s.statement)
	if err != nil {
		return nil, err
	}

	return &Rows{
		columns: result.Columns,
		iter:    result.Rows,
		ctx:     ctx,
	}, nil
}

func toInternalArgs(args []driver.Value) ([]any, error) {
	internalArgs := make([]any, 0, len(args))
	for _, arg := range args {
		//	int64
		//	float64
		//	bool
		//	[]byte
		//	string
		//	time.Time
		switch v := arg.(type) {
		case nil:
			return nil, fmt.Errorf("nil argument values are not supported; use IS NULL or IS NOT NULL instead")
		case int64, float64, bool:
			internalArgs = append(internalArgs, v)
		case string:
			internalArgs = append(internalArgs, minisql.NewTextPointer([]byte(v)))
		case time.Time:
			internalArgs = append(internalArgs, minisql.Time{
				Year:         int32(v.Year()),
				Month:        int8(v.Month()),
				Day:          int8(v.Day()),
				Hour:         int8(v.Hour()),
				Minutes:      int8(v.Minute()),
				Seconds:      int8(v.Second()),
				Microseconds: int32(v.Nanosecond() / 1000),
			})
		default:
			return nil, fmt.Errorf("unsupported argument type: %T", arg)
		}
	}
	return internalArgs, nil
}
