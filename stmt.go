package minisql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"time"
	"unsafe"

	"github.com/RichardKnop/minisql/internal/minisql"
)

// Stmt is a database/sql/driver.Stmt implementation for a prepared SQL
// statement. It holds a pre-parsed statement and binds arguments at execution
// time via ExecContext or QueryContext.
type Stmt struct {
	conn      *Conn
	query     string
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
	return nil, fmt.Errorf("Exec without context is not supported; use ExecContext instead")
}

// ExecContext executes the prepared statement with the given arguments.
// It binds placeholder values, runs the statement, and returns the affected
// row count and last-insert-id wrapped in a Result.
func (s Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (result driver.Result, err error) {
	start := time.Now()
	defer func() {
		s.conn.logSlowQuery(s.query, time.Since(start), err)
	}()

	internalArgs, err := toInternalArgs(args)
	if err != nil {
		return nil, err
	}

	stmtWithArgs, err := s.statement.BindArguments(internalArgs...)
	if err != nil {
		return nil, err
	}

	stmtResult, err := s.conn.executeStatement(ctx, stmtWithArgs)
	if err != nil {
		return nil, err
	}

	return Result{rowsAffected: int64(stmtResult.RowsAffected), lastInsertID: stmtResult.LastInsertID}, nil
}

// Query executes a query that may return rows, such as a
// SELECT.
//
// Deprecated: Drivers should implement StmtQueryContext instead (or additionally).
func (s Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, fmt.Errorf("Query without context is not supported; use QueryContext instead")
}

// QueryContext executes the prepared query with the given arguments and returns
// the result rows. A read-only snapshot transaction is opened automatically for
// SELECT statements and committed when the returned Rows is closed.
func (s Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (rows driver.Rows, err error) {
	start := time.Now()
	defer func() {
		s.conn.logSlowQuery(s.query, time.Since(start), err)
	}()

	internalArgs, err := toInternalArgs(args)
	if err != nil {
		return nil, err
	}

	stmtWithArgs, err := s.statement.BindArguments(internalArgs...)
	if err != nil {
		return nil, err
	}

	result, rowsCtx, readTx, err := s.conn.executeQueryStatement(ctx, stmtWithArgs)
	if err != nil {
		return nil, err
	}
	useRowViews := len(result.RowViewFieldIndexes) > 0

	return &Rows{
		columns:             result.Columns,
		iter:                result.Rows,
		rowViewIter:         result.RowViews,
		rowViewPager:        result.RowViewPager,
		rowViewFieldIndexes: result.RowViewFieldIndexes,
		useRowViews:         useRowViews,
		ctx:                 rowsCtx,
		txManager:           s.conn.db.GetTransactionManager(),
		tx:                  readTx,
	}, nil
}

func toInternalArgs(args []driver.NamedValue) ([]any, error) {
	internalArgs := make([]any, 0, len(args))
	// Supported argument types: int64, float64, bool, []byte, string, time.Time
	for _, arg := range args {
		switch v := arg.Value.(type) {
		case nil:
			internalArgs = append(internalArgs, nil)
		case int64, float64, bool:
			internalArgs = append(internalArgs, v)
		case []float32:
			internalArgs = append(internalArgs, minisql.VectorPointer{Dims: uint32(len(v)), Data: v})
		case string:
			// Reuse the string's backing bytes without copying. The TextPointer is
			// valid only for the duration of this Exec/Query call: `args` (and thus
			// the underlying string data) is kept alive by the caller's stack frame,
			// and the TextPointer is consumed before ExecContext/QueryContext returns.
			b := unsafe.Slice(unsafe.StringData(v), len(v))
			internalArgs = append(internalArgs, minisql.NewTextPointer(b))
		case time.Time:
			t := minisql.Time{
				Year:         int32(v.Year()),
				Month:        int8(v.Month()),
				Day:          int8(v.Day()),
				Hour:         int8(v.Hour()),
				Minutes:      int8(v.Minute()),
				Seconds:      int8(v.Second()),
				Microseconds: int32(v.Nanosecond() / 1000),
			}
			internalArgs = append(internalArgs, minisql.TimestampMicros(t.TotalMicroseconds()))
		default:
			return nil, fmt.Errorf("unsupported argument type: %T", arg)
		}
	}
	return internalArgs, nil
}
