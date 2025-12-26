package minisql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"sync"

	"github.com/RichardKnop/minisql/internal/minisql"
	"github.com/RichardKnop/minisql/internal/parser"
	"github.com/RichardKnop/minisql/internal/pkg/logging"
	"go.uber.org/zap"
)

const (
	driverName = "minisql"
)

func init() {
	sql.Register(driverName, &Driver{})
}

// Driver implements the database/sql/driver.Driver interface.
type Driver struct {
	mu        sync.Mutex
	databases map[string]*minisql.Database
	parser    minisql.Parser
	logger    *zap.Logger
}

// Open returns a new connection to the database.
// The name is a path to a database file.
func (d *Driver) Open(name string) (driver.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.databases == nil {
		d.databases = make(map[string]*minisql.Database)
	}

	// Initialize logger if not set
	if d.logger == nil {
		config := logging.DefaultConfig()
		config.Level.SetLevel(zap.WarnLevel) // Set to warn by default for driver
		logger, err := config.Build()
		if err != nil {
			return nil, fmt.Errorf("failed to create logger: %w", err)
		}
		d.logger = logger
	}

	if d.parser == nil {
		d.parser = parser.New()
	}

	// Check if database is already open
	db, exists := d.databases[name]
	if !exists {
		dbFile, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return nil, fmt.Errorf("failed to open database file: %w", err)
		}

		// Create new database instance
		pager, err := minisql.NewPager(dbFile, minisql.PageSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create pager: %w", err)
		}

		db, err = minisql.NewDatabase(
			context.Background(),
			d.logger,
			name,
			d.parser,
			pager,
			pager,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to open database: %w", err)
		}

		d.databases[name] = db
	}

	return &Conn{
		db:     db,
		parser: d.parser,
		logger: d.logger,
	}, nil
}

// Conn implements the database/sql/driver.Conn interface.
type Conn struct {
	db          *minisql.Database
	parser      minisql.Parser
	transaction *minisql.Transaction
	logger      *zap.Logger
	mu          sync.RWMutex
}

func (c *Conn) Ping(ctx context.Context) error {
	// TODO - implement a real ping?
	return nil
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
//
// Drivers must ensure all network calls made by Close
// do not block indefinitely (e.g. apply a timeout).
func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Rollback any active transaction
	if c.transaction != nil {
		// Transaction cleanup is handled internally
		c.transaction = nil
	}

	return c.db.Close()
}

// Prepare returns a prepared statement, bound to this connection.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext returns a prepared statement, bound to this connection.
// context is for the preparation of the statement,
// it must not store the context within the statement itself.
func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	statements, err := c.parser.Parse(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	if len(statements) == 0 {
		return nil, fmt.Errorf("no statements in query")
	}

	// For simplicity, we'll handle single statements
	// Multi-statement queries could be supported later
	if len(statements) > 1 {
		return nil, fmt.Errorf("multiple statements not supported in prepared statements")
	}

	return &Stmt{
		conn:      c,
		statement: statements[0],
	}, nil
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts and returns a new transaction.
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.transaction != nil {
		return nil, fmt.Errorf("transaction already in progress")
	}

	// Create a new transaction using the transaction manager
	c.transaction = c.db.GetTransactionManager().BeginTransaction(ctx)

	return &Tx{
		conn: c,
		tx:   c.transaction,
		ctx:  ctx,
	}, nil
}

// ExecContext executes a query that doesn't return rows.
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if len(args) > 0 {
		return nil, fmt.Errorf("query arguments not yet supported")
	}

	statements, err := c.parser.Parse(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	if len(statements) == 0 {
		return nil, nil
	}

	var totalRowsAffected int64

	for _, stmt := range statements {
		result, err := c.executeStatement(ctx, stmt)
		if err != nil {
			return nil, err
		}
		totalRowsAffected += int64(result.RowsAffected)
	}

	return Result{rowsAffected: totalRowsAffected}, nil
}

// QueryContext executes a query that may return rows.
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) > 0 {
		return nil, fmt.Errorf("query arguments not yet supported")
	}

	statements, err := c.parser.Parse(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	if len(statements) == 0 {
		return nil, fmt.Errorf("no statements in query")
	}

	if len(statements) > 1 {
		return nil, fmt.Errorf("multiple statements not supported")
	}

	stmt := statements[0]
	result, err := c.executeStatement(ctx, stmt)
	if err != nil {
		return nil, err
	}

	return &Rows{
		columns: result.Columns,
		iter:    result.Rows,
		ctx:     ctx,
	}, nil
}

func (c *Conn) SetTransaction(tx *minisql.Transaction) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.transaction = tx
}

func (c *Conn) HasActiveTransaction() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.transaction != nil
}

func (c *Conn) TransactionContext(ctx context.Context) context.Context {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.transaction != nil {
		return minisql.WithTransaction(ctx, c.transaction)
	}
	return ctx
}

func (c *Conn) executeStatement(ctx context.Context, stmt minisql.Statement) (minisql.StatementResult, error) {
	if c.HasActiveTransaction() {
		ctx = minisql.WithTransaction(ctx, c.transaction)
		return c.db.ExecuteStatement(ctx, stmt)
	}

	// Execute in auto-commit transaction
	var result minisql.StatementResult
	err := c.db.GetTransactionManager().ExecuteInTransaction(ctx, func(txCtx context.Context) error {
		var err error
		result, err = c.db.ExecuteStatement(txCtx, stmt)
		return err
	}, minisql.TxCommitter{Saver: c.db.GetSaver(), DDLSaver: c.db.GetDDLSaver()})

	return result, err
}

// Ensure interfaces are implemented
var _ driver.Driver = (*Driver)(nil)
var _ driver.Conn = (*Conn)(nil)
var _ driver.ConnPrepareContext = (*Conn)(nil)
var _ driver.ConnBeginTx = (*Conn)(nil)
var _ driver.ExecerContext = (*Conn)(nil)
var _ driver.QueryerContext = (*Conn)(nil)
