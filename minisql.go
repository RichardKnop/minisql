package minisql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"sync"
	"time"

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
	mu     sync.Mutex
	parser minisql.Parser
	logger *zap.Logger
}

// Open returns a new connection to the database.
// The name is a connection string with optional parameters:
//   - "./my.db" - simple path
//   - "./my.db?wal_checkpoint_threshold=500" - auto-checkpoint after 500 WAL frames
//   - "./my.db?log_level=debug" - enable debug logging
//   - "./my.db?wal_checkpoint_threshold=500&log_level=info" - multiple parameters
func (d *Driver) Open(name string) (driver.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Parse connection string
	config, err := ParseConnectionString(name)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	// Initialize logger if not set
	if d.logger == nil {
		logConfig := logging.DefaultConfig()
		logConfig.Level = config.GetZapLevel()
		logger, err := logConfig.Build()
		if err != nil {
			return nil, fmt.Errorf("failed to create logger: %w", err)
		}
		d.logger = logger
	}

	if d.parser == nil {
		d.parser = parser.New()
	}

	db, err := d.newDB(config)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	return &Conn{
		db:                 db,
		parser:             d.parser,
		logger:             d.logger,
		slowQueryThreshold: config.SlowQueryThreshold,
	}, nil
}

func (d *Driver) newDB(config *ConnectionConfig) (*minisql.Database, error) {
	// Open or create database file
	dbFile, err := os.OpenFile(config.FilePath, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("failed to open database file: %w", err)
	}

	pager, err := minisql.NewPager(dbFile, minisql.PageSize, config.MaxCachedPages)
	if err != nil {
		_ = dbFile.Close()
		return nil, fmt.Errorf("failed to create pager: %w", err)
	}

	walIndex := minisql.NewWALIndex()
	wal, recovered, err := minisql.OpenWALAndRebuildIndex(config.FilePath, minisql.PageSize, walIndex)
	if err != nil {
		_ = dbFile.Close()
		return nil, fmt.Errorf("failed to initialise WAL: %w", err)
	}

	if recovered {
		d.logger.Warn("WAL replay: uncommitted-to-disk frames found and replayed into WAL index",
			zap.String("db_path", config.FilePath),
			zap.Int("frames_in_index", walIndex.Size()))
	}

	dbOpts := []minisql.DatabaseOption{}
	if config.ParallelScan {
		dbOpts = append(dbOpts, minisql.WithParallelScanEnabled())
	}

	return minisql.NewDatabase(
		context.Background(),
		d.logger,
		config.FilePath,
		d.parser,
		pager,
		pager,
		&minisql.WALConfig{
			WAL:                 wal,
			Index:               walIndex,
			DBFile:              dbFile,
			CheckpointThreshold: config.WALCheckpointThreshold,
			WALWriteBufferSize:  config.WALWriteBufferSize,
			Synchronous:         config.Synchronous,
		},
		dbOpts...,
	)
}

// Conn implements the database/sql/driver.Conn interface.
type Conn struct {
	db                 *minisql.Database
	parser             minisql.Parser
	transaction        *minisql.Transaction
	logger             *zap.Logger
	mu                 sync.RWMutex
	slowQueryThreshold time.Duration
}

// Ping ...
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
	statement, err := c.db.PrepareStatement(ctx, query)
	if err != nil {
		return nil, err
	}

	return &Stmt{
		conn:      c,
		query:     query,
		statement: statement,
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
		ctx:  minisql.WithTransaction(ctx, c.transaction),
	}, nil
}

// ExecContext executes a query that doesn't return rows.
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (result driver.Result, err error) {
	start := time.Now()
	defer func() {
		c.logSlowQuery(query, time.Since(start), err)
	}()

	statements, err := c.parser.Parse(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	if len(statements) == 0 {
		return nil, fmt.Errorf("no statements in query")
	}

	internalArgs, err := toInternalArgs(args)
	if err != nil {
		return nil, err
	}

	var totalRowsAffected int64
	var lastInsertID int64

	for _, stmt := range statements {
		if len(internalArgs) > 0 {
			stmt, err = stmt.BindArguments(internalArgs...)
			if err != nil {
				return nil, err
			}
		}
		result, err := c.executeStatement(ctx, stmt)
		if err != nil {
			return nil, err
		}
		totalRowsAffected += int64(result.RowsAffected)
		if result.LastInsertID != 0 {
			lastInsertID = result.LastInsertID
		}
	}

	return Result{rowsAffected: totalRowsAffected, lastInsertID: lastInsertID}, nil
}

// QueryContext executes a query that may return rows.
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (rows driver.Rows, err error) {
	start := time.Now()
	defer func() {
		c.logSlowQuery(query, time.Since(start), err)
	}()

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

	internalArgs, err := toInternalArgs(args)
	if err != nil {
		return nil, err
	}

	stmt := statements[0]
	if len(internalArgs) > 0 {
		stmt, err = stmt.BindArguments(internalArgs...)
		if err != nil {
			return nil, err
		}
	}

	result, rowsCtx, readTx, err := c.executeQueryStatement(ctx, stmt)
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
		txManager:           c.db.GetTransactionManager(),
		tx:                  readTx,
	}, nil
}

// SetTransaction ...
func (c *Conn) SetTransaction(tx *minisql.Transaction) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.transaction = tx
}

// HasActiveTransaction ...
func (c *Conn) HasActiveTransaction() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.transaction != nil
}

// TransactionContext ...
func (c *Conn) TransactionContext(ctx context.Context) context.Context {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.transaction != nil {
		return minisql.WithTransaction(ctx, c.transaction)
	}
	return ctx
}

func (c *Conn) executeQueryStatement(ctx context.Context, stmt minisql.Statement) (minisql.StatementResult, context.Context, *minisql.Transaction, error) {
	if c.HasActiveTransaction() || (stmt.Kind != minisql.Select && stmt.Kind != minisql.Explain) {
		result, err := c.executeStatement(ctx, stmt)
		return result, ctx, nil, err
	}

	txManager := c.db.GetTransactionManager()
	tx := txManager.BeginReadOnlyTransaction(ctx)
	txCtx := minisql.WithTransaction(ctx, tx)

	result, err := c.db.ExecuteStatement(txCtx, stmt)
	if err != nil {
		txManager.RollbackTransaction(txCtx, tx)
		return minisql.StatementResult{}, ctx, nil, err
	}

	if len(result.RowViewFieldIndexes) > 0 {
		return result, txCtx, tx, nil
	}

	if err := txManager.CommitTransaction(txCtx, tx); err != nil {
		txManager.RollbackTransaction(txCtx, tx)
		return minisql.StatementResult{}, ctx, nil, err
	}

	return result, ctx, nil, nil
}

func (c *Conn) executeStatement(ctx context.Context, stmt minisql.Statement) (minisql.StatementResult, error) {
	if c.HasActiveTransaction() {
		ctx = minisql.WithTransaction(ctx, c.transaction)
		return c.db.ExecuteStatement(ctx, stmt)
	}

	// Execute in auto-commit transaction.  Use a read-only transaction for
	// SELECT statements so that per-page read tracking is skipped entirely,
	// eliminating per-page map writes and mutex acquisitions.
	var result minisql.StatementResult
	txFn := func(txCtx context.Context) error {
		var err error
		result, err = c.db.ExecuteStatement(txCtx, stmt)
		return err
	}
	var err error
	if stmt.Kind == minisql.Select || stmt.Kind == minisql.Explain {
		err = c.db.GetTransactionManager().ExecuteReadOnlyTransaction(ctx, txFn)
	} else {
		err = c.db.GetTransactionManager().ExecuteInTransaction(ctx, txFn)
	}

	return result, err
}

func (c *Conn) logSlowQuery(query string, elapsed time.Duration, err error) {
	if c.slowQueryThreshold <= 0 || elapsed < c.slowQueryThreshold || c.logger == nil {
		return
	}

	fields := []zap.Field{
		zap.String("query", query),
		zap.Duration("duration", elapsed),
		zap.Duration("threshold", c.slowQueryThreshold),
	}
	if err != nil {
		fields = append(fields, zap.Error(err))
	}
	c.logger.Warn("slow query", fields...)
}

// Ensure interfaces are implemented
var (
	_ driver.Driver             = (*Driver)(nil)
	_ driver.Conn               = (*Conn)(nil)
	_ driver.ConnPrepareContext = (*Conn)(nil)
	_ driver.ConnBeginTx        = (*Conn)(nil)
	_ driver.ExecerContext      = (*Conn)(nil)
	_ driver.QueryerContext     = (*Conn)(nil)
)
