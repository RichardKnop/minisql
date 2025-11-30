package minisql

import (
	"context"
	"fmt"
	"net"
	"sync"

	"go.uber.org/zap"
)

type ConnectionID uint64

type Connection struct {
	ID          ConnectionID
	tcpConn     net.Conn
	db          *Database
	transaction *Transaction
	// TODO - do we need a lock here?
	// Within a single connection, operations are sequential, but multiple goroutines
	// could be accessing the connection concurrently. Let's revisit this later.
	mu sync.RWMutex
}

func (d *Database) NewConnection(id ConnectionID, tcpConn net.Conn) *Connection {
	return &Connection{
		ID:      id,
		db:      d,
		tcpConn: tcpConn,
	}
}

func (c *Connection) Close() error {
	return c.tcpConn.Close()
}

func (c *Connection) SetTransaction(tx *Transaction) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.transaction = tx
}

func (c *Connection) TcpConn() net.Conn {
	return c.tcpConn
}

func (c *Connection) HasActiveTransaction() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.transaction != nil
}

func (c *Connection) TransactionContext(ctx context.Context) context.Context {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.transaction != nil {
		return WithTransaction(ctx, c.transaction)
	}
	return ctx
}

// Clean up any active transaction on disconnect
func (c *Connection) Cleanup(ctx context.Context) {
	if !c.HasActiveTransaction() {
		return
	}

	c.db.logger.Warn("connection closed with active transaction, rolling back",
		zap.Uint64("id", uint64(c.ID)))

	c.mu.Lock()
	defer c.mu.Unlock()

	c.db.txManager.RollbackTransaction(ctx, c.transaction)
	c.transaction = nil
}

func (c *Connection) ExecuteStatements(ctx context.Context, statements ...Statement) ([]StatementResult, error) {
	var results []StatementResult

	for _, stmt := range statements {
		// Add a check here to prevent user queries on system schema table.
		// We only allow internal system operations to access it.
		if stmt.TableName == SchemaTableName {
			return nil, fmt.Errorf("user queries on system schema table are not allowed")
		}
		if c.HasActiveTransaction() {
			switch stmt.Kind {
			case BeginTransaction:
				return results, fmt.Errorf("transaction already active on this connection")
			case CommitTransaction:
				if err := c.db.txManager.CommitTransaction(ctx, c.transaction, c.db.saver); err != nil {
					return results, err
				}
				c.SetTransaction(nil)

				results = append(results, StatementResult{})
			case RollbackTransaction:
				c.db.txManager.RollbackTransaction(ctx, c.transaction)
				c.SetTransaction(nil)
				results = append(results, StatementResult{})
			default:
				aResult, err := c.db.executeStatement(c.TransactionContext(ctx), stmt)
				if err != nil {
					return nil, err
				}
				results = append(results, aResult)
			}
		} else {
			// BEGIN, COMMIT, ROLLBACK outside transaction
			switch stmt.Kind {
			case BeginTransaction, CommitTransaction, RollbackTransaction:
				if stmt.Kind != BeginTransaction {
					return results, fmt.Errorf("no active transaction on this connection")
				}
				c.SetTransaction(c.db.txManager.BeginTransaction(ctx))
				results = append(results, StatementResult{})
				continue
			}
			// Everything wrap in a single statement transaction
			if err := c.db.txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
				aResult, err := c.db.executeStatement(ctx, stmt)
				if err != nil {
					return err
				}
				results = append(results, aResult)
				return nil
			}, c.db.saver); err != nil {
				return results, err
			}
		}
	}

	return results, nil
}
