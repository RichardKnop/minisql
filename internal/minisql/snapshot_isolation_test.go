package minisql

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// snapshotColumns are a minimal column set used by all snapshot isolation tests.
var snapshotColumns = []Column{
	{Kind: Int8, Size: 8, Name: "id"},
	{Kind: Varchar, Size: MaxInlineVarchar, Name: "name", Nullable: true},
}

// insertSnapshotRow inserts a single row (id, name) into table inside a new transaction.
func insertSnapshotRow(
	ctx context.Context,
	t *testing.T,
	table *Table,
	txManager *TransactionManager,
	id int64,
	name string,
) {
	t.Helper()
	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := table.Insert(ctx, Statement{
			Kind:      Insert,
			TableName: table.Name,
			Columns:   snapshotColumns,
			Fields:    fieldsFromColumns(snapshotColumns...),
			Inserts: [][]OptionalValue{
				{
					{Value: id, Valid: true},
					{Value: NewTextPointer([]byte(name)), Valid: true},
				},
			},
		})
		return err
	})
	require.NoError(t, err)
}

// selectAllSnapshot executes a full-table SELECT within the given context (which must
// carry a transaction) and returns all rows.
func selectAllSnapshot(
	ctx context.Context,
	t *testing.T,
	table *Table,
) []Row {
	t.Helper()
	stmt := Statement{
		Kind:      Select,
		TableName: table.Name,
		Columns:   snapshotColumns,
		Fields:    fieldsFromColumns(snapshotColumns...),
	}
	result, err := table.Select(ctx, stmt)
	require.NoError(t, err)
	var rows []Row
	for result.Rows.Next(ctx) {
		rows = append(rows, result.Rows.Row())
	}
	require.NoError(t, result.Rows.Err())
	return rows
}

// TestSnapshotIsolation_ReadDoesNotSeeWriteAfterStart verifies the primary
// invariant: a read-only transaction started at snapshot seq N cannot observe
// a write that commits at seq N+1.
func TestSnapshotIsolation_ReadDoesNotSeeWriteAfterStart(t *testing.T) {
	table, txManager, _ := newTestTable(t, snapshotColumns)
	ctx := context.Background()

	// Pre-populate one row so the table has at least one leaf page.
	insertSnapshotRow(ctx, t, table, txManager, 1, "alice")

	// Start a read-only transaction (snapshot seq = 1 after the first insert).
	readTx := txManager.BeginReadOnlyTransaction(ctx)
	readCtx := WithTransaction(ctx, readTx)

	snapshotSeq := readTx.SnapshotSeq
	assert.Equal(t, uint64(1), txManager.commitSeq, "expected one commit so far")
	assert.Equal(t, uint64(1), snapshotSeq)

	// Write a second row AFTER the read transaction started.
	insertSnapshotRow(ctx, t, table, txManager, 2, "bob")
	assert.Equal(t, uint64(2), txManager.commitSeq, "commit seq must advance after write")

	// The read transaction should see only "alice" (the row that existed at snapshot time).
	rows := selectAllSnapshot(readCtx, t, table)
	assert.Len(t, rows, 1, "snapshot reader must not see write committed after it started")
	if len(rows) == 1 {
		assert.Equal(t, int64(1), rows[0].Values[0].Value)
	}

	// Commit the read transaction — must always succeed.
	err := txManager.CommitTransaction(ctx, readTx)
	require.NoError(t, err)

	// A new read transaction started after the write must see both rows.
	newReadTx := txManager.BeginReadOnlyTransaction(ctx)
	newReadCtx := WithTransaction(ctx, newReadTx)
	assert.Equal(t, uint64(2), newReadTx.SnapshotSeq)

	rows = selectAllSnapshot(newReadCtx, t, table)
	assert.Len(t, rows, 2, "new snapshot reader must see all committed rows")

	require.NoError(t, txManager.CommitTransaction(ctx, newReadTx))
}

// TestSnapshotIsolation_MultipleSnapshots verifies that three concurrent
// readers each see only the rows committed before their respective snapshots.
func TestSnapshotIsolation_MultipleSnapshots(t *testing.T) {
	table, txManager, _ := newTestTable(t, snapshotColumns)
	ctx := context.Background()

	insertSnapshotRow(ctx, t, table, txManager, 1, "alice") // commitSeq=1

	// Reader A starts after 1 commit — snapshot seq 1.
	readerA := txManager.BeginReadOnlyTransaction(ctx)
	ctxA := WithTransaction(ctx, readerA)
	assert.Equal(t, uint64(1), readerA.SnapshotSeq)

	insertSnapshotRow(ctx, t, table, txManager, 2, "bob") // commitSeq=2

	// Reader B starts after 2 commits — snapshot seq 2.
	readerB := txManager.BeginReadOnlyTransaction(ctx)
	ctxB := WithTransaction(ctx, readerB)
	assert.Equal(t, uint64(2), readerB.SnapshotSeq)

	insertSnapshotRow(ctx, t, table, txManager, 3, "carol") // commitSeq=3

	// Reader C starts after 3 commits — snapshot seq 3.
	readerC := txManager.BeginReadOnlyTransaction(ctx)
	ctxC := WithTransaction(ctx, readerC)
	assert.Equal(t, uint64(3), readerC.SnapshotSeq)

	// Verify each reader sees only the rows committed at or before its snapshot.
	rowsA := selectAllSnapshot(ctxA, t, table)
	assert.Len(t, rowsA, 1, "reader A: only alice")

	rowsB := selectAllSnapshot(ctxB, t, table)
	assert.Len(t, rowsB, 2, "reader B: alice + bob")

	rowsC := selectAllSnapshot(ctxC, t, table)
	assert.Len(t, rowsC, 3, "reader C: alice + bob + carol")

	// Commit all three in reverse order — all must succeed without conflict.
	require.NoError(t, txManager.CommitTransaction(ctx, readerC))
	require.NoError(t, txManager.CommitTransaction(ctx, readerB))
	require.NoError(t, txManager.CommitTransaction(ctx, readerA))
}

// TestSnapshotIsolation_VersionHistoryGC verifies that page version history is
// freed once all snapshot readers have committed.
func TestSnapshotIsolation_VersionHistoryGC(t *testing.T) {
	table, txManager, _ := newTestTable(t, snapshotColumns)
	ctx := context.Background()

	insertSnapshotRow(ctx, t, table, txManager, 1, "alice") // commitSeq=1

	readTx := txManager.BeginReadOnlyTransaction(ctx)

	// Insert a second row while the read transaction is open.  This should
	// populate the version history for the modified leaf page.
	insertSnapshotRow(ctx, t, table, txManager, 2, "bob") // commitSeq=2

	txManager.mu.RLock()
	historySize := len(txManager.pageVersionHistory)
	txManager.mu.RUnlock()
	assert.Greater(t, historySize, 0, "version history must be populated while reader is active")

	// After the read transaction commits, version history must be cleared.
	require.NoError(t, txManager.CommitTransaction(ctx, readTx))

	txManager.mu.RLock()
	historySize = len(txManager.pageVersionHistory)
	txManager.mu.RUnlock()
	assert.Equal(t, 0, historySize, "version history must be empty after all readers commit")
}

// TestSnapshotIsolation_CheckpointBlockedByReader verifies that CheckpointWAL
// returns ErrCheckpointBlockedByReaders while a snapshot reader is active and
// succeeds once the reader has committed.
func TestSnapshotIsolation_CheckpointBlockedByReader(t *testing.T) {
	env := newWALCommitEnv(t)
	ctx := context.Background()

	// Commit a write so there is something to checkpoint.
	writeTx, err := env.txManager.BeginTransaction(ctx)
	require.NoError(t, err)
	writeTx.WriteSet[2] = WriteInfo{
		Page:  &Page{Index: PageIndex(2), LeafNode: NewLeafNode()},
		Table: "t",
	}
	env.saverMock.On("SavePage", ctx, PageIndex(2), writeTx.WriteSet[2].Page).Return(nil).Once()
	require.NoError(t, env.txManager.CommitTransaction(ctx, writeTx))

	// Open a snapshot reader.
	readTx := env.txManager.BeginReadOnlyTransaction(ctx)
	assert.True(t, env.txManager.hasActiveSnapshotReadersLocked(), "must have active reader")

	// Checkpoint must be blocked.
	checkErr := env.txManager.CheckpointWAL(env.dbFile)
	assert.ErrorIs(t, checkErr, ErrCheckpointBlockedByReaders)

	// After the reader commits, checkpoint must succeed.
	require.NoError(t, env.txManager.CommitTransaction(ctx, readTx))

	checkErr = env.txManager.CheckpointWAL(env.dbFile)
	require.NoError(t, checkErr)

	env.saverMock.AssertExpectations(t)
}

// TestSnapshotIsolation_ConcurrentReadWrite verifies that concurrent reads and
// writes do not race. Run with -race to detect data races.
func TestSnapshotIsolation_ConcurrentReadWrite(t *testing.T) {
	table, txManager, _ := newTestTable(t, snapshotColumns)
	ctx := context.Background()

	// Seed the table.
	insertSnapshotRow(ctx, t, table, txManager, 1, "seed")

	const (
		readers = 4
		writers = 4
		rounds  = 10
	)

	var wg sync.WaitGroup
	errs := make(chan error, readers*rounds+writers*rounds)

	// Concurrent readers.
	for r := 0; r < readers; r++ {
		wg.Go(func() {
			for i := 0; i < rounds; i++ {
				err := txManager.ExecuteReadOnlyTransaction(ctx, func(rCtx context.Context) error {
					selectAllSnapshot(rCtx, t, table)
					return nil
				})
				if err != nil {
					errs <- err
				}
			}
		})
	}

	// Concurrent writers. We allow ErrConcurrentWriter (only one writer allowed)
	// but not any other error.
	var (
		nextID = int64(100)
		idMu   sync.Mutex
	)
	for w := 0; w < writers; w++ {
		wg.Go(func() {
			for i := 0; i < rounds; i++ {
				idMu.Lock()
				id := nextID
				nextID += 1
				idMu.Unlock()

				err := txManager.ExecuteInTransaction(ctx, func(wCtx context.Context) error {
					_, err := table.Insert(wCtx, Statement{
						Kind:      Insert,
						TableName: table.Name,
						Columns:   snapshotColumns,
						Fields:    fieldsFromColumns(snapshotColumns...),
						Inserts: [][]OptionalValue{
							{
								{Value: id, Valid: true},
								{Value: NewTextPointer([]byte("concurrent")), Valid: true},
							},
						},
					})
					return err
				})
				if err != nil && !errors.Is(err, ErrConcurrentWriter) {
					errs <- err
				}
			}
		})
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
}

// TestSnapshotIsolation_SnapshotSeqMonotonic verifies that SnapshotSeq values
// are monotonically non-decreasing: a transaction started later never gets a
// smaller snapshot seq than one started earlier.
func TestSnapshotIsolation_SnapshotSeqMonotonic(t *testing.T) {
	_, txManager, _ := newTestTable(t, snapshotColumns)
	ctx := context.Background()

	const n = 20
	txs := make([]*Transaction, n)
	for i := range txs {
		txs[i] = txManager.BeginReadOnlyTransaction(ctx)
	}

	for i := 1; i < n; i++ {
		assert.GreaterOrEqual(t, txs[i].SnapshotSeq, txs[i-1].SnapshotSeq,
			"snapshot seq must be non-decreasing")
	}

	for _, tx := range txs {
		require.NoError(t, txManager.CommitTransaction(ctx, tx))
	}
}

// TestSnapshotIsolation_minActiveSnapshotSeqLocked verifies the helper logic
// directly, including the MaxUint64 sentinel when no readers are active.
func TestSnapshotIsolation_minActiveSnapshotSeqLocked(t *testing.T) {
	_, txManager, _ := newTestTable(t, snapshotColumns)
	ctx := context.Background()

	// No readers: must return MaxUint64.
	txManager.mu.Lock()
	got := txManager.minActiveSnapshotSeqLocked()
	txManager.mu.Unlock()
	assert.Equal(t, uint64(math.MaxUint64), got)

	// One reader at seq 0.
	tx1 := txManager.BeginReadOnlyTransaction(ctx)
	assert.Equal(t, uint64(0), tx1.SnapshotSeq)

	txManager.mu.Lock()
	got = txManager.minActiveSnapshotSeqLocked()
	txManager.mu.Unlock()
	assert.Equal(t, uint64(0), got)

	// Second reader at seq 0 (no writes in between).
	tx2 := txManager.BeginReadOnlyTransaction(ctx)
	assert.Equal(t, uint64(0), tx2.SnapshotSeq)

	txManager.mu.Lock()
	got = txManager.minActiveSnapshotSeqLocked()
	txManager.mu.Unlock()
	assert.Equal(t, uint64(0), got)

	// Commit tx1; min is still 0 (tx2 is still open at seq 0).
	require.NoError(t, txManager.CommitTransaction(ctx, tx1))
	txManager.mu.Lock()
	got = txManager.minActiveSnapshotSeqLocked()
	txManager.mu.Unlock()
	assert.Equal(t, uint64(0), got)

	// Commit tx2; no readers → MaxUint64.
	require.NoError(t, txManager.CommitTransaction(ctx, tx2))
	txManager.mu.Lock()
	got = txManager.minActiveSnapshotSeqLocked()
	txManager.mu.Unlock()
	assert.Equal(t, uint64(math.MaxUint64), got)
}
