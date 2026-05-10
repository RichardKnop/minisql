package minisql

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	minisqlErrors "github.com/RichardKnop/minisql/pkg/errors"
)

func TestTransactionManager_Commit(t *testing.T) {
	t.Parallel()

	t.Run("Read only transaction", func(t *testing.T) {
		var (
			ctx       = context.Background()
			saverMock = new(MockPageSaver)
			txManager = NewTransactionManager(zap.NewNop(), testDBName, nil, saverMock, nil)
		)

		// Setup initial global versions
		txManager.globalDBHeaderVersion = 4
		txManager.globalPageVersions[2] = 0
		txManager.globalPageVersions[3] = 2
		txManager.globalPageVersions[4] = 1

		tx := txManager.BeginTransaction(ctx)
		assert.Equal(t, TxActive, tx.Status)

		// Let's simulate some reads but no writes
		tx.DBHeaderRead = new(uint64)
		*tx.DBHeaderRead = 4
		tx.ReadSet = map[PageIndex]uint64{2: 0, 3: 2, 4: 1}

		err := txManager.CommitTransaction(ctx, tx)
		require.NoError(t, err)

		// Global versions should remain unchanged
		assert.Equal(t, 4, int(txManager.globalDBHeaderVersion))
		assert.Equal(t, 0, int(txManager.globalPageVersions[2]))
		assert.Equal(t, 2, int(txManager.globalPageVersions[3]))
		assert.Equal(t, 1, int(txManager.globalPageVersions[4]))

		mock.AssertExpectationsForObjects(t, saverMock)
	})

	t.Run("Write transaction", func(t *testing.T) {
		var (
			ctx       = context.Background()
			pagerMock = new(MockPager)
			saverMock = new(MockPageSaver)
			txManager = NewTransactionManager(zap.NewNop(), testDBName, mockPagerFactory(pagerMock), saverMock, nil)
		)

		// Setup initial global versions
		txManager.globalDBHeaderVersion = 3
		txManager.globalPageVersions[2] = 0
		txManager.globalPageVersions[3] = 2
		txManager.globalPageVersions[4] = 5

		tx := txManager.BeginTransaction(ctx)
		assert.Equal(t, TxActive, tx.Status)

		// Let's simulate some reads and writes
		tx.DBHeaderRead = new(uint64)
		*tx.DBHeaderRead = 3
		tx.DBHeaderWrite = &DatabaseHeader{FirstFreePage: 2, FreePageCount: 10}
		tx.ReadSet = map[PageIndex]uint64{2: 0, 3: 2, 4: 5}
		tx.WriteSet[4] = WriteInfo{
			Page:  &Page{Index: PageIndex(4)},
			Table: "users",
			Index: "pk_users",
		}

		// Setup expectations
		saverMock.On("SavePage", ctx, PageIndex(4), tx.WriteSet[4].Page).Return(nil).Once()
		saverMock.On("SaveHeader", ctx, *tx.DBHeaderWrite).Return(nil).Once()
		saverMock.On("FlushBatch", ctx, mock.MatchedBy(func(pages []PageIndex) bool {
			// Should flush header (page 0) and modified page (page 4)
			return len(pages) == 2 &&
				((pages[0] == PageIndex(0) && pages[1] == PageIndex(4)) ||
					(pages[0] == PageIndex(4) && pages[1] == PageIndex(0)))
		})).Return(nil).Once()

		err := txManager.CommitTransaction(ctx, tx)
		require.NoError(t, err)

		// Global versions should be updated accordingly
		assert.Equal(t, 4, int(txManager.globalDBHeaderVersion))
		assert.Equal(t, 0, int(txManager.globalPageVersions[2]))
		assert.Equal(t, 2, int(txManager.globalPageVersions[3]))
		assert.Equal(t, 6, int(txManager.globalPageVersions[4]))

		mock.AssertExpectationsForObjects(t, pagerMock, saverMock)
	})

	t.Run("Read only transaction conflict", func(t *testing.T) {
		var (
			ctx       = context.Background()
			pagerMock = new(MockPager)
			saverMock = new(MockPageSaver)
			txManager = NewTransactionManager(zap.NewNop(), testDBName, mockPagerFactory(pagerMock), saverMock, nil)
		)

		// Setup initial global versions
		txManager.globalDBHeaderVersion = 4
		txManager.globalPageVersions[2] = 0
		txManager.globalPageVersions[3] = 2
		txManager.globalPageVersions[4] = 1

		readTx := txManager.BeginTransaction(ctx)
		assert.Equal(t, TxActive, readTx.Status)

		writeTx := txManager.BeginTransaction(ctx)
		assert.Equal(t, TxActive, writeTx.Status)

		// Let's simulate some reads for first tx
		readTx.DBHeaderRead = new(uint64)
		*readTx.DBHeaderRead = 4
		readTx.ReadSet = map[PageIndex]uint64{2: 0, 3: 2, 4: 1}

		// Let's simulate a write for second tx that will conflict
		writeTx.WriteSet[3] = WriteInfo{
			Page:  &Page{Index: PageIndex(3), LeafNode: NewLeafNode()},
			Table: "orders",
			Index: "pk_orders",
		}

		// Setup expectations
		saverMock.On("SavePage", ctx, PageIndex(3), writeTx.WriteSet[3].Page).Return(nil).Once()

		// Commit the writing transaction first
		saverMock.On("FlushBatch", ctx, mock.MatchedBy(func(pages []PageIndex) bool {
			// Should flush only the modified page (page 3)
			return len(pages) == 1 && pages[0] == PageIndex(3)
		})).Return(nil).Once()
		err := txManager.CommitTransaction(ctx, writeTx)
		require.NoError(t, err)

		// Now, committing the reading transaction should fail due to conflict
		err = txManager.CommitTransaction(ctx, readTx)
		require.Error(t, err)
		assert.ErrorIs(t, err, minisqlErrors.ErrTxConflict)
		assert.Equal(t, "transaction conflict detected: tx 1 aborted due to conflict on page 3", err.Error())

		// Writing transaction should have updated page version, no other changes expected
		assert.Equal(t, 4, int(txManager.globalDBHeaderVersion))
		assert.Equal(t, 0, int(txManager.globalPageVersions[2]))
		assert.Equal(t, 3, int(txManager.globalPageVersions[3]))
		assert.Equal(t, 1, int(txManager.globalPageVersions[4]))

		mock.AssertExpectationsForObjects(t, pagerMock, saverMock)
	})

	t.Run("Write transaction error", func(t *testing.T) {
		var (
			ctx       = context.Background()
			pagerMock = new(MockPager)
			saverMock = new(MockPageSaver)
			txManager = NewTransactionManager(zap.NewNop(), testDBName, mockPagerFactory(pagerMock), saverMock, nil)
		)

		// Setup initial global versions
		txManager.globalDBHeaderVersion = 3
		txManager.globalPageVersions[2] = 0
		txManager.globalPageVersions[3] = 2
		txManager.globalPageVersions[4] = 5

		writeTx1 := txManager.BeginTransaction(ctx)
		assert.Equal(t, TxActive, writeTx1.Status)

		writeTx2 := txManager.BeginTransaction(ctx)
		assert.Equal(t, TxActive, writeTx2.Status)

		// Let's simulate some reads and a write for first tx
		writeTx1.DBHeaderRead = new(uint64)
		*writeTx1.DBHeaderRead = 3
		writeTx1.DBHeaderWrite = &DatabaseHeader{FirstFreePage: 2, FreePageCount: 10}
		writeTx1.ReadSet = map[PageIndex]uint64{2: 0, 3: 2, 4: 5}
		writeTx1.WriteSet[4] = WriteInfo{
			Page:  &Page{Index: PageIndex(4)},
			Table: "orders",
			Index: "pk_orders",
		}

		// Second tx will modify the same page to cause conflict
		writeTx2.ReadSet = map[PageIndex]uint64{4: 5}
		writeTx2.WriteSet[4] = WriteInfo{
			Page:  &Page{Index: PageIndex(4)},
			Table: "orders",
			Index: "pk_orders",
		}

		// Setup expectations
		saverMock.On("SavePage", ctx, PageIndex(4), writeTx2.WriteSet[4].Page).Return(nil).Once()
		saverMock.On("FlushBatch", ctx, mock.MatchedBy(func(pages []PageIndex) bool {
			// Should flush only the modified page (page 4)
			return len(pages) == 1 && pages[0] == PageIndex(4)
		})).Return(nil).Once()

		// Commit the second transaction first
		err := txManager.CommitTransaction(ctx, writeTx2)
		require.NoError(t, err)

		// Now, committing the first transaction should fail due to conflict
		err = txManager.CommitTransaction(ctx, writeTx1)
		require.Error(t, err)
		assert.ErrorIs(t, err, minisqlErrors.ErrTxConflict)
		assert.Equal(t, "transaction conflict detected: tx 1 aborted due to conflict on page 4", err.Error())

		// Second transaction should have updated page version, no other changes expected
		assert.Equal(t, 3, int(txManager.globalDBHeaderVersion))
		assert.Equal(t, 0, int(txManager.globalPageVersions[2]))
		assert.Equal(t, 2, int(txManager.globalPageVersions[3]))
		assert.Equal(t, 6, int(txManager.globalPageVersions[4]))

		mock.AssertExpectationsForObjects(t, pagerMock, saverMock)
	})
}

func TestTransactionManager_Rollback(t *testing.T) {
	t.Parallel()

	var (
		ctx       = context.Background()
		saverMock = new(MockPageSaver)
		txManager = NewTransactionManager(zap.NewNop(), testDBName, mockPagerFactory(nil), saverMock, nil)
	)

	// Setup initial global versions
	txManager.globalDBHeaderVersion = 3
	txManager.globalPageVersions[2] = 0
	txManager.globalPageVersions[3] = 2
	txManager.globalPageVersions[4] = 5

	tx := txManager.BeginTransaction(ctx)
	assert.Equal(t, TxActive, tx.Status)

	// Let's simulate some reads and writes
	tx.DBHeaderRead = new(uint64)
	*tx.DBHeaderRead = 3
	tx.DBHeaderWrite = &DatabaseHeader{FirstFreePage: 2, FreePageCount: 10}
	tx.ReadSet = map[PageIndex]uint64{2: 0, 3: 2, 4: 5}
	tx.WriteSet[4] = WriteInfo{
		Page:  &Page{Index: PageIndex(4)},
		Table: "users",
	}

	txManager.RollbackTransaction(ctx, tx)
	assert.Equal(t, TxAborted, tx.Status)

	// Global versions should remain unchanged
	assert.Equal(t, 3, int(txManager.globalDBHeaderVersion))
	assert.Equal(t, 0, int(txManager.globalPageVersions[2]))
	assert.Equal(t, 2, int(txManager.globalPageVersions[3]))
	assert.Equal(t, 5, int(txManager.globalPageVersions[4]))

	mock.AssertExpectationsForObjects(t, saverMock)
}

func TestTransactionManager_WAL_Commit(t *testing.T) {
	t.Parallel()

	t.Run("Read only transaction", func(t *testing.T) {
		t.Parallel()

		env := newWALCommitEnv(t)
		ctx := context.Background()

		txManager := env.txManager
		txManager.globalDBHeaderVersion = 4
		txManager.globalPageVersions[2] = 0
		txManager.globalPageVersions[3] = 2

		tx := txManager.BeginTransaction(ctx)
		tx.DBHeaderRead = new(uint64)
		*tx.DBHeaderRead = 4
		tx.ReadSet = map[PageIndex]uint64{2: 0, 3: 2}

		err := txManager.CommitTransaction(ctx, tx)
		require.NoError(t, err)
		assert.Equal(t, TxCommitted, tx.Status)

		// No WAL frames should have been written.
		assert.Equal(t, int64(0), env.wal.FrameCount())
		// Global versions unchanged.
		assert.Equal(t, uint64(4), txManager.globalDBHeaderVersion)
		assert.Equal(t, uint64(0), txManager.globalPageVersions[2])
		assert.Equal(t, uint64(2), txManager.globalPageVersions[3])

		mock.AssertExpectationsForObjects(t, env.saverMock)
	})

	t.Run("Write transaction", func(t *testing.T) {
		t.Parallel()

		env := newWALCommitEnv(t)
		ctx := context.Background()

		txManager := env.txManager
		txManager.globalDBHeaderVersion = 3
		txManager.globalPageVersions[4] = 5

		tx := txManager.BeginTransaction(ctx)
		tx.ReadSet = map[PageIndex]uint64{4: 5}
		tx.WriteSet[4] = WriteInfo{
			Page:  &Page{Index: PageIndex(4), LeafNode: NewLeafNode()},
			Table: "users",
			Index: "pk_users",
		}

		// SavePage must be called to update in-memory cache; FlushBatch must NOT be called.
		env.saverMock.On("SavePage", ctx, PageIndex(4), tx.WriteSet[4].Page).Return(nil).Once()

		err := txManager.CommitTransaction(ctx, tx)
		require.NoError(t, err)
		assert.Equal(t, TxCommitted, tx.Status)

		// One WAL frame should have been written for page 4.
		assert.Equal(t, int64(1), env.wal.FrameCount())
		// Page 4 version incremented.
		assert.Equal(t, uint64(6), txManager.globalPageVersions[4])
		// WAL index has page 4.
		assert.True(t, env.walIndex.Has(PageIndex(4)))

		mock.AssertExpectationsForObjects(t, env.saverMock)
	})

	t.Run("Write transaction with header change", func(t *testing.T) {
		t.Parallel()

		env := newWALCommitEnv(t)
		ctx := context.Background()

		txManager := env.txManager
		txManager.globalDBHeaderVersion = 1

		tx := txManager.BeginTransaction(ctx)
		tx.DBHeaderRead = new(uint64)
		*tx.DBHeaderRead = 1
		tx.DBHeaderWrite = &DatabaseHeader{FirstFreePage: 5, FreePageCount: 3}
		tx.WriteSet[2] = WriteInfo{
			Page:  &Page{Index: PageIndex(2), LeafNode: NewLeafNode()},
			Table: "orders",
			Index: "pk_orders",
		}

		env.saverMock.On("SaveHeader", ctx, *tx.DBHeaderWrite).Return(nil).Once()
		env.saverMock.On("SavePage", ctx, PageIndex(2), tx.WriteSet[2].Page).Return(nil).Once()
		// Page 0 frame needed (header changed): only GetPage is called to read
		// the current B-tree content; GetHeader is NOT called because the new
		// header is already in tx.DBHeaderWrite.
		env.pagerMock.On("GetPage", ctx, PageIndex(0)).Return(&Page{Index: 0, LeafNode: NewLeafNode()}, nil).Once()

		err := txManager.CommitTransaction(ctx, tx)
		require.NoError(t, err)

		assert.Equal(t, int64(2), env.wal.FrameCount())
		assert.Equal(t, uint64(2), txManager.globalDBHeaderVersion)
		assert.Equal(t, uint64(1), txManager.globalPageVersions[2])
		assert.True(t, env.walIndex.Has(PageIndex(0)))
		assert.True(t, env.walIndex.Has(PageIndex(2)))

		mock.AssertExpectationsForObjects(t, env.pagerMock, env.saverMock)
	})

	t.Run("OCC conflict aborts transaction", func(t *testing.T) {
		t.Parallel()

		env := newWALCommitEnv(t)
		ctx := context.Background()

		txManager := env.txManager
		txManager.globalPageVersions[5] = 3

		txA := txManager.BeginTransaction(ctx)
		txA.ReadSet = map[PageIndex]uint64{5: 3}
		txA.WriteSet[5] = WriteInfo{
			Page:  &Page{Index: PageIndex(5), LeafNode: NewLeafNode()},
			Table: "items",
		}

		txB := txManager.BeginTransaction(ctx)
		txB.ReadSet = map[PageIndex]uint64{5: 3}
		txB.WriteSet[5] = WriteInfo{
			Page:  &Page{Index: PageIndex(5), LeafNode: NewLeafNode()},
			Table: "items",
		}

		// Commit txB first — succeeds.
		env.saverMock.On("SavePage", ctx, PageIndex(5), txB.WriteSet[5].Page).Return(nil).Once()
		require.NoError(t, txManager.CommitTransaction(ctx, txB))
		assert.Equal(t, uint64(4), txManager.globalPageVersions[5])

		// txA now conflicts (page 5 version changed to 4, but txA read at version 3).
		err := txManager.CommitTransaction(ctx, txA)
		require.Error(t, err)
		assert.ErrorIs(t, err, minisqlErrors.ErrTxConflict)
		assert.Equal(t, TxAborted, txA.Status)

		// Only one WAL frame (for txB).
		assert.Equal(t, int64(1), env.wal.FrameCount())

		mock.AssertExpectationsForObjects(t, env.saverMock)
	})
}

func TestTransactionManager_WAL_CrashRecovery(t *testing.T) {
	t.Parallel()

	t.Run("crash before WAL append leaves no committed frames", func(t *testing.T) {
		t.Parallel()

		env := newWALCommitEnv(t)
		ctx := context.Background()

		env.txManager.commitHook = func(phase commitPhase) {
			if phase == commitPhaseBeforeWALAppend {
				panic("crash before WAL append")
			}
		}

		tx := env.txManager.BeginTransaction(ctx)
		tx.WriteSet[7] = WriteInfo{
			Page:  &Page{Index: PageIndex(7), LeafNode: NewLeafNode()},
			Table: "foo",
		}
		env.saverMock.On("SavePage", ctx, PageIndex(7), mock.Anything).Maybe()

		require.PanicsWithValue(t, "crash before WAL append", func() {
			_ = env.txManager.CommitTransaction(ctx, tx)
		})

		// No frames committed to WAL.
		assert.Equal(t, int64(0), env.wal.FrameCount())
	})

	t.Run("crash after WAL append leaves committed frame recoverable", func(t *testing.T) {
		t.Parallel()

		env := newWALCommitEnv(t)
		ctx := context.Background()

		env.txManager.commitHook = func(phase commitPhase) {
			if phase == commitPhaseAfterWALAppend {
				panic("crash after WAL append")
			}
		}

		tx := env.txManager.BeginTransaction(ctx)
		tx.WriteSet[8] = WriteInfo{
			Page:  &Page{Index: PageIndex(8), LeafNode: NewLeafNode()},
			Table: "bar",
		}
		// SavePage would be called in the in-memory update step (after the hook
		// that panics), so it must NOT be called.
		env.saverMock.AssertNotCalled(t, "SavePage")

		require.PanicsWithValue(t, "crash after WAL append", func() {
			_ = env.txManager.CommitTransaction(ctx, tx)
		})

		// One WAL frame was written (the append succeeded before the panic).
		assert.Equal(t, int64(1), env.wal.FrameCount())

		// Reading back WAL frames confirms page 8 is present.
		frames, err := env.wal.ReadAllFrames()
		require.NoError(t, err)
		require.Len(t, frames, 1)
		assert.Equal(t, PageIndex(8), frames[0].PageIndex)
	})
}

// walCommitEnv holds all resources needed for a WAL commit test.
type walCommitEnv struct {
	dbFile    *os.File
	wal       *WAL
	walIndex  *WALIndex
	pagerMock *MockPager
	saverMock *MockPageSaver
	txManager *TransactionManager
}

func newWALCommitEnv(t *testing.T) *walCommitEnv {
	t.Helper()

	dbFile, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = dbFile.Close()
		_ = os.Remove(dbFile.Name())
	})

	wal, err := CreateWAL(dbFile.Name(), PageSize)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = wal.Close()
		_ = os.Remove(dbFile.Name() + "-wal")
	})

	walIndex := NewWALIndex()
	pagerMock := new(MockPager)
	saverMock := new(MockPageSaver)

	txManager := NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(pagerMock), saverMock, nil)
	txManager.wal = wal
	txManager.walIndex = walIndex

	return &walCommitEnv{
		dbFile:    dbFile,
		wal:       wal,
		walIndex:  walIndex,
		pagerMock: pagerMock,
		saverMock: saverMock,
		txManager: txManager,
	}
}

func TestTransactionManager_CheckpointWAL(t *testing.T) {
	t.Parallel()

	t.Run("ErrNotWALMode when WAL not enabled", func(t *testing.T) {
		t.Parallel()

		dbFile, err := os.CreateTemp("", testDBName)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = dbFile.Close()
			_ = os.Remove(dbFile.Name())
		})

		pager, err := NewPager(dbFile, PageSize, 100)
		require.NoError(t, err)
		txManager := NewTransactionManager(zap.NewNop(), dbFile.Name(), nil, pager, nil)
		// WAL not enabled.

		err = txManager.CheckpointWAL(dbFile)
		require.ErrorIs(t, err, ErrNotWALMode)
	})

	t.Run("Checkpoint flushes WAL frames to DB file and resets index", func(t *testing.T) {
		t.Parallel()

		// Build environment: a real DB file, a real WAL, write some pages, then checkpoint.
		dbFile, err := os.CreateTemp("", testDBName)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = dbFile.Close()
			_ = os.Remove(dbFile.Name())
		})

		// Allocate space for a few pages so WAL checkpoint can write into the file.
		const numPages = 4
		blankData := make([]byte, int(PageSize)*numPages)
		_, err = dbFile.Write(blankData)
		require.NoError(t, err)

		wal, err := CreateWAL(dbFile.Name(), PageSize)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = wal.Close()
			_ = os.Remove(dbFile.Name() + "-wal")
		})

		walIndex := NewWALIndex()
		saverMock := new(MockPageSaver)
		txManager := NewTransactionManager(zap.NewNop(), dbFile.Name(), nil, saverMock, nil)
		txManager.wal = wal
		txManager.walIndex = walIndex

		ctx := context.Background()

		// Write one transaction with a modified page so a WAL frame is appended.
		tx := txManager.BeginTransaction(ctx)
		tx.WriteSet[2] = WriteInfo{
			Page:  &Page{Index: PageIndex(2), LeafNode: NewLeafNode()},
			Table: "t1",
		}
		saverMock.On("SavePage", ctx, PageIndex(2), tx.WriteSet[2].Page).Return(nil).Once()
		require.NoError(t, txManager.CommitTransaction(ctx, tx))

		// One frame must be present before checkpoint.
		assert.Equal(t, int64(1), wal.FrameCount())
		assert.True(t, walIndex.Has(PageIndex(2)))

		// Checkpoint: should succeed and reset state.
		require.NoError(t, txManager.CheckpointWAL(dbFile))

		// WAL file should be empty after checkpoint+truncate.
		assert.Equal(t, int64(0), wal.FrameCount())
		// WAL index should be cleared.
		assert.False(t, walIndex.Has(PageIndex(2)))

		mock.AssertExpectationsForObjects(t, saverMock)
	})

	t.Run("Checkpoint with no frames is a no-op", func(t *testing.T) {
		t.Parallel()

		env := newWALCommitEnv(t)
		// No transactions committed — WAL is empty.
		assert.Equal(t, int64(0), env.wal.FrameCount())

		err := env.txManager.CheckpointWAL(env.dbFile)
		require.NoError(t, err)

		// Still empty; index still clear.
		assert.Equal(t, int64(0), env.wal.FrameCount())
		assert.False(t, env.walIndex.Has(PageIndex(1)))
	})
}

func TestTransactionManager_ExecuteInTransaction(t *testing.T) {
	t.Parallel()

	var (
		pagerMock = new(MockPager)
		saverMock = new(MockPageSaver)
		txManager = NewTransactionManager(zap.NewNop(), testDBName, mockPagerFactory(pagerMock), saverMock, nil)
	)

	t.Run("No transaction in context", func(t *testing.T) {
		ctx := context.Background()

		fnRan := false
		fn := func(ctx context.Context) error {
			fnRan = true
			return nil
		}

		assert.Len(t, txManager.transactions, 0)

		err := txManager.ExecuteInTransaction(ctx, fn)
		require.NoError(t, err)

		assert.Len(t, txManager.transactions, 0)
		assert.True(t, fnRan)

		mock.AssertExpectationsForObjects(t, pagerMock, saverMock)
		resetMocks(&pagerMock.Mock, &saverMock.Mock)
	})

	t.Run("Active transaction in context", func(t *testing.T) {
		ctx := context.Background()
		tx := txManager.BeginTransaction(ctx)
		ctx = WithTransaction(ctx, tx)

		fnRan := false
		fn := func(ctx context.Context) error {
			fnRan = true
			return nil
		}

		assert.Len(t, txManager.transactions, 1)

		err := txManager.ExecuteInTransaction(ctx, fn)
		require.NoError(t, err)

		assert.Len(t, txManager.transactions, 1)
		assert.True(t, fnRan)

		mock.AssertExpectationsForObjects(t, pagerMock, saverMock)
		resetMocks(&pagerMock.Mock, &saverMock.Mock)
	})
}

func TestTransactionManager_SetCheckpointFunc(t *testing.T) {
	t.Parallel()

	tm := &TransactionManager{}
	called := false
	tm.SetCheckpointFunc(func() error { called = true; return nil })
	require.NotNil(t, tm.checkpointFn)
	require.NoError(t, tm.checkpointFn())
	assert.True(t, called)
}
