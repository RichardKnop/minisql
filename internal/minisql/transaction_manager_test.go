package minisql

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTransactionManager_Commit(t *testing.T) {
	t.Parallel()

	t.Run("Write tx with no writes commits as read-only", func(t *testing.T) {
		var (
			ctx       = context.Background()
			saverMock = new(MockPageSaver)
			txManager = NewTransactionManager(zap.NewNop(), testDBName, nil, saverMock, nil)
		)

		tx, err := txManager.BeginTransaction(ctx)
		require.NoError(t, err)
		assert.Equal(t, TxActive, tx.Status)
		assert.Equal(t, int32(1), txManager.activeWriters.Load())

		// No writes — commit should succeed and release the writer slot.
		err = txManager.CommitTransaction(ctx, tx)
		require.NoError(t, err)
		assert.Equal(t, TxCommitted, tx.Status)
		assert.Equal(t, int32(0), txManager.activeWriters.Load())

		mock.AssertExpectationsForObjects(t, saverMock)
	})

	t.Run("Write transaction with page and header write", func(t *testing.T) {
		var (
			ctx       = context.Background()
			pagerMock = new(MockPager)
			saverMock = new(MockPageSaver)
			txManager = NewTransactionManager(zap.NewNop(), testDBName, mockPagerFactory(pagerMock), saverMock, nil)
		)

		tx, err := txManager.BeginTransaction(ctx)
		require.NoError(t, err)
		assert.Equal(t, TxActive, tx.Status)

		tx.DBHeaderWrite = &DatabaseHeader{FirstFreePage: 2, FreePageCount: 10}
		tx.WriteSet[4] = WriteInfo{
			Page:  &Page{Index: PageIndex(4)},
			Table: "users",
			Index: "pk_users",
		}

		saverMock.On("SavePage", ctx, PageIndex(4), tx.WriteSet[4].Page).Return(nil).Once()
		saverMock.On("SaveHeader", ctx, *tx.DBHeaderWrite).Return(nil).Once()
		saverMock.On("FlushBatch", ctx, mock.MatchedBy(func(pages []PageIndex) bool {
			return len(pages) == 2 &&
				((pages[0] == PageIndex(0) && pages[1] == PageIndex(4)) ||
					(pages[0] == PageIndex(4) && pages[1] == PageIndex(0)))
		})).Return(nil).Once()

		err = txManager.CommitTransaction(ctx, tx)
		require.NoError(t, err)
		assert.Equal(t, TxCommitted, tx.Status)
		assert.Equal(t, int32(0), txManager.activeWriters.Load())

		mock.AssertExpectationsForObjects(t, pagerMock, saverMock)
	})

	t.Run("ErrConcurrentWriter when second write tx started", func(t *testing.T) {
		var (
			ctx       = context.Background()
			saverMock = new(MockPageSaver)
			txManager = NewTransactionManager(zap.NewNop(), testDBName, nil, saverMock, nil)
		)

		tx1, err := txManager.BeginTransaction(ctx)
		require.NoError(t, err)
		assert.Equal(t, int32(1), txManager.activeWriters.Load())

		_, err = txManager.BeginTransaction(ctx)
		require.ErrorIs(t, err, ErrConcurrentWriter)
		assert.Equal(t, int32(1), txManager.activeWriters.Load(), "counter must not change on rejection")

		// After rollback the slot is released and a new transaction is possible.
		txManager.RollbackTransaction(ctx, tx1)
		assert.Equal(t, int32(0), txManager.activeWriters.Load())

		tx2, err := txManager.BeginTransaction(ctx)
		require.NoError(t, err)
		txManager.RollbackTransaction(ctx, tx2)

		mock.AssertExpectationsForObjects(t, saverMock)
	})
}

func TestTransactionManager_Rollback(t *testing.T) {
	t.Parallel()

	var (
		ctx       = context.Background()
		saverMock = new(MockPageSaver)
		txManager = NewTransactionManager(zap.NewNop(), testDBName, mockPagerFactory(nil), saverMock, nil)
	)

	tx, err := txManager.BeginTransaction(ctx)
	require.NoError(t, err)
	assert.Equal(t, TxActive, tx.Status)
	assert.Equal(t, int32(1), txManager.activeWriters.Load())

	tx.DBHeaderWrite = &DatabaseHeader{FirstFreePage: 2, FreePageCount: 10}
	tx.WriteSet[4] = WriteInfo{
		Page:  &Page{Index: PageIndex(4)},
		Table: "users",
	}

	txManager.RollbackTransaction(ctx, tx)
	assert.Equal(t, TxAborted, tx.Status)
	assert.Equal(t, int32(0), txManager.activeWriters.Load())

	mock.AssertExpectationsForObjects(t, saverMock)
}

func TestTransactionManager_WAL_Commit(t *testing.T) {
	t.Parallel()

	t.Run("Write tx with no writes commits as read-only via WAL", func(t *testing.T) {
		t.Parallel()

		env := newWALCommitEnv(t)
		ctx := context.Background()

		tx, err := env.txManager.BeginTransaction(ctx)
		require.NoError(t, err)

		// No writes — no WAL frames written.
		err = env.txManager.CommitTransaction(ctx, tx)
		require.NoError(t, err)
		assert.Equal(t, TxCommitted, tx.Status)
		assert.Equal(t, int64(0), env.wal.FrameCount())
		assert.Equal(t, int32(0), env.txManager.activeWriters.Load())

		mock.AssertExpectationsForObjects(t, env.saverMock)
	})

	t.Run("Write transaction", func(t *testing.T) {
		t.Parallel()

		env := newWALCommitEnv(t)
		ctx := context.Background()

		tx, err := env.txManager.BeginTransaction(ctx)
		require.NoError(t, err)
		tx.WriteSet[4] = WriteInfo{
			Page:  &Page{Index: PageIndex(4), LeafNode: NewLeafNode()},
			Table: "users",
			Index: "pk_users",
		}

		// SavePage must be called to update in-memory cache; FlushBatch must NOT be called.
		env.saverMock.On("SavePage", ctx, PageIndex(4), tx.WriteSet[4].Page).Return(nil).Once()

		err = env.txManager.CommitTransaction(ctx, tx)
		require.NoError(t, err)
		assert.Equal(t, TxCommitted, tx.Status)
		assert.Equal(t, int32(0), env.txManager.activeWriters.Load())

		// One WAL frame should have been written for page 4.
		assert.Equal(t, int64(1), env.wal.FrameCount())
		// WAL index has page 4.
		assert.True(t, env.walIndex.Has(PageIndex(4)))

		mock.AssertExpectationsForObjects(t, env.saverMock)
	})

	t.Run("Write transaction with header change", func(t *testing.T) {
		t.Parallel()

		env := newWALCommitEnv(t)
		ctx := context.Background()

		tx, err := env.txManager.BeginTransaction(ctx)
		require.NoError(t, err)
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

		err = env.txManager.CommitTransaction(ctx, tx)
		require.NoError(t, err)

		assert.Equal(t, int64(2), env.wal.FrameCount())
		assert.True(t, env.walIndex.Has(PageIndex(0)))
		assert.True(t, env.walIndex.Has(PageIndex(2)))

		mock.AssertExpectationsForObjects(t, env.pagerMock, env.saverMock)
	})

	t.Run("ErrConcurrentWriter via WAL path", func(t *testing.T) {
		t.Parallel()

		env := newWALCommitEnv(t)
		ctx := context.Background()

		tx1, err := env.txManager.BeginTransaction(ctx)
		require.NoError(t, err)

		_, err = env.txManager.BeginTransaction(ctx)
		require.ErrorIs(t, err, ErrConcurrentWriter)
		assert.Equal(t, int32(1), env.txManager.activeWriters.Load())

		env.txManager.RollbackTransaction(ctx, tx1)
		assert.Equal(t, int32(0), env.txManager.activeWriters.Load())

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

		tx, err := env.txManager.BeginTransaction(ctx)
		require.NoError(t, err)
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

		tx, err := env.txManager.BeginTransaction(ctx)
		require.NoError(t, err)
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
		tx, err := txManager.BeginTransaction(ctx)
		require.NoError(t, err)
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

		assert.Empty(t, txManager.transactions)

		err := txManager.ExecuteInTransaction(ctx, fn)
		require.NoError(t, err)

		assert.Empty(t, txManager.transactions)
		assert.True(t, fnRan)

		mock.AssertExpectationsForObjects(t, pagerMock, saverMock)
		resetMocks(&pagerMock.Mock, &saverMock.Mock)
	})

	t.Run("Active transaction in context", func(t *testing.T) {
		ctx := context.Background()
		tx, err := txManager.BeginTransaction(ctx)
		require.NoError(t, err)
		ctx = WithTransaction(ctx, tx)

		fnRan := false
		fn := func(ctx context.Context) error {
			fnRan = true
			return nil
		}

		assert.Len(t, txManager.transactions, 1)

		err = txManager.ExecuteInTransaction(ctx, fn)
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
