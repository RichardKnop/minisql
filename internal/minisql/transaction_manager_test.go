package minisql

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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
		tx.ReadSet[2] = 0
		tx.ReadSet[3] = 2
		tx.ReadSet[4] = 1

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
		tx.ReadSet[2] = 0
		tx.ReadSet[3] = 2
		tx.ReadSet[4] = 5
		tx.WriteSet[4] = WriteInfo{
			&Page{Index: PageIndex(4)},
			"users",
			"pk_users",
		}

		// Setup expectations
		if txManager.journalEnabled {
			originalDBHeader := DatabaseHeader{FirstFreePage: 1, FreePageCount: 9}
			pagerMock.On("GetHeader", ctx).Return(originalDBHeader, nil).Once()
			originalPage := &Page{Index: PageIndex(4), LeafNode: NewLeafNode()}
			pagerMock.On("GetPage", ctx, PageIndex(4)).Return(originalPage, nil).Once()
		}
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
		readTx.ReadSet[2] = 0
		readTx.ReadSet[3] = 2
		readTx.ReadSet[4] = 1

		// Let's simulate a write for second tx that will conflict
		writeTx.WriteSet[3] = WriteInfo{
			&Page{Index: PageIndex(3), LeafNode: NewLeafNode()},
			"orders",
			"pk_orders",
		}

		// Setup expectations
		if txManager.journalEnabled {
			originalPage := &Page{Index: PageIndex(3), LeafNode: NewLeafNode()}
			pagerMock.On("GetPage", ctx, PageIndex(3)).Return(originalPage, nil).Once()
		}
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
		assert.ErrorIs(t, err, ErrTxConflict)
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
		writeTx1.ReadSet[2] = 0
		writeTx1.ReadSet[3] = 2
		writeTx1.ReadSet[4] = 5
		writeTx1.WriteSet[4] = WriteInfo{
			&Page{Index: PageIndex(4)},
			"orders",
			"pk_orders",
		}

		// Second tx will modify the same page to cause conflict
		writeTx2.ReadSet[4] = 5
		writeTx2.WriteSet[4] = WriteInfo{
			&Page{Index: PageIndex(4)},
			"orders",
			"pk_orders",
		}

		// Setup expectations
		if txManager.journalEnabled {
			originalPage := &Page{Index: PageIndex(4), LeafNode: NewLeafNode()}
			pagerMock.On("GetPage", ctx, PageIndex(4)).Return(originalPage, nil).Once()
		}
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
		assert.ErrorIs(t, err, ErrTxConflict)
		assert.Equal(t, "transaction conflict detected: tx 1 aborted due to conflict on page 4", err.Error())

		// Second transaction should have updated page version, no other changes expected
		assert.Equal(t, 3, int(txManager.globalDBHeaderVersion))
		assert.Equal(t, 0, int(txManager.globalPageVersions[2]))
		assert.Equal(t, 2, int(txManager.globalPageVersions[3]))
		assert.Equal(t, 6, int(txManager.globalPageVersions[4]))

		mock.AssertExpectationsForObjects(t, pagerMock, saverMock)
	})
}

func TestTransactionManager_CommitRecoveryWindows(t *testing.T) {
	t.Parallel()

	t.Run("crash before journal finalize leaves incomplete journal and original data", func(t *testing.T) {
		env := newCommitRecoveryEnv(t)
		env.txManager.commitHook = func(phase commitPhase) {
			if phase == commitPhaseBeforeJournalFinalize {
				panic("simulated crash before finalize")
			}
		}

		require.PanicsWithValue(t, "simulated crash before finalize", func() {
			_ = env.txManager.CommitTransaction(context.Background(), env.newWriteTx())
		})

		page := env.readLeafPage(t)
		assert.Equal(t, env.originalValue, string(readCellValue(t, page)))

		recovered, err := RecoverFromJournal(env.dbFile.Name(), PageSize)
		require.Error(t, err)
		assert.False(t, recovered)
		assert.ErrorContains(t, err, "trailing data")
	})

	t.Run("crash after journal finalize but before flush replays original data", func(t *testing.T) {
		env := newCommitRecoveryEnv(t)
		env.txManager.commitHook = func(phase commitPhase) {
			if phase == commitPhaseAfterJournalFinalize {
				panic("simulated crash after finalize")
			}
		}

		require.PanicsWithValue(t, "simulated crash after finalize", func() {
			_ = env.txManager.CommitTransaction(context.Background(), env.newWriteTx())
		})

		page := env.readLeafPage(t)
		assert.Equal(t, env.originalValue, string(readCellValue(t, page)))

		recovered, err := RecoverFromJournal(env.dbFile.Name(), PageSize)
		require.NoError(t, err)
		assert.True(t, recovered)
		page = env.readLeafPage(t)
		assert.Equal(t, env.originalValue, string(readCellValue(t, page)))
	})

	t.Run("partial flush failure replays original data", func(t *testing.T) {
		env := newCommitRecoveryEnv(t)
		env.txManager.saver = &faultInjectingSaver{
			PageSaver:    env.pager,
			flushPageCnt: 1,
			flushErr:     errors.New("simulated partial flush failure"),
		}

		require.Panics(t, func() {
			_ = env.txManager.CommitTransaction(context.Background(), env.newWriteTx())
		})

		page := env.readLeafPage(t)
		assert.Equal(t, env.modifiedValue, string(readCellValue(t, page)))

		recovered, err := RecoverFromJournal(env.dbFile.Name(), PageSize)
		require.NoError(t, err)
		assert.True(t, recovered)
		page = env.readLeafPage(t)
		assert.Equal(t, env.originalValue, string(readCellValue(t, page)))
	})

	t.Run("crash before flush replays original data", func(t *testing.T) {
		env := newCommitRecoveryEnv(t)
		env.txManager.commitHook = func(phase commitPhase) {
			if phase == commitPhaseBeforeFlush {
				panic("simulated crash before flush")
			}
		}

		require.PanicsWithValue(t, "simulated crash before flush", func() {
			_ = env.txManager.CommitTransaction(context.Background(), env.newWriteTx())
		})

		page := env.readLeafPage(t)
		assert.Equal(t, env.originalValue, string(readCellValue(t, page)))

		recovered, err := RecoverFromJournal(env.dbFile.Name(), PageSize)
		require.NoError(t, err)
		assert.True(t, recovered)
		page = env.readLeafPage(t)
		assert.Equal(t, env.originalValue, string(readCellValue(t, page)))
	})

	t.Run("crash after flush before journal delete rolls back committed pages", func(t *testing.T) {
		env := newCommitRecoveryEnv(t)
		env.txManager.commitHook = func(phase commitPhase) {
			if phase == commitPhaseAfterFlushBeforeDelete {
				panic("simulated crash after flush")
			}
		}

		require.PanicsWithValue(t, "simulated crash after flush", func() {
			_ = env.txManager.CommitTransaction(context.Background(), env.newWriteTx())
		})

		page := env.readLeafPage(t)
		assert.Equal(t, env.modifiedValue, string(readCellValue(t, page)))

		recovered, err := RecoverFromJournal(env.dbFile.Name(), PageSize)
		require.NoError(t, err)
		assert.True(t, recovered)
		page = env.readLeafPage(t)
		assert.Equal(t, env.originalValue, string(readCellValue(t, page)))
	})
}

type commitRecoveryEnv struct {
	dbFile        *os.File
	pager         *pagerImpl
	txManager     *TransactionManager
	columns       []Column
	leafPageIdx   PageIndex
	originalValue string
	modifiedValue string
}

func newCommitRecoveryEnv(t *testing.T) *commitRecoveryEnv {
	t.Helper()

	dbFile, err := os.CreateTemp("", testDBName)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = dbFile.Close()
		_ = os.Remove(dbFile.Name())
		_ = os.Remove(dbFile.Name() + "-journal")
	})

	columns := []Column{{
		Kind: Varchar,
		Size: MaxInlineVarchar,
		Name: "foo",
	}}
	rootPage, _, leafPages := newTestBtree()
	require.NotEmpty(t, leafPages)

	pager, err := NewPager(dbFile, PageSize, 1000)
	require.NoError(t, err)
	pager.pages = make([]*Page, 0, 1+len(leafPages))
	pager.pages = append(pager.pages, rootPage.Clone())
	for _, page := range leafPages {
		pager.pages = append(pager.pages, page.Clone())
	}
	pager.totalPages = uint32(len(pager.pages))

	for _, page := range pager.pages {
		require.NoError(t, pager.Flush(context.Background(), page.Index))
	}

	tablePager := pager.ForTable(columns)
	txManager := NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
	txManager.journalEnabled = true

	env := &commitRecoveryEnv{
		dbFile:      dbFile,
		pager:       pager,
		txManager:   txManager,
		columns:     columns,
		leafPageIdx: leafPages[0].Index,
	}
	page := env.readLeafPage(t)
	env.originalValue = string(readCellValue(t, page))
	env.modifiedValue = "crash-window-test"
	return env
}

func (e *commitRecoveryEnv) newWriteTx() *Transaction {
	tx := e.txManager.BeginTransaction(context.Background())
	modifiedPage := e.mustCloneLeafPage()
	modifiedPage.LeafNode.Cells[0].Value = prefixWithLength([]byte(e.modifiedValue))
	tx.WriteSet[e.leafPageIdx] = WriteInfo{
		Page:  modifiedPage,
		Table: "users",
	}
	return tx
}

func (e *commitRecoveryEnv) mustCloneLeafPage() *Page {
	page, err := e.pager.ForTable(e.columns).GetPage(context.Background(), e.leafPageIdx)
	if err != nil {
		panic(err)
	}
	return page.Clone()
}

func (e *commitRecoveryEnv) readLeafPage(t *testing.T) *Page {
	t.Helper()
	file, err := os.OpenFile(e.dbFile.Name(), os.O_RDWR, 0o600)
	require.NoError(t, err)
	defer file.Close()

	pager, err := NewPager(file, PageSize, 1000)
	require.NoError(t, err)
	page, err := pager.ForTable(e.columns).GetPage(context.Background(), e.leafPageIdx)
	require.NoError(t, err)
	return page
}

func readCellValue(t *testing.T, page *Page) []byte {
	t.Helper()
	require.NotNil(t, page.LeafNode)
	require.NotEmpty(t, page.LeafNode.Cells)
	value := page.LeafNode.Cells[0].Value
	require.GreaterOrEqual(t, len(value), 4)
	return value[4:]
}

type faultInjectingSaver struct {
	PageSaver
	flushPageCnt int
	flushErr     error
}

func (s *faultInjectingSaver) FlushBatch(ctx context.Context, pageIndices []PageIndex) error {
	for i, pageIdx := range pageIndices {
		if s.flushPageCnt >= 0 && i >= s.flushPageCnt {
			break
		}
		if err := s.Flush(ctx, pageIdx); err != nil {
			return err
		}
	}
	return s.flushErr
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
	tx.ReadSet[2] = 0
	tx.ReadSet[3] = 2
	tx.ReadSet[4] = 5
	tx.WriteSet[4] = WriteInfo{
		&Page{Index: PageIndex(4)},
		"users",
		"",
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
