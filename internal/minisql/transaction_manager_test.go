package minisql

import (
	"context"
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
			txManager = NewTransactionManager(zap.NewNop(), testDbName, nil, saverMock, nil)
		)

		// Setup initial global versions
		txManager.globalDbHeaderVersion = 4
		txManager.globalPageVersions[2] = 0
		txManager.globalPageVersions[3] = 2
		txManager.globalPageVersions[4] = 1

		tx := txManager.BeginTransaction(ctx)
		assert.Equal(t, TxActive, tx.Status)

		// Let's simulate some reads but no writes
		tx.DbHeaderRead = new(uint64)
		*tx.DbHeaderRead = 4
		tx.ReadSet[2] = 0
		tx.ReadSet[3] = 2
		tx.ReadSet[4] = 1

		err := txManager.CommitTransaction(ctx, tx)
		require.NoError(t, err)

		// Global versions should remain unchanged
		assert.Equal(t, 4, int(txManager.globalDbHeaderVersion))
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
			txManager = NewTransactionManager(zap.NewNop(), testDbName, mockPagerFactory(pagerMock), saverMock, nil)
		)

		// Setup initial global versions
		txManager.globalDbHeaderVersion = 3
		txManager.globalPageVersions[2] = 0
		txManager.globalPageVersions[3] = 2
		txManager.globalPageVersions[4] = 5

		tx := txManager.BeginTransaction(ctx)
		assert.Equal(t, TxActive, tx.Status)

		// Let's simulate some reads and writes
		tx.DbHeaderRead = new(uint64)
		*tx.DbHeaderRead = 3
		tx.DbHeaderWrite = &DatabaseHeader{FirstFreePage: 2, FreePageCount: 10}
		tx.ReadSet[2] = 0
		tx.ReadSet[3] = 2
		tx.ReadSet[4] = 5
		tx.WriteSet[4] = &Page{Index: PageIndex(4)}
		tx.WriteInfoSet[4] = WriteInfo{Table: "users", Index: "pk_users"}

		// Setup expectations
		if txManager.journalEnabled {
			originalPage := &Page{Index: PageIndex(4), LeafNode: NewLeafNode()}
			pagerMock.On("GetPage", ctx, PageIndex(4)).Return(originalPage, nil).Once()
		}
		saverMock.On("SavePage", ctx, PageIndex(4), tx.WriteSet[4]).Return(nil).Once()
		saverMock.On("SaveHeader", ctx, *tx.DbHeaderWrite).Return(nil).Once()
		saverMock.On("Flush", ctx, PageIndex(0)).Return(nil).Once()
		saverMock.On("Flush", ctx, PageIndex(4)).Return(nil).Once()

		err := txManager.CommitTransaction(ctx, tx)
		require.NoError(t, err)

		// Global versions should be updated accordingly
		assert.Equal(t, 4, int(txManager.globalDbHeaderVersion))
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
			txManager = NewTransactionManager(zap.NewNop(), testDbName, mockPagerFactory(pagerMock), saverMock, nil)
		)

		// Setup initial global versions
		txManager.globalDbHeaderVersion = 4
		txManager.globalPageVersions[2] = 0
		txManager.globalPageVersions[3] = 2
		txManager.globalPageVersions[4] = 1

		readTx := txManager.BeginTransaction(ctx)
		assert.Equal(t, TxActive, readTx.Status)

		writeTx := txManager.BeginTransaction(ctx)
		assert.Equal(t, TxActive, writeTx.Status)

		// Let's simulate some reads for first tx
		readTx.DbHeaderRead = new(uint64)
		*readTx.DbHeaderRead = 4
		readTx.ReadSet[2] = 0
		readTx.ReadSet[3] = 2
		readTx.ReadSet[4] = 1

		// Let's simulate a write for second tx that will conflict
		writeTx.WriteSet[3] = &Page{Index: PageIndex(3), LeafNode: NewLeafNode()}
		writeTx.WriteInfoSet[3] = WriteInfo{Table: "orders", Index: "pk_orders"}

		// Setup expectations
		if txManager.journalEnabled {
			originalPage := &Page{Index: PageIndex(3), LeafNode: NewLeafNode()}
			pagerMock.On("GetPage", ctx, PageIndex(3)).Return(originalPage, nil).Once()
		}
		saverMock.On("SavePage", ctx, PageIndex(3), writeTx.WriteSet[3]).Return(nil).Once()

		// Commit the writing transaction first
		saverMock.On("Flush", ctx, PageIndex(3)).Return(nil).Once()
		err := txManager.CommitTransaction(ctx, writeTx)
		require.NoError(t, err)

		// Now, committing the reading transaction should fail due to conflict
		err = txManager.CommitTransaction(ctx, readTx)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrTxConflict)
		assert.Equal(t, "transaction conflict detected: tx 1 aborted due to conflict on page 3", err.Error())

		// Writing transaction should have updated page version, no other changes expected
		assert.Equal(t, 4, int(txManager.globalDbHeaderVersion))
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
			txManager = NewTransactionManager(zap.NewNop(), testDbName, mockPagerFactory(pagerMock), saverMock, nil)
		)

		// Setup initial global versions
		txManager.globalDbHeaderVersion = 3
		txManager.globalPageVersions[2] = 0
		txManager.globalPageVersions[3] = 2
		txManager.globalPageVersions[4] = 5

		writeTx1 := txManager.BeginTransaction(ctx)
		assert.Equal(t, TxActive, writeTx1.Status)

		writeTx2 := txManager.BeginTransaction(ctx)
		assert.Equal(t, TxActive, writeTx2.Status)

		// Let's simulate some reads and a write for first tx
		writeTx1.DbHeaderRead = new(uint64)
		*writeTx1.DbHeaderRead = 3
		writeTx1.DbHeaderWrite = &DatabaseHeader{FirstFreePage: 2, FreePageCount: 10}
		writeTx1.ReadSet[2] = 0
		writeTx1.ReadSet[3] = 2
		writeTx1.ReadSet[4] = 5
		writeTx1.WriteSet[4] = &Page{Index: PageIndex(4)}
		writeTx1.WriteInfoSet[4] = WriteInfo{Table: "orders", Index: "pk_orders"}

		// Second tx will modify the same page to cause conflict
		writeTx2.ReadSet[4] = 5
		writeTx2.WriteSet[4] = &Page{Index: PageIndex(4)}
		writeTx2.WriteInfoSet[4] = WriteInfo{Table: "orders", Index: "pk_orders"}

		// Setup expectations
		if txManager.journalEnabled {
			originalPage := &Page{Index: PageIndex(4), LeafNode: NewLeafNode()}
			pagerMock.On("GetPage", ctx, PageIndex(4)).Return(originalPage, nil).Once()
		}
		saverMock.On("SavePage", ctx, PageIndex(4), writeTx2.WriteSet[4]).Return(nil).Once()
		saverMock.On("Flush", ctx, PageIndex(4)).Return(nil).Once()

		// Commit the second transaction first
		err := txManager.CommitTransaction(ctx, writeTx2)
		require.NoError(t, err)

		// Now, committing the first transaction should fail due to conflict
		err = txManager.CommitTransaction(ctx, writeTx1)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrTxConflict)
		assert.Equal(t, "transaction conflict detected: tx 1 aborted due to conflict on page 4", err.Error())

		// Second transaction should have updated page version, no other changes expected
		assert.Equal(t, 3, int(txManager.globalDbHeaderVersion))
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
		txManager = NewTransactionManager(zap.NewNop(), testDbName, mockPagerFactory(nil), saverMock, nil)
	)

	// Setup initial global versions
	txManager.globalDbHeaderVersion = 3
	txManager.globalPageVersions[2] = 0
	txManager.globalPageVersions[3] = 2
	txManager.globalPageVersions[4] = 5

	tx := txManager.BeginTransaction(ctx)
	assert.Equal(t, TxActive, tx.Status)

	// Let's simulate some reads and writes
	tx.DbHeaderRead = new(uint64)
	*tx.DbHeaderRead = 3
	tx.DbHeaderWrite = &DatabaseHeader{FirstFreePage: 2, FreePageCount: 10}
	tx.ReadSet[2] = 0
	tx.ReadSet[3] = 2
	tx.ReadSet[4] = 5
	tx.WriteSet[4] = &Page{Index: PageIndex(4)}

	txManager.RollbackTransaction(ctx, tx)
	assert.Equal(t, TxAborted, tx.Status)

	// Global versions should remain unchanged
	assert.Equal(t, 3, int(txManager.globalDbHeaderVersion))
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
		txManager = NewTransactionManager(zap.NewNop(), testDbName, mockPagerFactory(pagerMock), saverMock, nil)
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
