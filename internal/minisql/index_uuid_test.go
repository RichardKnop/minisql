package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestUUIDIndex_InsertAndSeek(t *testing.T) {
	// initTest calls t.Parallel() internally.
	pager, dbFile := initTest(t)
	ctx := context.Background()

	col := Column{Name: "id", Kind: UUID, Size: 16}
	indexPager, err := pager.ForIndex([]Column{col}, true)
	require.NoError(t, err)
	txManager := NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(indexPager), pager, nil)
	txPager := NewTransactionalPager(indexPager, txManager, "uuid_table", "uuid_index")

	idx, err := NewUniqueIndex[UUIDValue](testLogger, txManager, "uuid_index", []Column{col}, txPager, 0)
	require.NoError(t, err)

	uv1, _ := ParseUUID("00000000-0000-0000-0000-000000000001")
	uv2, _ := ParseUUID("00000000-0000-0000-0000-000000000002")
	uv3, _ := ParseUUID("ffffffff-ffff-ffff-ffff-ffffffffffff")

	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		if err := idx.Insert(ctx, uv1, RowID(1)); err != nil {
			return err
		}
		if err := idx.Insert(ctx, uv2, RowID(2)); err != nil {
			return err
		}
		return idx.Insert(ctx, uv3, RowID(3))
	})
	require.NoError(t, err)

	rootPage := pager.pages[0]

	t.Run("found keys", func(t *testing.T) {
		for _, key := range []UUIDValue{uv1, uv2, uv3} {
			_, ok, err := idx.Seek(ctx, rootPage, key)
			require.NoError(t, err)
			assert.True(t, ok)
		}
	})

	t.Run("missing key returns not found", func(t *testing.T) {
		missing, _ := ParseUUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
		_, ok, err := idx.Seek(ctx, rootPage, missing)
		require.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestMarshalIndexNode_UUID(t *testing.T) {
	t.Parallel()

	buf := make([]byte, PageSize)

	uv, _ := ParseUUID("550e8400-e29b-41d4-a716-446655440000")
	cell := NewIndexCell[UUIDValue](true)
	cell.Key = uv
	cell.UniqueRowID = 42
	node := NewRootIndexNode[UUIDValue](true, cell)

	err := marshalIndexNode(node, buf)
	require.NoError(t, err)

	// copyIndexNode must handle UUIDValue
	cloned := copyIndexNode(node)
	require.NotNil(t, cloned)
	clonedNode, ok := cloned.(*IndexNode[UUIDValue])
	require.True(t, ok)
	assert.Equal(t, node.Header, clonedNode.Header)
}
