package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/pkg/node"
)

var (
	// A root leaf node, this indicates an empty database
	anEmptyRootLeafNode = &node.LeafNode{
		Header: node.LeafNodeHeader{
			Header: node.Header{
				IsRoot: true,
			},
		},
	}
)

func TestTable_SeekMaxKey_EmptyDatabase(t *testing.T) {
	t.Parallel()
	t.Skip()

	ctx := context.Background()
	pagerMock := new(MockPager)
	rootPage := &Page{
		LeafNode: anEmptyRootLeafNode,
	}
	aTable := NewTable("foo", testColumns, pagerMock, 0)

	pagerMock.On("GetPage", mock.Anything, aTable, uint32(0)).Return(rootPage, nil).Once()

	rowID, ok, err := aTable.SeekMaxKey(ctx, aTable.RootPageIdx)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, uint32(0), rowID)

	mock.AssertExpectationsForObjects(t, pagerMock)
}

func TestTable_SeekMaxKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	pagerMock := new(MockPager)

	aTable := NewTable("foo", testColumns, pagerMock, 0)

	rootPage, internalPages, leafPages := newTestBtree()

	pagerMock.On("GetPage", mock.Anything, aTable, uint32(0)).Return(rootPage, nil).Once()
	pagerMock.On("GetPage", mock.Anything, aTable, uint32(2)).Return(internalPages[1], nil).Once()
	pagerMock.On("GetPage", mock.Anything, aTable, uint32(6)).Return(leafPages[3], nil).Once()

	rowID, found, err := aTable.SeekMaxKey(ctx, aTable.RootPageIdx)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, 21, int(rowID))

	mock.AssertExpectationsForObjects(t, pagerMock)
}
