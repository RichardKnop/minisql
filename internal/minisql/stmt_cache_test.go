package minisql

import (
	"context"
	"testing"

	"github.com/RichardKnop/minisql/pkg/lrucache"
	"github.com/stretchr/testify/assert"
)

// TestDatabase_PrepareStatementCaching tests the database-level statement caching
func TestDatabase_PrepareStatementCaching(t *testing.T) {
	t.Parallel()

	var (
		ctx        = context.Background()
		mockParser = new(MockParser)
		db         = &Database{
			parser:    mockParser,
			stmtCache: lrucache.New(100),
		}
	)

	// Mock the parser to return different statement instances
	query1 := `SELECT * FROM users WHERE id = ?"`
	stmt1Mock := Statement{
		Kind:      Select,
		TableName: "users",
		Conditions: OneOrMore{
			{
				FieldIsEqual("id", OperandPlaceholder, nil),
			},
		},
	}

	query2 := `SELECT * FROM posts WHERE user_id = ?`
	stmt2Mock := Statement{
		Kind:      Select,
		TableName: "posts",
		Conditions: OneOrMore{
			{
				FieldIsEqual("user_id", OperandPlaceholder, nil),
			},
		},
	}

	mockParser.On("Parse", ctx, query1).Return([]Statement{stmt1Mock}, nil).Once()

	// First call should parse
	stmt1, err := db.PrepareStatement(ctx, query1)
	assert.NoError(t, err)
	assert.Equal(t, stmt1Mock, stmt1)

	// Second call with same query should return cached statement
	stmt2, err := db.PrepareStatement(ctx, query1)
	assert.NoError(t, err)
	assert.Equal(t, stmt1Mock, stmt2)

	mockParser.On("Parse", ctx, query2).Return([]Statement{stmt2Mock}, nil).Once()

	// Different query should parse separately
	stmt3, err := db.PrepareStatement(ctx, query2)
	assert.NoError(t, err)
	assert.NotEqual(t, stmt1, stmt3, "Different queries should have different statements")
	assert.Equal(t, stmt2Mock, stmt3)
}
