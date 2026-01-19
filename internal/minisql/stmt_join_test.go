package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatement_AddJoin(t *testing.T) {
	t.Parallel()

	t.Run("error when statement is not SELECT", func(t *testing.T) {
		t.Parallel()

		stmt := Statement{
			Kind:      Insert,
			TableName: "users",
		}

		_, err := stmt.AddJoin(Inner, "users", "posts", "p", Conditions{})
		require.Error(t, err)
		assert.Equal(t, "joins can only be added to SELECT statements", err.Error())
	})

	t.Run("first join - from table matches statement table", func(t *testing.T) {
		t.Parallel()

		stmt := Statement{
			Kind:       Select,
			TableName:  "users",
			TableAlias: "u",
		}

		conditions := Conditions{
			{
				Operand1: Operand{Type: OperandField, Value: Field{AliasPrefix: "u", Name: "id"}},
				Operator: Eq,
				Operand2: Operand{Type: OperandField, Value: Field{AliasPrefix: "p", Name: "user_id"}},
			},
		}

		result, err := stmt.AddJoin(Inner, "u", "posts", "p", conditions)
		require.NoError(t, err)
		require.Len(t, result.Joins, 1)

		join := result.Joins[0]
		assert.Equal(t, Inner, join.Type)
		assert.Equal(t, "posts", join.TableName)
		assert.Equal(t, "p", join.TableAlias)
		assert.Equal(t, conditions, join.Conditions)
		assert.Len(t, join.Joins, 0)
	})

	t.Run("first join - from table does not match statement table", func(t *testing.T) {
		t.Parallel()

		stmt := Statement{
			Kind:       Select,
			TableName:  "users",
			TableAlias: "u",
		}

		_, err := stmt.AddJoin(Inner, "o", "posts", "p", Conditions{})
		require.Error(t, err)
		assert.Equal(t, `join from table alias "o" does not match statement table alias "u"`, err.Error())
	})

	t.Run("second join from statement table", func(t *testing.T) {
		t.Parallel()

		stmt := Statement{
			Kind:       Select,
			TableName:  "users",
			TableAlias: "u",
			Joins: []Join{
				{
					Type:       Inner,
					TableName:  "posts",
					TableAlias: "p",
				},
			},
		}

		conditions := Conditions{
			{
				Operand1: Operand{Type: OperandField, Value: Field{AliasPrefix: "u", Name: "id"}},
				Operator: Eq,
				Operand2: Operand{Type: OperandField, Value: Field{AliasPrefix: "c", Name: "user_id"}},
			},
		}

		result, err := stmt.AddJoin(Left, "u", "comments", "c", conditions)
		require.NoError(t, err)
		require.Len(t, result.Joins, 2)

		join := result.Joins[1]
		assert.Equal(t, Left, join.Type)
		assert.Equal(t, "comments", join.TableName)
		assert.Equal(t, "c", join.TableAlias)
		assert.Equal(t, conditions, join.Conditions)
	})

	t.Run("nested join from first level join table", func(t *testing.T) {
		t.Parallel()

		stmt := Statement{
			Kind:       Select,
			TableName:  "users",
			TableAlias: "u",
			Joins: []Join{
				{
					Type:       Inner,
					TableName:  "posts",
					TableAlias: "p",
				},
			},
		}

		conditions := Conditions{
			{
				Operand1: Operand{Type: OperandField, Value: Field{AliasPrefix: "p", Name: "id"}},
				Operator: Eq,
				Operand2: Operand{Type: OperandField, Value: Field{AliasPrefix: "c", Name: "post_id"}},
			},
		}

		result, err := stmt.AddJoin(Left, "p", "comments", "c", conditions)
		require.NoError(t, err)
		require.Len(t, result.Joins, 1)

		// The nested join should be added to the posts join
		postsJoin := result.Joins[0]
		require.Len(t, postsJoin.Joins, 1)

		nestedJoin := postsJoin.Joins[0]
		assert.Equal(t, Left, nestedJoin.Type)
		assert.Equal(t, "comments", nestedJoin.TableName)
		assert.Equal(t, "c", nestedJoin.TableAlias)
		assert.Equal(t, conditions, nestedJoin.Conditions)
	})

	t.Run("deeply nested join - three levels", func(t *testing.T) {
		t.Parallel()

		stmt := Statement{
			Kind:       Select,
			TableName:  "users",
			TableAlias: "u",
			Joins: []Join{
				{
					Type:       Inner,
					TableName:  "posts",
					TableAlias: "p",
					Joins: []Join{
						{
							Type:       Left,
							TableName:  "comments",
							TableAlias: "c",
						},
					},
				},
			},
		}

		conditions := Conditions{
			{
				Operand1: Operand{Type: OperandField, Value: Field{AliasPrefix: "c", Name: "id"}},
				Operator: Eq,
				Operand2: Operand{Type: OperandField, Value: Field{AliasPrefix: "l", Name: "comment_id"}},
			},
		}

		result, err := stmt.AddJoin(Right, "c", "likes", "l", conditions)
		require.NoError(t, err)
		require.Len(t, result.Joins, 1)

		// Navigate to the deeply nested join
		postsJoin := result.Joins[0]
		require.Len(t, postsJoin.Joins, 1)

		commentsJoin := postsJoin.Joins[0]
		require.Len(t, commentsJoin.Joins, 1)

		likesJoin := commentsJoin.Joins[0]
		assert.Equal(t, Right, likesJoin.Type)
		assert.Equal(t, "likes", likesJoin.TableName)
		assert.Equal(t, "l", likesJoin.TableAlias)
		assert.Equal(t, conditions, likesJoin.Conditions)
	})

	t.Run("from table not found in join tree", func(t *testing.T) {
		t.Parallel()

		stmt := Statement{
			Kind:       Select,
			TableName:  "users",
			TableAlias: "u",
			Joins: []Join{
				{
					Type:       Inner,
					TableName:  "posts",
					TableAlias: "p",
				},
			},
		}

		_, err := stmt.AddJoin(Inner, "x", "comments", "c", Conditions{})
		require.Error(t, err)
		assert.Equal(t, `could not find from table alias "x" in existing joins`, err.Error())
	})

	t.Run("multiple joins at same level", func(t *testing.T) {
		t.Parallel()

		stmt := Statement{
			Kind:       Select,
			TableName:  "users",
			TableAlias: "u",
			Joins: []Join{
				{
					Type:       Inner,
					TableName:  "posts",
					TableAlias: "p",
				},
				{
					Type:       Left,
					TableName:  "comments",
					TableAlias: "c",
				},
			},
		}

		// Add a join from the second join (comments)
		conditions := Conditions{
			{
				Operand1: Operand{Type: OperandField, Value: Field{AliasPrefix: "c", Name: "id"}},
				Operator: Eq,
				Operand2: Operand{Type: OperandField, Value: Field{AliasPrefix: "l", Name: "comment_id"}},
			},
		}

		result, err := stmt.AddJoin(Inner, "c", "likes", "l", conditions)
		require.NoError(t, err)
		require.Len(t, result.Joins, 2)

		// First join should remain unchanged
		assert.Equal(t, "posts", result.Joins[0].TableName)
		assert.Len(t, result.Joins[0].Joins, 0)

		// Second join should have the nested join
		commentsJoin := result.Joins[1]
		require.Len(t, commentsJoin.Joins, 1)

		likesJoin := commentsJoin.Joins[0]
		assert.Equal(t, "likes", likesJoin.TableName)
		assert.Equal(t, conditions, likesJoin.Conditions)
	})

	t.Run("complex join tree", func(t *testing.T) {
		t.Parallel()

		// Build a complex tree:
		// users
		//   -> posts
		//      -> comments
		//      -> tags
		//   -> profiles

		stmt := Statement{
			Kind:       Select,
			TableName:  "users",
			TableAlias: "u",
		}

		var err error

		// Add posts join
		stmt, err = stmt.AddJoin(Inner, "u", "posts", "p", Conditions{})
		require.NoError(t, err)

		// Add profiles join
		stmt, err = stmt.AddJoin(Left, "u", "profiles", "prof", Conditions{})
		require.NoError(t, err)

		// Add comments join to posts
		stmt, err = stmt.AddJoin(Left, "p", "comments", "c", Conditions{})
		require.NoError(t, err)

		// Add tags join to posts
		stmt, err = stmt.AddJoin(Inner, "p", "tags", "t", Conditions{})
		require.NoError(t, err)

		// Verify structure
		require.Len(t, stmt.Joins, 2)

		// Verify posts join
		postsJoin := stmt.Joins[0]
		assert.Equal(t, "posts", postsJoin.TableName)
		require.Len(t, postsJoin.Joins, 2)
		assert.Equal(t, "comments", postsJoin.Joins[0].TableName)
		assert.Equal(t, "tags", postsJoin.Joins[1].TableName)

		// Verify profiles join
		profilesJoin := stmt.Joins[1]
		assert.Equal(t, "profiles", profilesJoin.TableName)
		assert.Len(t, profilesJoin.Joins, 0)
	})

	t.Run("all join types", func(t *testing.T) {
		t.Parallel()

		stmt := Statement{
			Kind:       Select,
			TableName:  "users",
			TableAlias: "u",
		}

		var err error

		// Inner join
		stmt, err = stmt.AddJoin(Inner, "u", "posts", "p", Conditions{})
		require.NoError(t, err)
		assert.Equal(t, Inner, stmt.Joins[0].Type)

		// Left join
		stmt, err = stmt.AddJoin(Left, "u", "comments", "c", Conditions{})
		require.NoError(t, err)
		assert.Equal(t, Left, stmt.Joins[1].Type)

		// Right join
		stmt, err = stmt.AddJoin(Right, "u", "likes", "l", Conditions{})
		require.NoError(t, err)
		assert.Equal(t, Right, stmt.Joins[2].Type)
	})
}

func TestJoin_FromTableAlias(t *testing.T) {
	t.Parallel()

	aJoin := Join{
		TableName:  "posts",
		TableAlias: "p",
	}
	assert.Empty(t, aJoin.FromTableAlias())

	aJoin.Conditions = Conditions{
		{
			Operand1: Operand{Type: OperandField, Value: Field{AliasPrefix: "u", Name: "id"}},
			Operator: Eq,
			Operand2: Operand{Type: OperandField, Value: Field{AliasPrefix: "p", Name: "user_id"}},
		},
	}
	assert.Equal(t, "u", aJoin.FromTableAlias())
}
