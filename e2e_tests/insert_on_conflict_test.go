package e2etests

import (
	"context"

	"github.com/RichardKnop/minisql/internal/minisql"
)

func (s *TestSuite) TestInsertOnConflictDoNothing() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	// Insert an initial row
	s.execQuery(`insert into users("email", "name") values('alice@example.com', 'Alice');`, 1)

	s.Run("ON CONFLICT DO NOTHING skips duplicate unique-index row", func() {
		// alice@example.com already exists — should be silently ignored
		result, err := s.db.ExecContext(
			context.Background(),
			`insert into users("email", "name") values('alice@example.com', 'Alice Duplicate') ON CONFLICT DO NOTHING;`,
		)
		s.Require().NoError(err)
		rowsAffected, err := result.RowsAffected()
		s.Require().NoError(err)
		s.Equal(int64(0), rowsAffected)
	})

	s.Run("ON CONFLICT DO NOTHING inserts non-conflicting row", func() {
		result, err := s.db.ExecContext(
			context.Background(),
			`insert into users("email", "name") values('bob@example.com', 'Bob') ON CONFLICT DO NOTHING;`,
		)
		s.Require().NoError(err)
		rowsAffected, err := result.RowsAffected()
		s.Require().NoError(err)
		s.Equal(int64(1), rowsAffected)
	})

	s.Run("Without ON CONFLICT duplicate unique-index row returns error", func() {
		result, err := s.db.ExecContext(
			context.Background(),
			`insert into users("email", "name") values('alice@example.com', 'Alice Again');`,
		)
		s.Require().Error(err)
		s.ErrorIs(err, minisql.ErrDuplicateKey)
		s.Nil(result)
	})

	s.Run("ON CONFLICT DO NOTHING skips duplicate primary key row", func() {
		// Insert a row with explicit primary key
		s.execQuery(`insert into users("id", "email", "name") values(999, 'charlie@example.com', 'Charlie');`, 1)

		result, err := s.db.ExecContext(
			context.Background(),
			`insert into users("id", "email", "name") values(999, 'charlie2@example.com', 'Charlie Duplicate') ON CONFLICT DO NOTHING;`,
		)
		s.Require().NoError(err)
		rowsAffected, err := result.RowsAffected()
		s.Require().NoError(err)
		s.Equal(int64(0), rowsAffected)
	})
}
