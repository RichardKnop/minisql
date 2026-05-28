package e2etests

import (
	"context"

	"github.com/RichardKnop/minisql/internal/minisql"
)

func (s *TestSuite) TestInsertOnConflictDoUpdate() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	// Insert an initial row that will be the target of upserts.
	s.execQuery(`insert into users("email", "name") values('alice@example.com', 'Alice');`, 1)

	s.Run("ON CONFLICT DO UPDATE updates conflicting unique-index row", func() {
		result, err := s.db.ExecContext(
			context.Background(),
			`insert into users("email", "name") values('alice@example.com', 'Alice Updated') ON CONFLICT DO UPDATE SET name = 'Alice Updated';`,
		)
		s.Require().NoError(err)
		rowsAffected, err := result.RowsAffected()
		s.Require().NoError(err)
		s.Equal(int64(1), rowsAffected)

		// Verify the name was actually updated.
		users := s.collectUsers(`select id, email, name, created from users where email = 'alice@example.com';`)
		s.Require().Len(users, 1)
		s.Equal("Alice Updated", users[0].Name.String)
	})

	s.Run("ON CONFLICT DO UPDATE inserts non-conflicting row normally", func() {
		result, err := s.db.ExecContext(
			context.Background(),
			`insert into users("email", "name") values('bob@example.com', 'Bob') ON CONFLICT DO UPDATE SET name = 'Bob Updated';`,
		)
		s.Require().NoError(err)
		rowsAffected, err := result.RowsAffected()
		s.Require().NoError(err)
		s.Equal(int64(1), rowsAffected)

		users := s.collectUsers(`select id, email, name, created from users where email = 'bob@example.com';`)
		s.Require().Len(users, 1)
		s.Equal("Bob", users[0].Name.String)
	})

	s.Run("ON CONFLICT DO UPDATE with no actual change reports 0 rows affected", func() {
		// Same name value — cursor.update returns false → RowsAffected == 0.
		result, err := s.db.ExecContext(
			context.Background(),
			`insert into users("email", "name") values('alice@example.com', 'irrelevant') ON CONFLICT DO UPDATE SET name = 'Alice Updated';`,
		)
		s.Require().NoError(err)
		rowsAffected, err := result.RowsAffected()
		s.Require().NoError(err)
		s.Equal(int64(0), rowsAffected)
	})

	s.Run("ON CONFLICT DO UPDATE with explicit primary key conflict updates row", func() {
		s.execQuery(`insert into users("id", "email", "name") values(999, 'charlie@example.com', 'Charlie');`, 1)

		result, err := s.db.ExecContext(
			context.Background(),
			`insert into users("id", "email", "name") values(999, 'charlie2@example.com', 'Charlie Upserted') ON CONFLICT DO UPDATE SET name = 'Charlie Upserted';`,
		)
		s.Require().NoError(err)
		rowsAffected, err := result.RowsAffected()
		s.Require().NoError(err)
		s.Equal(int64(1), rowsAffected)

		users := s.collectUsers(`select id, email, name, created from users where id = 999;`)
		s.Require().Len(users, 1)
		s.Equal("Charlie Upserted", users[0].Name.String)
		// Email should be unchanged — it was not in the SET clause.
		s.Equal("charlie@example.com", users[0].Email.String)
	})

	s.Run("ON CONFLICT DO UPDATE with placeholder updates conflicting row", func() {
		stmt, err := s.db.PrepareContext(
			context.Background(),
			`insert into users("email", "name") values('alice@example.com', ?) ON CONFLICT DO UPDATE SET name = ?;`,
		)
		s.Require().NoError(err)
		defer stmt.Close()

		result, err := stmt.ExecContext(context.Background(), "irrelevant", "Alice Via Placeholder")
		s.Require().NoError(err)
		rowsAffected, err := result.RowsAffected()
		s.Require().NoError(err)
		s.Equal(int64(1), rowsAffected)

		users := s.collectUsers(`select id, email, name, created from users where email = 'alice@example.com';`)
		s.Require().Len(users, 1)
		s.Equal("Alice Via Placeholder", users[0].Name.String)
	})
}

func (s *TestSuite) TestInsertOnConflictDoUpdateNow() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	s.execQuery(`insert into users("email", "name") values('alice@example.com', 'Alice');`, 1)

	// Pin created to a known point in the past so the post-upsert assertion
	// is unambiguous regardless of clock resolution.
	s.execQuery(`update users set created = '2000-01-01 00:00:00.000000' where email = 'alice@example.com';`, 1)

	past := s.collectUsers(`select id, email, name, created from users where email = 'alice@example.com';`)
	s.Require().Len(past, 1)
	pastCreated := past[0].Created

	s.Run("ON CONFLICT DO UPDATE SET col = NOW() refreshes the timestamp", func() {
		result, err := s.db.ExecContext(
			context.Background(),
			`insert into users("email", "name") values('alice@example.com', 'Alice') ON CONFLICT DO UPDATE SET created = NOW();`,
		)
		s.Require().NoError(err)
		rowsAffected, err := result.RowsAffected()
		s.Require().NoError(err)
		s.Equal(int64(1), rowsAffected)

		after := s.collectUsers(`select id, email, name, created from users where email = 'alice@example.com';`)
		s.Require().Len(after, 1)
		// created must be strictly after the pinned past value.
		s.True(after[0].Created.After(pastCreated))
	})

	s.Run("ON CONFLICT DO UPDATE SET col = NOW() on non-conflicting row inserts normally", func() {
		result, err := s.db.ExecContext(
			context.Background(),
			`insert into users("email", "name") values('bob@example.com', 'Bob') ON CONFLICT DO UPDATE SET created = NOW();`,
		)
		s.Require().NoError(err)
		rowsAffected, err := result.RowsAffected()
		s.Require().NoError(err)
		s.Equal(int64(1), rowsAffected)

		bob := s.collectUsers(`select id, email, name, created from users where email = 'bob@example.com';`)
		s.Require().Len(bob, 1)
		s.False(bob[0].Created.IsZero())
	})
}

func (s *TestSuite) TestInsertOnConflictDoUpdateExcluded() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	s.execQuery(`insert into users("email", "name") values('alice@example.com', 'Alice');`, 1)
	s.execQuery(`insert into users("email", "name") values('bob@example.com', 'Bob');`, 1)

	s.Run("EXCLUDED.col resolves to the proposed value on conflict", func() {
		result, err := s.db.ExecContext(
			context.Background(),
			`insert into users("email", "name") values('alice@example.com', 'Alice V2') ON CONFLICT DO UPDATE SET name = EXCLUDED.name;`,
		)
		s.Require().NoError(err)
		rowsAffected, err := result.RowsAffected()
		s.Require().NoError(err)
		s.Equal(int64(1), rowsAffected)

		users := s.collectUsers(`select id, email, name, created from users where email = 'alice@example.com';`)
		s.Require().Len(users, 1)
		s.Equal("Alice V2", users[0].Name.String)
	})

	s.Run("EXCLUDED.col in multi-row insert applies the per-row proposed value", func() {
		// Both rows conflict; each should receive its own proposed name.
		result, err := s.db.ExecContext(
			context.Background(),
			`insert into users("email", "name") values('alice@example.com', 'Alice V3'), ('bob@example.com', 'Bob V3') ON CONFLICT DO UPDATE SET name = EXCLUDED.name;`,
		)
		s.Require().NoError(err)
		rowsAffected, err := result.RowsAffected()
		s.Require().NoError(err)
		s.Equal(int64(2), rowsAffected)

		alice := s.collectUsers(`select id, email, name, created from users where email = 'alice@example.com';`)
		s.Require().Len(alice, 1)
		s.Equal("Alice V3", alice[0].Name.String)

		bob := s.collectUsers(`select id, email, name, created from users where email = 'bob@example.com';`)
		s.Require().Len(bob, 1)
		s.Equal("Bob V3", bob[0].Name.String)
	})

	s.Run("EXCLUDED.col mixed with literal in same SET clause", func() {
		result, err := s.db.ExecContext(
			context.Background(),
			`insert into users("email", "name") values('alice@example.com', 'Alice V4') ON CONFLICT DO UPDATE SET name = EXCLUDED.name;`,
		)
		s.Require().NoError(err)
		rowsAffected, err := result.RowsAffected()
		s.Require().NoError(err)
		s.Equal(int64(1), rowsAffected)

		users := s.collectUsers(`select id, email, name, created from users where email = 'alice@example.com';`)
		s.Require().Len(users, 1)
		s.Equal("Alice V4", users[0].Name.String)
	})

	s.Run("non-conflicting row with EXCLUDED syntax inserts normally", func() {
		result, err := s.db.ExecContext(
			context.Background(),
			`insert into users("email", "name") values('charlie@example.com', 'Charlie') ON CONFLICT DO UPDATE SET name = EXCLUDED.name;`,
		)
		s.Require().NoError(err)
		rowsAffected, err := result.RowsAffected()
		s.Require().NoError(err)
		s.Equal(int64(1), rowsAffected)

		users := s.collectUsers(`select id, email, name, created from users where email = 'charlie@example.com';`)
		s.Require().Len(users, 1)
		s.Equal("Charlie", users[0].Name.String)
	})
}

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
		s.Require().ErrorIs(err, minisql.ErrDuplicateKey)
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
