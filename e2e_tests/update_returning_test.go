package e2etests

import (
	"context"
	"database/sql"
)

func (s *TestSuite) TestUpdateReturning() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	s.execQuery(`insert into users("email", "name") values
		('alice@example.com', 'Alice'),
		('bob@example.com', 'Bob'),
		('carol@example.com', 'Carol');`, 3)

	s.Run("RETURNING selected columns", func() {
		rows, err := s.db.QueryContext(
			context.Background(),
			`UPDATE users SET name = 'Alice Updated' WHERE id = 1 RETURNING id, name;`,
		)
		s.Require().NoError(err)
		defer rows.Close()

		var got []user
		for rows.Next() {
			var u user
			s.Require().NoError(rows.Scan(&u.ID, &u.Name))
			got = append(got, u)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(got, 1)
		s.Equal(int64(1), got[0].ID)
		s.Equal("Alice Updated", got[0].Name.String)
	})

	s.Run("RETURNING * returns all columns", func() {
		rows, err := s.db.QueryContext(
			context.Background(),
			`UPDATE users SET name = 'Bob Updated' WHERE id = 2 RETURNING *;`,
		)
		s.Require().NoError(err)
		defer rows.Close()

		var got []user
		for rows.Next() {
			var u user
			s.Require().NoError(rows.Scan(&u.ID, &u.Email, &u.Name, &u.Created))
			got = append(got, u)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(got, 1)
		s.Equal(int64(2), got[0].ID)
		s.Equal("Bob Updated", got[0].Name.String)
		s.Equal("bob@example.com", got[0].Email.String)
	})

	s.Run("RETURNING multiple rows", func() {
		rows, err := s.db.QueryContext(
			context.Background(),
			`UPDATE users SET name = 'Renamed' WHERE id = 1 OR id = 3 RETURNING id, name;`,
		)
		s.Require().NoError(err)
		defer rows.Close()

		type row struct {
			id   int64
			name string
		}
		var got []row
		for rows.Next() {
			var r row
			s.Require().NoError(rows.Scan(&r.id, &r.name))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(got, 2)
		for _, r := range got {
			s.Equal("Renamed", r.name)
		}
	})

	s.Run("RETURNING when WHERE matches no rows returns empty result", func() {
		rows, err := s.db.QueryContext(
			context.Background(),
			`UPDATE users SET name = 'Ghost' WHERE id = 9999 RETURNING id, name;`,
		)
		s.Require().NoError(err)
		defer rows.Close()

		s.False(rows.Next())
		s.Require().NoError(rows.Err())
	})

	s.Run("RETURNING with bind parameter", func() {
		var id int64
		var name sql.NullString
		err := s.db.QueryRowContext(
			context.Background(),
			`UPDATE users SET name = ? WHERE id = 2 RETURNING id, name;`,
			"Bob Final",
		).Scan(&id, &name)
		s.Require().NoError(err)
		s.Equal(int64(2), id)
		s.Equal("Bob Final", name.String)
	})
}

func (s *TestSuite) TestDeleteReturning() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	s.execQuery(`insert into users("email", "name") values
		('alice@example.com', 'Alice'),
		('bob@example.com', 'Bob'),
		('carol@example.com', 'Carol');`, 3)

	s.Run("RETURNING deleted row columns", func() {
		rows, err := s.db.QueryContext(
			context.Background(),
			`DELETE FROM users WHERE id = 1 RETURNING id, name;`,
		)
		s.Require().NoError(err)
		defer rows.Close()

		var got []user
		for rows.Next() {
			var u user
			s.Require().NoError(rows.Scan(&u.ID, &u.Name))
			got = append(got, u)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(got, 1)
		s.Equal(int64(1), got[0].ID)
		s.Equal("Alice", got[0].Name.String)

		// Confirm the row was actually deleted.
		remaining := s.collectUsers(`SELECT id, email, name, created FROM users;`)
		s.Len(remaining, 2)
	})

	s.Run("RETURNING * on deleted row", func() {
		rows, err := s.db.QueryContext(
			context.Background(),
			`DELETE FROM users WHERE id = 2 RETURNING *;`,
		)
		s.Require().NoError(err)
		defer rows.Close()

		var got []user
		for rows.Next() {
			var u user
			s.Require().NoError(rows.Scan(&u.ID, &u.Email, &u.Name, &u.Created))
			got = append(got, u)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(got, 1)
		s.Equal("bob@example.com", got[0].Email.String)
	})

	s.Run("RETURNING when WHERE matches no rows returns empty result", func() {
		rows, err := s.db.QueryContext(
			context.Background(),
			`DELETE FROM users WHERE id = 9999 RETURNING id, name;`,
		)
		s.Require().NoError(err)
		defer rows.Close()

		s.False(rows.Next())
		s.Require().NoError(rows.Err())
	})
}
