package e2etests

import (
	"context"
	"database/sql"
)

func (s *TestSuite) TestInsertSelect() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	var createArchivedUsersTableSQL = `create table "archived_users" (
		id int8 primary key autoincrement,
		email varchar(255),
		name text
	);`
	_, err = s.db.Exec(createArchivedUsersTableSQL)
	s.Require().NoError(err)

	// Seed some users.
	s.execQuery(`insert into users("email", "name") values('alice@example.com', 'Alice');`, 1)
	s.execQuery(`insert into users("email", "name") values('bob@example.com', 'Bob');`, 1)
	s.execQuery(`insert into users("email", "name") values('carol@example.com', 'Carol');`, 1)

	s.Run("basic INSERT SELECT copies rows", func() {
		result, err := s.db.ExecContext(
			context.Background(),
			`insert into archived_users("email", "name") select email, name from users;`,
		)
		s.Require().NoError(err)
		rowsAffected, err := result.RowsAffected()
		s.Require().NoError(err)
		s.Equal(int64(3), rowsAffected)

		rows, err := s.db.QueryContext(context.Background(), `select id, email, name from archived_users order by id;`)
		s.Require().NoError(err)
		defer rows.Close()

		type archivedUser struct {
			ID    int64
			Email sql.NullString
			Name  sql.NullString
		}
		var archived []archivedUser
		for rows.Next() {
			var u archivedUser
			s.Require().NoError(rows.Scan(&u.ID, &u.Email, &u.Name))
			archived = append(archived, u)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(archived, 3)
		s.Equal("alice@example.com", archived[0].Email.String)
		s.Equal("Alice", archived[0].Name.String)
		s.Equal("bob@example.com", archived[1].Email.String)
		s.Equal("carol@example.com", archived[2].Email.String)
	})

	s.Run("INSERT SELECT with WHERE filters rows", func() {
		// Truncate archived_users first.
		s.execQuery(`delete from archived_users where id > 0;`, 3)

		result, err := s.db.ExecContext(
			context.Background(),
			`insert into archived_users("email", "name") select email, name from users where name = 'Alice';`,
		)
		s.Require().NoError(err)
		rowsAffected, err := result.RowsAffected()
		s.Require().NoError(err)
		s.Equal(int64(1), rowsAffected)

		rows, err := s.db.QueryContext(context.Background(), `select id, email, name from archived_users;`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var id int64
			var email, name sql.NullString
			s.Require().NoError(rows.Scan(&id, &email, &name))
			names = append(names, name.String)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(names, 1)
		s.Equal("Alice", names[0])
	})

	s.Run("INSERT SELECT from empty result inserts 0 rows", func() {
		// Truncate archived_users first.
		s.execQuery(`delete from archived_users where id > 0;`, 1)

		result, err := s.db.ExecContext(
			context.Background(),
			`insert into archived_users("email", "name") select email, name from users where name = 'NoSuchUser';`,
		)
		s.Require().NoError(err)
		rowsAffected, err := result.RowsAffected()
		s.Require().NoError(err)
		s.Equal(int64(0), rowsAffected)

		rows, err := s.db.QueryContext(context.Background(), `select id, email, name from archived_users;`)
		s.Require().NoError(err)
		defer rows.Close()
		s.False(rows.Next())
		s.Require().NoError(rows.Err())
	})

	s.Run("INSERT SELECT with ON CONFLICT DO NOTHING skips duplicates", func() {
		_, err = s.db.Exec(`drop table archived_users;`)
		s.Require().NoError(err)
		_, err = s.db.Exec(`create table "archived_users" (
			id int8 primary key autoincrement,
			email varchar(255) unique,
			name text
		);`)
		s.Require().NoError(err)

		// Insert Alice first.
		s.execQuery(`insert into archived_users("email", "name") values('alice@example.com', 'Alice');`, 1)

		// INSERT SELECT all users ON CONFLICT DO NOTHING — Alice should be skipped.
		result, err := s.db.ExecContext(
			context.Background(),
			`insert into archived_users("email", "name") select email, name from users ON CONFLICT DO NOTHING;`,
		)
		s.Require().NoError(err)
		rowsAffected, err := result.RowsAffected()
		s.Require().NoError(err)
		// Bob and Carol inserted; Alice skipped.
		s.Equal(int64(2), rowsAffected)
	})

	s.Run("INSERT SELECT with RETURNING returns inserted rows", func() {
		_, err = s.db.Exec(`drop table archived_users;`)
		s.Require().NoError(err)
		_, err = s.db.Exec(`create table "archived_users" (
			id int8 primary key autoincrement,
			email varchar(255),
			name text
		);`)
		s.Require().NoError(err)

		rows, err := s.db.QueryContext(
			context.Background(),
			`insert into archived_users("email", "name") select email, name from users where name = 'Bob' RETURNING id, email, name;`,
		)
		s.Require().NoError(err)
		defer rows.Close()

		var count int
		for rows.Next() {
			var id int64
			var email, name sql.NullString
			s.Require().NoError(rows.Scan(&id, &email, &name))
			s.Equal("bob@example.com", email.String)
			s.Equal("Bob", name.String)
			count++
		}
		s.Require().NoError(rows.Err())
		s.Equal(1, count)
	})
}
