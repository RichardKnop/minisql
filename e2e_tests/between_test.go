package e2etests

import (
	"fmt"
)

func (s *TestSuite) TestBetween_IntegerRange() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	// Insert users with predictable IDs by specifying them explicitly
	for i := 1; i <= 10; i++ {
		_, err := s.db.Exec(
			fmt.Sprintf(`insert into "users" (id, email, name) values (%d, 'user%d@example.com', 'User %d')`, i, i, i),
		)
		s.Require().NoError(err)
	}

	rows, err := s.db.Query(`select id from "users" WHERE id BETWEEN 3 AND 7`)
	s.Require().NoError(err)
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		s.Require().NoError(rows.Scan(&id))
		ids = append(ids, id)
	}
	s.Require().NoError(rows.Err())
	s.Require().Len(ids, 5)
	for _, id := range ids {
		s.GreaterOrEqual(id, int64(3))
		s.LessOrEqual(id, int64(7))
	}
}

func (s *TestSuite) TestBetween_InclusiveBounds() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	for i := 1; i <= 5; i++ {
		_, err := s.db.Exec(
			fmt.Sprintf(`insert into "users" (id, email, name) values (%d, 'user%d@example.com', 'User %d')`, i, i, i),
		)
		s.Require().NoError(err)
	}

	// Lower and upper bounds are inclusive
	var count int
	err = s.db.QueryRow(`select count(*) from "users" WHERE id BETWEEN 1 AND 5`).Scan(&count)
	s.Require().NoError(err)
	s.Equal(5, count)

	err = s.db.QueryRow(`select count(*) from "users" WHERE id BETWEEN 1 AND 1`).Scan(&count)
	s.Require().NoError(err)
	s.Equal(1, count)

	err = s.db.QueryRow(`select count(*) from "users" WHERE id BETWEEN 5 AND 5`).Scan(&count)
	s.Require().NoError(err)
	s.Equal(1, count)
}

func (s *TestSuite) TestBetween_NoMatch() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	for i := 1; i <= 5; i++ {
		_, err := s.db.Exec(
			fmt.Sprintf(`insert into "users" (id, email, name) values (%d, 'user%d@example.com', 'User %d')`, i, i, i),
		)
		s.Require().NoError(err)
	}

	var count int
	err = s.db.QueryRow(`select count(*) from "users" WHERE id BETWEEN 10 AND 20`).Scan(&count)
	s.Require().NoError(err)
	s.Equal(0, count)
}

func (s *TestSuite) TestNotBetween_Basic() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	for i := 1; i <= 10; i++ {
		_, err := s.db.Exec(
			fmt.Sprintf(`insert into "users" (id, email, name) values (%d, 'user%d@example.com', 'User %d')`, i, i, i),
		)
		s.Require().NoError(err)
	}

	var count int
	err = s.db.QueryRow(`select count(*) from "users" WHERE id NOT BETWEEN 3 AND 7`).Scan(&count)
	s.Require().NoError(err)
	// IDs 1, 2, 8, 9, 10 = 5 rows
	s.Equal(5, count)
}

func (s *TestSuite) TestBetween_StringRange() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)

	for _, name := range []string{"Alice", "Bob", "Charlie", "Dave", "Eve", "Zara"} {
		_, err := stmt.Exec(fmt.Sprintf("%s@example.com", name), name)
		s.Require().NoError(err)
	}

	rows, err := s.db.Query(`select name from "users" WHERE name BETWEEN 'B' AND 'E'`)
	s.Require().NoError(err)
	defer rows.Close()

	var got []string
	for rows.Next() {
		var name string
		s.Require().NoError(rows.Scan(&name))
		got = append(got, name)
	}
	s.Require().NoError(rows.Err())
	// "Bob", "Charlie", "Dave" fall between 'B' and 'E' (lexicographic, inclusive)
	// "Eve" > "E" lexicographically so it is excluded
	s.Require().Len(got, 3)
}

func (s *TestSuite) TestBetween_WithPlaceholder() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	for i := 1; i <= 10; i++ {
		_, err := s.db.Exec(
			fmt.Sprintf(`insert into "users" (id, email, name) values (%d, 'user%d@example.com', 'User %d')`, i, i, i),
		)
		s.Require().NoError(err)
	}

	selectStmt, err := s.db.Prepare(`select count(*) from "users" WHERE id BETWEEN ? AND ?`)
	s.Require().NoError(err)

	var count int
	err = selectStmt.QueryRow(3, 7).Scan(&count)
	s.Require().NoError(err)
	s.Equal(5, count)
}

func (s *TestSuite) TestBetween_WithAdditionalAndCondition() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)

	data := []struct{ email, name string }{
		{"alice@example.com", "Alice"},
		{"bob@example.com", "Bob"},
		{"charlie@example.com", "Charlie"},
		{"dave@gmail.com", "Dave"},
		{"eve@example.com", "Eve"},
	}
	for _, d := range data {
		_, err := stmt.Exec(d.email, d.name)
		s.Require().NoError(err)
	}

	rows, err := s.db.Query(`select name from "users" WHERE name BETWEEN 'A' AND 'D' AND email LIKE '%@example.com'`)
	s.Require().NoError(err)
	defer rows.Close()

	var got []string
	for rows.Next() {
		var name string
		s.Require().NoError(rows.Scan(&name))
		got = append(got, name)
	}
	s.Require().NoError(rows.Err())
	// Alice, Bob, Charlie have @example.com and name <= 'D'; Dave has @gmail.com so excluded
	s.Require().Len(got, 3)
}

func (s *TestSuite) TestBetween_UpdateWithBetween() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	for i := 1; i <= 10; i++ {
		_, err := s.db.Exec(
			fmt.Sprintf(`insert into "users" (id, email, name) values (%d, 'user%d@example.com', 'User %d')`, i, i, i),
		)
		s.Require().NoError(err)
	}

	result, err := s.db.Exec(`update "users" set name = 'Updated' WHERE id BETWEEN 3 AND 7`)
	s.Require().NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.Require().NoError(err)
	s.Equal(int64(5), rowsAffected)
}

func (s *TestSuite) TestBetween_DeleteWithBetween() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	for i := 1; i <= 10; i++ {
		_, err := s.db.Exec(
			fmt.Sprintf(`insert into "users" (id, email, name) values (%d, 'user%d@example.com', 'User %d')`, i, i, i),
		)
		s.Require().NoError(err)
	}

	_, err = s.db.Exec(`delete from "users" WHERE id BETWEEN 3 AND 7`)
	s.Require().NoError(err)

	s.countRowsInTable("users", 5)
}
