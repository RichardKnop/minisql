package e2etests

import (
	"fmt"
)

func (s *TestSuite) TestLike_PrefixMatch() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)

	for _, name := range []string{"Alice Smith", "Alice Jones", "Bob Brown", "Charlie Davis"} {
		_, err := stmt.Exec(fmt.Sprintf("%s@example.com", name), name)
		s.Require().NoError(err)
	}

	rows, err := s.db.Query(`select name from "users" WHERE name LIKE 'Alice%'`)
	s.Require().NoError(err)
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		s.Require().NoError(rows.Scan(&name))
		names = append(names, name)
	}
	s.Require().NoError(rows.Err())
	s.Require().Len(names, 2)
	for _, name := range names {
		s.Contains(name, "Alice")
	}
}

func (s *TestSuite) TestLike_SuffixMatch() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)

	emails := []string{
		"alice@example.com",
		"bob@example.com",
		"charlie@gmail.com",
		"dave@yahoo.com",
	}
	for i, email := range emails {
		_, err := stmt.Exec(email, fmt.Sprintf("User%d", i))
		s.Require().NoError(err)
	}

	rows, err := s.db.Query(`select email from "users" WHERE email LIKE '%@example.com'`)
	s.Require().NoError(err)
	defer rows.Close()

	var got []string
	for rows.Next() {
		var email string
		s.Require().NoError(rows.Scan(&email))
		got = append(got, email)
	}
	s.Require().NoError(rows.Err())
	s.Require().Len(got, 2)
	for _, email := range got {
		s.Contains(email, "@example.com")
	}
}

func (s *TestSuite) TestLike_SubstringMatch() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)

	names := []string{"John Smith", "Jonathan Doe", "Jane Johnson", "Bob Jones"}
	for _, name := range names {
		_, err := stmt.Exec(fmt.Sprintf("%s@example.com", name), name)
		s.Require().NoError(err)
	}

	rows, err := s.db.Query(`select name from "users" WHERE name LIKE '%John%'`)
	s.Require().NoError(err)
	defer rows.Close()

	var got []string
	for rows.Next() {
		var name string
		s.Require().NoError(rows.Scan(&name))
		got = append(got, name)
	}
	s.Require().NoError(rows.Err())
	// "John Smith" and "Jane Johnson" contain "John"
	s.Require().Len(got, 2)
	for _, name := range got {
		s.Contains(name, "John")
	}
}

func (s *TestSuite) TestLike_UnderscoreWildcard() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)

	for _, name := range []string{"John", "Joan", "Jane", "Jobn"} {
		_, err := stmt.Exec(fmt.Sprintf("%s@example.com", name), name)
		s.Require().NoError(err)
	}

	rows, err := s.db.Query(`select name from "users" WHERE name LIKE 'Jo_n'`)
	s.Require().NoError(err)
	defer rows.Close()

	var got []string
	for rows.Next() {
		var name string
		s.Require().NoError(rows.Scan(&name))
		got = append(got, name)
	}
	s.Require().NoError(rows.Err())
	// "John", "Joan", "Jobn" all match "Jo_n"; "Jane" does not
	s.Require().Len(got, 3)
}

func (s *TestSuite) TestLike_NoMatch() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)

	for i, name := range []string{"Alice", "Bob", "Charlie"} {
		_, err := stmt.Exec(fmt.Sprintf("user%d@example.com", i), name)
		s.Require().NoError(err)
	}

	var count int
	err = s.db.QueryRow(`select count(*) from "users" WHERE name LIKE 'Z%'`).Scan(&count)
	s.Require().NoError(err)
	s.Equal(0, count)
}

func (s *TestSuite) TestNotLike_Basic() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)

	for _, name := range []string{"Alice Smith", "Alice Jones", "Bob Brown", "Charlie Davis"} {
		_, err := stmt.Exec(fmt.Sprintf("%s@example.com", name), name)
		s.Require().NoError(err)
	}

	rows, err := s.db.Query(`select name from "users" WHERE name NOT LIKE 'Alice%'`)
	s.Require().NoError(err)
	defer rows.Close()

	var got []string
	for rows.Next() {
		var name string
		s.Require().NoError(rows.Scan(&name))
		got = append(got, name)
	}
	s.Require().NoError(rows.Err())
	s.Require().Len(got, 2)
	for _, name := range got {
		s.NotContains(name, "Alice")
	}
}

func (s *TestSuite) TestLike_WithPlaceholder() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	insertStmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)

	for _, name := range []string{"Richard Knop", "Richard Nixon", "Bob Smith"} {
		_, err := insertStmt.Exec(fmt.Sprintf("%s@example.com", name), name)
		s.Require().NoError(err)
	}

	selectStmt, err := s.db.Prepare(`select name from "users" WHERE name LIKE ?`)
	s.Require().NoError(err)

	rows, err := selectStmt.Query("Richard%")
	s.Require().NoError(err)
	defer rows.Close()

	var got []string
	for rows.Next() {
		var name string
		s.Require().NoError(rows.Scan(&name))
		got = append(got, name)
	}
	s.Require().NoError(rows.Err())
	s.Require().Len(got, 2)
	for _, name := range got {
		s.Contains(name, "Richard")
	}
}

func (s *TestSuite) TestLike_CaseSensitive() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)

	for _, name := range []string{"Alice", "alice", "ALICE"} {
		_, err := stmt.Exec(fmt.Sprintf("%s@example.com", name), name)
		s.Require().NoError(err)
	}

	// LIKE is case-sensitive — only exact-case prefix should match
	var count int
	err = s.db.QueryRow(`select count(*) from "users" WHERE name LIKE 'Alice%'`).Scan(&count)
	s.Require().NoError(err)
	s.Equal(1, count)
}

func (s *TestSuite) TestLike_WithAndCondition() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)

	data := []struct{ email, name string }{
		{"alice@example.com", "Alice Smith"},
		{"alice@gmail.com", "Alice Jones"},
		{"bob@example.com", "Bob Brown"},
	}
	for _, d := range data {
		_, err := stmt.Exec(d.email, d.name)
		s.Require().NoError(err)
	}

	rows, err := s.db.Query(`select name from "users" WHERE name LIKE 'Alice%' AND email LIKE '%@example.com'`)
	s.Require().NoError(err)
	defer rows.Close()

	var got []string
	for rows.Next() {
		var name string
		s.Require().NoError(rows.Scan(&name))
		got = append(got, name)
	}
	s.Require().NoError(rows.Err())
	s.Require().Len(got, 1)
	s.Equal("Alice Smith", got[0])
}

func (s *TestSuite) TestLike_WithVarcharColumn() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)

	emails := []string{"alice@example.com", "alice@gmail.com", "bob@example.com"}
	for i, email := range emails {
		_, err := stmt.Exec(email, fmt.Sprintf("User%d", i))
		s.Require().NoError(err)
	}

	// email is a varchar(255) column
	rows, err := s.db.Query(`select email from "users" WHERE email LIKE 'alice%'`)
	s.Require().NoError(err)
	defer rows.Close()

	var got []string
	for rows.Next() {
		var email string
		s.Require().NoError(rows.Scan(&email))
		got = append(got, email)
	}
	s.Require().NoError(rows.Err())
	s.Require().Len(got, 2)
}

func (s *TestSuite) TestLike_UpdateWithLike() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)

	for _, name := range []string{"Alice Smith", "Bob Brown", "Alice Jones"} {
		_, err := stmt.Exec(fmt.Sprintf("%s@example.com", name), name)
		s.Require().NoError(err)
	}

	// UPDATE with LIKE in WHERE clause
	result, err := s.db.Exec(`update "users" set name = 'Updated' WHERE name LIKE 'Alice%'`)
	s.Require().NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.Require().NoError(err)
	s.Equal(int64(2), rowsAffected)
}

func (s *TestSuite) TestLike_DeleteWithLike() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)

	for _, name := range []string{"Alice Smith", "Bob Brown", "Alice Jones"} {
		_, err := stmt.Exec(fmt.Sprintf("%s@example.com", name), name)
		s.Require().NoError(err)
	}

	_, err = s.db.Exec(`delete from "users" WHERE name LIKE 'Alice%'`)
	s.Require().NoError(err)

	s.countRowsInTable("users", 1)
}
