package e2etests

import (
	"fmt"
)

// TestNestedWhere_OrInsideAnd tests WHERE (a OR b) AND c — the classic case that
// was impossible with the 1-level nesting restriction.
func (s *TestSuite) TestNestedWhere_OrInsideAnd() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)

	data := []struct{ email, name string }{
		{"alice@example.com", "Alice"},
		{"bob@example.com", "Bob"},
		{"charlie@example.com", "Charlie"},
		{"dave@other.com", "Dave"},
		{"eve@example.com", "Eve"},
	}
	for _, d := range data {
		_, err := stmt.Exec(d.email, d.name)
		s.Require().NoError(err)
	}

	// WHERE (name = 'Alice' OR name = 'Bob') AND email LIKE '%@example.com'
	// Expected: Alice and Bob (both have @example.com).
	// Dave is excluded (wrong domain). Charlie/Eve don't match name condition.
	rows, err := s.db.Query(`select name from "users" WHERE (name = 'Alice' OR name = 'Bob') AND email LIKE '%@example.com'`)
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
	s.Contains(got, "Alice")
	s.Contains(got, "Bob")
}

// TestNestedWhere_AndInsideOr tests WHERE a OR (b AND c).
func (s *TestSuite) TestNestedWhere_AndInsideOr() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)

	data := []struct{ email, name string }{
		{"alice@example.com", "Alice"},
		{"bob@other.com", "Bob"},
		{"charlie@example.com", "Charlie"},
	}
	for _, d := range data {
		_, err := stmt.Exec(d.email, d.name)
		s.Require().NoError(err)
	}

	// WHERE name = 'Alice' OR (name = 'Charlie' AND email LIKE '%@example.com')
	// Expected: Alice (matches name) and Charlie (matches both inner conditions).
	// Bob is excluded.
	rows, err := s.db.Query(`select name from "users" WHERE name = 'Alice' OR (name = 'Charlie' AND email LIKE '%@example.com')`)
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
	s.Contains(got, "Alice")
	s.Contains(got, "Charlie")
}

// TestNestedWhere_ThreeLevels tests deeper nesting: (a OR b) AND (c OR d).
func (s *TestSuite) TestNestedWhere_ThreeLevels() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	for i := 1; i <= 8; i++ {
		_, err := s.db.Exec(
			fmt.Sprintf(`insert into "users" (id, email, name) values (%d, 'user%d@example.com', 'User %d')`, i, i, i),
		)
		s.Require().NoError(err)
	}

	// WHERE (id = 1 OR id = 2) AND (id = 2 OR id = 3)
	// Only id = 2 satisfies both groups (1∩{2,3} = {2}).
	var count int
	err = s.db.QueryRow(`select count(*) from "users" WHERE (id = 1 OR id = 2) AND (id = 2 OR id = 3)`).Scan(&count)
	s.Require().NoError(err)
	s.Equal(1, count)
}

// TestNestedWhere_UpdateWithNestedCondition tests UPDATE with a nested WHERE.
func (s *TestSuite) TestNestedWhere_UpdateWithNestedCondition() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)

	data := []struct{ email, name string }{
		{"alice@example.com", "Alice"},
		{"bob@example.com", "Bob"},
		{"charlie@other.com", "Charlie"},
	}
	for _, d := range data {
		_, err := stmt.Exec(d.email, d.name)
		s.Require().NoError(err)
	}

	// UPDATE where (name = 'Alice' OR name = 'Bob') AND email LIKE '%@example.com'
	// Should update Alice and Bob (both @example.com), not Charlie.
	result, err := s.db.Exec(`update "users" set name = 'Updated' WHERE (name = 'Alice' OR name = 'Bob') AND email LIKE '%@example.com'`)
	s.Require().NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.Require().NoError(err)
	s.Equal(int64(2), rowsAffected)
}

// TestNestedWhere_DeleteWithNestedCondition tests DELETE with a nested WHERE.
func (s *TestSuite) TestNestedWhere_DeleteWithNestedCondition() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)

	data := []struct{ email, name string }{
		{"alice@example.com", "Alice"},
		{"bob@example.com", "Bob"},
		{"charlie@other.com", "Charlie"},
		{"dave@example.com", "Dave"},
	}
	for _, d := range data {
		_, err := stmt.Exec(d.email, d.name)
		s.Require().NoError(err)
	}

	// DELETE where (name = 'Alice' OR name = 'Bob') AND email LIKE '%@example.com'
	// Deletes Alice and Bob; Charlie and Dave remain.
	_, err = s.db.Exec(`delete from "users" WHERE (name = 'Alice' OR name = 'Bob') AND email LIKE '%@example.com'`)
	s.Require().NoError(err)

	s.countRowsInTable("users", 2)
}

// TestNestedWhere_RedundantParens tests that unnecessary parentheses do not
// change query behaviour.
func (s *TestSuite) TestNestedWhere_RedundantParens() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	for i := 1; i <= 5; i++ {
		_, err := s.db.Exec(
			fmt.Sprintf(`insert into "users" (id, email, name) values (%d, 'user%d@example.com', 'User %d')`, i, i, i),
		)
		s.Require().NoError(err)
	}

	var count int
	err = s.db.QueryRow(`select count(*) from "users" WHERE (id = 3)`).Scan(&count)
	s.Require().NoError(err)
	s.Equal(1, count)
}
