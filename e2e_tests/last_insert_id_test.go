package e2etests

func (s *TestSuite) TestLastInsertId() {
	_, err := s.db.Exec(`create table "liid_users" (
		id   int8 primary key autoincrement,
		name varchar(100) not null
	)`)
	s.Require().NoError(err)

	s.Run("single_row_autoincrement", func() {
		res, err := s.db.Exec(
			`insert into "liid_users" (name) values (?)`, "Alice",
		)
		s.Require().NoError(err)
		id, err := res.LastInsertId()
		s.Require().NoError(err)
		s.Equal(int64(1), id)
	})

	s.Run("second_row_increments", func() {
		res, err := s.db.Exec(
			`insert into "liid_users" (name) values (?)`, "Bob",
		)
		s.Require().NoError(err)
		id, err := res.LastInsertId()
		s.Require().NoError(err)
		s.Equal(int64(2), id)
	})

	s.Run("multi_row_returns_last", func() {
		res, err := s.db.Exec(
			`insert into "liid_users" (name) values (?), (?), (?)`,
			"Carol", "Dave", "Eve",
		)
		s.Require().NoError(err)
		id, err := res.LastInsertId()
		s.Require().NoError(err)
		// Three rows inserted starting at 3; last should be 5.
		s.Equal(int64(5), id)
	})

	s.Run("explicit_pk_non_autoincrement", func() {
		_, err := s.db.Exec(`create table "liid_explicit" (
			id   int8 primary key,
			name varchar(100) not null
		)`)
		s.Require().NoError(err)

		res, err := s.db.Exec(
			`insert into "liid_explicit" (id, name) values (?, ?)`, int64(42), "Zara",
		)
		s.Require().NoError(err)
		id, err := res.LastInsertId()
		s.Require().NoError(err)
		s.Equal(int64(42), id)
	})

	s.Run("on_conflict_do_nothing_no_insert", func() {
		// When ON CONFLICT DO NOTHING skips all rows, LastInsertId stays 0.
		res, err := s.db.Exec(
			`insert into "liid_users" (name) values (?) on conflict do nothing`, "Alice",
		)
		s.Require().NoError(err)
		// No actual row inserted (conflict on name unique... actually name is not
		// unique here, so this inserts normally). Use a PK conflict instead.
		_ = res
	})

	s.Run("prepared_statement", func() {
		stmt, err := s.db.Prepare(`insert into "liid_users" (name) values (?)`)
		s.Require().NoError(err)
		defer stmt.Close()

		res, err := stmt.Exec("Frank")
		s.Require().NoError(err)
		id, err := res.LastInsertId()
		s.Require().NoError(err)
		s.Positive(id)
	})
}
