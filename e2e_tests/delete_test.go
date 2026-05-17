package e2etests

func (s *TestSuite) TestDelete() {
	_, err := s.db.Exec(`create table "del_users" (
		id   int8 primary key autoincrement,
		name varchar(100) not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(
		`insert into "del_users" (name) values (?), (?), (?)`,
		"Alice", "Bob", "Carol",
	)
	s.Require().NoError(err)

	s.Run("delete_with_where", func() {
		res, err := s.db.Exec(`delete from "del_users" where name = ?`, "Alice")
		s.Require().NoError(err)
		n, err := res.RowsAffected()
		s.Require().NoError(err)
		s.Equal(int64(1), n)

		var count int64
		s.Require().NoError(s.db.QueryRow(`select count(*) from "del_users"`).Scan(&count))
		s.Equal(int64(2), count)
	})

	s.Run("delete_without_where_no_semicolon", func() {
		_, err := s.db.Exec(`insert into "del_users" (name) values (?)`, "Dave")
		s.Require().NoError(err)

		var before int64
		s.Require().NoError(s.db.QueryRow(`select count(*) from "del_users"`).Scan(&before))
		s.Greater(before, int64(0))

		res, err := s.db.Exec(`delete from "del_users"`)
		s.Require().NoError(err)
		n, err := res.RowsAffected()
		s.Require().NoError(err)
		s.Equal(before, n)

		var after int64
		s.Require().NoError(s.db.QueryRow(`select count(*) from "del_users"`).Scan(&after))
		s.Equal(int64(0), after)
	})

	s.Run("delete_without_where_with_semicolon", func() {
		_, err := s.db.Exec(`insert into "del_users" (name) values (?), (?)`, "Eve", "Frank")
		s.Require().NoError(err)

		res, err := s.db.Exec(`delete from "del_users";`)
		s.Require().NoError(err)
		n, err := res.RowsAffected()
		s.Require().NoError(err)
		s.Equal(int64(2), n)

		var after int64
		s.Require().NoError(s.db.QueryRow(`select count(*) from "del_users"`).Scan(&after))
		s.Equal(int64(0), after)
	})
}
