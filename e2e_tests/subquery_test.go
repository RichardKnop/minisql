package e2etests

func (s *TestSuite) TestSubquery() {
	// Schema: users and orders tables.
	_, err := s.db.Exec(`create table "users" (
		id    int8 primary key autoincrement,
		name  varchar(100) not null,
		score int8
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "orders" (
		id      int8 primary key autoincrement,
		user_id int8 not null,
		amount  int8 not null
	);`)
	s.Require().NoError(err)

	// Seed users.
	_, err = s.db.Exec(`insert into "users" (name, score) values ('Alice', 90)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "users" (name, score) values ('Bob', 70)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "users" (name, score) values ('Carol', 85)`)
	s.Require().NoError(err)

	// Seed orders referencing user IDs 1 and 2.
	_, err = s.db.Exec(`insert into "orders" (user_id, amount) values (1, 500)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "orders" (user_id, amount) values (1, 300)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "orders" (user_id, amount) values (2, 200)`)
	s.Require().NoError(err)

	s.Run("scalar_subquery_eq", func() {
		// Find users whose score equals Alice's score (90).
		rows, err := s.db.Query(
			`select name from "users" where score = (select score from "users" where name = 'Alice')`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var n string
			s.Require().NoError(rows.Scan(&n))
			names = append(names, n)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(names, 1)
		s.Equal("Alice", names[0])
	})

	s.Run("scalar_subquery_gt", func() {
		// Users with score > Bob's score (70).
		rows, err := s.db.Query(
			`select name from "users" where score > (select score from "users" where name = 'Bob')`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var n string
			s.Require().NoError(rows.Scan(&n))
			names = append(names, n)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(names, 2)
		s.ElementsMatch([]string{"Alice", "Carol"}, names)
	})

	s.Run("scalar_subquery_lte", func() {
		// Users with score <= Carol's score (85).
		rows, err := s.db.Query(
			`select name from "users" where score <= (select score from "users" where name = 'Carol')`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var n string
			s.Require().NoError(rows.Scan(&n))
			names = append(names, n)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(names, 2)
		s.ElementsMatch([]string{"Bob", "Carol"}, names)
	})

	s.Run("in_subquery_finds_users_with_orders", func() {
		// Users who have at least one order (user_ids 1 and 2).
		rows, err := s.db.Query(
			`select name from "users" where id IN (select user_id from "orders")`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var n string
			s.Require().NoError(rows.Scan(&n))
			names = append(names, n)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(names, 2)
		s.ElementsMatch([]string{"Alice", "Bob"}, names)
	})

	s.Run("not_in_subquery_excludes_users_with_orders", func() {
		// Users who have NO orders (only Carol, user_id 3).
		rows, err := s.db.Query(
			`select name from "users" where id NOT IN (select user_id from "orders")`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var n string
			s.Require().NoError(rows.Scan(&n))
			names = append(names, n)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(names, 1)
		s.Equal("Carol", names[0])
	})

	s.Run("scalar_subquery_no_rows_returns_null_no_match", func() {
		// Subquery returns no rows → NULL → equality always false.
		rows, err := s.db.Query(
			`select name from "users" where id = (select id from "orders" where amount = 9999)`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var n string
			s.Require().NoError(rows.Scan(&n))
			names = append(names, n)
		}
		s.Require().NoError(rows.Err())
		s.Empty(names)
	})

	s.Run("update_with_scalar_subquery_in_where", func() {
		// Set Carol's score to match Alice's (90).
		res, err := s.db.Exec(
			`update "users" set score = 90 where id = (select id from "users" where name = 'Carol')`)
		s.Require().NoError(err)
		affected, err := res.RowsAffected()
		s.Require().NoError(err)
		s.Equal(int64(1), affected)

		// Verify.
		row := s.db.QueryRow(`select score from "users" where name = 'Carol'`)
		var score int64
		s.Require().NoError(row.Scan(&score))
		s.Equal(int64(90), score)
	})

	s.Run("delete_with_in_subquery", func() {
		// Delete all orders belonging to Alice (user_id 1).
		res, err := s.db.Exec(
			`delete from "orders" where user_id IN (select id from "users" where name = 'Alice')`)
		s.Require().NoError(err)
		affected, err := res.RowsAffected()
		s.Require().NoError(err)
		s.Equal(int64(2), affected)
	})
}
