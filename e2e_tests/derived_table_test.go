package e2etests

func (s *TestSuite) TestDerivedTable() {
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

	// Seed orders: Alice 2, Bob 1, Carol 0.
	_, err = s.db.Exec(`insert into "orders" (user_id, amount) values (1, 500)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "orders" (user_id, amount) values (1, 300)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "orders" (user_id, amount) values (2, 200)`)
	s.Require().NoError(err)

	s.Run("simple_filter_subquery", func() {
		// Derived table filters users with score > 80; outer query selects all.
		rows, err := s.db.Query(
			`select t.name from (select name, score from "users" where score > 80) t`)
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

	s.Run("outer_where_filters_derived_rows", func() {
		// Inner: all users; outer: filter by score.
		rows, err := s.db.Query(
			`select t.name from (select name, score from "users") t where t.score >= 85`)
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

	s.Run("select_star_from_derived", func() {
		// SELECT * should expose all inner columns.
		rows, err := s.db.Query(
			`select * from (select name, score from "users" where score = 70) t`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		var scores []int64
		for rows.Next() {
			var n string
			var sc int64
			s.Require().NoError(rows.Scan(&n, &sc))
			names = append(names, n)
			scores = append(scores, sc)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(names, 1)
		s.Equal("Bob", names[0])
		s.Equal(int64(70), scores[0])
	})

	s.Run("aggregate_alias_count_group_by", func() {
		// Derived table: count orders per user; outer: filter by count.
		rows, err := s.db.Query(
			`select t.user_id, t.cnt
			 from (select user_id, COUNT(*) AS cnt from "orders" group by user_id) t
			 where t.cnt > 1`)
		s.Require().NoError(err)
		defer rows.Close()

		type result struct {
			userID int64
			cnt    int64
		}
		var results []result
		for rows.Next() {
			var r result
			s.Require().NoError(rows.Scan(&r.userID, &r.cnt))
			results = append(results, r)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(results, 1)
		s.Equal(int64(1), results[0].userID)
		s.Equal(int64(2), results[0].cnt)
	})

	s.Run("derived_table_with_limit", func() {
		// Inner: top 2 users by score; outer: select just the names.
		rows, err := s.db.Query(
			`select t.name from (select name, score from "users" order by score desc limit 2) t`)
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

	s.Run("derived_table_as_alias_keyword", func() {
		// AS keyword before alias should be optional and accepted.
		rows, err := s.db.Query(
			`select sub.name from (select name from "users" where score = 90) as sub`)
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
}
