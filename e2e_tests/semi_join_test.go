package e2etests

import "strings"

// TestSemiJoin verifies the semi-join and anti-semi-join optimisation that
// converts eligible IN/NOT IN (subquery) conditions into synthetic Semi /
// AntiSemi join plans instead of materialising the entire subquery result.
func (s *TestSuite) TestSemiJoin() {
	// users and orders tables used throughout.
	_, err := s.db.Exec(`create table "sj_users" (
		id    int8 primary key autoincrement,
		name  varchar(100) not null,
		dept  varchar(50)
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "sj_orders" (
		id      int8 primary key autoincrement,
		user_id int8 not null,
		amount  int8 not null
	)`)
	s.Require().NoError(err)

	// Seed: Alice(1), Bob(2), Carol(3). Alice and Bob have orders; Carol does not.
	_, err = s.db.Exec(`insert into "sj_users" (name, dept) values
		('Alice', 'eng'),
		('Bob',   'mkt'),
		('Carol', 'eng')`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "sj_orders" (user_id, amount) values
		(1, 500),
		(1, 300),
		(2, 200)`)
	s.Require().NoError(err)

	// -------------------------------------------------------------------------
	s.Run("in_subquery_basic", func() {
		// Users who have at least one order.
		rows, err := s.db.Query(
			`select name from "sj_users" where id in (select user_id from "sj_orders")`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var n string
			s.Require().NoError(rows.Scan(&n))
			names = append(names, n)
		}
		s.Require().NoError(rows.Err())
		s.ElementsMatch([]string{"Alice", "Bob"}, names)
	})

	// -------------------------------------------------------------------------
	s.Run("not_in_subquery_basic", func() {
		// Users who have NO orders.
		rows, err := s.db.Query(
			`select name from "sj_users" where id not in (select user_id from "sj_orders")`)
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

	// -------------------------------------------------------------------------
	s.Run("in_subquery_with_inner_where", func() {
		// Users who have at least one large order (amount > 400).
		// The inner WHERE pushes the amount filter down to the semi-join scan.
		rows, err := s.db.Query(
			`select name from "sj_users" where id in (select user_id from "sj_orders" where amount > 400)`)
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

	// -------------------------------------------------------------------------
	s.Run("not_in_subquery_with_inner_where", func() {
		// Users who have no order over 400 (i.e., Bob and Carol).
		rows, err := s.db.Query(
			`select name from "sj_users" where id not in (select user_id from "sj_orders" where amount > 400)`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var n string
			s.Require().NoError(rows.Scan(&n))
			names = append(names, n)
		}
		s.Require().NoError(rows.Err())
		s.ElementsMatch([]string{"Bob", "Carol"}, names)
	})

	// -------------------------------------------------------------------------
	s.Run("in_subquery_outer_has_explicit_alias", func() {
		// Same as in_subquery_basic but outer table has an explicit alias.
		rows, err := s.db.Query(
			`select u.name from "sj_users" AS u where u.id in (select user_id from "sj_orders")`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var n string
			s.Require().NoError(rows.Scan(&n))
			names = append(names, n)
		}
		s.Require().NoError(rows.Err())
		s.ElementsMatch([]string{"Alice", "Bob"}, names)
	})

	// -------------------------------------------------------------------------
	s.Run("in_subquery_combined_with_outer_where", func() {
		// Only eng-department users who have at least one order.
		rows, err := s.db.Query(
			`select name from "sj_users" where dept = 'eng' and id in (select user_id from "sj_orders")`)
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

	// -------------------------------------------------------------------------
	s.Run("in_subquery_no_matches_in_inner", func() {
		// Inner subquery returns no rows → no outer rows should match.
		rows, err := s.db.Query(
			`select name from "sj_users" where id in (select user_id from "sj_orders" where amount > 99999)`)
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

	// -------------------------------------------------------------------------
	s.Run("not_in_subquery_all_matched", func() {
		// Inner subquery covers all user IDs → NOT IN should return nothing.
		// Add a row for Carol first.
		_, err = s.db.Exec(`insert into "sj_orders" (user_id, amount) values (3, 100)`)
		s.Require().NoError(err)

		rows, err := s.db.Query(
			`select name from "sj_users" where id not in (select user_id from "sj_orders")`)
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

	// -------------------------------------------------------------------------
	s.Run("explain_shows_semi_join", func() {
		// EXPLAIN of an IN (subquery) should show a "semi" join type in the plan.
		rows := s.collectExplain(
			`explain select name from "sj_users" where id in (select user_id from "sj_orders")`)
		s.Require().NotEmpty(rows)
		found := false
		for _, r := range rows {
			if strings.Contains(r.Detail, "semi") {
				found = true
				break
			}
		}
		s.True(found, "EXPLAIN plan should mention 'semi' join type in Detail")
	})

	// -------------------------------------------------------------------------
	s.Run("explain_shows_anti_semi_join", func() {
		// EXPLAIN of a NOT IN (subquery) should show "anti_semi" in the plan.
		rows := s.collectExplain(
			`explain select name from "sj_users" where id not in (select user_id from "sj_orders")`)
		s.Require().NotEmpty(rows)
		found := false
		for _, r := range rows {
			if strings.Contains(r.Detail, "anti_semi") {
				found = true
				break
			}
		}
		s.True(found, "EXPLAIN plan should mention 'anti_semi' join type in Detail")
	})
}

// TestSemiJoinIneligible verifies that subqueries that cannot be converted to
// semi-joins (DISTINCT, GROUP BY, LIMIT, aggregates) still return correct
// results via the normal materialisation path.
func (s *TestSuite) TestSemiJoinIneligible() {
	_, err := s.db.Exec(`create table "sji_users" (
		id    int8 primary key autoincrement,
		name  varchar(100) not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "sji_orders" (
		id      int8 primary key autoincrement,
		user_id int8 not null,
		amount  int8 not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "sji_users" (name) values ('Alice'), ('Bob'), ('Carol')`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "sji_orders" (user_id, amount) values (1, 500), (1, 300), (2, 200)`)
	s.Require().NoError(err)

	s.Run("in_with_distinct_subquery_materialises", func() {
		// DISTINCT on the subquery prevents semi-join conversion; must still work.
		rows, err := s.db.Query(
			`select name from "sji_users" where id in (select distinct user_id from "sji_orders")`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var n string
			s.Require().NoError(rows.Scan(&n))
			names = append(names, n)
		}
		s.Require().NoError(rows.Err())
		s.ElementsMatch([]string{"Alice", "Bob"}, names)
	})

	s.Run("in_with_limit_subquery_materialises", func() {
		// LIMIT on the subquery prevents semi-join conversion; must still work.
		rows, err := s.db.Query(
			`select name from "sji_users" where id in (select user_id from "sji_orders" limit 1)`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var n string
			s.Require().NoError(rows.Scan(&n))
			names = append(names, n)
		}
		s.Require().NoError(rows.Err())
		// The subquery returns only the first user_id (1 = Alice); exactly one user matches.
		s.Require().Len(names, 1)
	})
}
