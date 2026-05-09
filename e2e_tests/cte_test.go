package e2etests

import "context"

func (s *TestSuite) TestCTE() {
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

	_, err = s.db.Exec(`insert into "users" (name, score) values ('Alice', 90)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "users" (name, score) values ('Bob', 70)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "users" (name, score) values ('Carol', 85)`)
	s.Require().NoError(err)

	// Alice=1: 2 orders, Bob=2: 1 order, Carol=3: 0 orders
	_, err = s.db.Exec(`insert into "orders" (user_id, amount) values (1, 500)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "orders" (user_id, amount) values (1, 300)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "orders" (user_id, amount) values (2, 200)`)
	s.Require().NoError(err)

	s.Run("basic_single_cte", func() {
		rows, err := s.db.Query(
			`WITH active AS (SELECT id, name FROM users WHERE score > 80)
			 SELECT active.name FROM active`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var n string
			s.Require().NoError(rows.Scan(&n))
			names = append(names, n)
		}
		s.Require().NoError(rows.Err())
		s.ElementsMatch([]string{"Alice", "Carol"}, names)
	})

	s.Run("cte_with_outer_where", func() {
		rows, err := s.db.Query(
			`WITH t AS (SELECT id, name, score FROM users)
			 SELECT t.name FROM t WHERE t.score > 85`)
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

	s.Run("cte_select_star", func() {
		rows, err := s.db.Query(
			`WITH t AS (SELECT id, name FROM users) SELECT * FROM t`)
		s.Require().NoError(err)
		defer rows.Close()

		var count int
		for rows.Next() {
			count += 1
			var id int64
			var name string
			s.Require().NoError(rows.Scan(&id, &name))
		}
		s.Require().NoError(rows.Err())
		s.Equal(3, count)
	})

	s.Run("multiple_ctes", func() {
		// cte2 references cte1.
		rows, err := s.db.Query(
			`WITH
			   high_scorers AS (SELECT id FROM users WHERE score > 80),
			   user_orders  AS (SELECT user_id, amount FROM orders)
			 SELECT user_orders.amount FROM user_orders
			 WHERE user_orders.user_id IN (SELECT id FROM high_scorers)`)
		s.Require().NoError(err)
		defer rows.Close()

		var amounts []int64
		for rows.Next() {
			var a int64
			s.Require().NoError(rows.Scan(&a))
			amounts = append(amounts, a)
		}
		s.Require().NoError(rows.Err())
		// Alice (high scorer, id=1) has orders 500 and 300.
		s.ElementsMatch([]int64{500, 300}, amounts)
	})

	s.Run("cte_with_aggregate", func() {
		rows, err := s.db.Query(
			`WITH totals AS (SELECT user_id, COUNT(*) AS cnt FROM orders GROUP BY user_id)
			 SELECT totals.user_id, totals.cnt FROM totals`)
		s.Require().NoError(err)
		defer rows.Close()

		type row struct {
			userID int64
			cnt    int64
		}
		var results []row
		for rows.Next() {
			var r row
			s.Require().NoError(rows.Scan(&r.userID, &r.cnt))
			results = append(results, r)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(results, 2)
		counts := map[int64]int64{}
		for _, r := range results {
			counts[r.userID] = r.cnt
		}
		s.Equal(int64(2), counts[1]) // Alice
		s.Equal(int64(1), counts[2]) // Bob
	})

	s.Run("cte_join_real_table", func() {
		// CTE joined with a real table.
		rows, err := s.db.Query(
			`WITH active AS (SELECT id FROM users WHERE score > 80)
			 SELECT u.name FROM users AS u INNER JOIN active AS a ON u.id = a.id`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var n string
			s.Require().NoError(rows.Scan(&n))
			names = append(names, n)
		}
		s.Require().NoError(rows.Err())
		s.ElementsMatch([]string{"Alice", "Carol"}, names)
	})

	s.Run("cte_with_limit", func() {
		rows, err := s.db.Query(
			`WITH t AS (SELECT id, name FROM users) SELECT t.name FROM t LIMIT 2`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var n string
			s.Require().NoError(rows.Scan(&n))
			names = append(names, n)
		}
		s.Require().NoError(rows.Err())
		s.Len(names, 2)
	})

	s.Run("cte_semicolon", func() {
		rows, err := s.db.Query(
			`WITH t AS (SELECT id, name FROM users) SELECT t.name FROM t;`)
		s.Require().NoError(err)
		defer rows.Close()
		var count int
		for rows.Next() {
			count += 1
			var n string
			s.Require().NoError(rows.Scan(&n))
		}
		s.Require().NoError(rows.Err())
		s.Equal(3, count)
	})
}

func (s *TestSuite) TestCTE_Explain() {
	s.execQuery(createUsersTableSQL, 0)
	s.execQuery(`insert into users("email", "name") values
('alice@example.com', 'Alice'),
('bob@example.com', 'Bob'),
('carol@example.com', 'Carol');`, 3)

	s.Run("explain_cte", func() {
		rows := s.collectExplain(
			`EXPLAIN WITH t AS (SELECT id, name FROM users) SELECT t.name FROM t`)
		s.Require().Len(rows, 2)

		s.Equal(int64(1), rows[0].Step)
		s.Equal("cte", rows[0].Operation)
		s.Contains(rows[0].Detail, "name=t")
		s.False(rows[0].RowsActual.Valid)

		s.Equal(int64(2), rows[1].Step)
		s.NotEmpty(rows[1].Operation)
		s.Contains(rows[1].Detail, "table=t")
	})

	s.Run("explain_analyze_cte", func() {
		rows := s.collectExplain(
			`EXPLAIN ANALYZE WITH t AS (SELECT id, name FROM users) SELECT t.name FROM t`)
		s.Require().Len(rows, 2)

		s.Equal("cte", rows[0].Operation)
		s.True(rows[0].RowsActual.Valid)
		s.Equal(int64(3), rows[0].RowsActual.Int64)
		s.True(rows[0].DurationUS.Valid)

		s.Equal(int64(2), rows[1].Step)
		s.True(rows[1].RowsActual.Valid)
		s.Equal(int64(3), rows[1].RowsActual.Int64)
	})

	s.Run("explain_multiple_ctes", func() {
		rows := s.collectExplain(
			`EXPLAIN WITH
			   cte1 AS (SELECT id FROM users),
			   cte2 AS (SELECT id FROM users WHERE id > 1)
			 SELECT cte1.id FROM cte1`)
		// 2 CTE steps + 1 scan step
		s.Require().Len(rows, 3)
		s.Equal("cte", rows[0].Operation)
		s.Contains(rows[0].Detail, "name=cte1")
		s.Equal("cte", rows[1].Operation)
		s.Contains(rows[1].Detail, "name=cte2")
		s.Equal(int64(3), rows[2].Step)
	})
}

func (s *TestSuite) TestCTE_PreparedStatement() {
	_, err := s.db.Exec(`create table "users" (
		id    int8 primary key autoincrement,
		name  varchar(100) not null,
		score int8
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "users" (name, score) values ('Alice', 90)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "users" (name, score) values ('Bob', 70)`)
	s.Require().NoError(err)

	stmt, err := s.db.PrepareContext(context.Background(),
		`WITH t AS (SELECT id, name FROM users WHERE score > ?) SELECT t.name FROM t WHERE t.id < ?`)
	s.Require().NoError(err)
	defer stmt.Close()

	rows, err := stmt.QueryContext(context.Background(), int64(80), int64(10))
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
}
