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
		// Plain scan CTE is inlined — no materialisation step in the plan.
		rows := s.collectExplain(
			`EXPLAIN WITH t AS (SELECT id, name FROM users) SELECT t.name FROM t`)
		s.Require().Len(rows, 1)

		s.Equal(int64(1), rows[0].Step)
		s.NotEmpty(rows[0].Operation)
		s.Contains(rows[0].Detail, "table=users")
		s.False(rows[0].RowsActual.Valid)
	})

	s.Run("explain_analyze_cte", func() {
		// Inlined CTE — EXPLAIN ANALYZE shows a single real-table scan step.
		rows := s.collectExplain(
			`EXPLAIN ANALYZE WITH t AS (SELECT id, name FROM users) SELECT t.name FROM t`)
		s.Require().Len(rows, 1)

		s.Equal(int64(1), rows[0].Step)
		s.NotEmpty(rows[0].Operation)
		s.True(rows[0].RowsActual.Valid)
		s.Equal(int64(3), rows[0].RowsActual.Int64)
	})

	s.Run("explain_multiple_ctes", func() {
		// cte1 (main FROM) is inlined; cte2 is unused and pruned.
		// Result: a single real-table scan step.
		rows := s.collectExplain(
			`EXPLAIN WITH
			   cte1 AS (SELECT id FROM users),
			   cte2 AS (SELECT id FROM users WHERE id > 1)
			 SELECT cte1.id FROM cte1`)
		s.Require().Len(rows, 1)
		s.Equal(int64(1), rows[0].Step)
		s.Contains(rows[0].Detail, "table=users")
	})

	s.Run("explain_cte_non_inlineable", func() {
		// DISTINCT CTE cannot be inlined — materialisation step still appears.
		rows := s.collectExplain(
			`EXPLAIN WITH t AS (SELECT DISTINCT id, name FROM users) SELECT t.name FROM t`)
		s.Require().Len(rows, 2)
		s.Equal("cte", rows[0].Operation)
		s.Contains(rows[0].Detail, "name=t")
		s.Equal(int64(2), rows[1].Step)
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

func (s *TestSuite) TestCTE_Inline() {
	_, err := s.db.Exec(`create table "products" (
		id    int8 primary key autoincrement,
		name  varchar(100) not null,
		price int8 not null,
		active boolean not null
	);`)
	s.Require().NoError(err)

	for _, row := range []struct {
		name   string
		price  int64
		active bool
	}{
		{"Widget", 10, true},
		{"Gadget", 20, true},
		{"Doohickey", 5, false},
		{"Thingamajig", 15, true},
		{"Whatchamacallit", 8, false},
	} {
		_, err = s.db.Exec(
			`insert into "products" (name, price, active) values (?, ?, ?)`,
			row.name, row.price, row.active,
		)
		s.Require().NoError(err)
	}

	s.Run("inline_body_condition_and_outer_condition_both_filter", func() {
		// CTE body filters active=true (3 rows); outer WHERE filters price > 12.
		// After inlining both conditions apply directly on the real table.
		rows, err := s.db.Query(
			`WITH p AS (SELECT id, name, price FROM products WHERE active = true)
			 SELECT p.name FROM p WHERE p.price > 12`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var n string
			s.Require().NoError(rows.Scan(&n))
			names = append(names, n)
		}
		s.Require().NoError(rows.Err())
		s.ElementsMatch([]string{"Gadget", "Thingamajig"}, names)
	})

	s.Run("inline_with_limit", func() {
		// LIMIT on the outer query propagates through inlining to the real scan.
		rows, err := s.db.Query(
			`WITH p AS (SELECT id, name FROM products WHERE active = true)
			 SELECT p.name FROM p LIMIT 2`)
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

	s.Run("inline_select_star", func() {
		// SELECT * against an inlined CTE must return the body's column subset
		// (id + name) not the full underlying table (id, name, price, active).
		rows, err := s.db.Query(
			`WITH p AS (SELECT id, name FROM products WHERE active = true) SELECT * FROM p`)
		s.Require().NoError(err)
		defer rows.Close()

		cols, err := rows.Columns()
		s.Require().NoError(err)
		s.ElementsMatch([]string{"id", "name"}, cols)

		var count int
		for rows.Next() {
			count++
			var id int64
			var name string
			s.Require().NoError(rows.Scan(&id, &name))
		}
		s.Require().NoError(rows.Err())
		s.Equal(3, count) // Widget, Gadget, Thingamajig
	})

	s.Run("non_inlineable_limit_cte_still_works", func() {
		// LIMIT CTE cannot be inlined — it must materialise correctly and the
		// outer query then filters the materialised subset.
		rows, err := s.db.Query(
			`WITH cheap AS (SELECT id, name, price FROM products WHERE active = true ORDER BY price LIMIT 2)
			 SELECT cheap.name FROM cheap`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var n string
			s.Require().NoError(rows.Scan(&n))
			names = append(names, n)
		}
		s.Require().NoError(rows.Err())
		// Two cheapest active products: Widget(10) and Thingamajig(15).
		s.Require().Len(names, 2)
		s.ElementsMatch([]string{"Widget", "Thingamajig"}, names)
	})

	s.Run("unused_cte_pruned", func() {
		// The unused CTE (orphan) must be silently dropped without error.
		rows, err := s.db.Query(
			`WITH
			   active AS (SELECT id, name FROM products WHERE active = true),
			   orphan AS (SELECT id FROM products WHERE price > 100)
			 SELECT active.name FROM active WHERE active.price > 12`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var n string
			s.Require().NoError(rows.Scan(&n))
			names = append(names, n)
		}
		s.Require().NoError(rows.Err())
		s.ElementsMatch([]string{"Gadget", "Thingamajig"}, names)
	})
}
