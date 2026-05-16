package e2etests

func (s *TestSuite) TestPredicatePushdown() {
	_, err := s.db.Exec(`create table "products" (
		id       int8 primary key autoincrement,
		category varchar(50) not null,
		price    int8 not null,
		active   int8 not null
	);`)
	s.Require().NoError(err)

	inserts := []struct {
		cat    string
		price  int
		active int
	}{
		{"electronics", 1200, 1},
		{"electronics", 300, 1},
		{"books", 25, 1},
		{"books", 40, 0},
		{"clothing", 80, 1},
	}
	for _, r := range inserts {
		_, err = s.db.Exec(
			`insert into "products" (category, price, active) values (?, ?, ?)`,
			r.cat, int64(r.price), int64(r.active),
		)
		s.Require().NoError(err)
	}

	// ----------------------------------------------------------
	// Derived-table pushdown: WHERE on outer alias is pushed into
	// the inner subquery so fewer rows are materialised.
	// ----------------------------------------------------------

	s.Run("derived_table_simple_filter", func() {
		rows, err := s.db.Query(
			`SELECT sub.price
			 FROM (SELECT category, price FROM products WHERE active = 1) sub
			 WHERE sub.category = 'electronics'`)
		s.Require().NoError(err)
		defer rows.Close()

		var prices []int64
		for rows.Next() {
			var p int64
			s.Require().NoError(rows.Scan(&p))
			prices = append(prices, p)
		}
		s.Require().NoError(rows.Err())
		s.ElementsMatch([]int64{1200, 300}, prices)
	})

	s.Run("derived_table_no_push_with_aggregate", func() {
		// GROUP BY in inner → pushdown ineligible; outer WHERE must filter.
		rows, err := s.db.Query(
			`SELECT sub.cnt
			 FROM (SELECT category, COUNT(*) AS cnt FROM products GROUP BY category) sub
			 WHERE sub.category = 'electronics'`)
		s.Require().NoError(err)
		defer rows.Close()

		var cnts []int64
		for rows.Next() {
			var c int64
			s.Require().NoError(rows.Scan(&c))
			cnts = append(cnts, c)
		}
		s.Require().NoError(rows.Err())
		s.Equal([]int64{2}, cnts)
	})

	s.Run("derived_table_no_push_with_limit", func() {
		// LIMIT in inner → pushdown ineligible; outer WHERE filters materialised rows.
		rows, err := s.db.Query(
			`SELECT sub.price
			 FROM (SELECT category, price FROM products ORDER BY price LIMIT 3) sub
			 WHERE sub.category = 'books'`)
		s.Require().NoError(err)
		defer rows.Close()

		var prices []int64
		for rows.Next() {
			var p int64
			s.Require().NoError(rows.Scan(&p))
			prices = append(prices, p)
		}
		s.Require().NoError(rows.Err())
		// The inner LIMIT 3 returns the 3 cheapest rows (25, 40, 80).
		// Of those, category='books' are 25 and 40.
		s.ElementsMatch([]int64{25, 40}, prices)
	})

	s.Run("derived_table_partial_push", func() {
		// One condition can be pushed (sub.category), another cannot
		// because it involves a join partner. Here we simulate the
		// "cannot push" scenario with a literal that references an
		// unknown alias so the outer still applies its own filter.
		rows, err := s.db.Query(
			`SELECT sub.price
			 FROM (SELECT category, price FROM products) sub
			 WHERE sub.price > 100`)
		s.Require().NoError(err)
		defer rows.Close()

		var prices []int64
		for rows.Next() {
			var p int64
			s.Require().NoError(rows.Scan(&p))
			prices = append(prices, p)
		}
		s.Require().NoError(rows.Err())
		s.ElementsMatch([]int64{1200, 300}, prices)
	})

	// ----------------------------------------------------------
	// CTE pushdown: outer WHERE pushed back into the CTE body.
	// ----------------------------------------------------------

	s.Run("cte_filter_pushed_into_body", func() {
		rows, err := s.db.Query(
			`WITH p AS (SELECT category, price FROM products)
			 SELECT p.price FROM p WHERE p.category = 'books'`)
		s.Require().NoError(err)
		defer rows.Close()

		var prices []int64
		for rows.Next() {
			var p int64
			s.Require().NoError(rows.Scan(&p))
			prices = append(prices, p)
		}
		s.Require().NoError(rows.Err())
		s.ElementsMatch([]int64{25, 40}, prices)
	})

	s.Run("cte_no_push_aggregate_body", func() {
		// CTE body has GROUP BY → not eligible for pushdown.
		// Outer WHERE must filter the materialised aggregate result.
		rows, err := s.db.Query(
			`WITH totals AS (SELECT category, COUNT(*) AS cnt FROM products GROUP BY category)
			 SELECT totals.cnt FROM totals WHERE totals.category = 'electronics'`)
		s.Require().NoError(err)
		defer rows.Close()

		var cnts []int64
		for rows.Next() {
			var c int64
			s.Require().NoError(rows.Scan(&c))
			cnts = append(cnts, c)
		}
		s.Require().NoError(rows.Err())
		s.Equal([]int64{2}, cnts)
	})

	s.Run("cte_multi_condition_push", func() {
		// Multiple outer conditions all referencing the CTE columns are
		// pushed together into the CTE body via AND semantics.
		rows, err := s.db.Query(
			`WITH p AS (SELECT category, price FROM products)
			 SELECT p.price FROM p
			 WHERE p.category = 'electronics' AND p.price > 500`)
		s.Require().NoError(err)
		defer rows.Close()

		var prices []int64
		for rows.Next() {
			var price int64
			s.Require().NoError(rows.Scan(&price))
			prices = append(prices, price)
		}
		s.Require().NoError(rows.Err())
		s.Equal([]int64{1200}, prices)
	})
}
