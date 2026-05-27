package e2etests

// TestWhereExpr_ArithmeticFilter verifies that arithmetic expressions in WHERE
// are evaluated correctly and can be used to filter rows.
func (s *TestSuite) TestWhereExpr_ArithmeticFilter() {
	_, err := s.db.Exec(`create table "products" (
		id    int8 primary key autoincrement,
		name  varchar(100) not null,
		price int8 not null,
		qty   int8 not null
	)`)
	s.Require().NoError(err)

	type product struct{ name string; price, qty int64 }
	products := []product{
		{"cheap-low", 5, 2},    // total=10, price*2=10
		{"cheap-high", 5, 100}, // total=500, price*2=10
		{"mid", 20, 3},         // total=60, price*2=40
		{"pricey", 50, 1},      // total=50, price*2=100
	}
	for _, p := range products {
		_, err = s.db.Exec(
			`insert into "products" (name, price, qty) values (?, ?, ?)`,
			p.name, p.price, p.qty,
		)
		s.Require().NoError(err)
	}

	s.Run("multiplication_filter", func() {
		// WHERE price * qty > 50 → mid(60) and cheap-high(500)
		rows, err := s.db.Query(`select name from "products" where price * qty > 50 order by name`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var n string
			s.Require().NoError(rows.Scan(&n))
			names = append(names, n)
		}
		s.Require().NoError(rows.Err())
		s.Equal([]string{"cheap-high", "mid"}, names)
	})

	s.Run("addition_filter", func() {
		// WHERE price + qty > 25 → cheap-high(105), mid(23→skip, 20+3=23 so skip), pricey(51)
		rows, err := s.db.Query(`select name from "products" where price + qty > 25 order by name`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var n string
			s.Require().NoError(rows.Scan(&n))
			names = append(names, n)
		}
		s.Require().NoError(rows.Err())
		// cheap-high: 5+100=105, pricey: 50+1=51 — both qualify
		s.Equal([]string{"cheap-high", "pricey"}, names)
	})

	s.Run("double_factor_filter", func() {
		// WHERE price * 2 >= 40 → mid(40) and pricey(100)
		rows, err := s.db.Query(`select name from "products" where price * 2 >= 40 order by name`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var n string
			s.Require().NoError(rows.Scan(&n))
			names = append(names, n)
		}
		s.Require().NoError(rows.Err())
		s.Equal([]string{"mid", "pricey"}, names)
	})
}

// TestWhereExpr_ArithmeticInUpdateSet verifies arithmetic expressions in the
// SET clause of UPDATE (e.g., SET price = price + 10).
func (s *TestSuite) TestWhereExpr_ArithmeticInUpdateSet() {
	_, err := s.db.Exec(`create table "counters" (
		id  int8 primary key autoincrement,
		cnt int8 not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "counters" (cnt) values (10)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "counters" (cnt) values (20)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`update "counters" set cnt = cnt + 5`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`select cnt from "counters" order by id`)
	s.Require().NoError(err)
	defer rows.Close()

	var cnts []int64
	for rows.Next() {
		var c int64
		s.Require().NoError(rows.Scan(&c))
		cnts = append(cnts, c)
	}
	s.Require().NoError(rows.Err())
	s.Equal([]int64{15, 25}, cnts)
}

// TestWhereExpr_PreparedStatementPlanReuse verifies that a prepared statement
// executed multiple times with different bound values returns the correct row
// each time (exercises the plan cache re-hydration path).
func (s *TestSuite) TestWhereExpr_PreparedStatementPlanReuse() {
	_, err := s.db.Exec(`create table "lookup" (
		id   int8 primary key autoincrement,
		name varchar(100) not null
	)`)
	s.Require().NoError(err)

	names := []string{"Alpha", "Beta", "Gamma", "Delta"}
	for _, n := range names {
		_, err = s.db.Exec(`insert into "lookup" (name) values (?)`, n)
		s.Require().NoError(err)
	}

	stmt, err := s.db.Prepare(`select name from "lookup" where id = ?`)
	s.Require().NoError(err)
	defer stmt.Close()

	for i, expected := range names {
		var got string
		s.Require().NoError(stmt.QueryRow(int64(i+1)).Scan(&got))
		s.Equal(expected, got, "mismatch at id=%d", i+1)
	}
}

// TestWhereExpr_UpdateIndexedColumnDeferred verifies that updating an indexed
// column (which triggers the deferred update path in update.go) correctly
// removes the old index entry and inserts the new one.
func (s *TestSuite) TestWhereExpr_UpdateIndexedColumnDeferred() {
	_, err := s.db.Exec(`create table "employees" (
		id    int8 primary key autoincrement,
		email varchar(200) unique,
		dept  varchar(50)
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "employees" (email, dept) values ('alice@corp.com', 'Engineering')`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "employees" (email, dept) values ('bob@corp.com', 'Sales')`)
	s.Require().NoError(err)

	// Change Alice's email (unique indexed column) — triggers deferred update path.
	res, err := s.db.Exec(`update "employees" set email = 'alice@newcorp.com' where email = 'alice@corp.com'`)
	s.Require().NoError(err)
	n, _ := res.RowsAffected()
	s.Equal(int64(1), n)

	// Old email must not be found.
	var count int64
	s.Require().NoError(s.db.QueryRow(`select count(*) from "employees" where email = 'alice@corp.com'`).Scan(&count))
	s.Equal(int64(0), count)

	// New email must resolve to Alice's dept.
	var dept string
	s.Require().NoError(s.db.QueryRow(`select dept from "employees" where email = 'alice@newcorp.com'`).Scan(&dept))
	s.Equal("Engineering", dept)

	// Bob's record must be unchanged.
	s.Require().NoError(s.db.QueryRow(`select dept from "employees" where email = 'bob@corp.com'`).Scan(&dept))
	s.Equal("Sales", dept)

	// Inserting a row with the old email should now succeed (unique slot freed).
	_, err = s.db.Exec(`insert into "employees" (email, dept) values ('alice@corp.com', 'HR')`)
	s.Require().NoError(err)
}
