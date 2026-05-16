package e2etests

func (s *TestSuite) TestConstantFolding() {
	_, err := s.db.Exec(`create table "items" (
		id     int8 primary key autoincrement,
		name   varchar(100) not null,
		status varchar(20) not null,
		price  int8 not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "items" (name, status, price) values ('Widget', 'active', 10)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "items" (name, status, price) values ('Gadget', 'ACTIVE', 20)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "items" (name, status, price) values ('Doohickey', 'inactive', 5)`)
	s.Require().NoError(err)

	s.Run("const_expr_rhs_folded_to_index_eligible", func() {
		// UPPER('active') is a constant expression; after folding, the condition
		// becomes status = 'ACTIVE' which is a normal field comparison.
		rows, err := s.db.Query(`select name from "items" where status = UPPER('active')`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var n string
			s.Require().NoError(rows.Scan(&n))
			names = append(names, n)
		}
		s.Require().NoError(rows.Err())
		s.ElementsMatch([]string{"Gadget"}, names)
	})

	s.Run("always_false_where_returns_no_rows", func() {
		// 1 = 2 is always false; the scan should be skipped entirely.
		rows, err := s.db.Query(`select name from "items" where 1 = 2`)
		s.Require().NoError(err)
		defer rows.Close()

		s.False(rows.Next(), "expected no rows for always-false WHERE")
		s.Require().NoError(rows.Err())
	})

	s.Run("always_true_where_returns_all_rows", func() {
		// 1 = 1 is always true; all rows should be returned.
		rows, err := s.db.Query(`select name from "items" where 1 = 1`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var n string
			s.Require().NoError(rows.Scan(&n))
			names = append(names, n)
		}
		s.Require().NoError(rows.Err())
		s.Len(names, 3, "expected all 3 rows for always-true WHERE")
	})

	s.Run("const_func_both_sides_tautology", func() {
		// LOWER('FOO') = LOWER('FOO') is always true → all rows returned.
		rows, err := s.db.Query(`select name from "items" where LOWER('FOO') = LOWER('foo')`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var n string
			s.Require().NoError(rows.Scan(&n))
			names = append(names, n)
		}
		s.Require().NoError(rows.Err())
		s.Len(names, 3, "always-true expression should return all rows")
	})
}
