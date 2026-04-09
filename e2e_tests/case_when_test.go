package e2etests

// ── CASE WHEN expressions ─────────────────────────────────────────────────────

func (s *TestSuite) TestCaseWhen_SearchedInSelect() {
	_, err := s.db.Exec(`create table "scores" (
		id int8 primary key autoincrement,
		score int8 not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "scores" (score) values (95)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "scores" (score) values (75)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "scores" (score) values (50)`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`
		select CASE WHEN score >= 90 THEN 'A' WHEN score >= 70 THEN 'B' ELSE 'C' END AS grade
		from "scores"
		order by id`)
	s.Require().NoError(err)
	defer rows.Close()

	var grades []string
	for rows.Next() {
		var g string
		s.Require().NoError(rows.Scan(&g))
		grades = append(grades, g)
	}
	s.Require().NoError(rows.Err())
	s.Require().Len(grades, 3)
	s.Equal("A", grades[0])
	s.Equal("B", grades[1])
	s.Equal("C", grades[2])
}

func (s *TestSuite) TestCaseWhen_NoElseReturnsNull() {
	_, err := s.db.Exec(`create table "flags" (
		id int8 primary key autoincrement,
		val int8 not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "flags" (val) values (1)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "flags" (val) values (99)`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`select CASE WHEN val = 1 THEN 'one' END from "flags" order by id`)
	s.Require().NoError(err)
	defer rows.Close()

	var results []*string
	for rows.Next() {
		var v *string
		s.Require().NoError(rows.Scan(&v))
		results = append(results, v)
	}
	s.Require().NoError(rows.Err())
	s.Require().Len(results, 2)
	s.Require().NotNil(results[0])
	s.Equal("one", *results[0])
	s.Nil(results[1]) // val=99, no match, no ELSE → NULL
}

func (s *TestSuite) TestCaseWhen_SimpleCaseInSelect() {
	_, err := s.db.Exec(`create table "statuses" (
		id int8 primary key autoincrement,
		code int8 not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "statuses" (code) values (1)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "statuses" (code) values (2)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "statuses" (code) values (3)`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`
		select CASE code WHEN 1 THEN 'active' WHEN 2 THEN 'pending' ELSE 'other' END AS label
		from "statuses"
		order by id`)
	s.Require().NoError(err)
	defer rows.Close()

	var labels []string
	for rows.Next() {
		var l string
		s.Require().NoError(rows.Scan(&l))
		labels = append(labels, l)
	}
	s.Require().NoError(rows.Err())
	s.Require().Len(labels, 3)
	s.Equal("active", labels[0])
	s.Equal("pending", labels[1])
	s.Equal("other", labels[2])
}

func (s *TestSuite) TestCaseWhen_InUpdateSet() {
	_, err := s.db.Exec(`create table "items2" (
		id int8 primary key autoincrement,
		score int8 not null,
		grade text
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "items2" (score) values (95)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "items2" (score) values (65)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`update "items2" set grade = CASE WHEN score >= 70 THEN 'pass' ELSE 'fail' END`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`select grade from "items2" order by id`)
	s.Require().NoError(err)
	defer rows.Close()

	var grades []string
	for rows.Next() {
		var g string
		s.Require().NoError(rows.Scan(&g))
		grades = append(grades, g)
	}
	s.Require().NoError(rows.Err())
	s.Require().Len(grades, 2)
	s.Equal("pass", grades[0])
	s.Equal("fail", grades[1])
}

func (s *TestSuite) TestCaseWhen_IsNullCondition() {
	_, err := s.db.Exec(`create table "nullable" (
		id int8 primary key autoincrement,
		val int8
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "nullable" (val) values (NULL)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "nullable" (val) values (42)`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`
		select CASE WHEN val IS NULL THEN 'missing' ELSE 'present' END AS status
		from "nullable"
		order by id`)
	s.Require().NoError(err)
	defer rows.Close()

	var statuses []string
	for rows.Next() {
		var st string
		s.Require().NoError(rows.Scan(&st))
		statuses = append(statuses, st)
	}
	s.Require().NoError(rows.Err())
	s.Require().Len(statuses, 2)
	s.Equal("missing", statuses[0])
	s.Equal("present", statuses[1])
}

func (s *TestSuite) TestCaseWhen_NestedInArithmetic() {
	_, err := s.db.Exec(`create table "discounts" (
		id int8 primary key autoincrement,
		price double not null,
		vip int8 not null
	)`)
	s.Require().NoError(err)

	istmt, err := s.db.Prepare(`insert into "discounts" (price, vip) values (?, ?)`)
	s.Require().NoError(err)
	_, err = istmt.Exec(float64(100.0), int64(1))
	s.Require().NoError(err)
	_, err = istmt.Exec(float64(100.0), int64(0))
	s.Require().NoError(err)

	rows, err := s.db.Query(`
		select price * CASE WHEN vip = 1 THEN 0.8 ELSE 1.0 END AS final_price
		from "discounts"
		order by id`)
	s.Require().NoError(err)
	defer rows.Close()

	var prices []float64
	for rows.Next() {
		var p float64
		s.Require().NoError(rows.Scan(&p))
		prices = append(prices, p)
	}
	s.Require().NoError(rows.Err())
	s.Require().Len(prices, 2)
	s.InDelta(80.0, prices[0], 1e-9)  // VIP: 100 * 0.8
	s.InDelta(100.0, prices[1], 1e-9) // non-VIP: 100 * 1.0
}
