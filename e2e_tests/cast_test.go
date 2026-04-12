package e2etests

// ── CAST expressions ─────────────────────────────────────────────────────────

func (s *TestSuite) TestCast_FloatToInt8() {
	_, err := s.db.Exec(`create table "prices" (
		id    int8 primary key autoincrement,
		price double not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "prices" (price) values (9.99)`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`SELECT CAST(price AS INT8) FROM "prices"`)
	s.Require().NoError(err)
	defer rows.Close()

	var vals []int64
	for rows.Next() {
		var v int64
		s.Require().NoError(rows.Scan(&v))
		vals = append(vals, v)
	}
	s.Require().NoError(rows.Err())
	s.Equal([]int64{9}, vals)
}

func (s *TestSuite) TestCast_IntToDouble() {
	_, err := s.db.Exec(`create table "counts" (
		id  int8 primary key autoincrement,
		cnt int8 not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "counts" (cnt) values (7)`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`SELECT CAST(cnt AS DOUBLE) FROM "counts"`)
	s.Require().NoError(err)
	defer rows.Close()

	var vals []float64
	for rows.Next() {
		var v float64
		s.Require().NoError(rows.Scan(&v))
		vals = append(vals, v)
	}
	s.Require().NoError(rows.Err())
	s.Equal([]float64{7.0}, vals)
}

func (s *TestSuite) TestCast_IntToText() {
	_, err := s.db.Exec(`create table "nums" (
		id  int8 primary key autoincrement,
		n   int8 not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "nums" (n) values (42)`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`SELECT CAST(n AS TEXT) FROM "nums"`)
	s.Require().NoError(err)
	defer rows.Close()

	var vals []string
	for rows.Next() {
		var v string
		s.Require().NoError(rows.Scan(&v))
		vals = append(vals, v)
	}
	s.Require().NoError(rows.Err())
	s.Equal([]string{"42"}, vals)
}

func (s *TestSuite) TestCast_WithAlias() {
	_, err := s.db.Exec(`create table "products" (
		id    int8 primary key autoincrement,
		price double not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "products" (price) values (19.95)`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`SELECT CAST(price AS INT8) AS int_price FROM "products"`)
	s.Require().NoError(err)
	defer rows.Close()

	var vals []int64
	for rows.Next() {
		var v int64
		s.Require().NoError(rows.Scan(&v))
		vals = append(vals, v)
	}
	s.Require().NoError(rows.Err())
	s.Equal([]int64{19}, vals)
}

func (s *TestSuite) TestCast_VarcharType() {
	_, err := s.db.Exec(`create table "ids" (
		id  int8 primary key autoincrement,
		num int8 not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "ids" (num) values (123)`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`SELECT CAST(num AS VARCHAR(20)) FROM "ids"`)
	s.Require().NoError(err)
	defer rows.Close()

	var vals []string
	for rows.Next() {
		var v string
		s.Require().NoError(rows.Scan(&v))
		vals = append(vals, v)
	}
	s.Require().NoError(rows.Err())
	s.Equal([]string{"123"}, vals)
}
