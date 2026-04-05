package e2etests

func (s *TestSuite) TestArithmetic_SelectComputedColumn() {
	_, err := s.db.Exec(createProductsTableSQL)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "products" (name, description, price) values ('Widget', 'A widget', 100)`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`select price * 2 from "products"`)
	s.Require().NoError(err)
	defer rows.Close()

	var vals []int64
	for rows.Next() {
		var v int64
		s.Require().NoError(rows.Scan(&v))
		vals = append(vals, v)
	}
	s.Require().NoError(rows.Err())
	s.Require().Len(vals, 1)
	s.Equal(int64(200), vals[0])
}

func (s *TestSuite) TestArithmetic_SelectWithAlias() {
	_, err := s.db.Exec(createProductsTableSQL)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "products" (name, description, price) values ('Gadget', 'A gadget', 50)`)
	s.Require().NoError(err)

	var discounted int64
	err = s.db.QueryRow(`select price - 10 AS discounted from "products"`).Scan(&discounted)
	s.Require().NoError(err)
	s.Equal(int64(40), discounted)
}

func (s *TestSuite) TestArithmetic_SelectAddTwoColumns() {
	_, err := s.db.Exec(createOrdersTableSQL)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "orders" (user_id, product_id, total_paid) values (1, 2, 30)`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`select user_id + product_id from "orders"`)
	s.Require().NoError(err)
	defer rows.Close()

	var vals []int64
	for rows.Next() {
		var v int64
		s.Require().NoError(rows.Scan(&v))
		vals = append(vals, v)
	}
	s.Require().NoError(rows.Err())
	s.Require().Len(vals, 1)
	s.Equal(int64(3), vals[0])
}

func (s *TestSuite) TestArithmetic_UpdateCountPlusOne() {
	_, err := s.db.Exec(`create table "counters" (
		id int8 primary key autoincrement,
		val int8 not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "counters" (val) values (10)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`update "counters" set val = val + 1`)
	s.Require().NoError(err)

	var val int64
	err = s.db.QueryRow(`select val from "counters"`).Scan(&val)
	s.Require().NoError(err)
	s.Equal(int64(11), val)
}

func (s *TestSuite) TestArithmetic_UpdatePriceMultiply() {
	_, err := s.db.Exec(createProductsTableSQL)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "products" (name, description, price) values ('Item', 'desc', 100)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`update "products" set price = price * 2`)
	s.Require().NoError(err)

	var price int64
	err = s.db.QueryRow(`select price from "products"`).Scan(&price)
	s.Require().NoError(err)
	s.Equal(int64(200), price)
}

func (s *TestSuite) TestArithmetic_UpdateColumnMinusColumn() {
	_, err := s.db.Exec(`create table "balances" (
		id int8 primary key autoincrement,
		total int8 not null,
		used int8 not null,
		remaining int8 not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "balances" (total, used, remaining) values (100, 30, 0)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`update "balances" set remaining = total - used`)
	s.Require().NoError(err)

	var remaining int64
	err = s.db.QueryRow(`select remaining from "balances"`).Scan(&remaining)
	s.Require().NoError(err)
	s.Equal(int64(70), remaining)
}
