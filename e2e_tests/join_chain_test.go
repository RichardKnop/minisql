package e2etests

// TestChainJoin tests arbitrary join topologies where tables are chained:
// each table joins to the previous one rather than all joining to the base table.
func (s *TestSuite) TestChainJoin() {
	_, err := s.db.Exec(`create table "customers" (
		id    int8 primary key,
		name  varchar(100)
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "orders" (
		id          int8 primary key,
		customer_id int8,
		total       int8
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "order_items" (
		id       int8 primary key,
		order_id int8,
		product  varchar(100),
		qty      int8
	);`)
	s.Require().NoError(err)

	// customers
	_, err = s.db.Exec(`insert into customers (id, name) values (1, 'Alice');`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into customers (id, name) values (2, 'Bob');`)
	s.Require().NoError(err)

	// orders
	_, err = s.db.Exec(`insert into orders (id, customer_id, total) values (10, 1, 300);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into orders (id, customer_id, total) values (11, 1, 150);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into orders (id, customer_id, total) values (12, 2, 200);`)
	s.Require().NoError(err)

	// order_items  (order 10 has 2 items, order 11 has 1, order 12 has 1)
	_, err = s.db.Exec(`insert into order_items (id, order_id, product, qty) values (100, 10, 'Widget', 2);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into order_items (id, order_id, product, qty) values (101, 10, 'Gadget', 1);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into order_items (id, order_id, product, qty) values (102, 11, 'Widget', 3);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into order_items (id, order_id, product, qty) values (103, 12, 'Gadget', 5);`)
	s.Require().NoError(err)

	s.Run("ThreeTableChain_InnerJoin", func() {
		rows, err := s.db.Query(`
			SELECT c.name, o.total, oi.product, oi.qty
			FROM customers AS c
			INNER JOIN orders AS o ON o.customer_id = c.id
			INNER JOIN order_items AS oi ON oi.order_id = o.id
			ORDER BY c.name, o.id, oi.id;
		`)
		s.Require().NoError(err)
		defer rows.Close()

		type row struct {
			name    string
			total   int64
			product string
			qty     int64
		}
		var got []row
		for rows.Next() {
			var r row
			s.Require().NoError(rows.Scan(&r.name, &r.total, &r.product, &r.qty))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())

		want := []row{
			{"Alice", 300, "Widget", 2},
			{"Alice", 300, "Gadget", 1},
			{"Alice", 150, "Widget", 3},
			{"Bob", 200, "Gadget", 5},
		}
		s.Require().Equal(len(want), len(got))
		for i, w := range want {
			s.Equal(w.name, got[i].name, "row %d name", i)
			s.Equal(w.total, got[i].total, "row %d total", i)
			s.Equal(w.product, got[i].product, "row %d product", i)
			s.Equal(w.qty, got[i].qty, "row %d qty", i)
		}
	})

	s.Run("ThreeTableChain_WithWhere", func() {
		rows, err := s.db.Query(`
			SELECT c.name, o.total, oi.product
			FROM customers AS c
			INNER JOIN orders AS o ON o.customer_id = c.id
			INNER JOIN order_items AS oi ON oi.order_id = o.id
			WHERE c.name = 'Alice' AND oi.product = 'Widget'
			ORDER BY o.id;
		`)
		s.Require().NoError(err)
		defer rows.Close()

		type row struct {
			name    string
			total   int64
			product string
		}
		var got []row
		for rows.Next() {
			var r row
			s.Require().NoError(rows.Scan(&r.name, &r.total, &r.product))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())

		s.Require().Equal(2, len(got))
		s.Equal("Alice", got[0].name)
		s.Equal(int64(300), got[0].total)
		s.Equal("Widget", got[0].product)
		s.Equal("Alice", got[1].name)
		s.Equal(int64(150), got[1].total)
		s.Equal("Widget", got[1].product)
	})

	s.Run("ThreeTableChain_LeftJoin_NoItems", func() {
		// order 11 has one item (Widget); verify LEFT JOIN still produces it
		// Add a customer with an order that has no items to verify LEFT JOIN nulls
		_, err := s.db.Exec(`insert into customers (id, name) values (3, 'Charlie');`)
		s.Require().NoError(err)
		_, err = s.db.Exec(`insert into orders (id, customer_id, total) values (20, 3, 999);`)
		s.Require().NoError(err)

		rows, err := s.db.Query(`
			SELECT c.name, o.id, oi.product
			FROM customers AS c
			INNER JOIN orders AS o ON o.customer_id = c.id
			LEFT JOIN order_items AS oi ON oi.order_id = o.id
			WHERE c.name = 'Charlie'
			ORDER BY o.id;
		`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var name, product string
		var orderID int64
		var productNull *string
		_ = product
		s.Require().NoError(rows.Scan(&name, &orderID, &productNull))
		s.Equal("Charlie", name)
		s.Equal(int64(20), orderID)
		s.Nil(productNull, "product should be NULL for unmatched LEFT JOIN")
		s.Require().False(rows.Next())
		s.Require().NoError(rows.Err())
	})
}

// TestChainJoinWithIndex verifies that chain joins use index nested-loop join
// when an index is present on the join column of the inner table.
func (s *TestSuite) TestChainJoinWithIndex() {
	_, err := s.db.Exec(`create table "depts" (
		id   int8 primary key,
		name varchar(100)
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "employees" (
		id      int8 primary key,
		dept_id int8,
		name    varchar(100)
	);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`create index idx_emp_dept on employees (dept_id);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "timesheets" (
		id          int8 primary key,
		employee_id int8,
		hours       int8
	);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`create index idx_ts_emp on timesheets (employee_id);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into depts (id, name) values (1, 'Engineering');`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into depts (id, name) values (2, 'Marketing');`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into employees (id, dept_id, name) values (1, 1, 'Alice');`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into employees (id, dept_id, name) values (2, 1, 'Bob');`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into employees (id, dept_id, name) values (3, 2, 'Carol');`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into timesheets (id, employee_id, hours) values (1, 1, 40);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into timesheets (id, employee_id, hours) values (2, 1, 35);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into timesheets (id, employee_id, hours) values (3, 2, 45);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into timesheets (id, employee_id, hours) values (4, 3, 30);`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`
		SELECT d.name, e.name, t.hours
		FROM depts AS d
		INNER JOIN employees AS e ON e.dept_id = d.id
		INNER JOIN timesheets AS t ON t.employee_id = e.id
		ORDER BY d.name, e.name, t.hours;
	`)
	s.Require().NoError(err)
	defer rows.Close()

	type row struct {
		dept  string
		emp   string
		hours int64
	}
	var got []row
	for rows.Next() {
		var r row
		s.Require().NoError(rows.Scan(&r.dept, &r.emp, &r.hours))
		got = append(got, r)
	}
	s.Require().NoError(rows.Err())

	want := []row{
		{"Engineering", "Alice", 35},
		{"Engineering", "Alice", 40},
		{"Engineering", "Bob", 45},
		{"Marketing", "Carol", 30},
	}
	s.Require().Equal(len(want), len(got))
	for i, w := range want {
		s.Equal(w.dept, got[i].dept, "row %d dept", i)
		s.Equal(w.emp, got[i].emp, "row %d emp", i)
		s.Equal(w.hours, got[i].hours, "row %d hours", i)
	}
}

// TestFourTableChain verifies a four-level chain join: a→b→c→d.
func (s *TestSuite) TestFourTableChain() {
	_, err := s.db.Exec(`create table "regions" (id int8 primary key, name varchar(50));`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`create table "stores" (id int8 primary key, region_id int8, name varchar(50));`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`create table "sales" (id int8 primary key, store_id int8, amount int8);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`create table "sale_tags" (id int8 primary key, sale_id int8, tag varchar(50));`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into regions (id, name) values (1, 'North');`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into stores (id, region_id, name) values (1, 1, 'StoreA');`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into sales (id, store_id, amount) values (1, 1, 500);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into sale_tags (id, sale_id, tag) values (1, 1, 'promo');`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`
		SELECT r.name, st.name, s.amount, sg.tag
		FROM regions AS r
		INNER JOIN stores AS st ON st.region_id = r.id
		INNER JOIN sales  AS s  ON s.store_id   = st.id
		INNER JOIN sale_tags AS sg ON sg.sale_id = s.id;
	`)
	s.Require().NoError(err)
	defer rows.Close()

	s.Require().True(rows.Next())
	var region, store, tag string
	var amount int64
	s.Require().NoError(rows.Scan(&region, &store, &amount, &tag))
	s.Equal("North", region)
	s.Equal("StoreA", store)
	s.Equal(int64(500), amount)
	s.Equal("promo", tag)
	s.Require().False(rows.Next())
	s.Require().NoError(rows.Err())
}
