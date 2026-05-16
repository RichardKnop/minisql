package e2etests

func (s *TestSuite) TestLeftJoin() {
	_, err := s.db.Exec(`create table "users" (
		id int8 primary key,
		name varchar(50)
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "orders" (
		id int8 primary key,
		user_id int8,
		amount int8
	);`)
	s.Require().NoError(err)

	// Alice has two orders, Bob has none, Charlie has one.
	_, err = s.db.Exec(`insert into users("id", "name") values(1, 'Alice');`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into users("id", "name") values(2, 'Bob');`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into users("id", "name") values(3, 'Charlie');`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into orders("id", "user_id", "amount") values(1, 1, 100);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into orders("id", "user_id", "amount") values(2, 1, 200);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into orders("id", "user_id", "amount") values(3, 3, 150);`)
	s.Require().NoError(err)

	s.Run("LEFT JOIN returns all users including those without orders", func() {
		rows, err := s.db.Query(`
			select u.id, u.name, o.id, o.amount
			from users as u
			left join orders as o on u.id = o.user_id
			order by u.id, o.id;
		`)
		s.Require().NoError(err)
		defer rows.Close()

		type result struct {
			userID  int64
			name    string
			orderID *int64
			amount  *int64
		}
		var got []result

		for rows.Next() {
			var r result
			s.Require().NoError(rows.Scan(&r.userID, &r.name, &r.orderID, &r.amount))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())

		int64p := func(v int64) *int64 { return &v }

		want := []result{
			{1, "Alice", int64p(1), int64p(100)},
			{1, "Alice", int64p(2), int64p(200)},
			{2, "Bob", nil, nil}, // no orders
			{3, "Charlie", int64p(3), int64p(150)},
		}

		s.Require().Len(got, len(want))
		for i, w := range want {
			s.Equal(w.userID, got[i].userID, "row %d: userID", i)
			s.Equal(w.name, got[i].name, "row %d: name", i)
			if w.orderID == nil {
				s.Nil(got[i].orderID, "row %d: orderID should be NULL", i)
				s.Nil(got[i].amount, "row %d: amount should be NULL", i)
			} else {
				s.Require().NotNil(got[i].orderID, "row %d: orderID should not be NULL", i)
				s.Require().NotNil(got[i].amount, "row %d: amount should not be NULL", i)
				s.Equal(*w.orderID, *got[i].orderID, "row %d: orderID", i)
				s.Equal(*w.amount, *got[i].amount, "row %d: amount", i)
			}
		}
	})

	s.Run("LEFT JOIN with WHERE on base table", func() {
		rows, err := s.db.Query(`
			select u.id, u.name, o.amount
			from users as u
			left join orders as o on u.id = o.user_id
			where u.id != 2
			order by u.id, o.id;
		`)
		s.Require().NoError(err)
		defer rows.Close()

		type result struct {
			userID int64
			name   string
			amount *int64
		}
		var got []result
		for rows.Next() {
			var r result
			s.Require().NoError(rows.Scan(&r.userID, &r.name, &r.amount))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())

		int64p := func(v int64) *int64 { return &v }
		want := []result{
			{1, "Alice", int64p(100)},
			{1, "Alice", int64p(200)},
			{3, "Charlie", int64p(150)},
		}
		s.Require().Len(got, len(want))
		for i, w := range want {
			s.Equal(w.userID, got[i].userID, "row %d: userID", i)
			s.Equal(w.name, got[i].name, "row %d: name", i)
			s.Require().NotNil(got[i].amount, "row %d: amount", i)
			s.Equal(*w.amount, *got[i].amount, "row %d: amount", i)
		}
	})
}

func (s *TestSuite) TestLeftJoin_WithIndex() {
	_, err := s.db.Exec(`create table "users" (
		id int8 primary key,
		name varchar(50)
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "orders" (
		id int8 primary key,
		user_id int8,
		amount int8
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create index "idx_user_id" on "orders" (user_id);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into users("id", "name") values(1, 'Alice');`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into users("id", "name") values(2, 'Bob');`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into orders("id", "user_id", "amount") values(1, 1, 100);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into orders("id", "user_id", "amount") values(2, 1, 200);`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`
		select u.id, u.name, o.id, o.amount
		from users as u
		left join orders as o on u.id = o.user_id
		order by u.id, o.id;
	`)
	s.Require().NoError(err)
	defer rows.Close()

	type result struct {
		userID  int64
		name    string
		orderID *int64
		amount  *int64
	}
	var got []result
	for rows.Next() {
		var r result
		s.Require().NoError(rows.Scan(&r.userID, &r.name, &r.orderID, &r.amount))
		got = append(got, r)
	}
	s.Require().NoError(rows.Err())

	int64p := func(v int64) *int64 { return &v }
	want := []result{
		{1, "Alice", int64p(1), int64p(100)},
		{1, "Alice", int64p(2), int64p(200)},
		{2, "Bob", nil, nil},
	}
	s.Require().Len(got, len(want))
	for i, w := range want {
		s.Equal(w.userID, got[i].userID, "row %d: userID", i)
		s.Equal(w.name, got[i].name, "row %d: name", i)
		if w.orderID == nil {
			s.Nil(got[i].orderID, "row %d: orderID should be NULL", i)
		} else {
			s.Require().NotNil(got[i].orderID, "row %d: orderID", i)
			s.Equal(*w.orderID, *got[i].orderID, "row %d: orderID", i)
			s.Equal(*w.amount, *got[i].amount, "row %d: amount", i)
		}
	}
}

func (s *TestSuite) TestFullOuterJoin() {
	_, err := s.db.Exec(`create table "users" (
		id int8 primary key,
		name varchar(50)
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "orders" (
		id int8 primary key,
		user_id int8,
		amount int8
	);`)
	s.Require().NoError(err)

	// Alice has two orders, Bob has none, Charlie has one.
	// Order 4 has user_id=99 which doesn't exist in users.
	_, err = s.db.Exec(`insert into users("id", "name") values(1, 'Alice');`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into users("id", "name") values(2, 'Bob');`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into users("id", "name") values(3, 'Charlie');`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into orders("id", "user_id", "amount") values(1, 1, 100);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into orders("id", "user_id", "amount") values(2, 1, 200);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into orders("id", "user_id", "amount") values(3, 3, 150);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into orders("id", "user_id", "amount") values(4, 99, 300);`)
	s.Require().NoError(err)

	s.Run("FULL OUTER JOIN returns all users and all orders", func() {
		rows, err := s.db.Query(`
			select u.id, u.name, o.id, o.amount
			from users as u
			full outer join orders as o on u.id = o.user_id
			order by u.id, o.id;
		`)
		s.Require().NoError(err)
		defer rows.Close()

		type result struct {
			userID  *int64
			name    *string
			orderID *int64
			amount  *int64
		}
		var got []result
		for rows.Next() {
			var r result
			s.Require().NoError(rows.Scan(&r.userID, &r.name, &r.orderID, &r.amount))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())

		int64p := func(v int64) *int64 { return &v }
		strp := func(v string) *string { return &v }

		// NULL sorts before any non-NULL value (NULLS FIRST), so the right-only
		// row (u.id = NULL) appears first when ordering by u.id.
		want := []result{
			// order 4: right-only (user_id=99 doesn't exist) — NULL u.id sorts first
			{nil, nil, int64p(4), int64p(300)},
			// Alice: two matched rows
			{int64p(1), strp("Alice"), int64p(1), int64p(100)},
			{int64p(1), strp("Alice"), int64p(2), int64p(200)},
			// Bob: left-only (no orders)
			{int64p(2), strp("Bob"), nil, nil},
			// Charlie: one matched row
			{int64p(3), strp("Charlie"), int64p(3), int64p(150)},
		}

		s.Require().Len(got, len(want))
		for i, w := range want {
			if w.userID == nil {
				s.Nil(got[i].userID, "row %d: userID should be NULL", i)
				s.Nil(got[i].name, "row %d: name should be NULL", i)
			} else {
				s.Require().NotNil(got[i].userID, "row %d: userID should not be NULL", i)
				s.Require().NotNil(got[i].name, "row %d: name should not be NULL", i)
				s.Equal(*w.userID, *got[i].userID, "row %d: userID", i)
				s.Equal(*w.name, *got[i].name, "row %d: name", i)
			}
			if w.orderID == nil {
				s.Nil(got[i].orderID, "row %d: orderID should be NULL", i)
				s.Nil(got[i].amount, "row %d: amount should be NULL", i)
			} else {
				s.Require().NotNil(got[i].orderID, "row %d: orderID should not be NULL", i)
				s.Require().NotNil(got[i].amount, "row %d: amount should not be NULL", i)
				s.Equal(*w.orderID, *got[i].orderID, "row %d: orderID", i)
				s.Equal(*w.amount, *got[i].amount, "row %d: amount", i)
			}
		}
	})

	s.Run("FULL JOIN (short syntax) works", func() {
		rows, err := s.db.Query(`
			select u.id, o.id
			from users as u
			full join orders as o on u.id = o.user_id
			order by u.id, o.id;
		`)
		s.Require().NoError(err)
		defer rows.Close()

		type result struct {
			userID  *int64
			orderID *int64
		}
		var got []result
		for rows.Next() {
			var r result
			s.Require().NoError(rows.Scan(&r.userID, &r.orderID))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())

		int64p := func(v int64) *int64 { return &v }
		// NULL u.id sorts first (NULLS FIRST behaviour).
		want := []result{
			{nil, int64p(4)},
			{int64p(1), int64p(1)},
			{int64p(1), int64p(2)},
			{int64p(2), nil},
			{int64p(3), int64p(3)},
		}
		s.Require().Len(got, len(want))
		for i, w := range want {
			if w.userID == nil {
				s.Nil(got[i].userID, "row %d: userID", i)
			} else {
				s.Require().NotNil(got[i].userID, "row %d: userID", i)
				s.Equal(*w.userID, *got[i].userID, "row %d: userID", i)
			}
			if w.orderID == nil {
				s.Nil(got[i].orderID, "row %d: orderID", i)
			} else {
				s.Require().NotNil(got[i].orderID, "row %d: orderID", i)
				s.Equal(*w.orderID, *got[i].orderID, "row %d: orderID", i)
			}
		}
	})
}

func (s *TestSuite) TestRightJoin() {
	_, err := s.db.Exec(`create table "users" (
		id int8 primary key,
		name varchar(50)
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "orders" (
		id int8 primary key,
		user_id int8,
		amount int8
	);`)
	s.Require().NoError(err)

	// Alice has orders, Bob has no orders.
	// Order 3 has user_id=99 which doesn't exist in users.
	_, err = s.db.Exec(`insert into users("id", "name") values(1, 'Alice');`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into users("id", "name") values(2, 'Bob');`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into orders("id", "user_id", "amount") values(1, 1, 100);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into orders("id", "user_id", "amount") values(2, 1, 200);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into orders("id", "user_id", "amount") values(3, 99, 300);`)
	s.Require().NoError(err)

	s.Run("RIGHT JOIN returns all orders including those without matching users", func() {
		rows, err := s.db.Query(`
			select u.id, u.name, o.id, o.amount
			from users as u
			right join orders as o on u.id = o.user_id
			order by o.id;
		`)
		s.Require().NoError(err)
		defer rows.Close()

		type result struct {
			userID  *int64
			name    *string
			orderID int64
			amount  int64
		}
		var got []result
		for rows.Next() {
			var r result
			s.Require().NoError(rows.Scan(&r.userID, &r.name, &r.orderID, &r.amount))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())

		int64p := func(v int64) *int64 { return &v }
		strp := func(v string) *string { return &v }
		want := []result{
			{int64p(1), strp("Alice"), 1, 100},
			{int64p(1), strp("Alice"), 2, 200},
			{nil, nil, 3, 300}, // user_id=99 has no matching user
		}
		s.Require().Len(got, len(want))
		for i, w := range want {
			if w.userID == nil {
				s.Nil(got[i].userID, "row %d: userID should be NULL", i)
				s.Nil(got[i].name, "row %d: name should be NULL", i)
			} else {
				s.Require().NotNil(got[i].userID, "row %d: userID", i)
				s.Require().NotNil(got[i].name, "row %d: name", i)
				s.Equal(*w.userID, *got[i].userID, "row %d: userID", i)
				s.Equal(*w.name, *got[i].name, "row %d: name", i)
			}
			s.Equal(w.orderID, got[i].orderID, "row %d: orderID", i)
			s.Equal(w.amount, got[i].amount, "row %d: amount", i)
		}
	})
}
