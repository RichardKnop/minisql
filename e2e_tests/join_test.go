package e2etests

func (s *TestSuite) TestInnerJoin() {
	// Create users table
	_, err := s.db.Exec(`create table "users" (
		id int8 primary key,
		name varchar(50),
		age int8
	);`)
	s.Require().NoError(err)

	// Create orders table
	_, err = s.db.Exec(`create table "orders" (
		id int8 primary key,
		user_id int8,
		amount int8
	);`)
	s.Require().NoError(err)

	// Insert test data - deliberately out of order
	_, err = s.db.Exec(`insert into users("id", "name", "age") values(2, 'Bob', 30);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into users("id", "name", "age") values(1, 'Alice', 25);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into users("id", "name", "age") values(3, 'Charlie', 35);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into orders("id", "user_id", "amount") values(2, 1, 200);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into orders("id", "user_id", "amount") values(1, 1, 100);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into orders("id", "user_id", "amount") values(3, 2, 150);`)
	s.Require().NoError(err)

	s.Run("SELECT with INNER JOIN", func() {
		rows, err := s.db.Query(`
			select 
				u.id, 
				u.name, 
				o.id, 
				o.amount 
			from users as u 
			inner join orders as o on u.id = o.user_id;
		`)
		s.Require().NoError(err)
		defer rows.Close()

		// Collect all results (order is not guaranteed without ORDER BY)
		type result struct {
			userID   int64
			username string
			orderID  int64
			amount   int64
		}
		var actualResults []result

		for rows.Next() {
			var (
				userID, orderID, amount int64
				username                string
			)
			err := rows.Scan(&userID, &username, &orderID, &amount)
			s.Require().NoError(err)
			actualResults = append(actualResults, result{userID, username, orderID, amount})
		}

		s.Require().NoError(rows.Err())

		// Verify we have the correct results (in any order)
		expectedResults := []result{
			{1, "Alice", 1, 100},
			{1, "Alice", 2, 200},
			{2, "Bob", 3, 150},
		}

		s.Equal(len(expectedResults), len(actualResults), "Expected %d rows, got %d", len(expectedResults), len(actualResults))

		// Check that each expected result exists in actual results
		for _, expected := range expectedResults {
			found := false
			for _, actual := range actualResults {
				if expected.userID == actual.userID &&
					expected.username == actual.username &&
					expected.orderID == actual.orderID &&
					expected.amount == actual.amount {
					found = true
					break
				}
			}
			s.True(found, "Expected result not found: %+v", expected)
		}
	})

	s.Run("SELECT with INNER JOIN and WHERE clause", func() {
		// Execute INNER JOIN with WHERE clause - only users older than 25
		rows, err := s.db.Query(`
			select
				u.name,
				o.amount
			from users as u inner join orders as o on u.id = o.user_id
			where u.age > 25;
		`)
		s.Require().NoError(err)
		defer rows.Close()

		// Verify results - should only get Bob and Charlie
		// and since Charlie has no orders, only Bob should be returned
		expectedResults := []struct {
			username string
			amount   int64
		}{
			{"Bob", 150},
		}

		i := 0
		for rows.Next() {
			s.Require().Less(i, len(expectedResults), "More rows than expected")

			var (
				username string
				amount   int64
			)
			err := rows.Scan(&username, &amount)
			s.Require().NoError(err)

			expected := expectedResults[i]
			s.Equal(expected.username, username, "Row %d: user_name mismatch", i)
			s.Equal(expected.amount, amount, "Row %d: amount mismatch", i)
			i++
		}

		s.Require().NoError(rows.Err())
		s.Equal(len(expectedResults), i, "Expected %d rows, got %d", len(expectedResults), i)
	})

	s.Run("SELECT with INNER JOIN and ORDER BY table column", func() {
		// Test ORDER BY on outer table column (u.name)
		rows, err := s.db.Query(`
			select
				u.id,
				u.name,
				o.amount
			from users as u inner join orders as o on u.id = o.user_id
			order by u.name;
		`)
		s.Require().NoError(err)

		expectedByName := []struct {
			userID   int64
			username string
			amount   int64
		}{
			{1, "Alice", 200}, // order_id=2 comes first in the orders table
			{1, "Alice", 100}, // order_id=1 comes second
			{2, "Bob", 150},
		}

		i := 0
		for rows.Next() {
			s.Require().Less(i, len(expectedByName), "More rows than expected")

			var (
				userID, amount int64
				username       string
			)
			err := rows.Scan(&userID, &username, &amount)
			s.Require().NoError(err)

			expected := expectedByName[i]
			s.Equal(expected.userID, userID, "Row %d: user_id mismatch", i)
			s.Equal(expected.username, username, "Row %d: user_name mismatch", i)
			s.Equal(expected.amount, amount, "Row %d: amount mismatch", i)
			i++
		}
		rows.Close()
		s.Require().NoError(rows.Err())
		s.Equal(len(expectedByName), i, "Expected %d rows, got %d", len(expectedByName), i)
	})

	s.Run("SELECT with INNER JOIN and ORDER BY inner join table column", func() {
		// Test ORDER BY on inner table column (o.amount DESC)
		rows, err := s.db.Query(`
			select
				u.name,
				o.amount
			from users as u inner join orders as o on u.id = o.user_id
			order by o.amount desc;
		`)
		s.Require().NoError(err)

		expectedByAmount := []struct {
			username string
			amount   int64
		}{
			{"Alice", 200},
			{"Bob", 150},
			{"Alice", 100},
		}

		i := 0
		for rows.Next() {
			s.Require().Less(i, len(expectedByAmount), "More rows than expected")

			var (
				username string
				amount   int64
			)
			err := rows.Scan(&username, &amount)
			s.Require().NoError(err)

			expected := expectedByAmount[i]
			s.Equal(expected.username, username, "Row %d: user_name mismatch", i)
			s.Equal(expected.amount, amount, "Row %d: amount mismatch", i)
			i++
		}
		rows.Close()
		s.Require().NoError(rows.Err())
		s.Equal(len(expectedByAmount), i, "Expected %d rows, got %d", len(expectedByAmount), i)
	})

	s.Run("SELECT with INNER JOIN and ORDER BY multiple columns", func() {
		// Test ORDER BY on multiple columns from different tables
		rows, err := s.db.Query(`
			select
				u.name,
				o.amount
			from users as u inner join orders as o on u.id = o.user_id
			order by u.name, o.amount desc;
		`)
		s.Require().NoError(err)

		expectedByMultiple := []struct {
			username string
			amount   int64
		}{
			{"Alice", 200},
			{"Alice", 100},
			{"Bob", 150},
		}

		i := 0
		for rows.Next() {
			s.Require().Less(i, len(expectedByMultiple), "More rows than expected")

			var (
				username string
				amount   int64
			)
			err := rows.Scan(&username, &amount)
			s.Require().NoError(err)

			expected := expectedByMultiple[i]
			s.Equal(expected.username, username, "Row %d: user_name mismatch", i)
			s.Equal(expected.amount, amount, "Row %d: amount mismatch", i)
			i++
		}
		rows.Close()
		s.Require().NoError(rows.Err())
		s.Equal(len(expectedByMultiple), i, "Expected %d rows, got %d", len(expectedByMultiple), i)
	})
}

func (s *TestSuite) TestInnerJoin_WithSecondaryIndex() {
	// Create users table with primary key
	_, err := s.db.Exec(`create table "users" (
		id int8 primary key,
		name varchar(50)
	);`)
	s.Require().NoError(err)

	// Create orders table with indexed foreign key
	_, err = s.db.Exec(`create table "orders" (
		id int8 primary key,
		user_id int8,
		amount int8
	);`)
	s.Require().NoError(err)

	// Create index on user_id for index nested loop join
	_, err = s.db.Exec(`create index "idx_user_id" on "orders" (user_id);`)
	s.Require().NoError(err)

	// Insert test data
	_, err = s.db.Exec(`insert into users("id", "name") values(1, 'Alice');`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into users("id", "name") values(2, 'Bob');`)
	s.Require().NoError(err)

	// Insert orders
	_, err = s.db.Exec(`insert into orders("id", "user_id", "amount") values(1, 1, 100);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into orders("id", "user_id", "amount") values(2, 1, 200);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into orders("id", "user_id", "amount") values(3, 2, 150);`)
	s.Require().NoError(err)

	// This JOIN should use index nested loop join
	// Orders table has index on user_id, so lookups should be efficient
	rows, err := s.db.Query(`
		select 
			u.id,
			u.name,
			o.id,
			o.amount
		from users as u 
		inner join orders as o on u.id = o.user_id
		order by u.id, o.id;
	`)
	s.Require().NoError(err)
	defer rows.Close()

	expectedResults := []struct {
		userID   int64
		username string
		orderID  int64
		amount   int64
	}{
		{1, "Alice", 1, 100},
		{1, "Alice", 2, 200},
		{2, "Bob", 3, 150},
	}

	i := 0
	for rows.Next() {
		s.Require().Less(i, len(expectedResults), "More rows than expected")

		var userID, orderID, amount int64
		var username string
		err := rows.Scan(&userID, &username, &orderID, &amount)
		s.Require().NoError(err)

		expected := expectedResults[i]
		s.Equal(expected.userID, userID, "Row %d: user_id mismatch", i)
		s.Equal(expected.username, username, "Row %d: user_name mismatch", i)
		s.Equal(expected.orderID, orderID, "Row %d: order_id mismatch", i)
		s.Equal(expected.amount, amount, "Row %d: amount mismatch", i)
		i++
	}

	s.Require().NoError(rows.Err())
	s.Equal(len(expectedResults), i, "Expected %d rows, got %d", len(expectedResults), i)
}
