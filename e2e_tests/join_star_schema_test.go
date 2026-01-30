package e2etests

import (
	"fmt"
)

// TestStarSchemaJoin tests multiple tables joining to a central base table
// Schema: users (central) <- orders, users <- addresses
func (s *TestSuite) TestStarSchemaJoin() {
	// Create users table (central/base table)
	_, err := s.db.Exec(`
		create table "users" (
			id int8 primary key,
			name varchar(255),
			email varchar(255)
		);
	`)
	s.Require().NoError(err)

	// Create orders table
	_, err = s.db.Exec(`
		create table "orders" (
			id int8 primary key,
			user_id int8,
			amount int8,
			status varchar(50)
		);
	`)
	s.Require().NoError(err)

	// Create index on user_id for optimization
	_, err = s.db.Exec(`
		create index "idx_orders_user_id" on "orders" (user_id);
	`)
	s.Require().NoError(err)

	// Create addresses table
	_, err = s.db.Exec(`
		create table "addresses" (
			id int8 primary key,
			user_id int8,
			street varchar(255),
			city varchar(100)
		);
	`)
	s.Require().NoError(err)

	// Create index on user_id for optimization
	_, err = s.db.Exec(`
		create index "idx_addresses_user_id" on "addresses" (user_id);
	`)
	s.Require().NoError(err)

	// Insert test data - users
	_, err = s.db.Exec(`insert into users ("id", "name", "email") values (1, 'Alice', 'alice@example.com');`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into users ("id", "name", "email") values (2, 'Bob', 'bob@example.com');`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into users ("id", "name", "email") values (3, 'Charlie', 'charlie@example.com');`)
	s.Require().NoError(err)

	// Insert test data - orders
	_, err = s.db.Exec(`insert into orders ("id", "user_id", "amount", "status") values (1, 1, 100, 'completed');`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into orders ("id", "user_id", "amount", "status") values (2, 1, 250, 'pending');`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into orders ("id", "user_id", "amount", "status") values (3, 2, 50, 'completed');`)
	s.Require().NoError(err)

	// Insert test data - addresses
	_, err = s.db.Exec(`insert into addresses ("id", "user_id", "street", "city") values (1, 1, '123 Main St', 'New York');`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into addresses ("id", "user_id", "street", "city") values (2, 1, '456 Oak Ave', 'Boston');`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into addresses ("id", "user_id", "street", "city") values (3, 3, '789 Pine Rd', 'Seattle');`)
	s.Require().NoError(err)

	// Test 1: Join users with orders and addresses (3-way star join)
	s.Run("ThreeWayStarJoin", func() {
		rows, err := s.db.Query(`
			SELECT u.name, o.amount, a.city
			FROM users AS u
			INNER JOIN orders AS o ON u.id = o.user_id
			INNER JOIN addresses AS a ON u.id = a.user_id
			ORDER BY u.name, o.amount, a.city;
		`)
		s.Require().NoError(err)
		defer rows.Close()

		// Alice has 2 orders × 2 addresses = 4 combinations
		// Bob has 1 order × 0 addresses = 0 combinations
		// Charlie has 0 orders × 1 address = 0 combinations
		// Total: 4 rows

		type result struct {
			name   string
			amount int64
			city   string
		}
		var results []result

		for rows.Next() {
			var r result
			err := rows.Scan(&r.name, &r.amount, &r.city)
			s.Require().NoError(err)
			results = append(results, r)
		}

		s.Require().Equal(4, len(results))

		// Verify the combinations (sorted by name, amount, city)
		s.Assert().Equal("Alice", results[0].name)
		s.Assert().Equal(int64(100), results[0].amount)
		s.Assert().Equal("Boston", results[0].city)

		s.Assert().Equal("Alice", results[1].name)
		s.Assert().Equal(int64(100), results[1].amount)
		s.Assert().Equal("New York", results[1].city)

		s.Assert().Equal("Alice", results[2].name)
		s.Assert().Equal(int64(250), results[2].amount)
		s.Assert().Equal("Boston", results[2].city)

		s.Assert().Equal("Alice", results[3].name)
		s.Assert().Equal(int64(250), results[3].amount)
		s.Assert().Equal("New York", results[3].city)
	})

	// Test 2: Star join with WHERE clause on joined tables
	s.Run("StarJoinWithWhere", func() {
		rows, err := s.db.Query(`
			SELECT u.name, o.amount, a.city
			FROM users AS u
			INNER JOIN orders AS o ON u.id = o.user_id
			INNER JOIN addresses AS a ON u.id = a.user_id
			WHERE o.status = 'completed' AND a.city = 'New York'
			ORDER BY u.name;
		`)
		s.Require().NoError(err)
		defer rows.Close()

		// Only Alice's completed order (100) with New York address
		type result struct {
			name   string
			amount int64
			city   string
		}
		var results []result

		for rows.Next() {
			var r result
			err := rows.Scan(&r.name, &r.amount, &r.city)
			s.Require().NoError(err)
			results = append(results, r)
		}

		s.Require().Equal(1, len(results))
		s.Assert().Equal("Alice", results[0].name)
		s.Assert().Equal(int64(100), results[0].amount)
		s.Assert().Equal("New York", results[0].city)
	})

	// Test 3: Two-table join (subset of star schema)
	s.Run("TwoTableSubset", func() {
		rows, err := s.db.Query(`
			SELECT u.name, o.amount
			FROM users AS u
			INNER JOIN orders AS o ON u.id = o.user_id
			ORDER BY u.name, o.amount;
		`)
		s.Require().NoError(err)
		defer rows.Close()

		type result struct {
			name   string
			amount int64
		}
		var results []result

		for rows.Next() {
			var r result
			err := rows.Scan(&r.name, &r.amount)
			s.Require().NoError(err)
			results = append(results, r)
		}

		s.Require().Equal(3, len(results))
		s.Assert().Equal("Alice", results[0].name)
		s.Assert().Equal(int64(100), results[0].amount)
		s.Assert().Equal("Alice", results[1].name)
		s.Assert().Equal(int64(250), results[1].amount)
		s.Assert().Equal("Bob", results[2].name)
		s.Assert().Equal(int64(50), results[2].amount)
	})

	// Test 4: Star join with all columns selected
	s.Run("StarJoinSelectAll", func() {
		rows, err := s.db.Query(`
			SELECT u.id, u.name, u.email, o.id, o.amount, a.city
			FROM users AS u
			INNER JOIN orders AS o ON u.id = o.user_id
			INNER JOIN addresses AS a ON u.id = a.user_id
			WHERE u.name = 'Alice'
			ORDER BY o.amount, a.city;
		`)
		s.Require().NoError(err)
		defer rows.Close()

		var count int
		for rows.Next() {
			var userID, orderID, amount int64
			var name, email, city string
			err := rows.Scan(&userID, &name, &email, &orderID, &amount, &city)
			s.Require().NoError(err)
			count++
			// All rows should be for Alice (user_id = 1)
			s.Assert().Equal(int64(1), userID)
			s.Assert().Equal("Alice", name)
			s.Assert().Equal("alice@example.com", email)
		}

		s.Require().Equal(4, count) // Alice: 2 orders × 2 addresses = 4
	})
}

// TestStarSchemaNoIndexes tests star schema when joined tables don't have indexes
func (s *TestSuite) TestStarSchemaNoIndexes() {
	// Create tables without indexes
	_, err := s.db.Exec(`
		create table "users" (
			id int8 primary key,
			name varchar(255)
		);
	`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`
		create table "orders" (
			id int8 primary key,
			user_id int8,
			amount int8
		);
	`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`
		create table "payments" (
			id int8 primary key,
			user_id int8,
			method varchar(50)
		);
	`)
	s.Require().NoError(err)

	// Insert test data
	_, err = s.db.Exec(`insert into users ("id", "name") values (1, 'Alice');`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into orders ("id", "user_id", "amount") values (1, 1, 100);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into payments ("id", "user_id", "method") values (1, 1, 'credit');`)
	s.Require().NoError(err)

	// Three-way join without indexes should still work (using sequential scans)
	rows, err := s.db.Query(`
		SELECT u.name, o.amount, p.method
		FROM users AS u
		INNER JOIN orders AS o ON u.id = o.user_id
		INNER JOIN payments AS p ON u.id = p.user_id;
	`)
	s.Require().NoError(err)
	defer rows.Close()

	s.Require().True(rows.Next())
	var name, method string
	var amount int64
	err = rows.Scan(&name, &amount, &method)
	s.Require().NoError(err)
	s.Assert().Equal("Alice", name)
	s.Assert().Equal(int64(100), amount)
	s.Assert().Equal("credit", method)

	// Should be only one row
	s.Require().False(rows.Next())
}

// TestStarSchemaLargeResult tests star schema with cartesian product
func (s *TestSuite) TestStarSchemaLargeResult() {
	// Create tables
	_, err := s.db.Exec(`
		create table "users" (
			id int8 primary key,
			name varchar(255)
		);
	`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`
		create table "orders" (
			id int8 primary key,
			user_id int8,
			product varchar(255)
		);
	`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`
		create index "idx_orders_user_id" on "orders" (user_id);
	`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`
		create table "reviews" (
			id int8 primary key,
			user_id int8,
			rating int8
		);
	`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`
		create index "idx_reviews_user_id" on "reviews" (user_id);
	`)
	s.Require().NoError(err)

	// Insert 1 user with 3 orders and 2 reviews = 6 result rows (3×2)
	_, err = s.db.Exec(`insert into users ("id", "name") values (1, 'Alice');`)
	s.Require().NoError(err)

	for i := 1; i <= 3; i++ {
		_, err = s.db.Exec(fmt.Sprintf(`insert into orders ("id", "user_id", "product") values (%d, 1, 'Product %d');`, i, i))
		s.Require().NoError(err)
	}

	for i := 1; i <= 2; i++ {
		_, err = s.db.Exec(fmt.Sprintf(`insert into reviews ("id", "user_id", "rating") values (%d, 1, %d);`, i, i+3))
		s.Require().NoError(err)
	}

	rows, err := s.db.Query(`
		SELECT u.name, o.product, r.rating
		FROM users AS u
		INNER JOIN orders AS o ON u.id = o.user_id
		INNER JOIN reviews AS r ON u.id = r.user_id
		ORDER BY o.product, r.rating;
	`)
	s.Require().NoError(err)
	defer rows.Close()

	var count int
	for rows.Next() {
		var name, product string
		var rating int64
		err := rows.Scan(&name, &product, &rating)
		s.Require().NoError(err)
		count++
		s.Assert().Equal("Alice", name)
	}

	// 3 orders × 2 reviews = 6 combinations
	s.Require().Equal(6, count)
}
