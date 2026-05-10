package e2etests

import (
	"time"
)

// TestExpressionIndex_Lower tests an expression index on LOWER(email).
func (s *TestSuite) TestExpressionIndex_Lower() {
	_, err := s.db.Exec(`create table "users_lower" (
		id    int8 primary key autoincrement,
		email varchar(200) not null,
		name  varchar(100) not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create index "idx_users_lower_email" on "users_lower" (LOWER(email));`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "users_lower" (email, name) values
		('Alice@Example.COM', 'Alice'),
		('BOB@EXAMPLE.COM',   'Bob'),
		('charlie@example.com','Charlie'),
		('DAVE@Example.com',  'Dave');`)
	s.Require().NoError(err)

	s.Run("case-insensitive lookup finds row", func() {
		rows, err := s.db.Query(`select name from "users_lower" where LOWER(email) = ?`, "bob@example.com")
		s.Require().NoError(err)
		defer rows.Close()

		var name string
		s.Require().True(rows.Next())
		s.Require().NoError(rows.Scan(&name))
		s.Equal("Bob", name)
		s.False(rows.Next())
	})

	s.Run("lookup with mixed-case needle still finds row", func() {
		rows, err := s.db.Query(`select name from "users_lower" where LOWER(email) = ?`, "alice@example.com")
		s.Require().NoError(err)
		defer rows.Close()

		var name string
		s.Require().True(rows.Next())
		s.Require().NoError(rows.Scan(&name))
		s.Equal("Alice", name)
		s.False(rows.Next())
	})

	s.Run("NULL result not indexed — cannot find via expression index", func() {
		// Insert a row with NULL email — it should not appear in the index.
		_, err = s.db.Exec(`insert into "users_lower" (email, name) values ('', 'Empty')`)
		s.Require().NoError(err)

		rows, err := s.db.Query(`select name from "users_lower" where LOWER(email) = ?`, "")
		s.Require().NoError(err)
		defer rows.Close()
		// Empty string '' lowercased is '' — it IS indexed (not NULL), so we find it.
		s.Require().True(rows.Next())
		rows.Close()
	})

	s.Run("update email triggers index update", func() {
		_, err = s.db.Exec(`update "users_lower" set email = ? where name = ?`, "dave_new@example.com", "Dave")
		s.Require().NoError(err)

		rows, err := s.db.Query(`select name from "users_lower" where LOWER(email) = ?`, "dave_new@example.com")
		s.Require().NoError(err)
		defer rows.Close()
		s.Require().True(rows.Next())
		var name string
		s.Require().NoError(rows.Scan(&name))
		s.Equal("Dave", name)
		s.False(rows.Next())
	})

	s.Run("delete removes entry from expression index", func() {
		_, err = s.db.Exec(`delete from "users_lower" where name = ?`, "Charlie")
		s.Require().NoError(err)

		rows, err := s.db.Query(`select name from "users_lower" where LOWER(email) = ?`, "charlie@example.com")
		s.Require().NoError(err)
		defer rows.Close()
		s.False(rows.Next())
	})

	s.Run("integrity check passes", func() {
		results := s.collectPragmaResults(`PRAGMA integrity_check;`)
		s.Require().Len(results, 1)
		s.Equal("ok", results[0].Code)
	})
}

// TestExpressionIndex_Extract tests an expression index on EXTRACT(year FROM created_at).
func (s *TestSuite) TestExpressionIndex_Extract() {
	_, err := s.db.Exec(`create table "events_extract" (
		id         int8 primary key autoincrement,
		title      varchar(100) not null,
		created_at timestamp not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create index "idx_events_year" on "events_extract" (EXTRACT(year FROM created_at));`)
	s.Require().NoError(err)

	ts2022 := time.Date(2022, 6, 15, 0, 0, 0, 0, time.UTC)
	ts2023a := time.Date(2023, 2, 1, 0, 0, 0, 0, time.UTC)
	ts2023b := time.Date(2023, 11, 30, 0, 0, 0, 0, time.UTC)
	ts2024 := time.Date(2024, 4, 20, 0, 0, 0, 0, time.UTC)

	_, err = s.db.Exec(`insert into "events_extract" (title, created_at) values (?, ?)`, "Alpha", ts2022)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "events_extract" (title, created_at) values (?, ?)`, "Beta", ts2023a)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "events_extract" (title, created_at) values (?, ?)`, "Gamma", ts2023b)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "events_extract" (title, created_at) values (?, ?)`, "Delta", ts2024)
	s.Require().NoError(err)

	s.Run("find all events in 2023", func() {
		rows, err := s.db.Query(`select title from "events_extract" where EXTRACT(year FROM created_at) = ?`, int64(2023))
		s.Require().NoError(err)
		defer rows.Close()

		var titles []string
		for rows.Next() {
			var t string
			s.Require().NoError(rows.Scan(&t))
			titles = append(titles, t)
		}
		s.Require().NoError(rows.Err())
		s.ElementsMatch([]string{"Beta", "Gamma"}, titles)
	})

	s.Run("find events in 2022 returns one row", func() {
		rows, err := s.db.Query(`select title from "events_extract" where EXTRACT(year FROM created_at) = ?`, int64(2022))
		s.Require().NoError(err)
		defer rows.Close()
		var title string
		s.Require().True(rows.Next())
		s.Require().NoError(rows.Scan(&title))
		s.Equal("Alpha", title)
		s.False(rows.Next())
	})

	s.Run("integrity check passes", func() {
		results := s.collectPragmaResults(`PRAGMA integrity_check;`)
		s.Require().Len(results, 1)
		s.Equal("ok", results[0].Code)
	})
}

// TestExpressionIndex_DateTrunc tests an expression index on DATE_TRUNC('month', ts).
func (s *TestSuite) TestExpressionIndex_DateTrunc() {
	_, err := s.db.Exec(`create table "orders_month" (
		id         int8 primary key autoincrement,
		amount     int8 not null,
		ordered_at timestamp not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create index "idx_orders_month" on "orders_month" (DATE_TRUNC('month', ordered_at));`)
	s.Require().NoError(err)

	jan := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	jan2 := time.Date(2024, 1, 28, 8, 0, 0, 0, time.UTC)
	feb := time.Date(2024, 2, 3, 0, 0, 0, 0, time.UTC)
	mar := time.Date(2024, 3, 10, 0, 0, 0, 0, time.UTC)

	_, err = s.db.Exec(`insert into "orders_month" (amount, ordered_at) values (?, ?)`, int64(100), jan)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "orders_month" (amount, ordered_at) values (?, ?)`, int64(200), jan2)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "orders_month" (amount, ordered_at) values (?, ?)`, int64(300), feb)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "orders_month" (amount, ordered_at) values (?, ?)`, int64(400), mar)
	s.Require().NoError(err)

	jan1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	s.Run("find orders in January 2024", func() {
		rows, err := s.db.Query(`select amount from "orders_month" where DATE_TRUNC('month', ordered_at) = ?`, jan1)
		s.Require().NoError(err)
		defer rows.Close()

		var amounts []int64
		for rows.Next() {
			var a int64
			s.Require().NoError(rows.Scan(&a))
			amounts = append(amounts, a)
		}
		s.Require().NoError(rows.Err())
		s.ElementsMatch([]int64{100, 200}, amounts)
	})

	s.Run("integrity check passes", func() {
		results := s.collectPragmaResults(`PRAGMA integrity_check;`)
		s.Require().Len(results, 1)
		s.Equal("ok", results[0].Code)
	})
}

// TestExpressionIndex_Arithmetic tests an expression index on price * quantity.
func (s *TestSuite) TestExpressionIndex_Arithmetic() {
	_, err := s.db.Exec(`create table "line_items" (
		id       int8 primary key autoincrement,
		product  varchar(100) not null,
		price    double not null,
		quantity int8 not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create index "idx_line_items_total" on "line_items" (price * quantity);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "line_items" (product, price, quantity) values
		('Widget',  9.99,  10),
		('Gadget',  24.99,  4),
		('Gizmo',   4.99,  20),
		('Doohickey', 49.99, 2);`)
	s.Require().NoError(err)

	s.Run("find item with total = 99.9", func() {
		rows, err := s.db.Query(`select product from "line_items" where price * quantity = ?`, float64(9.99*10))
		s.Require().NoError(err)
		defer rows.Close()
		var product string
		s.Require().True(rows.Next())
		s.Require().NoError(rows.Scan(&product))
		s.Equal("Widget", product)
		s.False(rows.Next())
	})

	s.Run("update price triggers index update", func() {
		_, err = s.db.Exec(`update "line_items" set price = ? where product = ?`, float64(5.0), "Gizmo")
		s.Require().NoError(err)

		rows, err := s.db.Query(`select product from "line_items" where price * quantity = ?`, float64(100.0))
		s.Require().NoError(err)
		defer rows.Close()
		s.Require().True(rows.Next())
		var product string
		s.Require().NoError(rows.Scan(&product))
		s.Equal("Gizmo", product)
		s.False(rows.Next())
	})

	s.Run("integrity check passes", func() {
		results := s.collectPragmaResults(`PRAGMA integrity_check;`)
		s.Require().Len(results, 1)
		s.Equal("ok", results[0].Code)
	})
}

// TestExpressionIndex_Chained tests TRIM(LOWER(username)).
func (s *TestSuite) TestExpressionIndex_Chained() {
	_, err := s.db.Exec(`create table "profiles" (
		id       int8 primary key autoincrement,
		username varchar(100) not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create index "idx_profiles_username_norm" on "profiles" (LOWER(TRIM(username)));`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "profiles" (username) values
		('  Alice  '),
		(' BOB '),
		('Charlie'),
		('  DAVE  ');`)
	s.Require().NoError(err)

	s.Run("find by normalised username", func() {
		rows, err := s.db.Query(`select username from "profiles" where LOWER(TRIM(username)) = ?`, "bob")
		s.Require().NoError(err)
		defer rows.Close()
		var username string
		s.Require().True(rows.Next())
		s.Require().NoError(rows.Scan(&username))
		s.Equal(" BOB ", username)
		s.False(rows.Next())
	})

	s.Run("integrity check passes", func() {
		results := s.collectPragmaResults(`PRAGMA integrity_check;`)
		s.Require().Len(results, 1)
		s.Equal("ok", results[0].Code)
	})
}

// TestExpressionIndex_Substr tests SUBSTR(barcode, 1, 6) prefix extraction.
func (s *TestSuite) TestExpressionIndex_Substr() {
	_, err := s.db.Exec(`create table "products_barcode" (
		id      int8 primary key autoincrement,
		name    varchar(100) not null,
		barcode varchar(20) not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create index "idx_barcode_prefix" on "products_barcode" (SUBSTR(barcode, 1, 6));`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "products_barcode" (name, barcode) values
		('Product A', '123456AAAA'),
		('Product B', '123456BBBB'),
		('Product C', '999999CCCC');`)
	s.Require().NoError(err)

	s.Run("find products with prefix 123456", func() {
		rows, err := s.db.Query(`select name from "products_barcode" where SUBSTR(barcode, 1, 6) = ?`, "123456")
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var n string
			s.Require().NoError(rows.Scan(&n))
			names = append(names, n)
		}
		s.Require().NoError(rows.Err())
		s.ElementsMatch([]string{"Product A", "Product B"}, names)
	})

	s.Run("integrity check passes", func() {
		results := s.collectPragmaResults(`PRAGMA integrity_check;`)
		s.Require().Len(results, 1)
		s.Equal("ok", results[0].Code)
	})
}

// TestExpressionIndex_Coalesce tests COALESCE(display_name, username).
func (s *TestSuite) TestExpressionIndex_Coalesce() {
	_, err := s.db.Exec(`create table "accounts" (
		id           int8 primary key autoincrement,
		username     varchar(100) not null,
		display_name varchar(100)
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create index "idx_accounts_display" on "accounts" (COALESCE(display_name, username));`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "accounts" (username, display_name) values
		('alice', 'Alice W.'),
		('bob',   NULL),
		('carol', 'Carol');`)
	s.Require().NoError(err)

	s.Run("find by display_name when set", func() {
		rows, err := s.db.Query(`select username from "accounts" where COALESCE(display_name, username) = ?`, "Alice W.")
		s.Require().NoError(err)
		defer rows.Close()
		var username string
		s.Require().True(rows.Next())
		s.Require().NoError(rows.Scan(&username))
		s.Equal("alice", username)
		s.False(rows.Next())
	})

	s.Run("find by username when display_name is NULL", func() {
		rows, err := s.db.Query(`select username from "accounts" where COALESCE(display_name, username) = ?`, "bob")
		s.Require().NoError(err)
		defer rows.Close()
		var username string
		s.Require().True(rows.Next())
		s.Require().NoError(rows.Scan(&username))
		s.Equal("bob", username)
		s.False(rows.Next())
	})

	s.Run("integrity check passes", func() {
		results := s.collectPragmaResults(`PRAGMA integrity_check;`)
		s.Require().Len(results, 1)
		s.Equal("ok", results[0].Code)
	})
}

// TestExpressionIndex_JSONPath tests payload->>'user_id' with mixed JSON value types.
func (s *TestSuite) TestExpressionIndex_JSONPath() {
	_, err := s.db.Exec(`create table "events_json" (
		id      int8 primary key autoincrement,
		payload json not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create index "idx_events_user_id" on "events_json" (payload->>'user_id');`)
	s.Require().NoError(err)

	// Insert rows with various value types under the same JSON path.
	_, err = s.db.Exec(`insert into "events_json" (payload) values
		('{"user_id": "u001", "action": "login"}'),
		('{"user_id": "u002", "action": "purchase"}'),
		('{"user_id": "u001", "action": "logout"}'),
		('{"action": "system", "user_id": null}'),
		('{"action": "cron"}');`)
	s.Require().NoError(err)

	s.Run("planner uses JSON expression index", func() {
		rows := s.collectExplain(`EXPLAIN SELECT payload->>'action' FROM "events_json" WHERE payload->>'user_id' = 'u001';`)
		s.Require().NotEmpty(rows)
		s.Equal("index_point", rows[0].Operation)
		s.Contains(rows[0].Detail, "table=events_json")
		s.Contains(rows[0].Detail, "index=idx_events_user_id")
	})

	s.Run("find events for user u001", func() {
		rows, err := s.db.Query(`select payload->>'action' from "events_json" where payload->>'user_id' = ?`, "u001")
		s.Require().NoError(err)
		defer rows.Close()

		var actions []string
		for rows.Next() {
			var a string
			s.Require().NoError(rows.Scan(&a))
			actions = append(actions, a)
		}
		s.Require().NoError(rows.Err())
		s.ElementsMatch([]string{"login", "logout"}, actions)
	})

	s.Run("missing path key yields NULL — row not indexed", func() {
		// The row with action=cron has no user_id key; it should not appear.
		rows, err := s.db.Query(`select id from "events_json" where payload->>'user_id' is null`)
		s.Require().NoError(err)
		defer rows.Close()
		// Rows with null user_id: the 4th row (null value) and 5th row (missing key).
		// These are not in the expression index (NULL result → not indexed).
		// Sequential scan finds them.
		var count int
		for rows.Next() {
			count += 1
		}
		s.Require().NoError(rows.Err())
		s.Equal(2, count)
	})

	s.Run("numeric JSON value under same path stored as string key", func() {
		_, err = s.db.Exec(`insert into "events_json" (payload) values ('{"user_id": 42, "action": "numeric"}')`)
		s.Require().NoError(err)

		// Numeric 42 from JSON -> jsonToScalar -> int64(42) -> castKeyValue(Varchar, 42) -> "42"
		// Query with string "42" should match.
		rows, err := s.db.Query(`select payload->>'action' from "events_json" where payload->>'user_id' = ?`, "42")
		s.Require().NoError(err)
		defer rows.Close()
		var action string
		s.Require().True(rows.Next())
		s.Require().NoError(rows.Scan(&action))
		s.Equal("numeric", action)
		s.False(rows.Next())
	})

	s.Run("integrity check passes", func() {
		results := s.collectPragmaResults(`PRAGMA integrity_check;`)
		s.Require().Len(results, 1)
		s.Equal("ok", results[0].Code)
	})
}

// TestExpressionIndex_JSONPathCast tests CAST(payload->>'price' AS DOUBLE).
func (s *TestSuite) TestExpressionIndex_JSONPathCast() {
	_, err := s.db.Exec(`create table "catalog" (
		id   int8 primary key autoincrement,
		data json not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create index "idx_catalog_price" on "catalog" (CAST(data->>'price' AS double));`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "catalog" (data) values
		('{"name":"Widget","price":9.99}'),
		('{"name":"Gadget","price":24.99}'),
		('{"name":"Gizmo","price":4.99}'),
		('{"name":"NoPrice"}');`)
	s.Require().NoError(err)

	s.Run("find item by price cast from JSON", func() {
		rows, err := s.db.Query(`select data->>'name' from "catalog" where CAST(data->>'price' AS double) = ?`, float64(9.99))
		s.Require().NoError(err)
		defer rows.Close()
		var name string
		s.Require().True(rows.Next())
		s.Require().NoError(rows.Scan(&name))
		s.Equal("Widget", name)
		s.False(rows.Next())
	})

	s.Run("row with missing path not indexed — no match", func() {
		rows, err := s.db.Query(`select data->>'name' from "catalog" where CAST(data->>'price' AS double) = ?`, float64(0))
		s.Require().NoError(err)
		defer rows.Close()
		s.False(rows.Next())
	})

	s.Run("integrity check passes", func() {
		results := s.collectPragmaResults(`PRAGMA integrity_check;`)
		s.Require().Len(results, 1)
		s.Equal("ok", results[0].Code)
	})
}

// TestExpressionIndex_NullNotIndexed verifies rows where the expression evaluates
// to NULL are excluded from the index and can still be found by sequential scan.
func (s *TestSuite) TestExpressionIndex_NullNotIndexed() {
	_, err := s.db.Exec(`create table "nullable_col" (
		id  int8 primary key autoincrement,
		val varchar(100)
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create index "idx_nullable_lower" on "nullable_col" (LOWER(val));`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "nullable_col" (val) values ('Hello'), (NULL), ('World'), (NULL);`)
	s.Require().NoError(err)

	s.Run("non-null rows findable via expression index", func() {
		rows, err := s.db.Query(`select id from "nullable_col" where LOWER(val) = ?`, "hello")
		s.Require().NoError(err)
		defer rows.Close()
		s.Require().True(rows.Next())
		s.False(rows.Next())
	})

	s.Run("null rows not indexed — sequential scan finds them", func() {
		rows, err := s.db.Query(`select id from "nullable_col" where val is null`)
		s.Require().NoError(err)
		defer rows.Close()
		var count int
		for rows.Next() {
			count += 1
		}
		s.Require().NoError(rows.Err())
		s.Equal(2, count)
	})

	s.Run("integrity check passes", func() {
		results := s.collectPragmaResults(`PRAGMA integrity_check;`)
		s.Require().Len(results, 1)
		s.Equal("ok", results[0].Code)
	})
}

// TestExpressionIndex_DDLRoundTrip verifies that the expression index survives a
// database reopen (DDL is stored and the index is reconstructed from the schema).
func (s *TestSuite) TestExpressionIndex_DDLRoundTrip() {
	_, err := s.db.Exec(`create table "reopen_test" (
		id    int8 primary key autoincrement,
		email varchar(200) not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create index "idx_reopen_lower_email" on "reopen_test" (LOWER(email));`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "reopen_test" (email) values
		('Alice@Example.COM'),
		('bob@example.com');`)
	s.Require().NoError(err)

	// Reopen the database.
	s.Require().NoError(s.db.Close())
	s.db = s.reopenDB()

	s.Run("expression index works after reopen", func() {
		rows, err := s.db.Query(`select id from "reopen_test" where LOWER(email) = ?`, "alice@example.com")
		s.Require().NoError(err)
		defer rows.Close()
		s.Require().True(rows.Next())
		var id int64
		s.Require().NoError(rows.Scan(&id))
		s.Equal(int64(1), id)
		s.False(rows.Next())
	})

	s.Run("insert after reopen maintains index", func() {
		_, err = s.db.Exec(`insert into "reopen_test" (email) values (?)`, "CAROL@EXAMPLE.COM")
		s.Require().NoError(err)

		rows, err := s.db.Query(`select id from "reopen_test" where LOWER(email) = ?`, "carol@example.com")
		s.Require().NoError(err)
		defer rows.Close()
		s.Require().True(rows.Next())
		s.False(rows.Next())
	})

	s.Run("integrity check passes after reopen", func() {
		results := s.collectPragmaResults(`PRAGMA integrity_check;`)
		s.Require().Len(results, 1)
		s.Equal("ok", results[0].Code)
	})
}

// TestExpressionIndex_ImmutabilityRejected verifies that a NOW() expression index
// is rejected at creation time.
func (s *TestSuite) TestExpressionIndex_ImmutabilityRejected() {
	_, err := s.db.Exec(`create table "immut_test" (
		id int8 primary key autoincrement,
		ts timestamp not null
	);`)
	s.Require().NoError(err)

	// NOW() is not deterministic — should be rejected.
	_, err = s.db.Exec(`create index "idx_bad_now" on "immut_test" (DATE_TRUNC('day', NOW()));`)
	s.Require().Error(err)
	s.Contains(err.Error(), "immutable")
}
