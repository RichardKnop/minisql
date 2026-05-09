package e2etests

func (s *TestSuite) TestPartialIndex() {
	_, err := s.db.Exec(`create table "orders" (
		id     int8 primary key autoincrement,
		status varchar(20) not null,
		amount int8 not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create index "idx_orders_active_amount" on "orders" (amount) where status = 'active';`)
	s.Require().NoError(err)

	// Insert rows: 3 active, 2 inactive.
	_, err = s.db.Exec(`insert into "orders" (status, amount) values
		('active',   100),
		('inactive',  50),
		('active',   200),
		('inactive',  75),
		('active',   150);`)
	s.Require().NoError(err)

	s.Run("select with implied where uses partial index", func() {
		rows, err := s.db.Query(`select id, amount from "orders" where status = 'active' and amount = 200`)
		s.Require().NoError(err)
		defer rows.Close()

		var id, amount int64
		s.Require().True(rows.Next())
		s.Require().NoError(rows.Scan(&id, &amount))
		s.Equal(int64(200), amount)
		s.False(rows.Next())
	})

	s.Run("select without implied where falls back to sequential scan", func() {
		// This query does not include status = 'active', so the partial index
		// must NOT be used (it would miss inactive rows with amount = 50).
		rows, err := s.db.Query(`select id, amount from "orders" where amount = 50`)
		s.Require().NoError(err)
		defer rows.Close()

		var id, amount int64
		s.Require().True(rows.Next())
		s.Require().NoError(rows.Scan(&id, &amount))
		s.Equal(int64(50), amount)
		s.False(rows.Next())
	})

	s.Run("select returns only matching rows under partial index predicate", func() {
		rows, err := s.db.Query(`select id, amount from "orders" where status = 'active' order by amount`)
		s.Require().NoError(err)
		defer rows.Close()

		var amounts []int64
		for rows.Next() {
			var id, amount int64
			s.Require().NoError(rows.Scan(&id, &amount))
			amounts = append(amounts, amount)
		}
		s.Require().NoError(rows.Err())
		s.Equal([]int64{100, 150, 200}, amounts)
	})
}

func (s *TestSuite) TestPartialIndex_DML() {
	_, err := s.db.Exec(`create table "products" (
		id       int8 primary key autoincrement,
		archived boolean not null,
		price    int8 not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create index "idx_active_products" on "products" (price) where archived = false;`)
	s.Require().NoError(err)

	// Insert: 2 active, 1 archived.
	_, err = s.db.Exec(`insert into "products" (archived, price) values (false, 10), (true, 20), (false, 30);`)
	s.Require().NoError(err)

	s.Run("delete active row removes it from partial index", func() {
		_, err := s.db.Exec(`delete from "products" where price = 10 and archived = false`)
		s.Require().NoError(err)

		// Verify the row is gone.
		rows, err := s.db.Query(`select id from "products" where price = 10`)
		s.Require().NoError(err)
		defer rows.Close()
		s.False(rows.Next(), "deleted row should not be found")
	})

	s.Run("delete archived row does not affect partial index", func() {
		_, err := s.db.Exec(`delete from "products" where price = 20`)
		s.Require().NoError(err)

		// Archived row was not in the partial index; verify remaining active row still queryable.
		rows, err := s.db.Query(`select id, price from "products" where archived = false and price = 30`)
		s.Require().NoError(err)
		defer rows.Close()
		s.Require().True(rows.Next())
		var id, price int64
		s.Require().NoError(rows.Scan(&id, &price))
		s.Equal(int64(30), price)
		s.False(rows.Next())
	})
}

func (s *TestSuite) TestPartialIndex_UpdatePredicateColumn() {
	_, err := s.db.Exec(`create table "items" (
		id     int8 primary key autoincrement,
		status varchar(20) not null,
		price  int8 not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create index "idx_active_price" on "items" (price) where status = 'active';`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "items" (status, price) values ('inactive', 99);`)
	s.Require().NoError(err)

	s.Run("activating an inactive row adds it to the partial index", func() {
		// Only the predicate column changes; the indexed column (price) stays the same.
		_, err := s.db.Exec(`update "items" set status = 'active' where price = 99`)
		s.Require().NoError(err)

		rows, err := s.db.Query(`select id, price from "items" where status = 'active' and price = 99`)
		s.Require().NoError(err)
		defer rows.Close()
		s.Require().True(rows.Next(), "row should be found after activation")
		var id, price int64
		s.Require().NoError(rows.Scan(&id, &price))
		s.Equal(int64(99), price)
		s.False(rows.Next())

		// Integrity check confirms the index is consistent.
		results := s.collectPragmaResults(`PRAGMA integrity_check;`)
		s.Require().Len(results, 1)
		s.Equal("ok", results[0].Code)
	})

	s.Run("deactivating an active row removes it from the partial index", func() {
		_, err := s.db.Exec(`update "items" set status = 'inactive' where price = 99`)
		s.Require().NoError(err)

		// With no implied partial-index predicate the planner uses a seq scan;
		// the row should still be found.
		rows, err := s.db.Query(`select id, price from "items" where price = 99`)
		s.Require().NoError(err)
		defer rows.Close()
		s.Require().True(rows.Next(), "row should still exist in the table")
		var id, price int64
		s.Require().NoError(rows.Scan(&id, &price))
		s.Equal(int64(99), price)
		s.False(rows.Next())

		// Integrity check confirms no stale entry remains in the partial index.
		results := s.collectPragmaResults(`PRAGMA integrity_check;`)
		s.Require().Len(results, 1)
		s.Equal("ok", results[0].Code)
	})
}

func (s *TestSuite) TestPartialIndex_DDLRoundTrip() {
	_, err := s.db.Exec(`create table "events" (
		id      int8 primary key autoincrement,
		visible boolean not null,
		name    varchar(100) not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create index "idx_visible_events" on "events" (name) where visible = true;`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "events" (visible, name) values (true, 'alpha'), (false, 'beta'), (true, 'gamma');`)
	s.Require().NoError(err)

	// Reopen the database and verify the partial index still works correctly.
	s.Require().NoError(s.db.Close())
	s.db = s.reopenDB()

	s.Run("partial index survives reopen", func() {
		rows, err := s.db.Query(`select name from "events" where visible = true and name = 'alpha'`)
		s.Require().NoError(err)
		defer rows.Close()
		s.Require().True(rows.Next())
		var name string
		s.Require().NoError(rows.Scan(&name))
		s.Equal("alpha", name)
		s.False(rows.Next())
	})

	s.Run("invisible row not returned via partial index path", func() {
		// Query includes the partial index predicate so the index could be used;
		// but the row 'beta' has visible=false and was never indexed.
		rows, err := s.db.Query(`select name from "events" where visible = true`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var name string
			s.Require().NoError(rows.Scan(&name))
			names = append(names, name)
		}
		s.Require().NoError(rows.Err())
		s.ElementsMatch([]string{"alpha", "gamma"}, names)
	})
}
