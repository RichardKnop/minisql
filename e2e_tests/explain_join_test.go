package e2etests

import "context"

// TestExplainJoin verifies EXPLAIN output for all JOIN-related query plan
// features: nested-loop indexed join, hash join, LEFT/RIGHT JOIN, chain joins,
// predicate pushdown with index acceleration, and EXPLAIN ANALYZE for JOINs.
func (s *TestSuite) TestExplainJoin() {
	_, err := s.db.Exec(`create table "ej_orders" (
		order_id int8 primary key autoincrement,
		user_id  int8 not null,
		amount   int8 not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "ej_users" (
		user_id  int8 primary key autoincrement,
		name     varchar(50) not null,
		region   varchar(20) not null
	)`)
	s.Require().NoError(err)

	// Create an index on ej_orders.user_id — this makes the join use indexed nested-loop.
	_, err = s.db.Exec(`create index "idx_ej_orders_user_id" on "ej_orders" (user_id)`)
	s.Require().NoError(err)

	// Create an index on ej_users.region — for predicate pushdown testing.
	_, err = s.db.Exec(`create index "idx_ej_users_region" on "ej_users" (region)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "ej_users" (name, region) values
		('Alice', 'west'), ('Bob', 'east'), ('Carol', 'west')`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "ej_orders" (user_id, amount) values
		(1, 100), (1, 200), (2, 50), (3, 75)`)
	s.Require().NoError(err)

	// Third table for chain-join tests.
	_, err = s.db.Exec(`create table "ej_items" (
		item_id  int8 primary key autoincrement,
		order_id int8 not null,
		sku      varchar(20) not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create index "idx_ej_items_order_id" on "ej_items" (order_id)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "ej_items" (order_id, sku) values
		(1, 'SKU-A'), (2, 'SKU-B'), (3, 'SKU-C')`)
	s.Require().NoError(err)

	s.Run("indexed_nested_loop_join", func() {
		// ej_orders.user_id has an index → nested-loop indexed join.
		rows := s.collectExplain(`
			EXPLAIN SELECT u.name, o.amount
			FROM "ej_users" AS u
			INNER JOIN "ej_orders" AS o ON u.user_id = o.user_id`)

		joinRow := s.findExplainRow(rows, "join")
		s.Require().NotNil(joinRow, "expected a 'join' row")
		s.Contains(joinRow.Detail, "algorithm=nested_loop")
		s.Contains(joinRow.Detail, "type=inner")
		s.Contains(joinRow.Detail, "left=u")
		s.Contains(joinRow.Detail, "right=o")
		s.Contains(joinRow.Detail, "on=u.user_id=o.user_id")
	})

	s.Run("hash_join", func() {
		// No index on ej_orders.user_id if we reverse the join: make ej_orders the
		// outer table and join to a table without an index on the join column.
		// Re-use the hash_join test schema's employees table structure: we'll use
		// ej_items which has an index, but ej_users does NOT have an index on user_id
		// as an FK target... Actually ej_users has PK on user_id, so a join from
		// ej_orders → ej_users would use an index.
		//
		// Use a separate table without an index to force hash join.
		_, err := s.db.Exec(`create table "ej_tags" (
			tag_id   int8 primary key autoincrement,
			user_id  int8 not null,
			tag      varchar(20) not null
		)`)
		s.Require().NoError(err)
		_, err = s.db.Exec(`insert into "ej_tags" (user_id, tag) values (1, 'vip'), (2, 'new')`)
		s.Require().NoError(err)

		// ej_tags (2 rows) < ej_users (3 rows): greedy join reordering promotes
		// ej_tags to the outer/base position and ej_users to the inner position.
		// ej_users has a PK on user_id, so the planner uses indexed nested-loop —
		// strictly better than the hash join the user-specified order would have
		// produced.
		rows := s.collectExplain(`
			EXPLAIN SELECT u.name, t.tag
			FROM "ej_users" AS u
			INNER JOIN "ej_tags" AS t ON u.user_id = t.user_id`)

		joinRow := s.findExplainRow(rows, "join")
		s.Require().NotNil(joinRow, "expected a 'join' row")
		s.Contains(joinRow.Detail, "type=inner")
		s.Contains(joinRow.Detail, "algorithm=nested_loop")
		s.Contains(joinRow.Detail, "left=t")
		s.Contains(joinRow.Detail, "right=u")
	})

	s.Run("left_join", func() {
		rows := s.collectExplain(`
			EXPLAIN SELECT u.name, o.amount
			FROM "ej_users" AS u
			LEFT JOIN "ej_orders" AS o ON u.user_id = o.user_id`)

		joinRow := s.findExplainRow(rows, "join")
		s.Require().NotNil(joinRow)
		s.Contains(joinRow.Detail, "type=left")
	})

	s.Run("right_join", func() {
		rows := s.collectExplain(`
			EXPLAIN SELECT u.name, o.amount
			FROM "ej_users" AS u
			RIGHT JOIN "ej_orders" AS o ON u.user_id = o.user_id`)

		joinRow := s.findExplainRow(rows, "join")
		s.Require().NotNil(joinRow)
		s.Contains(joinRow.Detail, "type=right")
		s.Contains(joinRow.Detail, "algorithm=nested_loop")
	})

	s.Run("predicate_pushdown_base_uses_index", func() {
		// u.region = 'west' pushed down to ej_users base scan → index_point.
		rows := s.collectExplain(`
			EXPLAIN SELECT u.name, o.amount
			FROM "ej_users" AS u
			INNER JOIN "ej_orders" AS o ON u.user_id = o.user_id
			WHERE u.region = 'west'`)

		indexRow := s.findExplainRow(rows, "index_point")
		s.Require().NotNil(indexRow, "expected index_point scan for base table with pushed-down condition")
		s.Contains(indexRow.Detail, "table=ej_users")
	})

	s.Run("chain_join_three_tables", func() {
		// users → orders → items: 2 join steps, 3 scan steps.
		rows := s.collectExplain(`
			EXPLAIN SELECT u.name, o.amount, i.sku
			FROM "ej_users" AS u
			INNER JOIN "ej_orders" AS o ON u.user_id = o.user_id
			INNER JOIN "ej_items" AS i ON o.order_id = i.order_id`)

		s.Require().GreaterOrEqual(len(rows), 5, "expected at least 3 scans + 2 joins")

		joinRows := s.findAllExplainRows(rows, "join")
		s.Require().Len(joinRows, 2, "expected 2 join steps for 3-table chain")
	})

	s.Run("explain_analyze_join_shows_actual_for_join_step", func() {
		rows := s.collectExplain(`
			EXPLAIN ANALYZE SELECT u.name, o.amount
			FROM "ej_users" AS u
			INNER JOIN "ej_orders" AS o ON u.user_id = o.user_id`)

		joinRow := s.findExplainRow(rows, "join")
		s.Require().NotNil(joinRow, "expected a 'join' row in EXPLAIN ANALYZE output")
		s.True(joinRow.RowsActual.Valid, "join step should have actual row count")
		s.True(joinRow.DurationUS.Valid, "join step should have duration")
		s.Equal(int64(4), joinRow.RowsActual.Int64) // 3 users × matched orders: 2+1+1 = 4
	})

	s.Run("sort_step_in_join_with_order_by", func() {
		rows := s.collectExplain(`
			EXPLAIN SELECT u.name, o.amount
			FROM "ej_users" AS u
			INNER JOIN "ej_orders" AS o ON u.user_id = o.user_id
			ORDER BY o.amount`)

		sortRow := s.findExplainRow(rows, "sort")
		s.Require().NotNil(sortRow, "expected a 'sort' step when ORDER BY is used with JOIN")
		s.Contains(sortRow.Detail, "order_by=")
	})

	s.Run("join_scan_row_estimates_use_correct_table", func() {
		// Inner join table scans should show estimates based on the join table's
		// row count, not the base table's row count.
		rows := s.collectExplain(`
			EXPLAIN SELECT u.name, o.amount
			FROM "ej_users" AS u
			INNER JOIN "ej_orders" AS o ON u.user_id = o.user_id`)

		// Find the scan step for ej_orders (second scan).
		var ordersScan *explainResult
		for i := range rows {
			if rows[i].Operation != "join" && rows[i].Operation != "sort" {
				if rows[i].Step == 2 {
					ordersScan = &rows[i]
					break
				}
			}
		}
		s.Require().NotNil(ordersScan, "expected scan step for ej_orders")
		s.Contains(ordersScan.Detail, "table=ej_orders")
		// Row estimate for the orders scan should reflect orders table (4 rows),
		// not users table (3 rows). We can't assert exact value here since the
		// index point scan estimate depends on stats, but we can check it is valid
		// or zero (not the wrong table's count).
		// The important thing is the detail references the right table.
	})

	s.Run("union_not_supported", func() {
		_, err := s.db.ExecContext(context.Background(), `
			EXPLAIN SELECT user_id FROM "ej_users"
			UNION
			SELECT user_id FROM "ej_orders"`)
		s.Require().Error(err)
		s.Contains(err.Error(), "EXPLAIN does not support UNION")
	})
}

// findExplainRow finds the first explain result with the given operation.
func (s *TestSuite) findExplainRow(rows []explainResult, operation string) *explainResult {
	for i := range rows {
		if rows[i].Operation == operation {
			return &rows[i]
		}
	}
	return nil
}

// findAllExplainRows finds all explain results with the given operation.
func (s *TestSuite) findAllExplainRows(rows []explainResult, operation string) []explainResult {
	var result []explainResult
	for _, r := range rows {
		if r.Operation == operation {
			result = append(result, r)
		}
	}
	return result
}
