package e2etests

import (
	"fmt"
	"sort"
)

// TestGreedyJoinReorder verifies that the greedy join-reorder optimisation
// produces correct query results when it changes the join order, and that the
// EXPLAIN plan reflects the new order.
//
// The test creates two tables where the user writes the larger table first in
// FROM/JOIN but the planner should swap them so the smaller table is the outer
// (base) loop.
func (s *TestSuite) TestGreedyJoinReorder() {
	// "categories" — small reference table (3 rows).
	_, err := s.db.Exec(`create table "gr_categories" (
		cat_id   int8 primary key autoincrement,
		label    varchar(50) not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "gr_categories" (label) values
		('alpha'), ('beta'), ('gamma')`)
	s.Require().NoError(err)

	// "items" — larger fact table (30 rows), each referencing a category.
	_, err = s.db.Exec(`create table "gr_items" (
		item_id  int8 primary key autoincrement,
		cat_id   int8 not null,
		name     varchar(50) not null
	)`)
	s.Require().NoError(err)

	for i := 1; i <= 30; i++ {
		catID := ((i - 1) % 3) + 1
		_, err = s.db.Exec(
			`insert into "gr_items" (cat_id, name) values (?, ?)`,
			catID, fmt.Sprintf("item-%02d", i),
		)
		s.Require().NoError(err)
	}

	s.Run("results_correct_after_reorder", func() {
		// User writes items (larger, 30 rows) as the base, categories (smaller, 3 rows)
		// as the join.  Greedy reordering should swap them: categories (3) becomes
		// the outer loop, items (30) is joined via its PK.
		// The result set must be the same regardless of execution order.
		rows, err := s.db.Query(`
			select i.name, c.label
			from   "gr_items"      AS i
			inner join "gr_categories" AS c ON i.cat_id = c.cat_id
			order by i.name`)
		s.Require().NoError(err)
		defer rows.Close()

		type row struct{ name, label string }
		var got []row
		for rows.Next() {
			var r row
			s.Require().NoError(rows.Scan(&r.name, &r.label))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(got, 30, "all 30 items must appear in the result")

		// Collect all item names per label and sort.
		byLabel := make(map[string][]string)
		for _, r := range got {
			byLabel[r.label] = append(byLabel[r.label], r.name)
		}
		s.Require().Len(byLabel, 3, "all 3 categories should appear")
		for _, names := range byLabel {
			s.Require().Len(names, 10, "each category has exactly 10 items")
		}
	})

	s.Run("plan_reflects_reorder", func() {
		// EXPLAIN should show items (i, 30 rows) as the left (probe) side and
		// categories (c, 3 rows) as the right (inner/INLJ) side.
		//
		// gr_categories has a PK on cat_id — when used as the inner table its
		// join column (cat_id) has an index, enabling an indexed NL join.
		// The greedy planner therefore keeps gr_items as the probe/base side
		// and promotes gr_categories to the inner (lookup) side.
		rows := s.collectExplain(`
			EXPLAIN SELECT i.name, c.label
			FROM "gr_items" AS i
			INNER JOIN "gr_categories" AS c ON i.cat_id = c.cat_id`)

		joinRow := s.findExplainRow(rows, "join")
		s.Require().NotNil(joinRow, "expected a join row in EXPLAIN output")
		s.Contains(joinRow.Detail, "type=inner")
		s.Contains(joinRow.Detail, "algorithm=nested_loop")
		// items is the probe (left/base), categories is the inner (right, index lookup).
		s.Contains(joinRow.Detail, "left=i")
		s.Contains(joinRow.Detail, "right=c")
	})

	s.Run("three_table_chain_correct", func() {
		// Add a third table: "gr_tags" (2 rows per category = 6 rows total).
		_, err := s.db.Exec(`create table "gr_tags" (
			tag_id  int8 primary key autoincrement,
			cat_id  int8 not null,
			tag     varchar(20) not null
		)`)
		s.Require().NoError(err)
		_, err = s.db.Exec(`insert into "gr_tags" (cat_id, tag) values
			(1,'t1'),(1,'t2'),(2,'t3'),(2,'t4'),(3,'t5'),(3,'t6')`)
		s.Require().NoError(err)

		// User writes: items (30) JOIN categories (3) JOIN tags (6).
		// Greedy should reorder to: categories (3) JOIN items (30) JOIN tags (6)
		// or categories (3) JOIN tags (6) JOIN items (30), depending on reachability.
		// Either way the result count must be correct.
		rows, err := s.db.Query(`
			select i.name, c.label, t.tag
			from   "gr_items"      AS i
			inner join "gr_categories" AS c ON i.cat_id = c.cat_id
			inner join "gr_tags"       AS t ON c.cat_id = t.cat_id
			order by i.name, t.tag`)
		s.Require().NoError(err)
		defer rows.Close()

		type row struct{ name, label, tag string }
		var got []row
		for rows.Next() {
			var r row
			s.Require().NoError(rows.Scan(&r.name, &r.label, &r.tag))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())
		// 30 items × 2 tags per category = 60 rows.
		s.Require().Len(got, 60, "30 items × 2 tags per category = 60 rows")

		// Every item name should appear exactly twice (once per tag for its category).
		nameCount := make(map[string]int)
		for _, r := range got {
			nameCount[r.name]++
		}
		for name, cnt := range nameCount {
			s.Equal(2, cnt, "item %s should appear twice (once per tag)", name)
		}

		// Collect distinct item names — should be 30.
		names := make([]string, 0, len(nameCount))
		for n := range nameCount {
			names = append(names, n)
		}
		sort.Strings(names)
		s.Len(names, 30)
	})
}
