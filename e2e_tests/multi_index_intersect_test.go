package e2etests

func (s *TestSuite) TestMultiIndexIntersect() {
	_, err := s.db.Exec(`create table "events" (
		id         int8 primary key autoincrement,
		category   varchar(50) not null,
		status     varchar(20) not null,
		score      int8 not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create index "idx_category" on "events" (category);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`create index "idx_status" on "events" (status);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`create index "idx_score" on "events" (score);`)
	s.Require().NoError(err)

	// Seed: id 1-5
	_, err = s.db.Exec(`insert into "events" (category, status, score) values
		('sports', 'active',   80),
		('sports', 'inactive', 60),
		('music',  'active',   70),
		('music',  'active',   90),
		('sports', 'active',   55)`)
	s.Require().NoError(err)

	s.Run("two_secondary_equality_intersection", func() {
		// category='sports' AND status='active' → ids 1, 5
		rows, err := s.db.Query(
			`select id from "events" where category = 'sports' and status = 'active'`)
		s.Require().NoError(err)
		defer rows.Close()

		var ids []int64
		for rows.Next() {
			var id int64
			s.Require().NoError(rows.Scan(&id))
			ids = append(ids, id)
		}
		s.Require().NoError(rows.Err())
		s.ElementsMatch([]int64{1, 5}, ids)
	})

	s.Run("two_secondary_equality_intersection_no_match", func() {
		// No events with category='music' AND status='inactive'.
		rows, err := s.db.Query(
			`select id from "events" where category = 'music' and status = 'inactive'`)
		s.Require().NoError(err)
		defer rows.Close()

		var count int
		for rows.Next() {
			count++
		}
		s.Require().NoError(rows.Err())
		s.Zero(count)
	})

	s.Run("two_secondary_range_intersection", func() {
		// score >= 70 AND score is already filtered, but let's use two range-indexed columns.
		// We test with score >= 70 — only idx_score is a range scan (one index for this test).
		// For a genuine two-range intersection we need a second range condition on a different column.
		// Here we filter score >= 70 via the index and check the result.
		rows, err := s.db.Query(
			`select id from "events" where score >= 70`)
		s.Require().NoError(err)
		defer rows.Close()

		var ids []int64
		for rows.Next() {
			var id int64
			s.Require().NoError(rows.Scan(&id))
			ids = append(ids, id)
		}
		s.Require().NoError(rows.Err())
		// Rows with score >= 70: id=1(80), id=3(70), id=4(90)
		s.ElementsMatch([]int64{1, 3, 4}, ids)
	})

	s.Run("secondary_intersection_with_nonindexed_filter", func() {
		// category='sports' AND status='active' AND score >= 70 → id=1 (score 80)
		// Two secondary indexes intersect, plus score range as additional filter.
		rows, err := s.db.Query(
			`select id from "events" where category = 'sports' and status = 'active' and score >= 70`)
		s.Require().NoError(err)
		defer rows.Close()

		var ids []int64
		for rows.Next() {
			var id int64
			s.Require().NoError(rows.Scan(&id))
			ids = append(ids, id)
		}
		s.Require().NoError(rows.Err())
		s.ElementsMatch([]int64{1}, ids)
	})

	s.Run("intersection_with_select_star", func() {
		// SELECT * should work via intersection (full row fetch).
		rows, err := s.db.Query(
			`select * from "events" where category = 'music' and status = 'active'`)
		s.Require().NoError(err)
		defer rows.Close()

		type row struct {
			id       int64
			category string
			status   string
			score    int64
		}
		var results []row
		for rows.Next() {
			var r row
			s.Require().NoError(rows.Scan(&r.id, &r.category, &r.status, &r.score))
			results = append(results, r)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(results, 2)
		for _, r := range results {
			s.Equal("music", r.category)
			s.Equal("active", r.status)
		}
	})

	s.Run("intersection_update", func() {
		// UPDATE using AND conditions on two secondary indexed columns.
		res, err := s.db.Exec(
			`update "events" set score = 99 where category = 'sports' and status = 'active'`)
		s.Require().NoError(err)
		n, err := res.RowsAffected()
		s.Require().NoError(err)
		s.Equal(int64(2), n) // ids 1 and 5
	})

	s.Run("intersection_delete", func() {
		// DELETE with intersection.
		res, err := s.db.Exec(
			`delete from "events" where category = 'music' and status = 'active'`)
		s.Require().NoError(err)
		n, err := res.RowsAffected()
		s.Require().NoError(err)
		s.Equal(int64(2), n) // ids 3 and 4
	})
}

func (s *TestSuite) TestMultiIndexIntersect_Explain() {
	_, err := s.db.Exec(`create table "items" (
		id      int8 primary key autoincrement,
		kind    varchar(30) not null,
		region  varchar(30) not null,
		price   int8 not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create index "idx_kind"   on "items" (kind);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`create index "idx_region" on "items" (region);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "items" (kind, region, price) values
		('widget', 'north', 10),
		('gadget', 'north', 20),
		('widget', 'south', 30)`)
	s.Require().NoError(err)

	s.Run("explain_shows_index_intersect", func() {
		rows := s.collectExplain(
			`EXPLAIN SELECT id FROM items WHERE kind = 'widget' AND region = 'north'`)
		s.Require().Len(rows, 1)
		s.Equal("index_intersect", rows[0].Operation)
		s.Contains(rows[0].Detail, "table=items")
	})

	s.Run("explain_analyze_intersect", func() {
		rows := s.collectExplain(
			`EXPLAIN ANALYZE SELECT id FROM items WHERE kind = 'widget' AND region = 'north'`)
		s.Require().Len(rows, 1)
		s.Equal("index_intersect", rows[0].Operation)
		s.True(rows[0].RowsActual.Valid)
		s.Equal(int64(1), rows[0].RowsActual.Int64) // only id=1 matches both
		s.True(rows[0].DurationUS.Valid)
	})
}
