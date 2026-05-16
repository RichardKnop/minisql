package e2etests

import "sort"

func (s *TestSuite) TestORIndexUnion() {
	_, err := s.db.Exec(`create table "tickets" (
		id       int8 primary key autoincrement,
		status   varchar(20) not null,
		priority varchar(20) not null,
		score    int8 not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create index idx_tickets_status on "tickets" (status);`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`create index idx_tickets_priority on "tickets" (priority);`)
	s.Require().NoError(err)

	// Row 1: open, low, 10
	_, err = s.db.Exec(`insert into "tickets" (status, priority, score) values ('open', 'low', 10)`)
	s.Require().NoError(err)
	// Row 2: closed, critical, 20
	_, err = s.db.Exec(`insert into "tickets" (status, priority, score) values ('closed', 'critical', 20)`)
	s.Require().NoError(err)
	// Row 3: open, critical, 30  ← matches BOTH OR groups
	_, err = s.db.Exec(`insert into "tickets" (status, priority, score) values ('open', 'critical', 30)`)
	s.Require().NoError(err)
	// Row 4: closed, low, 40  ← matches neither
	_, err = s.db.Exec(`insert into "tickets" (status, priority, score) values ('closed', 'low', 40)`)
	s.Require().NoError(err)

	s.Run("or_two_indexed_columns", func() {
		// Row 1 (status=open), Row 2 (priority=critical), Row 3 (both).
		// Row 3 must appear exactly once (deduplication).
		rows, err := s.db.Query(`select id from "tickets" where status = 'open' OR priority = 'critical'`)
		s.Require().NoError(err)
		defer rows.Close()

		var ids []int
		for rows.Next() {
			var id int
			s.Require().NoError(rows.Scan(&id))
			ids = append(ids, id)
		}
		s.Require().NoError(rows.Err())
		sort.Ints(ids)
		s.Equal([]int{1, 2, 3}, ids, "rows 1, 2, 3 match; row 3 must appear once")
	})

	s.Run("or_with_additional_and_condition", func() {
		// (status = 'open' AND score < 20) OR priority = 'critical'
		// Row 1: open, score=10 (<20) → matches group 0
		// Row 2: priority=critical → matches group 1
		// Row 3: open AND score=30 (not <20), priority=critical → matches group 1
		rows, err := s.db.Query(
			`select id from "tickets" where status = 'open' AND score < 20 OR priority = 'critical'`)
		s.Require().NoError(err)
		defer rows.Close()

		var ids []int
		for rows.Next() {
			var id int
			s.Require().NoError(rows.Scan(&id))
			ids = append(ids, id)
		}
		s.Require().NoError(rows.Err())
		sort.Ints(ids)
		s.Equal([]int{1, 2, 3}, ids)
	})

	s.Run("or_no_match_returns_empty", func() {
		rows, err := s.db.Query(
			`select id from "tickets" where status = 'pending' OR priority = 'blocker'`)
		s.Require().NoError(err)
		defer rows.Close()
		s.False(rows.Next(), "no tickets have status=pending or priority=blocker")
		s.Require().NoError(rows.Err())
	})

	s.Run("or_with_limit", func() {
		rows, err := s.db.Query(
			`select id from "tickets" where status = 'open' OR priority = 'critical' LIMIT 2`)
		s.Require().NoError(err)
		defer rows.Close()

		var ids []int
		for rows.Next() {
			var id int
			s.Require().NoError(rows.Scan(&id))
			ids = append(ids, id)
		}
		s.Require().NoError(rows.Err())
		s.Len(ids, 2, "LIMIT 2 must cap the union scan at 2 rows")
	})

	s.Run("or_one_group_no_index_sequential_fallback", func() {
		// score has no index — falls back to sequential scan but must still return correct rows.
		rows, err := s.db.Query(
			`select id from "tickets" where status = 'open' OR score = 20`)
		s.Require().NoError(err)
		defer rows.Close()

		var ids []int
		for rows.Next() {
			var id int
			s.Require().NoError(rows.Scan(&id))
			ids = append(ids, id)
		}
		s.Require().NoError(rows.Err())
		sort.Ints(ids)
		// Row 1 (open), Row 2 (score=20), Row 3 (open, score=30)
		s.Equal([]int{1, 2, 3}, ids)
	})
}
