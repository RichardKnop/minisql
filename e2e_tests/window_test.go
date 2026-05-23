package e2etests

// TestWindowFunctions verifies window function support (ROW_NUMBER, RANK,
// DENSE_RANK, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE,
// SUM/AVG/COUNT/MIN/MAX OVER).
func (s *TestSuite) TestWindowFunctions() {
	_, err := s.db.Exec(`create table "wf_scores" (
		id     int8 primary key autoincrement,
		name   varchar(100) not null,
		dept   varchar(50)  not null,
		score  int8         not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "wf_scores" (name, dept, score) values
		('Alice', 'eng', 90),
		('Bob',   'eng', 85),
		('Carol', 'mkt', 70),
		('Dave',  'mkt', 70),
		('Eve',   'eng', 95)`)
	s.Require().NoError(err)

	// ── ROW_NUMBER ─────────────────────────────────────────────────────────
	s.Run("row_number_no_partition", func() {
		rows, err := s.db.Query(
			`select name, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn
			 from "wf_scores" order by rn`)
		s.Require().NoError(err)
		defer rows.Close()

		type rec struct {
			name string
			rn   int64
		}
		var got []rec
		for rows.Next() {
			var r rec
			s.Require().NoError(rows.Scan(&r.name, &r.rn))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(got, 5)
		s.Equal("Eve", got[0].name)
		s.Equal(int64(1), got[0].rn)
		s.Equal("Alice", got[1].name)
		s.Equal(int64(2), got[1].rn)
		s.Equal("Bob", got[2].name)
		s.Equal(int64(3), got[2].rn)
	})

	s.Run("row_number_with_partition", func() {
		rows, err := s.db.Query(
			`select name, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY score DESC) AS rn
			 from "wf_scores" order by dept, rn`)
		s.Require().NoError(err)
		defer rows.Close()

		type rec struct {
			name string
			dept string
			rn   int64
		}
		var got []rec
		for rows.Next() {
			var r rec
			s.Require().NoError(rows.Scan(&r.name, &r.dept, &r.rn))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(got, 5)
		// eng partition: Eve(95)=1, Alice(90)=2, Bob(85)=3
		engRows := filterDept(got, "eng", func(r rec) string { return r.dept })
		s.Require().Len(engRows, 3)
		s.Equal("Eve", engRows[0].name)
		s.Equal(int64(1), engRows[0].rn)
	})

	// ── RANK ───────────────────────────────────────────────────────────────
	s.Run("rank_ties", func() {
		// Carol and Dave both score 70 — they should share the same rank.
		rows, err := s.db.Query(
			`select name, RANK() OVER (ORDER BY score) AS rnk
			 from "wf_scores" order by rnk, name`)
		s.Require().NoError(err)
		defer rows.Close()

		type rec struct {
			name string
			rnk  int64
		}
		ranks := map[string]int64{}
		for rows.Next() {
			var r rec
			s.Require().NoError(rows.Scan(&r.name, &r.rnk))
			ranks[r.name] = r.rnk
		}
		s.Require().NoError(rows.Err())
		// Carol and Dave share rank 1; next rank is 3 (gap).
		s.Equal(ranks["Carol"], ranks["Dave"])
		s.Equal(int64(1), ranks["Carol"])
		s.Equal(int64(3), ranks["Bob"])
	})

	// ── DENSE_RANK ─────────────────────────────────────────────────────────
	s.Run("dense_rank_no_gaps", func() {
		rows, err := s.db.Query(
			`select name, DENSE_RANK() OVER (ORDER BY score) AS dr
			 from "wf_scores" order by dr, name`)
		s.Require().NoError(err)
		defer rows.Close()

		type rec struct {
			name string
			dr   int64
		}
		dense := map[string]int64{}
		for rows.Next() {
			var r rec
			s.Require().NoError(rows.Scan(&r.name, &r.dr))
			dense[r.name] = r.dr
		}
		s.Require().NoError(rows.Err())
		// Carol and Dave share dense rank 1; Bob = 2 (no gap).
		s.Equal(dense["Carol"], dense["Dave"])
		s.Equal(int64(1), dense["Carol"])
		s.Equal(int64(2), dense["Bob"])
		s.Equal(int64(3), dense["Alice"])
		s.Equal(int64(4), dense["Eve"])
	})

	// ── NTILE ──────────────────────────────────────────────────────────────
	s.Run("ntile_2_buckets", func() {
		rows, err := s.db.Query(
			`select name, NTILE(2) OVER (ORDER BY score DESC) AS bucket
			 from "wf_scores" order by bucket, name`)
		s.Require().NoError(err)
		defer rows.Close()

		buckets := map[string]int64{}
		for rows.Next() {
			var name string
			var bucket int64
			s.Require().NoError(rows.Scan(&name, &bucket))
			buckets[name] = bucket
		}
		s.Require().NoError(rows.Err())
		// 5 rows into 2 buckets: bucket 1 has 3, bucket 2 has 2.
		bucket1Count := countBucket(buckets, 1)
		bucket2Count := countBucket(buckets, 2)
		s.Equal(3, bucket1Count)
		s.Equal(2, bucket2Count)
	})

	// ── LAG / LEAD ─────────────────────────────────────────────────────────
	s.Run("lag_score", func() {
		rows, err := s.db.Query(
			`select name, score, LAG(score, 1) OVER (ORDER BY score) AS prev_score
			 from "wf_scores" order by score`)
		s.Require().NoError(err)
		defer rows.Close()

		type rec struct {
			name      string
			score     int64
			prevScore *int64
		}
		var got []rec
		for rows.Next() {
			var r rec
			s.Require().NoError(rows.Scan(&r.name, &r.score, &r.prevScore))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())
		// First row (lowest score) has no predecessor.
		s.Nil(got[0].prevScore, "first row lag should be NULL")
		// Second distinct score row should have the previous score.
		s.NotNil(got[2].prevScore)
	})

	s.Run("lead_score", func() {
		rows, err := s.db.Query(
			`select name, score, LEAD(score, 1) OVER (ORDER BY score) AS next_score
			 from "wf_scores" order by score`)
		s.Require().NoError(err)
		defer rows.Close()

		type rec struct {
			name      string
			score     int64
			nextScore *int64
		}
		var got []rec
		for rows.Next() {
			var r rec
			s.Require().NoError(rows.Scan(&r.name, &r.score, &r.nextScore))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())
		// Last row (highest score) has no successor.
		s.Nil(got[len(got)-1].nextScore, "last row lead should be NULL")
		s.NotNil(got[0].nextScore)
	})

	// ── FIRST_VALUE / LAST_VALUE ───────────────────────────────────────────
	s.Run("first_value_per_partition", func() {
		rows, err := s.db.Query(
			`select name, dept,
				FIRST_VALUE(score) OVER (PARTITION BY dept ORDER BY score DESC) AS top_score
			 from "wf_scores" order by dept, name`)
		s.Require().NoError(err)
		defer rows.Close()

		topByDept := map[string]int64{}
		for rows.Next() {
			var name, dept string
			var top int64
			s.Require().NoError(rows.Scan(&name, &dept, &top))
			topByDept[dept] = top
		}
		s.Require().NoError(rows.Err())
		s.Equal(int64(95), topByDept["eng"]) // Eve has 95
		s.Equal(int64(70), topByDept["mkt"]) // Carol/Dave both 70
	})

	s.Run("last_value_current_row_frame", func() {
		// Default frame for LAST_VALUE is ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW,
		// so LAST_VALUE == each row's own score when ordered by score.
		rows, err := s.db.Query(
			`select name, score,
				LAST_VALUE(score) OVER (ORDER BY score) AS lv
			 from "wf_scores" order by score`)
		s.Require().NoError(err)
		defer rows.Close()

		for rows.Next() {
			var name string
			var score, lv int64
			s.Require().NoError(rows.Scan(&name, &score, &lv))
			s.Equal(score, lv, "LAST_VALUE with default frame should equal own score for %s", name)
		}
		s.Require().NoError(rows.Err())
	})

	// ── NTH_VALUE ─────────────────────────────────────────────────────────
	s.Run("nth_value_2nd", func() {
		rows, err := s.db.Query(
			`select name, NTH_VALUE(score, 2) OVER (ORDER BY score DESC) AS second_best
			 from "wf_scores" order by score desc`)
		s.Require().NoError(err)
		defer rows.Close()

		type rec struct {
			name       string
			secondBest *int64
		}
		var got []rec
		for rows.Next() {
			var r rec
			s.Require().NoError(rows.Scan(&r.name, &r.secondBest))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())
		// All rows see 2nd best = Alice's score (90).
		for _, r := range got {
			s.Require().NotNil(r.secondBest)
			s.Equal(int64(90), *r.secondBest)
		}
	})

	// ── Aggregate window functions ─────────────────────────────────────────
	s.Run("sum_running_total", func() {
		rows, err := s.db.Query(
			`select name, score,
				SUM(score) OVER (ORDER BY score) AS running_sum
			 from "wf_scores" order by score`)
		s.Require().NoError(err)
		defer rows.Close()

		prev := int64(0)
		for rows.Next() {
			var name string
			var score, runSum int64
			s.Require().NoError(rows.Scan(&name, &score, &runSum))
			s.GreaterOrEqual(runSum, prev, "running sum must be non-decreasing")
			prev = runSum
		}
		s.Require().NoError(rows.Err())
	})

	s.Run("sum_over_partition", func() {
		rows, err := s.db.Query(
			`select name, dept, score,
				SUM(score) OVER (PARTITION BY dept) AS dept_total
			 from "wf_scores" order by dept, name`)
		s.Require().NoError(err)
		defer rows.Close()

		totals := map[string]int64{}
		for rows.Next() {
			var name, dept string
			var score, total int64
			s.Require().NoError(rows.Scan(&name, &dept, &score, &total))
			totals[dept] = total
		}
		s.Require().NoError(rows.Err())
		s.Equal(int64(90+85+95), totals["eng"]) // 270
		s.Equal(int64(70+70), totals["mkt"])    // 140
	})

	s.Run("avg_over_partition", func() {
		rows, err := s.db.Query(
			`select name, dept,
				AVG(score) OVER (PARTITION BY dept) AS dept_avg
			 from "wf_scores" order by dept, name`)
		s.Require().NoError(err)
		defer rows.Close()

		avgs := map[string]float64{}
		for rows.Next() {
			var name, dept string
			var avg float64
			s.Require().NoError(rows.Scan(&name, &dept, &avg))
			avgs[dept] = avg
		}
		s.Require().NoError(rows.Err())
		s.InDelta(float64(270)/3, avgs["eng"], 0.001)
		s.InDelta(float64(140)/2, avgs["mkt"], 0.001)
	})

	s.Run("count_over_all", func() {
		rows, err := s.db.Query(
			`select name, COUNT(*) OVER () AS total_rows
			 from "wf_scores"`)
		s.Require().NoError(err)
		defer rows.Close()

		for rows.Next() {
			var name string
			var total int64
			s.Require().NoError(rows.Scan(&name, &total))
			s.Equal(int64(5), total)
		}
		s.Require().NoError(rows.Err())
	})

	s.Run("min_max_per_partition", func() {
		// MIN/MAX window over partition — every row in a dept sees the partition's min/max.
		rows, err := s.db.Query(
			`select name, dept, score,
				MIN(score) OVER (PARTITION BY dept) AS min_s,
				MAX(score) OVER (PARTITION BY dept) AS max_s
			 from "wf_scores" order by dept, name`)
		s.Require().NoError(err)
		defer rows.Close()

		for rows.Next() {
			var name, dept string
			var score, minS, maxS int64
			s.Require().NoError(rows.Scan(&name, &dept, &score, &minS, &maxS))
			if dept == "eng" {
				s.Equal(int64(85), minS, "eng min for %s", name)
				s.Equal(int64(95), maxS, "eng max for %s", name)
			} else {
				s.Equal(int64(70), minS, "mkt min for %s", name)
				s.Equal(int64(70), maxS, "mkt max for %s", name)
			}
		}
		s.Require().NoError(rows.Err())
	})

	s.Run("multiple_window_funcs", func() {
		// SELECT with both ROW_NUMBER and SUM OVER in the same query.
		rows, err := s.db.Query(
			`select name, score,
				ROW_NUMBER() OVER (ORDER BY score DESC) AS rn,
				SUM(score)   OVER (PARTITION BY dept)   AS dept_total
			 from "wf_scores" order by rn`)
		s.Require().NoError(err)
		defer rows.Close()

		count := 0
		for rows.Next() {
			var name string
			var score, rn, deptTotal int64
			s.Require().NoError(rows.Scan(&name, &score, &rn, &deptTotal))
			s.Equal(int64(count+1), rn)
			count++
		}
		s.Require().NoError(rows.Err())
		s.Equal(5, count)
	})

	s.Run("window_with_where_filter", func() {
		// WHERE filters rows before window functions are computed.
		rows, err := s.db.Query(
			`select name, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn
			 from "wf_scores" where dept = 'eng' order by rn`)
		s.Require().NoError(err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var name string
			var rn int64
			s.Require().NoError(rows.Scan(&name, &rn))
			names = append(names, name)
		}
		s.Require().NoError(rows.Err())
		// Only eng rows: Eve(95)=1, Alice(90)=2, Bob(85)=3.
		s.Equal([]string{"Eve", "Alice", "Bob"}, names)
	})

	s.Run("window_with_limit", func() {
		rows, err := s.db.Query(
			`select name, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn
			 from "wf_scores" order by rn limit 2`)
		s.Require().NoError(err)
		defer rows.Close()

		count := 0
		for rows.Next() {
			var name string
			var rn int64
			s.Require().NoError(rows.Scan(&name, &rn))
			count++
			s.Equal(int64(count), rn)
		}
		s.Require().NoError(rows.Err())
		s.Equal(2, count)
	})

	s.Run("rows_between_frame", func() {
		// ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING — rolling 3-row average.
		rows, err := s.db.Query(
			`select name, score,
				AVG(score) OVER (ORDER BY score ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS roll_avg
			 from "wf_scores" order by score`)
		s.Require().NoError(err)
		defer rows.Close()

		type rec struct {
			name    string
			score   int64
			rollAvg float64
		}
		var got []rec
		for rows.Next() {
			var r rec
			s.Require().NoError(rows.Scan(&r.name, &r.score, &r.rollAvg))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(got, 5)
		// Middle rows should include neighbours.
		s.Greater(got[2].rollAvg, float64(0))
	})
}

// ── helpers ────────────────────────────────────────────────────────────────

func filterDept[T any](items []T, dept string, getDept func(T) string) []T {
	var out []T
	for _, item := range items {
		if getDept(item) == dept {
			out = append(out, item)
		}
	}
	return out
}

func countBucket(m map[string]int64, bucket int64) int {
	n := 0
	for _, v := range m {
		if v == bucket {
			n++
		}
	}
	return n
}
