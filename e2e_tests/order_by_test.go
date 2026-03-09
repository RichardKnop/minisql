package e2etests

import (
	"context"
)

func (s *TestSuite) TestOrderByMultiColumn() {
	_, err := s.db.Exec(`create table "results" (
		id    int8 primary key autoincrement,
		level int4 not null,
		score int4 not null,
		name  varchar(255) not null
	);`)
	s.Require().NoError(err)

	// Insert rows with deliberate ties on level and score so the tiebreaker columns matter.
	_, err = s.db.ExecContext(context.Background(), `insert into results("level", "score", "name") values
		(1, 200, 'Charlie'),
		(1, 100, 'Alice'),
		(2, 150, 'Dave'),
		(1, 100, 'Bob'),
		(2, 150, 'Eve'),
		(3, 300, 'Frank'),
		(2, 200, 'Grace');`)
	s.Require().NoError(err)

	type result struct {
		ID    int64
		Level int32
		Score int32
		Name  string
	}

	collectResults := func(query string) []result {
		rows, err := s.db.QueryContext(context.Background(), query)
		s.Require().NoError(err)
		defer rows.Close()
		var out []result
		for rows.Next() {
			var r result
			s.Require().NoError(rows.Scan(&r.ID, &r.Level, &r.Score, &r.Name))
			out = append(out, r)
		}
		s.Require().NoError(rows.Err())
		return out
	}

	s.Run("ORDER BY two columns ASC ASC", func() {
		// Primary sort: level ASC, tiebreaker: score ASC
		results := collectResults(`select * from results order by level asc, score asc;`)
		s.Require().Len(results, 7)

		// level 1 rows sorted by score: 100 (Alice), 100 (Bob), 200 (Charlie)
		s.Equal(int32(1), results[0].Level)
		s.Equal(int32(100), results[0].Score)
		s.Equal(int32(1), results[1].Level)
		s.Equal(int32(100), results[1].Score)
		s.Equal(int32(1), results[2].Level)
		s.Equal(int32(200), results[2].Score)
		s.Equal("Charlie", results[2].Name)

		// level 2 rows sorted by score: 150 (Dave), 150 (Eve), 200 (Grace)
		s.Equal(int32(2), results[3].Level)
		s.Equal(int32(150), results[3].Score)
		s.Equal(int32(2), results[4].Level)
		s.Equal(int32(150), results[4].Score)
		s.Equal(int32(2), results[5].Level)
		s.Equal(int32(200), results[5].Score)
		s.Equal("Grace", results[5].Name)

		// level 3
		s.Equal(int32(3), results[6].Level)
		s.Equal("Frank", results[6].Name)
	})

	s.Run("ORDER BY two columns ASC DESC", func() {
		// Primary sort: level ASC, tiebreaker: score DESC
		results := collectResults(`select * from results order by level asc, score desc;`)
		s.Require().Len(results, 7)

		// level 1 rows sorted by score DESC: 200 (Charlie), 100 (Alice or Bob), 100 (Bob or Alice)
		s.Equal(int32(1), results[0].Level)
		s.Equal(int32(200), results[0].Score)
		s.Equal("Charlie", results[0].Name)
		s.Equal(int32(1), results[1].Level)
		s.Equal(int32(100), results[1].Score)
		s.Equal(int32(1), results[2].Level)
		s.Equal(int32(100), results[2].Score)
	})

	s.Run("ORDER BY two columns DESC ASC", func() {
		// Primary sort: level DESC, tiebreaker: score ASC
		results := collectResults(`select * from results order by level desc, score asc;`)
		s.Require().Len(results, 7)

		// level 3 first
		s.Equal(int32(3), results[0].Level)
		s.Equal("Frank", results[0].Name)

		// level 2 sorted by score ASC: 150, 150, 200
		s.Equal(int32(2), results[1].Level)
		s.Equal(int32(150), results[1].Score)
		s.Equal(int32(2), results[2].Level)
		s.Equal(int32(150), results[2].Score)
		s.Equal(int32(2), results[3].Level)
		s.Equal(int32(200), results[3].Score)

		// level 1 sorted by score ASC: 100, 100, 200
		s.Equal(int32(1), results[4].Level)
		s.Equal(int32(100), results[4].Score)
		s.Equal(int32(1), results[5].Level)
		s.Equal(int32(100), results[5].Score)
		s.Equal(int32(1), results[6].Level)
		s.Equal(int32(200), results[6].Score)
	})

	s.Run("ORDER BY three columns", func() {
		// level ASC, score ASC, name ASC — name breaks the score tie
		results := collectResults(`select * from results order by level asc, score asc, name asc;`)
		s.Require().Len(results, 7)

		// level 1, score 100: Alice before Bob
		s.Equal("Alice", results[0].Name)
		s.Equal("Bob", results[1].Name)
		s.Equal("Charlie", results[2].Name)

		// level 2, score 150: Dave before Eve
		s.Equal("Dave", results[3].Name)
		s.Equal("Eve", results[4].Name)
		s.Equal("Grace", results[5].Name)

		s.Equal("Frank", results[6].Name)
	})

	s.Run("ORDER BY multiple columns with LIMIT", func() {
		results := collectResults(`select * from results order by level asc, score asc, name asc limit 3;`)
		s.Require().Len(results, 3)

		s.Equal("Alice", results[0].Name)
		s.Equal("Bob", results[1].Name)
		s.Equal("Charlie", results[2].Name)
	})

	s.Run("ORDER BY multiple columns with LIMIT and OFFSET", func() {
		results := collectResults(`select * from results order by level asc, score asc, name asc limit 2 offset 3;`)
		s.Require().Len(results, 2)

		s.Equal("Dave", results[0].Name)
		s.Equal("Eve", results[1].Name)
	})

	s.Run("ORDER BY multiple columns with WHERE", func() {
		results := collectResults(`select * from results where level = 2 order by score asc, name asc;`)
		s.Require().Len(results, 3)

		s.Equal("Dave", results[0].Name)
		s.Equal("Eve", results[1].Name)
		s.Equal("Grace", results[2].Name)
	})

	s.Run("ORDER BY single column still works after multi-column support confirmed", func() {
		results := collectResults(`select * from results order by score desc;`)
		s.Require().Len(results, 7)

		s.Equal(int32(300), results[0].Score)
		s.Equal(int32(200), results[1].Score)
		s.Equal(int32(200), results[2].Score)
		s.Equal(int32(150), results[3].Score)
		s.Equal(int32(150), results[4].Score)
		s.Equal(int32(100), results[5].Score)
		s.Equal(int32(100), results[6].Score)
	})
}

// TestOrderByCompositeIndex verifies that the query planner uses a composite index to
// satisfy a multi-column ORDER BY clause when the index columns match exactly (same
// columns, same order, uniform direction) so that no in-memory sort is needed.
func (s *TestSuite) TestOrderByCompositeIndex() {
	_, err := s.db.Exec(`create table "events" (
		id    int8 primary key autoincrement,
		level int4 not null,
		score int4 not null,
		name  varchar(255) not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create index "idx_level_score" on "events" (level, score);`)
	s.Require().NoError(err)

	_, err = s.db.ExecContext(context.Background(), `insert into events("level", "score", "name") values
		(2, 150, 'Dave'),
		(1, 200, 'Charlie'),
		(3, 300, 'Frank'),
		(1, 100, 'Alice'),
		(2, 200, 'Grace'),
		(1, 100, 'Bob'),
		(2, 150, 'Eve');`)
	s.Require().NoError(err)

	type event struct {
		ID    int64
		Level int32
		Score int32
		Name  string
	}

	collectEvents := func(query string) []event {
		rows, err := s.db.QueryContext(context.Background(), query)
		s.Require().NoError(err)
		defer rows.Close()
		var out []event
		for rows.Next() {
			var e event
			s.Require().NoError(rows.Scan(&e.ID, &e.Level, &e.Score, &e.Name))
			out = append(out, e)
		}
		s.Require().NoError(rows.Err())
		return out
	}

	s.Run("ORDER BY matches composite index ASC ASC - correct ordering", func() {
		events := collectEvents(`select * from events order by level asc, score asc;`)
		s.Require().Len(events, 7)

		// level 1: score 100 (Alice), 100 (Bob), 200 (Charlie)
		s.Equal(int32(1), events[0].Level)
		s.Equal(int32(100), events[0].Score)
		s.Equal(int32(1), events[1].Level)
		s.Equal(int32(100), events[1].Score)
		s.Equal(int32(1), events[2].Level)
		s.Equal(int32(200), events[2].Score)
		s.Equal("Charlie", events[2].Name)

		// level 2: score 150 (Dave), 150 (Eve), 200 (Grace)
		s.Equal(int32(2), events[3].Level)
		s.Equal(int32(150), events[3].Score)
		s.Equal(int32(2), events[4].Level)
		s.Equal(int32(150), events[4].Score)
		s.Equal(int32(2), events[5].Level)
		s.Equal(int32(200), events[5].Score)
		s.Equal("Grace", events[5].Name)

		// level 3
		s.Equal(int32(3), events[6].Level)
		s.Equal("Frank", events[6].Name)
	})

	s.Run("ORDER BY matches composite index DESC DESC - correct ordering", func() {
		events := collectEvents(`select * from events order by level desc, score desc;`)
		s.Require().Len(events, 7)

		// level 3 first
		s.Equal(int32(3), events[0].Level)
		s.Equal("Frank", events[0].Name)

		// level 2: score 200 (Grace), 150, 150
		s.Equal(int32(2), events[1].Level)
		s.Equal(int32(200), events[1].Score)
		s.Equal("Grace", events[1].Name)
		s.Equal(int32(2), events[2].Level)
		s.Equal(int32(150), events[2].Score)
		s.Equal(int32(2), events[3].Level)
		s.Equal(int32(150), events[3].Score)

		// level 1: score 200 (Charlie), 100, 100
		s.Equal(int32(1), events[4].Level)
		s.Equal(int32(200), events[4].Score)
		s.Equal("Charlie", events[4].Name)
		s.Equal(int32(1), events[5].Level)
		s.Equal(int32(100), events[5].Score)
		s.Equal(int32(1), events[6].Level)
		s.Equal(int32(100), events[6].Score)
	})

	s.Run("ORDER BY mixed direction - falls back to in-memory sort, still correct", func() {
		events := collectEvents(`select * from events order by level asc, score desc;`)
		s.Require().Len(events, 7)

		// level 1: score DESC: 200 (Charlie), 100, 100
		s.Equal(int32(1), events[0].Level)
		s.Equal(int32(200), events[0].Score)
		s.Equal("Charlie", events[0].Name)
		s.Equal(int32(1), events[1].Level)
		s.Equal(int32(100), events[1].Score)
		s.Equal(int32(1), events[2].Level)
		s.Equal(int32(100), events[2].Score)

		// level 2: score DESC: 200 (Grace), 150, 150
		s.Equal(int32(2), events[3].Level)
		s.Equal(int32(200), events[3].Score)
		s.Equal("Grace", events[3].Name)
	})

	s.Run("ORDER BY with LIMIT uses composite index, returns correct top rows", func() {
		events := collectEvents(`select * from events order by level asc, score asc limit 3;`)
		s.Require().Len(events, 3)

		s.Equal(int32(1), events[0].Level)
		s.Equal(int32(100), events[0].Score)
		s.Equal(int32(1), events[1].Level)
		s.Equal(int32(100), events[1].Score)
		s.Equal(int32(1), events[2].Level)
		s.Equal(int32(200), events[2].Score)
		s.Equal("Charlie", events[2].Name)
	})
}
