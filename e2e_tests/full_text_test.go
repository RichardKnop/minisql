package e2etests

func (s *TestSuite) TestFullTextSearch_SequentialMatchAndRank() {
	_, err := s.db.Exec(`create table "articles" (
		id    int8 primary key autoincrement,
		title varchar(100) not null,
		body  text not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "articles" (title, body) values
		('MiniSQL', 'MiniSQL is a tiny embedded database. MiniSQL stores rows in B tree pages and database pages.'),
		('Postgres', 'Postgres has a generalized inverted index for full text search.'),
		('SQLite', 'SQLite has FTS5 tables for full text search.'),
		('Storage', 'A small database stores data in pages.');`)
	s.Require().NoError(err)

	s.Run("MATCH filters rows with implicit AND semantics", func() {
		rows, err := s.db.Query(`select title from "articles" where MATCH(body, 'minisql database');`)
		s.Require().NoError(err)
		defer rows.Close()

		var titles []string
		for rows.Next() {
			var title string
			s.Require().NoError(rows.Scan(&title))
			titles = append(titles, title)
		}
		s.Require().NoError(rows.Err())
		s.ElementsMatch([]string{"MiniSQL"}, titles)
	})

	s.Run("ts_rank can be projected and ordered by alias", func() {
		rows, err := s.db.Query(`
			select title, ts_rank(body, 'database pages') as score
			from "articles"
			where MATCH(body, 'database')
			order by score desc;
		`)
		s.Require().NoError(err)
		defer rows.Close()

		var titles []string
		var scores []float64
		for rows.Next() {
			var title string
			var score float64
			s.Require().NoError(rows.Scan(&title, &score))
			titles = append(titles, title)
			scores = append(scores, score)
		}
		s.Require().NoError(rows.Err())

		s.Equal([]string{"MiniSQL", "Storage"}, titles)
		s.Len(scores, 2)
		s.Greater(scores[0], scores[1])
	})

	s.Run("EXPLAIN shows sequential scan while no full-text index exists", func() {
		rows := s.collectExplain(`EXPLAIN SELECT title FROM "articles" WHERE MATCH(body, 'database');`)
		s.Require().NotEmpty(rows)
		s.Equal("sequential", rows[0].Operation)
		s.Contains(rows[0].Detail, "table=articles")
	})
}
