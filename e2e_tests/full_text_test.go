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

func (s *TestSuite) TestFullTextSearch_FullTextIndex() {
	_, err := s.db.Exec(`create table "articles_fts" (
		id    int8 primary key autoincrement,
		title varchar(100) not null,
		body  text not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "articles_fts" (title, body) values
		('MiniSQL', 'MiniSQL is a tiny embedded database. MiniSQL stores rows in B tree pages and database pages.'),
		('Postgres', 'Postgres has a generalized inverted index for full text search.'),
		('SQLite', 'SQLite has FTS5 tables for full text search.'),
		('Storage', 'A small database stores data in pages.');`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create fulltext index "idx_articles_fts_body" on "articles_fts" (body) with (tokenizer = 'simple');`)
	s.Require().NoError(err)

	s.Run("EXPLAIN uses full-text index for literal MATCH query", func() {
		rows := s.collectExplain(`EXPLAIN SELECT title FROM "articles_fts" WHERE MATCH(body, 'database pages');`)
		s.Require().NotEmpty(rows)
		s.Equal("fulltext", rows[0].Operation)
		s.Contains(rows[0].Detail, "index=idx_articles_fts_body")
		s.Contains(rows[0].Detail, "keys=[database pages]")
	})

	s.Run("EXPLAIN ANALYZE executes full-text index scan", func() {
		rows := s.collectExplain(`EXPLAIN ANALYZE SELECT title FROM "articles_fts" WHERE MATCH(body, 'database pages');`)
		s.Require().Len(rows, 1)
		s.Equal("fulltext", rows[0].Operation)
		s.Contains(rows[0].Detail, "index=idx_articles_fts_body")
		s.True(rows[0].RowsActual.Valid)
		s.Equal(int64(2), rows[0].RowsActual.Int64)
		s.True(rows[0].DurationUS.Valid)
	})

	s.Run("MATCH results come from posting list intersection", func() {
		rows, err := s.db.Query(`select title from "articles_fts" where MATCH(body, 'database pages') order by title;`)
		s.Require().NoError(err)
		defer rows.Close()

		var titles []string
		for rows.Next() {
			var title string
			s.Require().NoError(rows.Scan(&title))
			titles = append(titles, title)
		}
		s.Require().NoError(rows.Err())
		s.Equal([]string{"MiniSQL", "Storage"}, titles)
	})

	s.Run("quoted phrases require adjacent indexed positions", func() {
		rows, err := s.db.Query(`select title from "articles_fts" where MATCH(body, '"database pages"');`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var title string
		s.Require().NoError(rows.Scan(&title))
		s.Equal("MiniSQL", title)
		s.False(rows.Next())
		s.Require().NoError(rows.Err())
	})

	s.Run("index reloads and passes integrity checks", func() {
		s.Require().NoError(s.db.Close())
		s.db = s.reopenDB()

		rows := s.collectExplain(`EXPLAIN SELECT title FROM "articles_fts" WHERE MATCH(body, 'database pages');`)
		s.Require().NotEmpty(rows)
		s.Equal("fulltext", rows[0].Operation)
		s.Contains(rows[0].Detail, "index=idx_articles_fts_body")

		checkRows, err := s.db.Query(`PRAGMA integrity_check;`)
		s.Require().NoError(err)
		defer checkRows.Close()
		s.Require().True(checkRows.Next())
		var checkName string
		var status string
		var tableName, indexName any
		var message string
		s.Require().NoError(checkRows.Scan(&checkName, &status, &tableName, &indexName, &message))
		s.Equal("integrity_check", checkName)
		s.Equal("ok", status)
		s.Equal("ok", message)
		s.Require().NoError(checkRows.Err())
	})

	s.Run("index maintenance tracks insert update and delete", func() {
		_, err := s.db.Exec(`insert into "articles_fts" (title, body) values ('New', 'A database search article with fresh tokens.');`)
		s.Require().NoError(err)

		rows, err := s.db.Query(`select title from "articles_fts" where MATCH(body, 'fresh tokens');`)
		s.Require().NoError(err)
		s.Require().True(rows.Next())
		var title string
		s.Require().NoError(rows.Scan(&title))
		s.Equal("New", title)
		s.Require().NoError(rows.Err())
		s.Require().NoError(rows.Close())

		_, err = s.db.Exec(`update "articles_fts" set body = 'Updated document mentions index maintenance.' where title = 'New';`)
		s.Require().NoError(err)

		rows, err = s.db.Query(`select title from "articles_fts" where MATCH(body, 'fresh tokens');`)
		s.Require().NoError(err)
		s.False(rows.Next())
		s.Require().NoError(rows.Err())
		s.Require().NoError(rows.Close())

		rows, err = s.db.Query(`select title from "articles_fts" where MATCH(body, 'index maintenance');`)
		s.Require().NoError(err)
		s.Require().True(rows.Next())
		s.Require().NoError(rows.Scan(&title))
		s.Equal("New", title)
		s.Require().NoError(rows.Err())
		s.Require().NoError(rows.Close())

		_, err = s.db.Exec(`delete from "articles_fts" where title = 'New';`)
		s.Require().NoError(err)

		rows, err = s.db.Query(`select title from "articles_fts" where MATCH(body, 'index maintenance');`)
		s.Require().NoError(err)
		defer rows.Close()
		s.False(rows.Next())
		s.Require().NoError(rows.Err())
	})
}
