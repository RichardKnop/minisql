package e2etests

func (s *TestSuite) TestIndexMethods_StrictScaffold() {
	_, err := s.db.Exec(`create table "articles_index_method" (
		id int8 primary key autoincrement,
		body text not null,
		title varchar(100) not null,
		score int8 not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "events_index_method" (
		id int8 primary key autoincrement,
		payload json not null,
		name varchar(100) not null
	);`)
	s.Require().NoError(err)

	s.Run("fulltext index on text is created", func() {
		_, err := s.db.Exec(`create fulltext index "idx_articles_body_fts" on "articles_index_method" (body) with (tokenizer = 'simple');`)
		s.Require().NoError(err)
	})

	s.Run("fulltext index rejects non-text column before not implemented error", func() {
		_, err := s.db.Exec(`create fulltext index "idx_articles_score_fts" on "articles_index_method" (score);`)
		s.Require().Error(err)
		s.Contains(err.Error(), `full-text index column "score" must be TEXT or VARCHAR`)
	})

	s.Run("fulltext index rejects unsupported tokenizer", func() {
		_, err := s.db.Exec(`create fulltext index "idx_articles_body_porter" on "articles_index_method" (body) with (tokenizer = 'porter');`)
		s.Require().Error(err)
		s.Contains(err.Error(), `unsupported full-text tokenizer "porter"`)
	})

	s.Run("inverted index on JSON is created", func() {
		_, err := s.db.Exec(`create inverted index "idx_events_payload_inv" on "events_index_method" (payload);`)
		s.Require().NoError(err)
	})

	s.Run("inverted index rejects non-JSON column", func() {
		_, err := s.db.Exec(`create inverted index "idx_events_name_inv" on "events_index_method" (name);`)
		s.Require().Error(err)
		s.Contains(err.Error(), `inverted index column "name" must be JSON`)
	})
}
