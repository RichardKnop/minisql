package e2etests

func (s *TestSuite) TestJSONInvertedIndex() {
	_, err := s.db.Exec(`create table "events_inv" (
		id      int8 primary key autoincrement,
		name    varchar(100) not null,
		payload json not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "events_inv" (name, payload) values
		('click-web', '{"type":"click","user":{"id":"u1"},"tags":["web","checkout"],"active":true}'),
		('click-mobile', '{"type":"click","user":{"id":"u2"},"tags":["mobile"],"active":true}'),
		('view-web', '{"type":"view","user":{"id":"u1"},"tags":["web"],"active":false}');`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create inverted index "idx_events_inv_payload" on "events_inv" (payload);`)
	s.Require().NoError(err)

	s.Run("EXPLAIN uses inverted index for literal JSON_CONTAINS query", func() {
		rows := s.collectExplain(`EXPLAIN SELECT name FROM "events_inv" WHERE JSON_CONTAINS(payload, '{"type":"click"}');`)
		s.Require().NotEmpty(rows)
		s.Equal("inverted", rows[0].Operation)
		s.Contains(rows[0].Detail, "index=idx_events_inv_payload")
		s.Contains(rows[0].Detail, `kv:type:s:"click"`)
	})

	s.Run("EXPLAIN ANALYZE executes inverted index scan", func() {
		rows := s.collectExplain(`EXPLAIN ANALYZE SELECT name FROM "events_inv" WHERE JSON_CONTAINS(payload, '{"type":"click"}');`)
		s.Require().Len(rows, 1)
		s.Equal("inverted", rows[0].Operation)
		s.True(rows[0].RowsActual.Valid)
		s.Equal(int64(2), rows[0].RowsActual.Int64)
		s.True(rows[0].DurationUS.Valid)
	})

	s.Run("JSON_CONTAINS supports object subset and array membership", func() {
		names := s.collectEventNames(`select name from "events_inv" where JSON_CONTAINS(payload, '{"type":"click","tags":["web"]}') order by name;`)
		s.Equal([]string{"click-web"}, names)

		names = s.collectEventNames(`select name from "events_inv" where JSON_CONTAINS(payload, '{"user":{"id":"u1"}}') order by name;`)
		s.Equal([]string{"click-web", "view-web"}, names)
	})

	s.Run("index reloads and passes integrity checks", func() {
		s.Require().NoError(s.db.Close())
		s.db = s.reopenDB()

		rows := s.collectExplain(`EXPLAIN SELECT name FROM "events_inv" WHERE JSON_CONTAINS(payload, '{"type":"click"}');`)
		s.Require().NotEmpty(rows)
		s.Equal("inverted", rows[0].Operation)
		s.Contains(rows[0].Detail, "index=idx_events_inv_payload")

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
		_, err := s.db.Exec(`insert into "events_inv" (name, payload) values ('signup-web', '{"type":"signup","tags":["web"]}');`)
		s.Require().NoError(err)

		names := s.collectEventNames(`select name from "events_inv" where JSON_CONTAINS(payload, '{"type":"signup"}');`)
		s.Equal([]string{"signup-web"}, names)

		_, err = s.db.Exec(`update "events_inv" set payload = '{"type":"purchase","tags":["web"]}' where name = 'signup-web';`)
		s.Require().NoError(err)

		names = s.collectEventNames(`select name from "events_inv" where JSON_CONTAINS(payload, '{"type":"signup"}');`)
		s.Empty(names)
		names = s.collectEventNames(`select name from "events_inv" where JSON_CONTAINS(payload, '{"type":"purchase"}');`)
		s.Equal([]string{"signup-web"}, names)

		_, err = s.db.Exec(`delete from "events_inv" where name = 'signup-web';`)
		s.Require().NoError(err)

		names = s.collectEventNames(`select name from "events_inv" where JSON_CONTAINS(payload, '{"type":"purchase"}');`)
		s.Empty(names)
	})
}

func (s *TestSuite) collectEventNames(query string) []string {
	rows, err := s.db.Query(query)
	s.Require().NoError(err)
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		s.Require().NoError(rows.Scan(&name))
		names = append(names, name)
	}
	s.Require().NoError(rows.Err())
	return names
}
