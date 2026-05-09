package e2etests

func (s *TestSuite) TestJSON() {
	_, err := s.db.Exec(`create table events (
		id      int8 primary key autoincrement,
		name    varchar(100) not null,
		payload json
	);`)
	s.Require().NoError(err)

	s.Run("INSERT_valid_json_object", func() {
		_, err := s.db.Exec(
			`insert into events (name, payload) values (?, ?)`,
			"e1", `{"type":"click","x":10}`,
		)
		s.Require().NoError(err)
	})

	s.Run("INSERT_normalises_whitespace", func() {
		_, err := s.db.Exec(
			`insert into events (name, payload) values (?, ?)`,
			"e2", `{ "type" : "scroll" , "y" : 99 }`,
		)
		s.Require().NoError(err)

		rows, err := s.db.Query(`select payload from events where name = ?`, "e2")
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var payload string
		s.Require().NoError(rows.Scan(&payload))
		// Stored in compact form
		s.Equal(`{"type":"scroll","y":99}`, payload)
	})

	s.Run("INSERT_json_array", func() {
		_, err := s.db.Exec(
			`insert into events (name, payload) values (?, ?)`,
			"e3", `[1,2,3]`,
		)
		s.Require().NoError(err)
	})

	s.Run("INSERT_null_payload", func() {
		_, err := s.db.Exec(
			`insert into events (name, payload) values (?, ?)`,
			"e4", nil,
		)
		s.Require().NoError(err)
	})

	s.Run("INSERT_invalid_json_rejected", func() {
		_, err := s.db.Exec(
			`insert into events (name, payload) values (?, ?)`,
			"bad", `{not valid json`,
		)
		s.Require().Error(err)
	})

	s.Run("SELECT_arrow_operator_string_key", func() {
		_, err := s.db.Exec(
			`insert into events (name, payload) values (?, ?)`,
			"arrow", `{"action":"login","uid":42}`,
		)
		s.Require().NoError(err)

		rows, err := s.db.Query(`select payload -> 'action' from events where name = 'arrow'`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var frag string
		s.Require().NoError(rows.Scan(&frag))
		// -> returns JSON fragment (quoted string)
		s.Equal(`"login"`, frag)
	})

	s.Run("SELECT_arrowarrow_operator_string_key", func() {
		rows, err := s.db.Query(`select payload ->> 'action' from events where name = 'arrow'`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var val string
		s.Require().NoError(rows.Scan(&val))
		// ->> returns SQL scalar (unquoted string)
		s.Equal("login", val)
	})

	s.Run("SELECT_arrowarrow_operator_integer_key", func() {
		rows, err := s.db.Query(`select payload ->> 'uid' from events where name = 'arrow'`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var uid int64
		s.Require().NoError(rows.Scan(&uid))
		s.Equal(int64(42), uid)
	})

	s.Run("SELECT_arrow_operator_array_index", func() {
		_, err := s.db.Exec(
			`insert into events (name, payload) values (?, ?)`,
			"arr", `["a","b","c"]`,
		)
		s.Require().NoError(err)

		rows, err := s.db.Query(`select payload ->> 1 from events where name = 'arr'`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var val string
		s.Require().NoError(rows.Scan(&val))
		s.Equal("b", val)
	})

	s.Run("JSON_EXTRACT_function", func() {
		rows, err := s.db.Query(`select JSON_EXTRACT(payload, '$.action') from events where name = 'arrow'`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var val string
		s.Require().NoError(rows.Scan(&val))
		s.Equal("login", val)
	})

	s.Run("JSON_VALID_true", func() {
		rows, err := s.db.Query(`select JSON_VALID(payload) from events where name = 'arrow'`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var v int64
		s.Require().NoError(rows.Scan(&v))
		s.Equal(int64(1), v)
	})

	s.Run("JSON_VALID_false_for_non_json_text", func() {
		_, err := s.db.Exec(`create table raw_text (val text);`)
		s.Require().NoError(err)
		_, err = s.db.Exec(`insert into raw_text (val) values (?)`, "not json")
		s.Require().NoError(err)

		rows, err := s.db.Query(`select JSON_VALID(val) from raw_text`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var v int64
		s.Require().NoError(rows.Scan(&v))
		s.Equal(int64(0), v)
	})

	s.Run("JSON_TYPE_object", func() {
		rows, err := s.db.Query(`select JSON_TYPE(payload) from events where name = 'arrow'`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var typeName string
		s.Require().NoError(rows.Scan(&typeName))
		s.Equal("object", typeName)
	})

	s.Run("JSON_TYPE_array", func() {
		rows, err := s.db.Query(`select JSON_TYPE(payload) from events where name = 'arr'`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var typeName string
		s.Require().NoError(rows.Scan(&typeName))
		s.Equal("array", typeName)
	})

	s.Run("JSON_TYPE_with_path", func() {
		rows, err := s.db.Query(`select JSON_TYPE(payload, '$.action') from events where name = 'arrow'`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var typeName string
		s.Require().NoError(rows.Scan(&typeName))
		s.Equal("text", typeName)
	})

	s.Run("JSON_ARRAY_LENGTH", func() {
		rows, err := s.db.Query(`select JSON_ARRAY_LENGTH(payload) from events where name = 'arr'`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var n int64
		s.Require().NoError(rows.Scan(&n))
		s.Equal(int64(3), n)
	})

	s.Run("UPDATE_json_column_normalises", func() {
		_, err := s.db.Exec(
			`insert into events (name, payload) values (?, ?)`,
			"upd", `{"v":1}`,
		)
		s.Require().NoError(err)

		_, err = s.db.Exec(
			`update events set payload = ? where name = 'upd'`,
			`{ "v" : 2 , "extra" : true }`,
		)
		s.Require().NoError(err)

		rows, err := s.db.Query(`select payload from events where name = 'upd'`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var payload string
		s.Require().NoError(rows.Scan(&payload))
		s.Equal(`{"v":2,"extra":true}`, payload)
	})

	s.Run("CAST_AS_JSON", func() {
		rows, err := s.db.Query(`select CAST('{"x":1}' AS JSON) from events where name = 'arrow'`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var val string
		s.Require().NoError(rows.Scan(&val))
		s.Equal(`{"x":1}`, val)
	})

	s.Run("WHERE_filter_on_json_extract", func() {
		rows, err := s.db.Query(
			`select name from events where payload ->> 'action' = 'login'`,
		)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var name string
		s.Require().NoError(rows.Scan(&name))
		s.Equal("arrow", name)
		s.Require().False(rows.Next())
	})
}

func (s *TestSuite) TestJSON_IndexOnJSONColumnDisallowed() {
	_, err := s.db.Exec(`create table "docs" (
		id      int8 primary key autoincrement,
		payload json
	);`)
	s.Require().NoError(err)

	s.Run("CREATE INDEX on JSON column is rejected", func() {
		_, err = s.db.Exec(`create index "idx_payload" on "docs" (payload);`)
		s.Require().Error(err)
		s.ErrorContains(err, "b-tree index on JSON column is not supported")
	})

	s.Run("CREATE TABLE with JSON primary key is rejected", func() {
		_, err := s.db.Exec(`create table "t1" (data json primary key);`)
		s.Require().Error(err)
		s.ErrorContains(err, "primary key cannot be of type JSON")
	})

	s.Run("CREATE TABLE with JSON unique column is rejected", func() {
		_, err := s.db.Exec(`create table "t2" (id int8 primary key autoincrement, data json unique);`)
		s.Require().Error(err)
		s.ErrorContains(err, "unique key cannot be of type JSON")
	})
}
