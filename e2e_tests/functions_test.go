package e2etests

// ── String functions ─────────────────────────────────────────────────────────

func (s *TestSuite) TestStringFunctions_UPPER_LOWER() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "users" (email, name) values ('alice@example.com', 'Alice Smith')`)
	s.Require().NoError(err)

	var upper, lower string
	err = s.db.QueryRow(`select UPPER(name), LOWER(name) from "users"`).Scan(&upper, &lower)
	s.Require().NoError(err)
	s.Equal("ALICE SMITH", upper)
	s.Equal("alice smith", lower)
}

func (s *TestSuite) TestStringFunctions_TRIM() {
	_, err := s.db.Exec(`create table "notes" (
		id int8 primary key autoincrement,
		body text not null
	)`)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "notes" (body) values (?)`)
	s.Require().NoError(err)
	_, err = stmt.Exec("  hello world  ")
	s.Require().NoError(err)

	var trimmed, ltrimmed, rtrimmed string
	err = s.db.QueryRow(`select TRIM(body), LTRIM(body), RTRIM(body) from "notes"`).Scan(&trimmed, &ltrimmed, &rtrimmed)
	s.Require().NoError(err)
	s.Equal("hello world", trimmed)
	s.Equal("hello world  ", ltrimmed)
	s.Equal("  hello world", rtrimmed)
}

func (s *TestSuite) TestStringFunctions_LENGTH() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "users" (email, name) values ('bob@example.com', 'Bob')`)
	s.Require().NoError(err)

	var length int64
	err = s.db.QueryRow(`select LENGTH(name) from "users"`).Scan(&length)
	s.Require().NoError(err)
	s.Equal(int64(3), length)
}

func (s *TestSuite) TestStringFunctions_SUBSTR() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "users" (email, name) values ('carol@example.com', 'Carol')`)
	s.Require().NoError(err)

	var sub2, sub2len3 string
	err = s.db.QueryRow(`select SUBSTR(name, 2), SUBSTR(name, 2, 3) from "users"`).Scan(&sub2, &sub2len3)
	s.Require().NoError(err)
	s.Equal("arol", sub2)
	s.Equal("aro", sub2len3)
}

func (s *TestSuite) TestStringFunctions_REPLACE() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "users" (email, name) values ('dave@example.com', 'Dave')`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`update "users" set email = REPLACE(email, 'example.com', 'test.org')`)
	s.Require().NoError(err)

	var email string
	err = s.db.QueryRow(`select email from "users"`).Scan(&email)
	s.Require().NoError(err)
	s.Equal("dave@test.org", email)
}

func (s *TestSuite) TestStringFunctions_CONCAT() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "users" (email, name) values ('eve@example.com', 'Eve')`)
	s.Require().NoError(err)

	var full string
	err = s.db.QueryRow(`select CONCAT(name, ' <', email, '>') from "users"`).Scan(&full)
	s.Require().NoError(err)
	s.Equal("Eve <eve@example.com>", full)
}

func (s *TestSuite) TestStringFunctions_CONCAT_SkipsNulls() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	// name is NULL
	_, err = s.db.Exec(`insert into "users" (email, name) values ('frank@example.com', NULL)`)
	s.Require().NoError(err)

	var result string
	err = s.db.QueryRow(`select CONCAT('user:', email) from "users"`).Scan(&result)
	s.Require().NoError(err)
	s.Equal("user:frank@example.com", result)
}

func (s *TestSuite) TestStringFunctions_NestedFunctions() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "users" (email, name) values ('grace@example.com', '  Grace  ')`)
	s.Require().NoError(err)

	var result string
	err = s.db.QueryRow(`select UPPER(TRIM(name)) from "users"`).Scan(&result)
	s.Require().NoError(err)
	s.Equal("GRACE", result)
}

// ── NULL-handling functions ───────────────────────────────────────────────────

func (s *TestSuite) TestFunctions_COALESCE_SelectFallbackToLiteral() {
	_, err := s.db.Exec(`create table "scores" (
		id int8 primary key autoincrement,
		name text not null,
		score int8
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "scores" (name, score) values ('Alice', 95)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "scores" (name, score) values ('Bob', NULL)`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`select name, COALESCE(score, 0) from "scores" order by id`)
	s.Require().NoError(err)
	defer rows.Close()

	type result struct {
		name  string
		score int64
	}
	var got []result
	for rows.Next() {
		var r result
		s.Require().NoError(rows.Scan(&r.name, &r.score))
		got = append(got, r)
	}
	s.Require().NoError(rows.Err())

	s.Require().Len(got, 2)
	s.Equal("Alice", got[0].name)
	s.Equal(int64(95), got[0].score)
	s.Equal("Bob", got[1].name)
	s.Equal(int64(0), got[1].score) // NULL replaced by 0
}

func (s *TestSuite) TestFunctions_COALESCE_SelectFirstNonNull() {
	_, err := s.db.Exec(`create table "profiles" (
		id int8 primary key autoincrement,
		nickname text,
		username text,
		display_name text
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "profiles" (nickname, username, display_name) values (NULL, 'jsmith', 'John Smith')`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "profiles" (nickname, username, display_name) values ('JJ', 'jjones', 'Jim Jones')`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`select COALESCE(nickname, username) from "profiles" order by id`)
	s.Require().NoError(err)
	defer rows.Close()

	var names []string
	for rows.Next() {
		var n string
		s.Require().NoError(rows.Scan(&n))
		names = append(names, n)
	}
	s.Require().NoError(rows.Err())
	s.Require().Len(names, 2)
	s.Equal("jsmith", names[0]) // nickname was NULL, falls back to username
	s.Equal("JJ", names[1])     // nickname was set
}

func (s *TestSuite) TestFunctions_COALESCE_UpdateSetNullToDefault() {
	_, err := s.db.Exec(`create table "items" (
		id int8 primary key autoincrement,
		quantity int8
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "items" (quantity) values (NULL)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "items" (quantity) values (5)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`update "items" set quantity = COALESCE(quantity, 10)`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`select quantity from "items" order by id`)
	s.Require().NoError(err)
	defer rows.Close()

	var quantities []int64
	for rows.Next() {
		var q int64
		s.Require().NoError(rows.Scan(&q))
		quantities = append(quantities, q)
	}
	s.Require().NoError(rows.Err())
	s.Require().Len(quantities, 2)
	s.Equal(int64(10), quantities[0]) // was NULL → replaced by 10
	s.Equal(int64(5), quantities[1])  // was 5 → unchanged
}

func (s *TestSuite) TestFunctions_NULLIF_SelectReturnsNullOnMatch() {
	_, err := s.db.Exec(`create table "stats" (
		id int8 primary key autoincrement,
		hits int8 not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "stats" (hits) values (0)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "stats" (hits) values (42)`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`select NULLIF(hits, 0) from "stats" order by id`)
	s.Require().NoError(err)
	defer rows.Close()

	var results []*int64
	for rows.Next() {
		var v *int64
		s.Require().NoError(rows.Scan(&v))
		results = append(results, v)
	}
	s.Require().NoError(rows.Err())
	s.Require().Len(results, 2)
	s.Nil(results[0])               // hits=0 → NULLIF returns NULL
	s.Equal(int64(42), *results[1]) // hits=42 → returned as-is
}

func (s *TestSuite) TestFunctions_COALESCE_WithAlias() {
	_, err := s.db.Exec(`create table "products2" (
		id int8 primary key autoincrement,
		price int8
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "products2" (price) values (NULL)`)
	s.Require().NoError(err)

	var effective int64
	err = s.db.QueryRow(`select COALESCE(price, 99) AS effective_price from "products2"`).Scan(&effective)
	s.Require().NoError(err)
	s.Equal(int64(99), effective)
}
