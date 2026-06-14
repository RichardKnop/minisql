package e2etests

import (
	"time"
)

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

// ── Numeric functions ─────────────────────────────────────────────────────────

func (s *TestSuite) TestNumericFunctions_ABS() {
	_, err := s.db.Exec(`create table "readings" (
		id int8 primary key autoincrement,
		delta int8 not null
	)`)
	s.Require().NoError(err)

	istmt, err := s.db.Prepare(`insert into "readings" (delta) values (?)`)
	s.Require().NoError(err)
	_, err = istmt.Exec(int64(-42))
	s.Require().NoError(err)
	_, err = istmt.Exec(int64(17))
	s.Require().NoError(err)

	rows, err := s.db.Query(`select ABS(delta) from "readings" order by id`)
	s.Require().NoError(err)
	defer rows.Close()

	var vals []int64
	for rows.Next() {
		var v int64
		s.Require().NoError(rows.Scan(&v))
		vals = append(vals, v)
	}
	s.Require().NoError(rows.Err())
	s.Require().Len(vals, 2)
	s.Equal(int64(42), vals[0])
	s.Equal(int64(17), vals[1])
}

func (s *TestSuite) TestNumericFunctions_FLOOR_CEIL() {
	_, err := s.db.Exec(`create table "prices" (
		id int8 primary key autoincrement,
		amount double not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "prices" (amount) values (9.99)`)
	s.Require().NoError(err)

	var floored, ceiled float64
	err = s.db.QueryRow(`select FLOOR(amount), CEIL(amount) from "prices"`).Scan(&floored, &ceiled)
	s.Require().NoError(err)
	s.InDelta(float64(9), floored, 1e-9)
	s.InDelta(float64(10), ceiled, 1e-9)
}

func (s *TestSuite) TestNumericFunctions_ROUND() {
	_, err := s.db.Exec(`create table "measurements" (
		id int8 primary key autoincrement,
		value double not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "measurements" (value) values (3.14159)`)
	s.Require().NoError(err)

	var rounded, rounded2 float64
	err = s.db.QueryRow(`select ROUND(value), ROUND(value, 2) from "measurements"`).Scan(&rounded, &rounded2)
	s.Require().NoError(err)
	s.InDelta(float64(3), rounded, 1e-9)
	s.InDelta(float64(3.14), rounded2, 1e-9)
}

func (s *TestSuite) TestNumericFunctions_MOD() {
	_, err := s.db.Exec(`create table "numbers" (
		id int8 primary key autoincrement,
		val int8 not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "numbers" (val) values (10)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "numbers" (val) values (9)`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`select MOD(val, 3) from "numbers" order by id`)
	s.Require().NoError(err)
	defer rows.Close()

	var vals []int64
	for rows.Next() {
		var v int64
		s.Require().NoError(rows.Scan(&v))
		vals = append(vals, v)
	}
	s.Require().NoError(rows.Err())
	s.Require().Len(vals, 2)
	s.Equal(int64(1), vals[0]) // 10 % 3 = 1
	s.Equal(int64(0), vals[1]) // 9  % 3 = 0
}

func (s *TestSuite) TestNumericFunctions_UpdateWithABS() {
	_, err := s.db.Exec(`create table "adjustments" (
		id int8 primary key autoincrement,
		amount int8 not null
	)`)
	s.Require().NoError(err)

	istmt, err := s.db.Prepare(`insert into "adjustments" (amount) values (?)`)
	s.Require().NoError(err)
	_, err = istmt.Exec(int64(-50))
	s.Require().NoError(err)

	_, err = s.db.Exec(`update "adjustments" set amount = ABS(amount)`)
	s.Require().NoError(err)

	var amount int64
	err = s.db.QueryRow(`select amount from "adjustments"`).Scan(&amount)
	s.Require().NoError(err)
	s.Equal(int64(50), amount)
}

// ── Date/time functions ───────────────────────────────────────────────────────

func (s *TestSuite) TestDateTimeFunctions_NOW_InSelect() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "users" (email, name) values ('h@example.com', 'H')`)
	s.Require().NoError(err)

	before := time.Now().UTC()
	var result time.Time
	err = s.db.QueryRow(`select NOW() from "users"`).Scan(&result)
	after := time.Now().UTC()
	s.Require().NoError(err)

	s.False(result.Before(before.Truncate(time.Second)), "NOW() returned time before query")
	s.False(result.After(after.Add(time.Second)), "NOW() returned time after query")
}

func (s *TestSuite) TestDateTimeFunctions_DATE_TRUNC() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name, created) values ('i@example.com', 'I', ?)`)
	s.Require().NoError(err)
	_, err = stmt.Exec("2024-06-15 14:32:45")
	s.Require().NoError(err)

	var truncated time.Time
	err = s.db.QueryRow(`select DATE_TRUNC('day', created) from "users"`).Scan(&truncated)
	s.Require().NoError(err)
	s.Equal("2024-06-15 00:00:00", truncated.UTC().Format("2006-01-02 15:04:05"))
}

func (s *TestSuite) TestDateTimeFunctions_DATE_TRUNC_Month() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name, created) values ('j@example.com', 'J', ?)`)
	s.Require().NoError(err)
	_, err = stmt.Exec("2024-06-15 14:32:45")
	s.Require().NoError(err)

	var truncated time.Time
	err = s.db.QueryRow(`select DATE_TRUNC('month', created) from "users"`).Scan(&truncated)
	s.Require().NoError(err)
	s.Equal("2024-06-01 00:00:00", truncated.UTC().Format("2006-01-02 15:04:05"))
}

func (s *TestSuite) TestDateTimeFunctions_EXTRACT() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name, created) values ('k@example.com', 'K', ?)`)
	s.Require().NoError(err)
	_, err = stmt.Exec("2024-06-15 14:32:45")
	s.Require().NoError(err)

	var year, month, day, hour, minute, second int64
	err = s.db.QueryRow(`select
		EXTRACT('year',   created),
		EXTRACT('month',  created),
		EXTRACT('day',    created),
		EXTRACT('hour',   created),
		EXTRACT('minute', created),
		EXTRACT('second', created)
		from "users"`).Scan(&year, &month, &day, &hour, &minute, &second)
	s.Require().NoError(err)
	s.Equal(int64(2024), year)
	s.Equal(int64(6), month)
	s.Equal(int64(15), day)
	s.Equal(int64(14), hour)
	s.Equal(int64(32), minute)
	s.Equal(int64(45), second)
}

func (s *TestSuite) TestDateTimeFunctions_DATE_PART() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name, created) values ('l@example.com', 'L', ?)`)
	s.Require().NoError(err)
	_, err = stmt.Exec("2024-06-15 14:32:45")
	s.Require().NoError(err)

	var year int64
	err = s.db.QueryRow(`select DATE_PART('year', created) from "users"`).Scan(&year)
	s.Require().NoError(err)
	s.Equal(int64(2024), year)
}

func (s *TestSuite) TestDateTimeFunctions_TO_TIMESTAMP() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name, created) values ('m@example.com', 'M', ?)`)
	s.Require().NoError(err)
	_, err = stmt.Exec("2024-01-01 00:00:00")
	s.Require().NoError(err)

	// Use TO_TIMESTAMP in UPDATE SET
	_, err = s.db.Exec(`update "users" set created = TO_TIMESTAMP('2025-06-01 12:00:00')`)
	s.Require().NoError(err)

	var created time.Time
	err = s.db.QueryRow(`select created from "users"`).Scan(&created)
	s.Require().NoError(err)
	s.Equal("2025-06-01 12:00:00", created.UTC().Format("2006-01-02 15:04:05"))
}

// ── NATURAL_SORT ─────────────────────────────────────────────────────────────

func (s *TestSuite) TestNaturalSort_OrderBySemver() {
	_, err := s.db.Exec(`create table "releases" (
		id      int8 primary key autoincrement,
		version varchar(64) not null
	)`)
	s.Require().NoError(err)

	versions := []string{"1.10.2", "1.2.0", "1.9.1", "2.0.0", "1.2.10", "10.0.0"}
	stmt, err := s.db.Prepare(`insert into "releases" (version) values (?)`)
	s.Require().NoError(err)
	for _, v := range versions {
		_, err = stmt.Exec(v)
		s.Require().NoError(err)
	}
	stmt.Close()

	rows, err := s.db.Query(`select version from "releases" order by NATURAL_SORT(version)`)
	s.Require().NoError(err)
	defer rows.Close()

	var got []string
	for rows.Next() {
		var v string
		s.Require().NoError(rows.Scan(&v))
		got = append(got, v)
	}
	s.Require().NoError(rows.Err())

	want := []string{"1.2.0", "1.2.10", "1.9.1", "1.10.2", "2.0.0", "10.0.0"}
	s.Equal(want, got)
}

func (s *TestSuite) TestNaturalSort_MixedAlphaNumeric() {
	_, err := s.db.Exec(`create table "files" (
		id   int8 primary key autoincrement,
		name varchar(64) not null
	)`)
	s.Require().NoError(err)

	names := []string{"file10.txt", "file2.txt", "file1.txt", "file20.txt"}
	stmt, err := s.db.Prepare(`insert into "files" (name) values (?)`)
	s.Require().NoError(err)
	for _, n := range names {
		_, err = stmt.Exec(n)
		s.Require().NoError(err)
	}
	stmt.Close()

	rows, err := s.db.Query(`select name from "files" order by NATURAL_SORT(name)`)
	s.Require().NoError(err)
	defer rows.Close()

	var got []string
	for rows.Next() {
		var n string
		s.Require().NoError(rows.Scan(&n))
		got = append(got, n)
	}
	s.Require().NoError(rows.Err())

	s.Equal([]string{"file1.txt", "file2.txt", "file10.txt", "file20.txt"}, got)
}

func (s *TestSuite) TestNaturalSort_NullPropagation() {
	_, err := s.db.Exec(`create table "items" (
		id    int8 primary key autoincrement,
		label varchar(64)
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "items" (label) values (NULL)`)
	s.Require().NoError(err)

	var result *string
	err = s.db.QueryRow(`select NATURAL_SORT(label) from "items"`).Scan(&result)
	s.Require().NoError(err)
	s.Nil(result)
}

func (s *TestSuite) TestNaturalSort_SelectKey() {
	_, err := s.db.Exec(`create table "pkgs" (
		id      int8 primary key autoincrement,
		version varchar(64) not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "pkgs" (version) values ('3.1.0')`)
	s.Require().NoError(err)

	var key string
	err = s.db.QueryRow(`select NATURAL_SORT(version) from "pkgs"`).Scan(&key)
	s.Require().NoError(err)
	// 3 → 20-digit padded, dots preserved
	s.Equal("00000000000000000003.00000000000000000001.00000000000000000000", key)
}

func (s *TestSuite) TestNaturalSort_OrderByDesc() {
	_, err := s.db.Exec(`create table "tags" (
		id   int8 primary key autoincrement,
		name varchar(64) not null
	)`)
	s.Require().NoError(err)

	for _, n := range []string{"v1.9.0", "v1.10.0", "v1.2.0"} {
		_, err = s.db.Exec(`insert into "tags" (name) values (?)`, n)
		s.Require().NoError(err)
	}

	rows, err := s.db.Query(`select name from "tags" order by NATURAL_SORT(name) desc`)
	s.Require().NoError(err)
	defer rows.Close()

	var got []string
	for rows.Next() {
		var n string
		s.Require().NoError(rows.Scan(&n))
		got = append(got, n)
	}
	s.Require().NoError(rows.Err())

	s.Equal([]string{"v1.10.0", "v1.9.0", "v1.2.0"}, got)
}

func (s *TestSuite) TestNaturalSort_WithLimit() {
	_, err := s.db.Exec(`create table "versions" (
		id  int8 primary key autoincrement,
		ver varchar(64) not null
	)`)
	s.Require().NoError(err)

	for _, v := range []string{"1.10.0", "1.2.0", "1.9.0", "2.0.0", "0.9.0"} {
		_, err = s.db.Exec(`insert into "versions" (ver) values (?)`, v)
		s.Require().NoError(err)
	}

	// Top 3 oldest versions.
	rows, err := s.db.Query(`select ver from "versions" order by NATURAL_SORT(ver) limit 3`)
	s.Require().NoError(err)
	defer rows.Close()

	var got []string
	for rows.Next() {
		var v string
		s.Require().NoError(rows.Scan(&v))
		got = append(got, v)
	}
	s.Require().NoError(rows.Err())

	s.Equal([]string{"0.9.0", "1.2.0", "1.9.0"}, got)
}

// ── Password hashing functions ────────────────────────────────────────────────

func (s *TestSuite) TestPasswordFunctions_Argon2id_HashAndVerify() {
	_, err := s.db.Exec(`create table "users" (
		id       int8 primary key autoincrement,
		email    varchar(255) not null,
		password text not null
	)`)
	s.Require().NoError(err)

	// Insert a row, hashing the password at INSERT time.
	_, err = s.db.Exec(
		`insert into "users" (email, password) values (?, ARGON2ID_HASH(?))`,
		"alice@example.com", "s3cr3t",
	)
	s.Require().NoError(err)

	// The stored hash must look like a PHC Argon2id string.
	var stored string
	s.Require().NoError(
		s.db.QueryRow(`select password from "users" where email = 'alice@example.com'`).Scan(&stored),
	)
	s.Contains(stored, "$argon2id$")

	// ARGON2ID_VERIFY returns 1 for the correct password, 0 for wrong.
	var match int64
	s.Require().NoError(
		s.db.QueryRow(
			`select ARGON2ID_VERIFY(?, password) from "users" where email = ?`,
			"s3cr3t", "alice@example.com",
		).Scan(&match),
	)
	s.Equal(int64(1), match)

	s.Require().NoError(
		s.db.QueryRow(
			`select ARGON2ID_VERIFY(?, password) from "users" where email = ?`,
			"wrong", "alice@example.com",
		).Scan(&match),
	)
	s.Equal(int64(0), match)
}

func (s *TestSuite) TestPasswordFunctions_Argon2id_UniqueHashes() {
	_, err := s.db.Exec(`create table "accounts" (
		id   int8 primary key autoincrement,
		hash text not null
	)`)
	s.Require().NoError(err)

	// Same password inserted twice must produce different hashes (random salt).
	for range 2 {
		_, err = s.db.Exec(`insert into "accounts" (hash) values (ARGON2ID_HASH('password'))`)
		s.Require().NoError(err)
	}

	rows, err := s.db.Query(`select hash from "accounts"`)
	s.Require().NoError(err)
	defer rows.Close()

	var hashes []string
	for rows.Next() {
		var h string
		s.Require().NoError(rows.Scan(&h))
		hashes = append(hashes, h)
	}
	s.Require().NoError(rows.Err())
	s.Require().Len(hashes, 2)
	s.NotEqual(hashes[0], hashes[1])
}

func (s *TestSuite) TestPasswordFunctions_Argon2id_NullPropagation() {
	_, err := s.db.Exec(`create table "nullpw" (pw text)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "nullpw" (pw) values (NULL)`)
	s.Require().NoError(err)

	var hashResult *string
	s.Require().NoError(
		s.db.QueryRow(`select ARGON2ID_HASH(pw) from "nullpw"`).Scan(&hashResult),
	)
	s.Nil(hashResult)

	var verifyResult *int64
	s.Require().NoError(
		s.db.QueryRow(`select ARGON2ID_VERIFY('x', pw) from "nullpw"`).Scan(&verifyResult),
	)
	s.Nil(verifyResult)
}

func (s *TestSuite) TestPasswordFunctions_Bcrypt_HashAndVerify() {
	_, err := s.db.Exec(`create table "bcrypt_users" (
		id       int8 primary key autoincrement,
		email    varchar(255) not null,
		password text not null
	)`)
	s.Require().NoError(err)

	// Use cost 4 (minimum) so the test runs quickly.
	_, err = s.db.Exec(
		`insert into "bcrypt_users" (email, password) values (?, BCRYPT_HASH(?, 4))`,
		"bob@example.com", "hunter2",
	)
	s.Require().NoError(err)

	var stored string
	s.Require().NoError(
		s.db.QueryRow(`select password from "bcrypt_users" where email = 'bob@example.com'`).Scan(&stored),
	)
	s.NotEmpty(stored)
	s.Contains(stored, "$2")

	var match int64
	s.Require().NoError(
		s.db.QueryRow(
			`select BCRYPT_VERIFY(?, password) from "bcrypt_users" where email = ?`,
			"hunter2", "bob@example.com",
		).Scan(&match),
	)
	s.Equal(int64(1), match)

	s.Require().NoError(
		s.db.QueryRow(
			`select BCRYPT_VERIFY(?, password) from "bcrypt_users" where email = ?`,
			"wrong", "bob@example.com",
		).Scan(&match),
	)
	s.Equal(int64(0), match)
}

func (s *TestSuite) TestPasswordFunctions_Bcrypt_DefaultCost() {
	// BCRYPT_HASH with no cost argument should succeed (uses default cost).
	_, err := s.db.Exec(`create table "bcrypt_cost_test" (id int8 primary key)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "bcrypt_cost_test" (id) values (1)`)
	s.Require().NoError(err)

	var h string
	s.Require().NoError(
		s.db.QueryRow(`select BCRYPT_HASH('secret') from "bcrypt_cost_test"`).Scan(&h),
	)
	s.Contains(h, "$2")
}

func (s *TestSuite) TestPasswordFunctions_Bcrypt_NullPropagation() {
	_, err := s.db.Exec(`create table "nullpw2" (pw text)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "nullpw2" (pw) values (NULL)`)
	s.Require().NoError(err)

	var hashResult *string
	s.Require().NoError(
		s.db.QueryRow(`select BCRYPT_HASH(pw) from "nullpw2"`).Scan(&hashResult),
	)
	s.Nil(hashResult)

	var verifyResult *int64
	s.Require().NoError(
		s.db.QueryRow(`select BCRYPT_VERIFY('x', pw) from "nullpw2"`).Scan(&verifyResult),
	)
	s.Nil(verifyResult)
}

func (s *TestSuite) TestPasswordFunctions_UsedInWhere() {
	_, err := s.db.Exec(`create table "members" (
		id       int8 primary key autoincrement,
		email    varchar(255) not null,
		password text not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(
		`insert into "members" (email, password) values (?, ARGON2ID_HASH(?))`,
		"carol@example.com", "p@ssw0rd",
	)
	s.Require().NoError(err)

	// Authenticate: select the user only when the password matches.
	var email string
	err = s.db.QueryRow(
		`select email from "members" where ARGON2ID_VERIFY(?, password) = 1`,
		"p@ssw0rd",
	).Scan(&email)
	s.Require().NoError(err)
	s.Equal("carol@example.com", email)

	// Wrong password returns no rows.
	err = s.db.QueryRow(
		`select email from "members" where ARGON2ID_VERIFY(?, password) = 1`,
		"wrong",
	).Scan(&email)
	s.ErrorContains(err, "no rows")
}
