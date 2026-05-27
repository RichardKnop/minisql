package e2etests

import "database/sql"

// TestNullSemantics_GroupByNullFormsOwnGroup verifies that NULL values in a
// GROUP BY key form their own group (standard SQL behaviour).
func (s *TestSuite) TestNullSemantics_GroupByNullFormsOwnGroup() {
	_, err := s.db.Exec(`create table "scores" (
		id       int8 primary key autoincrement,
		category varchar(50),
		val      int8 not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "scores" (category, val) values ('A', 10)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "scores" (category, val) values ('A', 20)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "scores" (category, val) values ('B', 5)`)
	s.Require().NoError(err)
	// Two rows with NULL category — they should form a single group.
	_, err = s.db.Exec(`insert into "scores" (val) values (99)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "scores" (val) values (88)`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`select category, count(*), sum(val) from "scores" group by category order by category`)
	s.Require().NoError(err)
	defer rows.Close()

	type groupRow struct {
		cat   sql.NullString
		count int64
		sum   int64
	}
	var groups []groupRow
	for rows.Next() {
		var r groupRow
		s.Require().NoError(rows.Scan(&r.cat, &r.count, &r.sum))
		groups = append(groups, r)
	}
	s.Require().NoError(rows.Err())

	// NULL group sorts first (nulls first in our ORDER BY); then A, then B.
	s.Require().Len(groups, 3)

	nullGroup := groups[0]
	s.False(nullGroup.cat.Valid)
	s.Equal(int64(2), nullGroup.count)
	s.Equal(int64(187), nullGroup.sum)

	aGroup := groups[1]
	s.Equal("A", aGroup.cat.String)
	s.Equal(int64(2), aGroup.count)
	s.Equal(int64(30), aGroup.sum)

	bGroup := groups[2]
	s.Equal("B", bGroup.cat.String)
	s.Equal(int64(1), bGroup.count)
	s.Equal(int64(5), bGroup.sum)
}

// TestNullSemantics_IsNullVsEqualsNull verifies that IS NULL matches NULL rows
// while = NULL never matches (SQL three-value logic).
func (s *TestSuite) TestNullSemantics_IsNullVsEqualsNull() {
	_, err := s.db.Exec(`create table "nullable" (
		id  int8 primary key autoincrement,
		val int8
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "nullable" (val) values (1)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "nullable" (val) values (2)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "nullable" (val) values (NULL)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "nullable" (val) values (NULL)`)
	s.Require().NoError(err)

	var isNullCount, isNotNullCount int64
	s.Require().NoError(s.db.QueryRow(`select count(*) from "nullable" where val IS NULL`).Scan(&isNullCount))
	s.Require().NoError(s.db.QueryRow(`select count(*) from "nullable" where val IS NOT NULL`).Scan(&isNotNullCount))

	s.Equal(int64(2), isNullCount)
	s.Equal(int64(2), isNotNullCount)
}

// TestNullSemantics_NotInWithLiteralList verifies NOT IN behaviour when the
// literal list does NOT contain NULL — all non-matching rows are returned.
func (s *TestSuite) TestNullSemantics_NotInWithLiteralList() {
	_, err := s.db.Exec(`create table "items" (
		id  int8 primary key autoincrement,
		val int8 not null
	)`)
	s.Require().NoError(err)

	for _, v := range []int{1, 2, 3, 4, 5} {
		_, err = s.db.Exec(`insert into "items" (val) values (?)`, int64(v))
		s.Require().NoError(err)
	}

	rows, err := s.db.Query(`select val from "items" where val not in (2, 4) order by val`)
	s.Require().NoError(err)
	defer rows.Close()

	var vals []int64
	for rows.Next() {
		var v int64
		s.Require().NoError(rows.Scan(&v))
		vals = append(vals, v)
	}
	s.Require().NoError(rows.Err())
	s.Equal([]int64{1, 3, 5}, vals)
}

// TestNullSemantics_NullableColumnAggregate verifies aggregate functions ignore
// NULL values: SUM/AVG/MIN/MAX skip NULLs; COUNT(*) counts all rows.
func (s *TestSuite) TestNullSemantics_NullableColumnAggregate() {
	_, err := s.db.Exec(`create table "readings" (
		id  int8 primary key autoincrement,
		val int8
	)`)
	s.Require().NoError(err)

	for _, v := range []any{int64(10), int64(20), nil, nil, int64(30)} {
		_, err = s.db.Exec(`insert into "readings" (val) values (?)`, v)
		s.Require().NoError(err)
	}

	var countStar, sumVal int64
	var avgVal float64
	var minVal, maxVal int64

	s.Require().NoError(s.db.QueryRow(`select count(*) from "readings"`).Scan(&countStar))
	s.Require().NoError(s.db.QueryRow(`select sum(val) from "readings"`).Scan(&sumVal))
	s.Require().NoError(s.db.QueryRow(`select avg(val) from "readings"`).Scan(&avgVal))
	s.Require().NoError(s.db.QueryRow(`select min(val) from "readings"`).Scan(&minVal))
	s.Require().NoError(s.db.QueryRow(`select max(val) from "readings"`).Scan(&maxVal))

	s.Equal(int64(5), countStar)
	s.Equal(int64(60), sumVal)      // 10+20+30, NULLs excluded
	s.InDelta(20.0, avgVal, 0.001)  // 60/3
	s.Equal(int64(10), minVal)
	s.Equal(int64(30), maxVal)
}

// TestNullSemantics_UpdateSetsColumnToNull verifies that UPDATE can set a
// nullable column to NULL and that subsequent reads reflect the change.
func (s *TestSuite) TestNullSemantics_UpdateSetsColumnToNull() {
	_, err := s.db.Exec(`create table "users" (
		id    int8 primary key autoincrement,
		name  varchar(100) not null,
		score int8
	)`)
	s.Require().NoError(err)

	var id int64
	s.Require().NoError(s.db.QueryRow(
		`insert into "users" (name, score) values ('Alice', ?) returning id`, int64(42),
	).Scan(&id))

	_, err = s.db.Exec(`update "users" set score = NULL where id = ?`, id)
	s.Require().NoError(err)

	var score sql.NullInt64
	s.Require().NoError(s.db.QueryRow(`select score from "users" where id = ?`, id).Scan(&score))
	s.False(score.Valid, "score should be NULL after update")
}
