package e2etests

import (
	"context"
	"database/sql"
)

func (s *TestSuite) TestSelectDistinct() {
	// Create a table where we can easily control duplicate values
	_, err := s.db.Exec(`create table "scores" (
		id int8 primary key autoincrement,
		player varchar(255) not null,
		level int4 not null,
		score int4 not null
	);`)
	s.Require().NoError(err)

	// Insert rows with intentional duplicates in player and level columns
	_, err = s.db.ExecContext(context.Background(), `insert into scores("player", "level", "score") values
		('Alice', 1, 100),
		('Alice', 1, 200),
		('Alice', 2, 150),
		('Bob',   1, 120),
		('Bob',   2, 180),
		('Bob',   2, 90),
		('Carol', 3, 300);`)
	s.Require().NoError(err)

	s.Run("DISTINCT on single column removes duplicates", func() {
		rows, err := s.db.QueryContext(context.Background(), `select distinct player from scores order by player;`)
		s.Require().NoError(err)
		defer rows.Close()

		var players []string
		for rows.Next() {
			var p string
			s.Require().NoError(rows.Scan(&p))
			players = append(players, p)
		}
		s.Require().NoError(rows.Err())

		s.Require().Len(players, 3)
		s.Equal("Alice", players[0])
		s.Equal("Bob", players[1])
		s.Equal("Carol", players[2])
	})

	s.Run("DISTINCT on integer column removes duplicates", func() {
		rows, err := s.db.QueryContext(context.Background(), `select distinct level from scores order by level;`)
		s.Require().NoError(err)
		defer rows.Close()

		var levels []int32
		for rows.Next() {
			var l int32
			s.Require().NoError(rows.Scan(&l))
			levels = append(levels, l)
		}
		s.Require().NoError(rows.Err())

		s.Require().Len(levels, 3)
		s.Equal(int32(1), levels[0])
		s.Equal(int32(2), levels[1])
		s.Equal(int32(3), levels[2])
	})

	s.Run("DISTINCT on multiple columns treats combination as unit", func() {
		rows, err := s.db.QueryContext(context.Background(), `select distinct player, level from scores order by player;`)
		s.Require().NoError(err)
		defer rows.Close()

		type pair struct {
			Player string
			Level  int32
		}
		var pairs []pair
		for rows.Next() {
			var p pair
			s.Require().NoError(rows.Scan(&p.Player, &p.Level))
			pairs = append(pairs, p)
		}
		s.Require().NoError(rows.Err())

		// Alice×1, Alice×2, Bob×1, Bob×2, Carol×3 = 5 distinct (player, level) pairs
		s.Require().Len(pairs, 5)
	})

	s.Run("DISTINCT with WHERE clause", func() {
		rows, err := s.db.QueryContext(context.Background(), `select distinct player from scores where level = 1;`)
		s.Require().NoError(err)
		defer rows.Close()

		var players []string
		for rows.Next() {
			var p string
			s.Require().NoError(rows.Scan(&p))
			players = append(players, p)
		}
		s.Require().NoError(rows.Err())

		// Alice and Bob both have level=1 rows, Carol does not
		s.Require().Len(players, 2)
	})

	s.Run("DISTINCT with LIMIT", func() {
		rows, err := s.db.QueryContext(context.Background(), `select distinct player from scores order by player limit 2;`)
		s.Require().NoError(err)
		defer rows.Close()

		var players []string
		for rows.Next() {
			var p string
			s.Require().NoError(rows.Scan(&p))
			players = append(players, p)
		}
		s.Require().NoError(rows.Err())

		s.Require().Len(players, 2)
		s.Equal("Alice", players[0])
		s.Equal("Bob", players[1])
	})

	s.Run("DISTINCT on column with all unique values returns all rows", func() {
		rows, err := s.db.QueryContext(context.Background(), `select distinct id from scores order by id;`)
		s.Require().NoError(err)
		defer rows.Close()

		var ids []int64
		for rows.Next() {
			var id int64
			s.Require().NoError(rows.Scan(&id))
			ids = append(ids, id)
		}
		s.Require().NoError(rows.Err())

		s.Require().Len(ids, 7)
	})

	s.Run("DISTINCT on empty result returns no rows", func() {
		rows, err := s.db.QueryContext(context.Background(), `select distinct player from scores where level = 99;`)
		s.Require().NoError(err)
		defer rows.Close()

		var players []string
		for rows.Next() {
			var p string
			s.Require().NoError(rows.Scan(&p))
			players = append(players, p)
		}
		s.Require().NoError(rows.Err())

		s.Require().Len(players, 0)
	})

	s.Run("DISTINCT with nullable column treats NULLs as equal", func() {
		_, err := s.db.Exec(`create table "nullable_test" (
			id int8 primary key autoincrement,
			val varchar(255)
		);`)
		s.Require().NoError(err)

		_, err = s.db.ExecContext(context.Background(), `insert into nullable_test("val") values
			('foo'), ('foo'), ('bar'), (null), (null);`)
		s.Require().NoError(err)

		rows, err := s.db.QueryContext(context.Background(), `select distinct val from nullable_test order by val;`)
		s.Require().NoError(err)
		defer rows.Close()

		var vals []sql.NullString
		for rows.Next() {
			var v sql.NullString
			s.Require().NoError(rows.Scan(&v))
			vals = append(vals, v)
		}
		s.Require().NoError(rows.Err())

		// Expect: NULL, 'bar', 'foo' — 3 distinct values
		s.Require().Len(vals, 3)
	})
}
