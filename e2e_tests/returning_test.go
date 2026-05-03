package e2etests

import "time"

func (s *TestSuite) TestReturning() {
	_, err := s.db.Exec(`create table "users" (
		id         int8 primary key autoincrement,
		name       varchar(100) not null,
		email      varchar(200) not null,
		score      int8,
		created_at timestamp
	);`)
	s.Require().NoError(err)

	s.Run("INSERT_RETURNING_id", func() {
		rows, err := s.db.Query(
			`insert into users (name, email, score, created_at) values (?, ?, ?, ?) returning id`,
			"Alice", "alice@example.com", int64(10), time.Now(),
		)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var id int64
		s.Require().NoError(rows.Scan(&id))
		s.Greater(id, int64(0))
		s.Require().False(rows.Next())
		s.Require().NoError(rows.Err())
	})

	s.Run("INSERT_RETURNING_multiple_columns", func() {
		rows, err := s.db.Query(
			`insert into users (name, email, score, created_at) values (?, ?, ?, ?) returning id, name, score`,
			"Bob", "bob@example.com", int64(20), time.Now(),
		)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var id, score int64
		var name string
		s.Require().NoError(rows.Scan(&id, &name, &score))
		s.Greater(id, int64(0))
		s.Equal("Bob", name)
		s.Equal(int64(20), score)
		s.Require().False(rows.Next())
		s.Require().NoError(rows.Err())
	})

	s.Run("INSERT_multi_row_RETURNING", func() {
		rows, err := s.db.Query(
			`insert into users (name, email, score, created_at) values (?, ?, ?, ?), (?, ?, ?, ?) returning id, name`,
			"Carol", "carol@example.com", int64(30), time.Now(),
			"Dave", "dave@example.com", int64(40), time.Now(),
		)
		s.Require().NoError(err)
		defer rows.Close()

		var results []struct {
			id   int64
			name string
		}
		for rows.Next() {
			var id int64
			var name string
			s.Require().NoError(rows.Scan(&id, &name))
			results = append(results, struct {
				id   int64
				name string
			}{id, name})
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(results, 2)
		s.Equal("Carol", results[0].name)
		s.Equal("Dave", results[1].name)
		s.Greater(results[1].id, results[0].id)
	})

	s.Run("UPDATE_RETURNING_single_column", func() {
		// First insert a known row
		var id int64
		row := s.db.QueryRow(
			`insert into users (name, email, score, created_at) values (?, ?, ?, ?) returning id`,
			"Eve", "eve@example.com", int64(50), time.Now(),
		)
		s.Require().NoError(row.Scan(&id))

		rows, err := s.db.Query(
			`update users set score = ? where id = ? returning score`,
			int64(99), id,
		)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var score int64
		s.Require().NoError(rows.Scan(&score))
		s.Equal(int64(99), score)
		s.Require().False(rows.Next())
		s.Require().NoError(rows.Err())
	})

	s.Run("UPDATE_RETURNING_multiple_columns", func() {
		var id int64
		row := s.db.QueryRow(
			`insert into users (name, email, score, created_at) values (?, ?, ?, ?) returning id`,
			"Frank", "frank@example.com", int64(5), time.Now(),
		)
		s.Require().NoError(row.Scan(&id))

		rows, err := s.db.Query(
			`update users set name = ?, score = ? where id = ? returning id, name, score`,
			"Franklin", int64(55), id,
		)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var gotID, gotScore int64
		var gotName string
		s.Require().NoError(rows.Scan(&gotID, &gotName, &gotScore))
		s.Equal(id, gotID)
		s.Equal("Franklin", gotName)
		s.Equal(int64(55), gotScore)
		s.Require().False(rows.Next())
		s.Require().NoError(rows.Err())
	})

	s.Run("DELETE_RETURNING_id", func() {
		var id int64
		row := s.db.QueryRow(
			`insert into users (name, email, score, created_at) values (?, ?, ?, ?) returning id`,
			"Grace", "grace@example.com", int64(1), time.Now(),
		)
		s.Require().NoError(row.Scan(&id))

		rows, err := s.db.Query(
			`delete from users where id = ? returning id, name`,
			id,
		)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var gotID int64
		var gotName string
		s.Require().NoError(rows.Scan(&gotID, &gotName))
		s.Equal(id, gotID)
		s.Equal("Grace", gotName)
		s.Require().False(rows.Next())
		s.Require().NoError(rows.Err())

		// Confirm row is actually gone
		var count int64
		s.Require().NoError(s.db.QueryRow(`select count(*) from users where id = ?`, id).Scan(&count))
		s.Equal(int64(0), count)
	})

	s.Run("DELETE_RETURNING_multiple_rows", func() {
		// Insert several rows, delete a subset, return them
		for i := range 3 {
			_, err := s.db.Exec(
				`insert into users (name, email, score, created_at) values (?, ?, ?, ?)`,
				"TempUser", "temp@example.com", int64(i+100), time.Now(),
			)
			s.Require().NoError(err)
		}

		rows, err := s.db.Query(`delete from users where name = ? returning score`, "TempUser")
		s.Require().NoError(err)
		defer rows.Close()

		var scores []int64
		for rows.Next() {
			var score int64
			s.Require().NoError(rows.Scan(&score))
			scores = append(scores, score)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(scores, 3)
	})

	s.Run("INSERT_ON_CONFLICT_DO_NOTHING_no_rows_returned", func() {
		// Insert a row first
		var id int64
		s.Require().NoError(s.db.QueryRow(
			`insert into users (name, email, score, created_at) values (?, ?, ?, ?) returning id`,
			"Unique", "unique@example.com", int64(1), time.Now(),
		).Scan(&id))

		// DO NOTHING conflict — nothing inserted, no rows returned
		rows, err := s.db.Query(
			`insert into users (id, name, email, score, created_at) values (?, ?, ?, ?, ?) on conflict do nothing returning id`,
			id, "Unique", "unique@example.com", int64(1), time.Now(),
		)
		s.Require().NoError(err)
		defer rows.Close()
		s.Require().False(rows.Next(), "DO NOTHING should return no rows")
		s.Require().NoError(rows.Err())
	})
}
