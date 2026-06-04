package e2etests

func (s *TestSuite) TestNegativeLiterals() {
	_, err := s.db.Exec(`create table "scores" (
		id    int8 primary key autoincrement,
		name  text not null,
		score int8 not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(
		`insert into "scores" (name, score) values (?, ?), (?, ?), (?, ?)`,
		"Alice", -50,
		"Bob", 0,
		"Carol", 100,
	)
	s.Require().NoError(err)

	s.Run("WHERE equality with negative integer literal", func() {
		var name string
		err := s.db.QueryRow(`SELECT name FROM "scores" WHERE score = -50`).Scan(&name)
		s.Require().NoError(err)
		s.Equal("Alice", name)
	})

	s.Run("WHERE greater-than with negative integer literal", func() {
		var count int64
		err := s.db.QueryRow(`SELECT COUNT(*) FROM "scores" WHERE score > -1`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(int64(2), count)
	})

	s.Run("WHERE less-than with negative integer literal", func() {
		var count int64
		err := s.db.QueryRow(`SELECT COUNT(*) FROM "scores" WHERE score < -1`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(int64(1), count)
	})

	s.Run("WHERE BETWEEN with negative bounds", func() {
		var count int64
		err := s.db.QueryRow(`SELECT COUNT(*) FROM "scores" WHERE score BETWEEN -100 AND -1`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(int64(1), count)
	})

	s.Run("WHERE IN with negative values", func() {
		var count int64
		err := s.db.QueryRow(`SELECT COUNT(*) FROM "scores" WHERE score IN (-50, 0)`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(int64(2), count)
	})

	s.Run("INSERT with negative literal in VALUES", func() {
		_, err := s.db.Exec(`INSERT INTO "scores" (name, score) VALUES ('Dave', -999)`)
		s.Require().NoError(err)

		var score int64
		err = s.db.QueryRow(`SELECT score FROM "scores" WHERE name = 'Dave'`).Scan(&score)
		s.Require().NoError(err)
		s.Equal(int64(-999), score)
	})

	s.Run("UPDATE SET with negative literal", func() {
		res, err := s.db.Exec(`UPDATE "scores" SET score = -1 WHERE name = 'Bob'`)
		s.Require().NoError(err)
		n, err := res.RowsAffected()
		s.Require().NoError(err)
		s.Equal(int64(1), n)

		var score int64
		err = s.db.QueryRow(`SELECT score FROM "scores" WHERE name = 'Bob'`).Scan(&score)
		s.Require().NoError(err)
		s.Equal(int64(-1), score)
	})

	s.Run("arithmetic still works correctly (positive subtraction)", func() {
		var result int64
		err := s.db.QueryRow(`SELECT score - 10 FROM "scores" WHERE name = 'Carol'`).Scan(&result)
		s.Require().NoError(err)
		s.Equal(int64(90), result)
	})
}
