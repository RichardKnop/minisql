package e2etests

func (s *TestSuite) TestTransaction() {
	s.Run("Create a table in a transaction but rollback before commit", func() {
		tx, err := s.db.Begin()
		s.Require().NoError(err)

		aResult, err := tx.Exec(createUsersTableSQL)
		s.Require().NoError(err)

		rowsAffected, err := aResult.RowsAffected()
		s.Require().NoError(err)
		s.Require().Equal(int64(0), rowsAffected)

		// You should be able to see the table in the transaction
		var count int
		err = tx.QueryRow(`select count(*) from minisql_schema;`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(4, count)

		err = tx.Rollback()
		s.Require().NoError(err)

		// After rollback, the table should not exist
		// You should be able to see the table in the transaction
		err = s.db.QueryRow(`select count(*) from minisql_schema;`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(1, count)
	})

	s.Run("Now create table in a transaction and commit", func() {
		tx, err := s.db.Begin()
		s.Require().NoError(err)

		aResult, err := tx.Exec(createUsersTableSQL)
		s.Require().NoError(err)

		rowsAffected, err := aResult.RowsAffected()
		s.Require().NoError(err)
		s.Require().Equal(int64(0), rowsAffected)

		var count int
		err = tx.QueryRow(`select count(*) from minisql_schema;`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(4, count)

		err = tx.Commit()
		s.Require().NoError(err)

		// After commit, the table should exist
		err = s.db.QueryRow(`select count(*) from minisql_schema;`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(4, count)
	})

	s.Run("Drop a table in a transaction but rollback before commit", func() {
		tx, err := s.db.Begin()
		s.Require().NoError(err)

		aResult, err := tx.Exec(`drop table "users";`)
		s.Require().NoError(err)

		rowsAffected, err := aResult.RowsAffected()
		s.Require().NoError(err)
		s.Require().Equal(int64(0), rowsAffected)

		// You should be able to see the table being deleted in the transaction
		var count int
		err = tx.QueryRow(`select count(*) from minisql_schema;`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(1, count)

		err = tx.Rollback()
		s.Require().NoError(err)

		// After rollback, the table should still exist
		err = s.db.QueryRow(`select count(*) from minisql_schema;`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(4, count)
	})

	s.Run("Now drop a table in a transaction and commit", func() {
		tx, err := s.db.Begin()
		s.Require().NoError(err)

		aResult, err := tx.Exec(`drop table "users";`)
		s.Require().NoError(err)

		rowsAffected, err := aResult.RowsAffected()
		s.Require().NoError(err)
		s.Require().Equal(int64(0), rowsAffected)

		var count int
		err = tx.QueryRow(`select count(*) from minisql_schema;`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(1, count)

		err = tx.Commit()
		s.Require().NoError(err)

		// After commit, the table should be deleted
		err = s.db.QueryRow(`select count(*) from minisql_schema;`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(1, count)
	})
}
