package e2etests

// TestTransaction_DML_ReadsOwnWrites verifies that an explicit transaction can
// read its own uncommitted DML changes (INSERT, UPDATE, DELETE) and that those
// changes are either persisted after COMMIT or fully reversed after ROLLBACK.
func (s *TestSuite) TestTransaction_DML_ReadsOwnWrites() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	s.Run("INSERT visible within tx, absent after ROLLBACK", func() {
		tx, err := s.db.Begin()
		s.Require().NoError(err)

		_, err = tx.Exec(`insert into "users" ("email", "name") values (?, ?);`, "alice@example.com", "Alice")
		s.Require().NoError(err)

		// Row must be visible inside the same transaction.
		var count int
		err = tx.QueryRow(`select count(*) from "users";`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(1, count)

		s.Require().NoError(tx.Rollback())

		// Row must be gone after rollback.
		err = s.db.QueryRow(`select count(*) from "users";`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(0, count)
	})

	s.Run("INSERT visible within tx, persisted after COMMIT", func() {
		tx, err := s.db.Begin()
		s.Require().NoError(err)

		_, err = tx.Exec(`insert into "users" ("email", "name") values (?, ?);`, "bob@example.com", "Bob")
		s.Require().NoError(err)

		var count int
		err = tx.QueryRow(`select count(*) from "users";`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(1, count)

		s.Require().NoError(tx.Commit())

		err = s.db.QueryRow(`select count(*) from "users";`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(1, count)
	})

	s.Run("UPDATE visible within tx, reversed after ROLLBACK", func() {
		// Ensure at least one row exists (from previous committed subtest).
		var count int
		err := s.db.QueryRow(`select count(*) from "users";`).Scan(&count)
		s.Require().NoError(err)
		s.Require().Equal(1, count)

		tx, err := s.db.Begin()
		s.Require().NoError(err)

		_, err = tx.Exec(`update "users" set "name" = ? where "name" = ?;`, "Bobby", "Bob")
		s.Require().NoError(err)

		// Updated name must be visible within the same transaction.
		var name string
		err = tx.QueryRow(`select "name" from "users" where "email" = ?;`, "bob@example.com").Scan(&name)
		s.Require().NoError(err)
		s.Equal("Bobby", name)

		s.Require().NoError(tx.Rollback())

		// Original name must be restored after rollback.
		err = s.db.QueryRow(`select "name" from "users" where "email" = ?;`, "bob@example.com").Scan(&name)
		s.Require().NoError(err)
		s.Equal("Bob", name)
	})

	s.Run("DELETE visible within tx, reversed after ROLLBACK", func() {
		var count int
		err := s.db.QueryRow(`select count(*) from "users";`).Scan(&count)
		s.Require().NoError(err)
		s.Require().Equal(1, count)

		tx, err := s.db.Begin()
		s.Require().NoError(err)

		_, err = tx.Exec(`delete from "users" where "email" = ?;`, "bob@example.com")
		s.Require().NoError(err)

		// Row must be gone inside the transaction.
		err = tx.QueryRow(`select count(*) from "users";`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(0, count)

		s.Require().NoError(tx.Rollback())

		// Row must be restored after rollback.
		err = s.db.QueryRow(`select count(*) from "users";`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(1, count)
	})
}

func (s *TestSuite) TestTransaction() {
	s.Run("Create a table in a transaction but rollback before commit", func() {
		tx, err := s.db.Begin()
		s.Require().NoError(err)

		result, err := tx.Exec(createUsersTableSQL)
		s.Require().NoError(err)

		rowsAffected, err := result.RowsAffected()
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

		result, err := tx.Exec(createUsersTableSQL)
		s.Require().NoError(err)

		rowsAffected, err := result.RowsAffected()
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

		result, err := tx.Exec(`drop table "users";`)
		s.Require().NoError(err)

		rowsAffected, err := result.RowsAffected()
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

		result, err := tx.Exec(`drop table "users";`)
		s.Require().NoError(err)

		rowsAffected, err := result.RowsAffected()
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
