package e2etests

import (
	"time"
)

func (s *TestSuite) TestPreparedStmts() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	s.Run("Insert user", func() {
		stmt, err := s.db.Prepare(`insert into users("email", "name", "created") values(?, ?, ?)`)
		s.Require().NoError(err)

		aResult, err := stmt.Exec("Danny_Mason2966@xqj6f.tech", "Danny Mason", "2024-01-01 12:00:00")
		s.Require().NoError(err)

		rowsAffected, err := aResult.RowsAffected()
		s.Require().NoError(err)
		s.Require().Equal(int64(1), rowsAffected)
	})

	s.Run("Select user", func() {
		stmt, err := s.db.Prepare(`select * from users where id = ?;`)
		s.Require().NoError(err)

		var user user
		err = stmt.QueryRow(int64(1)).Scan(&user.ID, &user.Email, &user.Name, &user.Created)
		s.Require().NoError(err)
		s.Equal(int64(1), user.ID)
		s.Equal("Danny Mason", user.Name.String)
		s.Equal("Danny_Mason2966@xqj6f.tech", user.Email.String)
		s.Equal(time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC), user.Created)
	})

	s.Run("Update user", func() {
		stmt, err := s.db.Prepare(`update users set name = ?, created = now() where id = ?;`)
		s.Require().NoError(err)

		aResult, err := stmt.Exec("New Name", int64(1))
		s.Require().NoError(err)

		rowsAffected, err := aResult.RowsAffected()
		s.Require().NoError(err)
		s.Require().Equal(int64(1), rowsAffected)

		user := s.collectUser(`select * from users where id = 1;`)
		s.Equal(int64(1), user.ID)
		s.Equal("New Name", user.Name.String)
		s.Equal("Danny_Mason2966@xqj6f.tech", user.Email.String)
		s.NotEqual(time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC), user.Created)
	})
}
