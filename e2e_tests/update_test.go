package e2etests

import (
	"context"
	"database/sql"
)

func (s *TestSuite) TestUpdate() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	// Insert test users
	aResult, err := s.db.ExecContext(context.Background(), `insert into users("name", "email") values('Danny Mason', 'Danny_Mason2966@xqj6f.tech'),
('Johnathan Walker', 'Johnathan_Walker250@ptr6k.page'),
('Tyson Weldon', 'Tyson_Weldon2108@zynuu.video'),
('Mason Callan', 'Mason_Callan9524@bu2lo.edu'),
('Logan Flynn', 'Logan_Flynn9019@xtwt3.pro'),
('Beatrice Uttley', 'Beatrice_Uttley1670@1wa8o.org'),
('Harry Johnson', 'Harry_Johnson5515@jcf8v.video'),
('Carl Thomson', 'Carl_Thomson4218@kyb7t.host'),
('Kaylee Johnson', 'Kaylee_Johnson8112@c2nyu.design'),
('Cristal Duvall', 'Cristal_Duvall6639@yvu30.press');`)
	s.Require().NoError(err)
	rowsAffected, err := aResult.RowsAffected()
	s.Require().NoError(err)
	s.Require().Equal(int64(10), rowsAffected)

	expectedNames := []sql.NullString{
		{String: "Danny Mason", Valid: true},
		{String: "Johnathan Walker", Valid: true},
		{String: "Tyson Weldon", Valid: true},
		{String: "Mason Callan", Valid: true},
		{String: "Logan Flynn", Valid: true},
		{String: "Beatrice Uttley", Valid: true},
		{String: "Harry Johnson", Valid: true},
		{String: "Carl Thomson", Valid: true},
		{String: "Kaylee Johnson", Valid: true},
		{String: "Cristal Duvall", Valid: true},
	}

	expectedEmails := []sql.NullString{
		{String: "Danny_Mason2966@xqj6f.tech", Valid: true},
		{String: "Johnathan_Walker250@ptr6k.page", Valid: true},
		{String: "Tyson_Weldon2108@zynuu.video", Valid: true},
		{String: "Mason_Callan9524@bu2lo.edu", Valid: true},
		{String: "Logan_Flynn9019@xtwt3.pro", Valid: true},
		{String: "Beatrice_Uttley1670@1wa8o.org", Valid: true},
		{String: "Harry_Johnson5515@jcf8v.video", Valid: true},
		{String: "Carl_Thomson4218@kyb7t.host", Valid: true},
		{String: "Kaylee_Johnson8112@c2nyu.design", Valid: true},
		{String: "Cristal_Duvall6639@yvu30.press", Valid: true},
	}

	s.Run("Update with where matching no rows", func() {
		aResult, err := s.db.ExecContext(context.Background(), `update users set name = 'Updated Name' where id = 9999;`)
		s.Require().NoError(err)
		rowsAffected, err := aResult.RowsAffected()
		s.Require().NoError(err)
		s.Require().Equal(int64(0), rowsAffected)

		users := s.collectUsers(`select * from users;`)
		s.Require().Len(users, 10)
		for i, aUser := range users {
			s.Equal(expectedNames[i], aUser.Name)
			s.Equal(expectedEmails[i], aUser.Email)
		}
	})

	s.Run("Update single row", func() {
		aResult, err := s.db.ExecContext(context.Background(), `update users set name = 'Tyson Weldon Jr' where id = 3;`)
		s.Require().NoError(err)
		rowsAffected, err := aResult.RowsAffected()
		s.Require().NoError(err)
		s.Require().Equal(int64(1), rowsAffected)

		expectedNames[2].String = "Tyson Weldon Jr"

		users := s.collectUsers(`select * from users;`)
		s.Require().Len(users, 10)
		for i, aUser := range users {
			s.Equal(expectedNames[i], aUser.Name)
			s.Equal(expectedEmails[i], aUser.Email)
		}
	})

	s.Run("Update multiple rows", func() {
		aResult, err := s.db.ExecContext(context.Background(), `update users set name = 'N/A' where id = 4 or id = 6;`)
		s.Require().NoError(err)
		rowsAffected, err := aResult.RowsAffected()
		s.Require().NoError(err)
		s.Require().Equal(int64(2), rowsAffected)

		expectedNames[3].String = "N/A"
		expectedNames[5].String = "N/A"

		users := s.collectUsers(`select * from users;`)
		s.Require().Len(users, 10)
		for i, aUser := range users {
			s.Equal(expectedNames[i], aUser.Name)
			s.Equal(expectedEmails[i], aUser.Email)
		}
	})

	s.Run("Update to NULL and from NULL back", func() {
		aResult, err := s.db.ExecContext(context.Background(), `update users set name = NULL where id = 9;`)
		s.Require().NoError(err)
		rowsAffected, err := aResult.RowsAffected()
		s.Require().NoError(err)
		s.Require().Equal(int64(1), rowsAffected)

		expectedNames[8] = sql.NullString{}

		users := s.collectUsers(`select * from users;`)
		s.Require().Len(users, 10)
		for i, aUser := range users {
			s.Equal(expectedNames[i], aUser.Name)
			s.Equal(expectedEmails[i], aUser.Email)
		}

		aResult, err = s.db.ExecContext(context.Background(), `update users set name = 'Kaylee Johnson' where id = 9;`)
		s.Require().NoError(err)
		rowsAffected, err = aResult.RowsAffected()
		s.Require().NoError(err)
		s.Require().Equal(int64(1), rowsAffected)

		expectedNames[8] = sql.NullString{String: "Kaylee Johnson", Valid: true}

		users = s.collectUsers(`select * from users;`)
		s.Require().Len(users, 10)
		for i, aUser := range users {
			s.Equal(expectedNames[i], aUser.Name)
			s.Equal(expectedEmails[i], aUser.Email)
		}
	})

	s.Run("Updating primary key to NULL fails", func() {
		aResult, err := s.db.ExecContext(context.Background(), `update users set id = null;`)
		s.Require().Error(err)
		s.ErrorContains(err, "cannot update primary key pkey__users to NULL")
		s.Nil(aResult)
	})

	s.Run("Updating unique index key to NULL succeeds", func() {
		aResult, err := s.db.ExecContext(context.Background(), `update users set email = null where id = 3 or id = 7;`)
		s.Require().NoError(err)
		rowsAffected, err = aResult.RowsAffected()
		s.Require().NoError(err)
		s.Require().Equal(int64(2), rowsAffected)

		expectedEmails[2] = sql.NullString{}
		expectedEmails[6] = sql.NullString{}

		users := s.collectUsers(`select * from users;`)
		s.Require().Len(users, 10)
		for i, aUser := range users {
			s.Equal(expectedNames[i], aUser.Name)
			s.Equal(expectedEmails[i], aUser.Email)
		}
	})
}
