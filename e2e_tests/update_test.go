package e2etests

import (
	"context"
	"database/sql"
)

func (s *TestSuite) TestUpdate() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	s.Run("Insert some test users", func() {
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
	})

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
		}
	})
}
