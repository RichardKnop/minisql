package e2etests

func (s *TestSuite) TestDelete() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	// Insert test users
	s.execQuery(`insert into users("name", "email") values('Danny Mason', 'Danny_Mason2966@xqj6f.tech'),
('Johnathan Walker', 'Johnathan_Walker250@ptr6k.page'),
('Tyson Weldon', 'Tyson_Weldon2108@zynuu.video'),
('Mason Callan', 'Mason_Callan9524@bu2lo.edu'),
('Logan Flynn', 'Logan_Flynn9019@xtwt3.pro'),
('Beatrice Uttley', 'Beatrice_Uttley1670@1wa8o.org'),
('Harry Johnson', 'Harry_Johnson5515@jcf8v.video'),
('Carl Thomson', 'Carl_Thomson4218@kyb7t.host'),
('Kaylee Johnson', 'Kaylee_Johnson8112@c2nyu.design'),
('Cristal Duvall', 'Cristal_Duvall6639@yvu30.press');`, 10)

	s.Run("Delete with where matching no rows", func() {
		s.execQuery(`delete from users where id = 9999;`, 0)

		users := s.collectUsers(`select * from users;`)
		s.Require().Len(users, 10)
	})

	s.Run("Delete one row", func() {
		s.execQuery(`delete from users where id = 9;`, 1)

		users := s.collectUsers(`select * from users;`)
		s.Require().Len(users, 9)
		expectedIDs := []int64{1, 2, 3, 4, 5, 6, 7, 8, 10}
		for i := 0; i < 9; i++ {
			s.Equal(expectedIDs[i], users[i].ID)
		}
	})

	s.Run("Delete multiple rows", func() {
		s.execQuery(`delete from users where id = 1 or id = 5;`, 2)

		users := s.collectUsers(`select * from users;`)
		s.Require().Len(users, 7)
		expectedIDs := []int64{2, 3, 4, 6, 7, 8, 10}
		for i := 0; i < 7; i++ {
			s.Equal(expectedIDs[i], users[i].ID)
		}
	})

	s.Run("Delete all rows", func() {
		s.execQuery(`delete from users;`, 7)

		users := s.collectUsers(`select * from users;`)
		s.Require().Len(users, 0)
	})
}
