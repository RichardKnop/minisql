package e2etests

import (
	"database/sql"
	"sync"
)

func (s *TestSuite) TestConcurrency() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)
	_, err = s.db.Exec(createUsersTimestampIndexSQL)
	s.Require().NoError(err)

	// Insert 1000 test users
	usersToInsert := gen.Users(1000)
	for _, aUser := range usersToInsert {
		s.prepareAndExecQuery(`insert into users("email", "name") values(?, ?);`, 1, aUser.Email.String, aUser.Name.String)
	}

	// Ensure all auto-commit transactions have completed and flushed
	// by performing a simple query that forces synchronization
	var syncCheck int
	err = s.db.QueryRow(`select count(*) from users`).Scan(&syncCheck)
	s.Require().NoError(err)
	s.Equal(1000, syncCheck)

	s.countRowsInTable("users", 1000)

	s.Run("Reinitialise to force unmarshaling from disk", func() {
		// Close database connection first to ensure all transactions are committed and flushed
		err := s.db.Close()
		s.Require().NoError(err)

		s.db, err = sql.Open("minisql", s.dbFile.Name())
		s.Require().NoError(err)

		s.countRowsInTable("users", 1000)
	})

	s.Run("Concurrently run select queries", func() {

		workerPool := make(chan struct{}, 20) // limit concurrency to 20 goroutines
		for range 20 {
			workerPool <- struct{}{}
		}
		numQueries := 100

		wg := sync.WaitGroup{}

		for i := range numQueries {
			<-workerPool

			idx := i

			wg.Go(func() {
				defer func() { workerPool <- struct{}{} }()

				stmt, err := s.db.Prepare(`select * from users where id = ?;`)
				s.Require().NoError(err)

				var user user
				err = stmt.QueryRow(int64(idx+1)).Scan(&user.ID, &user.Email, &user.Name, &user.Created)
				s.Require().NoError(err)
				s.Equal(int64(idx+1), user.ID)

				s.Equal(usersToInsert[idx].Name.String, user.Name.String)
				s.Equal(usersToInsert[idx].Email.String, user.Email.String)
				s.False(user.Created.IsZero())
			})
		}

		wg.Wait()

	})
}
