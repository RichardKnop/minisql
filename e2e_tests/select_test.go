package e2etests

import (
	"context"
	"database/sql"
	"time"

	"github.com/RichardKnop/minisql/internal/minisql"
)

func (s *TestSuite) TestSelect() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)
	_, err = s.db.Exec(createUsersTimestampIndexSQL)
	s.Require().NoError(err)

	// First insert one row with explicitely set timestamp for created column
	s.execQuery(`insert into users("email", "name", "created") 
values('Danny_Mason2966@xqj6f.tech', 'Danny Mason', '2024-01-01 12:00:00');`, 1)

	// Next try to specify primary key manually without using autoincrement
	s.execQuery(`insert into users("id", "email", "name", "created") 
values(100, 'Johnathan_Walker250@ptr6k.page', 'Johnathan Walker', '2024-01-02 15:30:27');`, 1)

	// Next insert multiple rows without specifying created column (should default to now())
	// Also switch order of name and email to ensure columns are mapped correctly
	s.execQuery(`insert into users("name", "email") values('Tyson Weldon', 'Tyson_Weldon2108@zynuu.video'),
('Mason Callan', 'Mason_Callan9524@bu2lo.edu'),
('Logan Flynn', 'Logan_Flynn9019@xtwt3.pro'),
('Beatrice Uttley', 'Beatrice_Uttley1670@1wa8o.org'),
('Harry Johnson', 'Harry_Johnson5515@jcf8v.video'),
('Carl Thomson', 'Carl_Thomson4218@kyb7t.host'),
('Kaylee Johnson', 'Kaylee_Johnson8112@c2nyu.design');`, 7)

	// Insert one more row to test using NOW() function for created timestamp
	s.execQuery(`insert into users("email", "name", "created") 
values('Cristal_Duvall6639@yvu30.press', 'Cristal Duvall', NOW());`, 1)

	// Inserting user with duplicate primary key should fail
	aResult, err := s.db.ExecContext(context.Background(), `insert into users("id", "email", "name", "created") 
values(100, 'Johnathan_Walker250+new@ptr6k.page', 'Johnathan Walker', '2024-01-02 15:30:27');`)
	s.Require().Error(err)
	s.ErrorIs(err, minisql.ErrDuplicateKey)
	s.Equal("failed to insert primary key pkey__users: duplicate key", err.Error())
	s.Nil(aResult)

	// Inserting user with duplicate unique index key should fail
	aResult, err = s.db.ExecContext(context.Background(), `insert into users("name", "email", "created") 
values('Johnathan Walker', 'Johnathan_Walker250@ptr6k.page', '2024-01-02 15:30:27');`)
	s.Require().Error(err)
	s.ErrorIs(err, minisql.ErrDuplicateKey)
	s.Equal("failed to insert key for unique index key__users__email: duplicate key", err.Error())
	s.Nil(aResult)

	s.Run("Basic select query", func() {
		users := s.collectUsers(`select * from users order by id;`)
		s.Require().Len(users, 10)

		s.Equal(user{
			ID:      1,
			Name:    sql.NullString{String: "Danny Mason", Valid: true},
			Email:   sql.NullString{String: "Danny_Mason2966@xqj6f.tech", Valid: true},
			Created: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
		}, users[0])

		s.Equal(user{
			ID:      100,
			Name:    sql.NullString{String: "Johnathan Walker", Valid: true},
			Email:   sql.NullString{String: "Johnathan_Walker250@ptr6k.page", Valid: true},
			Created: time.Date(2024, 1, 2, 15, 30, 27, 0, time.UTC),
		}, users[1])

		now := time.Now().UTC()
		for i := 2; i < 10; i++ {
			s.Equal(int64(100+i-1), users[i].ID) // id should continue from 100
			s.Equal(now.Year(), users[i].Created.Year())
			s.Equal(now.Month(), users[i].Created.Month())
			s.Equal(now.Day(), users[i].Created.Day())
			s.Equal(now.Hour(), users[i].Created.Hour())
			s.Equal(now.Minute(), users[i].Created.Minute())
			s.Equal(now.Second(), users[i].Created.Second())
		}
	})

	s.Run("Limit offset", func() {
		users := s.collectUsers(`select * from users limit 1;`)
		s.Require().Len(users, 1)
		s.Equal(int64(1), users[0].ID)

		users = s.collectUsers(`select * from users offset 9;`)
		s.Require().Len(users, 1)
		s.Equal(int64(108), users[0].ID)

		users = s.collectUsers(`select * from users limit 2 offset 4;`)
		s.Require().Len(users, 2)
		s.Equal(int64(103), users[0].ID)
		s.Equal(int64(104), users[1].ID)
	})

	s.Run("Where conditions on primary key", func() {
		users := s.collectUsers(`select * from users where id = 107;`)
		s.Require().Len(users, 1)
		s.Equal("Kaylee Johnson", users[0].Name.String)

		users = s.collectUsers(`select * from users where id != 105;`)
		s.Require().Len(users, 9)
		for _, aUser := range users {
			s.Require().NotEqual("Harry Johnson", aUser.Name.String)
		}

		users = s.collectUsers(`select * from users where id in (103, 105);`)
		s.Require().Len(users, 2)
		s.Equal("Logan Flynn", users[0].Name.String)
		s.Equal("Harry Johnson", users[1].Name.String)

		users = s.collectUsers(`select * from users where id not in (100, 1, 107, 106, 105, 101, 102);`)
		s.Require().Len(users, 3)
		s.Equal("Logan Flynn", users[0].Name.String)
		s.Equal("Beatrice Uttley", users[1].Name.String)
		s.Equal("Cristal Duvall", users[2].Name.String)

		users = s.collectUsers(`select * from users where id = 102 or id = 104;`)
		s.Require().Len(users, 2)
		s.Equal("Mason Callan", users[0].Name.String)
		s.Equal("Beatrice Uttley", users[1].Name.String)

		users = s.collectUsers(`select * from users where id > 105;`)
		s.Require().Len(users, 3)
		s.Equal("Carl Thomson", users[0].Name.String)
		s.Equal("Kaylee Johnson", users[1].Name.String)
		s.Equal("Cristal Duvall", users[2].Name.String)
	})

	s.Run("Reinitialise to force unmarshaling from disk", func() {
		s.db, err = sql.Open("minisql", s.dbFile.Name())
		s.Require().NoError(err)

		users := s.collectUsers(`select * from users order by id desc;`)
		s.Require().Len(users, 10)

		expectedIDs := []int64{108, 107, 106, 105, 104, 103, 102, 101, 100, 1}
		for i := 9; i >= 0; i-- {
			s.Equal(expectedIDs[i], users[i].ID)
		}
	})

	s.Run("Selecting based on timestamp column", func() {
		twentiethCentury := time.Date(1999, 7, 19, 22, 11, 56, 112456*1000, time.UTC).Format("2006-01-02 15:04:05")
		users := s.collectUsers(`select * from users where created < '` + twentiethCentury + `';`)
		s.Require().Empty(users)

		aMinuteAgo := time.Now().Add(-1 * time.Minute).UTC().Format("2006-01-02 15:04:05")
		users = s.collectUsers(`select * from users where created < '` + aMinuteAgo + `';`)
		s.Require().Len(users, 2)
		s.Equal(user{
			ID:      1,
			Name:    sql.NullString{String: "Danny Mason", Valid: true},
			Email:   sql.NullString{String: "Danny_Mason2966@xqj6f.tech", Valid: true},
			Created: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
		}, users[0])
		s.Equal(user{
			ID:      100,
			Name:    sql.NullString{String: "Johnathan Walker", Valid: true},
			Email:   sql.NullString{String: "Johnathan_Walker250@ptr6k.page", Valid: true},
			Created: time.Date(2024, 1, 2, 15, 30, 27, 0, time.UTC),
		}, users[1])

		aMinuteLater := time.Now().Add(1 * time.Minute).UTC().Format("2006-01-02 15:04:05")
		expectedIDs := []int64{101, 102, 103, 104, 105, 106, 107, 108}
		users = s.collectUsers(`select * from users where created > '` + aMinuteAgo + `' and created < '` + aMinuteLater + `';`)
		s.Require().Len(users, 8)
		for i := range 8 {
			s.Equal(expectedIDs[i], users[i].ID)
		}
	})

	s.Run("Drop and recreate timestamp index to make sure it gets repopulated", func() {
		_, err := s.db.Exec(dropUsersTimestampIndexSQL)
		s.Require().NoError(err)
		_, err = s.db.Exec(createUsersTimestampIndexSQL)
		s.Require().NoError(err)

		twentiethCentury := time.Date(1999, 7, 19, 22, 11, 56, 112456*1000, time.UTC).Format("2006-01-02 15:04:05")
		users := s.collectUsers(`select * from users where created < '` + twentiethCentury + `';`)
		s.Require().Empty(users)

		aMinuteAgo := time.Now().Add(-1 * time.Minute).UTC().Format("2006-01-02 15:04:05")
		users = s.collectUsers(`select * from users where created < '` + aMinuteAgo + `';`)
		s.Require().Len(users, 2)
		s.Equal(user{
			ID:      1,
			Name:    sql.NullString{String: "Danny Mason", Valid: true},
			Email:   sql.NullString{String: "Danny_Mason2966@xqj6f.tech", Valid: true},
			Created: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
		}, users[0])
		s.Equal(user{
			ID:      100,
			Name:    sql.NullString{String: "Johnathan Walker", Valid: true},
			Email:   sql.NullString{String: "Johnathan_Walker250@ptr6k.page", Valid: true},
			Created: time.Date(2024, 1, 2, 15, 30, 27, 0, time.UTC),
		}, users[1])

		aMinuteLater := time.Now().Add(1 * time.Minute).UTC().Format("2006-01-02 15:04:05")
		expectedIDs := []int64{101, 102, 103, 104, 105, 106, 107, 108}
		users = s.collectUsers(`select * from users where created > '` + aMinuteAgo + `' and created < '` + aMinuteLater + `';`)
		s.Require().Len(users, 8)
		for i := range 8 {
			s.Equal(expectedIDs[i], users[i].ID)
		}
	})

	s.Run("Selecting based on NULL values", func() {
		// Insert a user with NULL email
		aResult, err := s.db.ExecContext(context.Background(), `insert into users("name") values('Null Email User');`)
		s.Require().NoError(err)
		rowsAffected, err := aResult.RowsAffected()
		s.Require().NoError(err)
		s.Require().Equal(int64(1), rowsAffected)

		users := s.collectUsers(`select * from users where email is null;`)
		s.Require().Len(users, 1)
		s.False(users[0].Email.Valid)
		s.Empty(users[0].Email.String)
		s.Equal("Null Email User", users[0].Name.String)

		expectedIDs := []int64{1, 100, 101, 102, 103, 104, 105, 106, 107, 108}
		users = s.collectUsers(`select * from users where email is not null;`)
		s.Require().Len(users, 10)
		for i := range 10 {
			s.Equal(expectedIDs[i], users[i].ID)
			s.True(users[i].Email.Valid)
			s.NotEmpty(users[i].Email.String)
		}
	})

	s.Run("Selecting only specific fields", func() {
		var name sql.NullString
		err := s.db.QueryRowContext(context.Background(), `select name from users where id =106;`).Scan(&name)
		s.Require().NoError(err)
		s.True(name.Valid)
		s.Equal("Carl Thomson", name.String)
	})

	// Let's create more tables and insert additional test data to ensure selects still work correctly
	_, err = s.db.Exec(createProductsTableSQL)
	s.Require().NoError(err)

	_, err = s.db.Exec(createOrdersTableSQL)
	s.Require().NoError(err)

	s.execQuery(`insert into products("product_id", "name", "description", "price") values
(25, 'Gaming Laptop', 'High performance laptop for gaming', 1500),
(26, 'Wireless Mouse', 'Ergonomic wireless mouse', 50),
(27, 'Mechanical Keyboard', 'RGB backlit mechanical keyboard', 120);`, 3)

	s.execQuery(`insert into orders("user_id", "product_id", "total_paid") values
(100, 25, 1500),
(101, 26, 50),
(102, 27, 120),
(100, 27, 120);`, 4)

	s.Run("More basic selects from multiple tables", func() {
		orders := s.collectOrders(`select * from orders where user_id = 100;`)
		s.Require().Len(orders, 2)

		s.Equal(1, int(orders[0].ID))
		s.Equal(25, int(orders[0].ProductID))
		s.Equal(100, int(orders[0].UserID))
		s.Equal(1500, int(orders[0].TotalPaid))

		s.Equal(4, int(orders[1].ID))
		s.Equal(27, int(orders[1].ProductID))
		s.Equal(100, int(orders[1].UserID))
		s.Equal(120, int(orders[1].TotalPaid))

		orders = s.collectOrders(`select * from orders where product_id = 27;`)
		s.Require().Len(orders, 2)

		s.Equal(3, int(orders[0].ID))
		s.Equal(27, int(orders[0].ProductID))
		s.Equal(102, int(orders[0].UserID))
		s.Equal(120, int(orders[0].TotalPaid))

		s.Equal(4, int(orders[1].ID))
		s.Equal(27, int(orders[1].ProductID))
		s.Equal(100, int(orders[1].UserID))
		s.Equal(120, int(orders[1].TotalPaid))
	})
}

func (s TestSuite) execQuery(query string, expectedRowsAffected int) {
	aResult, err := s.db.ExecContext(context.Background(), query)
	s.Require().NoError(err)
	rowsAffected, err := aResult.RowsAffected()
	s.Require().NoError(err)
	s.Require().Equal(expectedRowsAffected, int(rowsAffected))
}

type user struct {
	ID      int64
	Name    sql.NullString
	Email   sql.NullString
	Created time.Time
}

func (s TestSuite) collectUsers(query string) []user {
	rows, err := s.db.QueryContext(context.Background(), query)
	s.Require().NoError(err)
	defer rows.Close()

	var users []user
	for rows.Next() {
		var aUser user
		err := rows.Scan(&aUser.ID, &aUser.Email, &aUser.Name, &aUser.Created)
		s.Require().NoError(err)
		users = append(users, aUser)
	}
	s.Require().NoError(rows.Err())
	return users
}

func (s TestSuite) collectUser(query string) user {
	var user user
	err := s.db.QueryRow(query).Scan(&user.ID, &user.Email, &user.Name, &user.Created)
	s.Require().NoError(err)
	return user
}

type order struct {
	ID        int64
	UserID    int64
	ProductID int64
	TotalPaid int32
	Created   time.Time
}

func (s TestSuite) collectOrders(query string) []order {
	rows, err := s.db.QueryContext(context.Background(), query)
	s.Require().NoError(err)
	defer rows.Close()

	var orders []order
	for rows.Next() {
		var anOrder order
		err := rows.Scan(&anOrder.ID, &anOrder.UserID, &anOrder.ProductID, &anOrder.TotalPaid, &anOrder.Created)
		s.Require().NoError(err)
		orders = append(orders, anOrder)
	}
	s.Require().NoError(rows.Err())
	return orders
}
