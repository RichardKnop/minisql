package e2etests

import (
	"context"
	"fmt"

	"github.com/RichardKnop/minisql/internal/minisql"
)

var createCompositeUsersTableSQL = `create table "users" (
	first_name varchar(100) not null,
	last_name varchar(100) not null,
	email varchar(255),
	dob timestamp,
	created timestamp default now(),
	primary key (first_name, last_name)
);`

func (s *TestSuite) TestCompositeIndex() {
	_, err := s.db.Exec(createCompositeUsersTableSQL)
	s.Require().NoError(err)

	// There should be 3 rows one for minisql_schema and one for users table
	// plus one for the users table composite primary key
	s.countRowsInTable("minisql_schema", 3)

	schemas := s.scanSchemas()
	s.Require().Len(schemas, 3)
	s.assertSchemaTable(schemas[0])

	s.Equal(minisql.SchemaTable, schemas[1].Type)
	s.Equal("users", schemas[1].Name)
	s.Empty(schemas[1].TableName())
	s.Equal(1, int(schemas[1].RootPage))
	s.Equal(createCompositeUsersTableSQL, schemas[1].SQL())

	s.Equal(minisql.SchemaPrimaryKey, schemas[2].Type)
	s.Equal("pkey__users", schemas[2].Name)
	s.Equal("users", schemas[2].TableName())
	s.Equal(2, int(schemas[2].RootPage))
	s.Empty(schemas[2].SQL())

	// Insert test users
	s.execQuery(`insert into users("first_name", "last_name", "email") values('Lilian', 'Tacey', null),
('Winfield', 'Wolfe', null),
('Ambrosine', 'Deeann', null),
('Tyron', 'Skylynn', null),
('Westley', 'Maud', null),
('Kiefer', 'Shevon', 'kiefer.shevon@example.com'),
('Lianne', 'Neil', null),
('Ciara', 'Dione', null),
('Cody', 'Christabel', null),
('Westley', 'Willis', 'westley.willis@example.com');`, 10)

	s.Run("Inserting duplicate primary key should fail", func() {
		stmt, err := s.db.Prepare(`insert into users("first_name", "last_name") values(?, ?);`)
		s.Require().NoError(err)
		defer stmt.Close()

		aResult, err := stmt.Exec("Winfield", "Wolfe")
		s.Require().Error(err)
		s.ErrorIs(err, minisql.ErrDuplicateKey)
		s.Equal("failed to insert primary key pkey__users: duplicate key", err.Error())
		s.Nil(aResult)
	})

	s.Run("Select with partial composite key", func() {
		users := s.collectCompositeUsers(`select * from users where first_name = 'Westley';`)
		s.Require().Len(users, 2)

		s.Equal("Westley", users[0].FirstName)
		s.Equal("Maud", users[0].LastName)
		s.Equal("Westley", users[1].FirstName)
		s.Equal("Willis", users[1].LastName)
	})

	s.Run("Select with range query", func() {
		users := s.collectCompositeUsers(`select * from users where first_name >= 'Kiefer' and first_name <= 'Tyron';`)
		s.Require().Len(users, 4)

		fmt.Println(users)

		s.Equal("Lilian", users[0].FirstName)
		s.Equal("Tacey", users[0].LastName)
		s.Equal("Tyron", users[1].FirstName)
		s.Equal("Skylynn", users[1].LastName)
		s.Equal("Kiefer", users[2].FirstName)
		s.Equal("Shevon", users[2].LastName)
		s.Equal("Lianne", users[3].FirstName)
		s.Equal("Neil", users[3].LastName)
	})

	s.Run("Delete a user by composite primary key", func() {
		s.execQuery(`delete from users where first_name = 'Ambrosine' and last_name = 'Deeann';`, 1)

		users := s.collectCompositeUsers(`select * from users where first_name = 'Ambrosine' and last_name = 'Deeann';`)
		s.Require().Len(users, 0)

		s.countRowsInTable("users", 9)
	})

	s.Run("Delete all users", func() {
		s.execQuery(`delete from users;`, 9)

		s.countRowsInTable("users", 0)
	})

	s.Run("Drop table all users", func() {
		s.execQuery(`drop table users;`, 0)
	})
}

func (s TestSuite) collectCompositeUsers(query string) []compositeUser {
	rows, err := s.db.QueryContext(context.Background(), query)
	s.Require().NoError(err)
	defer rows.Close()

	var users []compositeUser
	for rows.Next() {
		var aUser compositeUser
		err := rows.Scan(&aUser.FirstName, &aUser.LastName, &aUser.Email, &aUser.DateOfBirth, &aUser.Created)
		s.Require().NoError(err)
		users = append(users, aUser)
	}
	s.Require().NoError(rows.Err())
	return users
}

func (s TestSuite) collectCompositeUser(query string) compositeUser {
	var user compositeUser
	err := s.db.QueryRow(query).Scan(&user.FirstName, &user.LastName, &user.Email, &user.DateOfBirth, &user.Created)
	s.Require().NoError(err)
	return user
}
