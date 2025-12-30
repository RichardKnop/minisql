package e2etests

import (
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/suite"
)

var createUsersTableSQL = `create table "users" (
	id int8 primary key autoincrement,
	email varchar(255) unique,
	name text,
	created timestamp default now()
);`

var createUsersTimestampIndexSQL = `create index "idx_created" on "users" (
	created
);`

var dropUsersTimestampIndexSQL = `drop index "idx_created";`

var createUsersTableIfNotExistsSQL = `create table if not exists "users" (
	id int8 primary key autoincrement,
	email varchar(255) unique,
	name text,
	created timestamp default now()
);`

var createProductsTableSQL = `create table "products" (
	product_id int8 primary key autoincrement,
	name text not null,
	description text,
	price int4 not null,
	created timestamp default now()
);`

var createOrdersTableSQL = `create table "orders" (
	order_id int8 primary key autoincrement,
	user_id int8 not null,
	product_id int4 not null,
	total_paid int4 not null,
	created timestamp default now()
);`

type dataGen struct {
	*gofakeit.Faker
}

func newDataGen(seed uint64) *dataGen {
	g := dataGen{
		Faker: gofakeit.New(seed),
	}

	return &g
}

func (g *dataGen) Users(number int) []user {
	var (
		emailMap = map[string]struct{}{}
		users    = make([]user, 0, number)
	)
	for range number {
		aUser := g.User()

		// Ensure unique email
		_, ok := emailMap[aUser.Email.String]
		for ok {
			aUser = g.User()
			_, ok = emailMap[aUser.Email.String]
		}

		users = append(users, aUser)
		emailMap[aUser.Email.String] = struct{}{}
	}
	return users
}

func (g *dataGen) User() user {
	return user{
		Email: sql.NullString{String: g.Email(), Valid: true},
		Name:  sql.NullString{String: g.Name(), Valid: true},
	}
}

var gen = newDataGen(uint64(time.Now().Unix()))

type TestSuite struct {
	suite.Suite
	dbFile *os.File
	db     *sql.DB
}

func TestTestSuite(t *testing.T) {
	suite.Run(t, new(TestSuite))
}

func (s *TestSuite) SetupSuite() {
}

func (s *TestSuite) TearDownSuite() {
}

func (s *TestSuite) SetupTest() {
	tempFile, err := os.CreateTemp("", "")
	s.Require().NoError(err)
	s.dbFile = tempFile

	db, err := sql.Open("minisql", s.dbFile.Name())
	s.Require().NoError(err)
	s.db = db
}

func (s *TestSuite) TearDownTest() {
	s.Require().NoError(s.db.Close())
	err := os.Remove(s.dbFile.Name())
	s.Require().NoError(err)
}
