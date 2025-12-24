package e2etests

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/RichardKnop/minisql"
	"github.com/RichardKnop/minisql/internal/minisql"
	"github.com/stretchr/testify/suite"
)

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
	tempFile, err := os.CreateTemp("", "testdb")
	s.Require().NoError(err)
	s.dbFile = tempFile

	db, err := sql.Open("minisql", s.dbFile.Name())
	s.Require().NoError(err)
	s.db = db
}

func (s *TestSuite) TearDownTest() {
	err := s.db.Close()
	s.Require().NoError(err)
	err = os.Remove(s.dbFile.Name())
	s.Require().NoError(err)
}

type table struct {
	SchemaType minisql.SchemaType
	Name       string
	RootPage   int
	SQL        *string
}

func (s *TestSuite) TestEmptyDatabase() {
	err := s.db.Ping()
	s.Require().NoError(err)

	// There should be only one row for the minisql_schema table
	var count int
	err = s.db.QueryRow(`select count(*) from minisql_schema;`).Scan(&count)
	s.Require().NoError(err)
	s.Equal(1, count)

	var aTable table
	err = s.db.QueryRow(`select * from minisql_schema;`).Scan(&aTable.SchemaType, &aTable.Name, &aTable.RootPage, &aTable.SQL)
	s.Require().NoError(err)
	s.assertSchemaTable(aTable)
}

var createUsersTableSQL = `create table "users" (
	id int8 primary key autoincrement,
	name varchar(255),
	email text,
	created timestamp default now()
);`

func (s *TestSuite) TestCreateTable() {
	s.Run("create users table", func() {
		aResult, err := s.db.Exec(createUsersTableSQL)
		s.Require().NoError(err)

		rowsAffected, err := aResult.RowsAffected()
		s.Require().NoError(err)
		s.Require().Equal(int64(0), rowsAffected)

		// There should be 3 rows one for minisql_schema and one for users table
		// plus one for the users table primary key index
		var count int
		err = s.db.QueryRow(`select count(*) from minisql_schema;`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(3, count)

		tables := s.scanSchemaTables()
		s.Require().Equal(3, len(tables))
		s.assertSchemaTable(tables[0])

		// Check newly created rows for users table and its index
		s.assertUsersTable(tables[1], tables[2])
	})

	s.Run("create table fails if table already exists", func() {
		_, err := s.db.Exec(createUsersTableSQL)
		s.Require().Error(err)
		s.Equal("table already exists", err.Error())
	})

	s.Run("create table with IF EXISTS does not fail if table exists", func() {
		aResult, err := s.db.Exec(`create table if not exists "users" (
			id int8 primary key autoincrement,
			name varchar(255),
			email text,
			created timestamp default now()
		);`)
		s.Require().NoError(err)

		rowsAffected, err := aResult.RowsAffected()
		s.Require().NoError(err)
		s.Require().Equal(int64(0), rowsAffected)

		// Nothing should have changed, still the same 3 rows
		var count int
		err = s.db.QueryRow(`select count(*) from minisql_schema;`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(3, count)

		tables := s.scanSchemaTables()
		s.Require().Equal(3, len(tables))
		s.assertSchemaTable(tables[0])
		s.assertUsersTable(tables[1], tables[2])
	})

	s.Run("drop table", func() {
		_, err := s.db.Exec(`drop table "users";`)
		s.Require().NoError(err)

		var count int
		err = s.db.QueryRow(`select count(*) from minisql_schema;`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(1, count)

		tables := s.scanSchemaTables()
		s.Require().Equal(1, len(tables))
		s.assertSchemaTable(tables[0])
	})
}

func (s *TestSuite) scanSchemaTables() []table {
	var tables []table
	rows, err := s.db.Query(`select * from minisql_schema;`)
	s.Require().NoError(err)
	for rows.Next() {
		var aTable table
		err := rows.Scan(&aTable.SchemaType, &aTable.Name, &aTable.RootPage, &aTable.SQL)
		s.Require().NoError(err)
		tables = append(tables, aTable)
	}
	s.Require().NoError(rows.Err())
	return tables
}

func (s *TestSuite) assertUsersTable(table, pk table) {
	s.Equal(minisql.SchemaTable, table.SchemaType)
	s.Equal("users", table.Name)
	s.Equal(1, int(table.RootPage))
	s.Equal(createUsersTableSQL, *table.SQL)

	s.Equal(minisql.SchemaPrimaryKey, pk.SchemaType)
	s.Equal("pkey__users", pk.Name)
	s.Equal(2, int(pk.RootPage))
	s.Nil(pk.SQL)
}

func (s *TestSuite) assertSchemaTable(aTable table) {
	s.Equal(minisql.SchemaTable, aTable.SchemaType)
	s.Equal(minisql.SchemaTableName, aTable.Name)
	s.Equal(0, int(aTable.RootPage))
	s.Equal(`create table "minisql_schema" (
	type int4 not null,
	name varchar(255) not null,
	root_page int4,
	sql text
);`, *aTable.SQL)
}
