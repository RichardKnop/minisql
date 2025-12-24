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

type schema struct {
	Type     minisql.SchemaType
	Name     string
	RootPage int
	SQL      *string
}

func (s *TestSuite) TestEmptyDatabase() {
	err := s.db.Ping()
	s.Require().NoError(err)

	// There should be only one row for the minisql_schema table
	var count int
	err = s.db.QueryRow(`select count(*) from minisql_schema;`).Scan(&count)
	s.Require().NoError(err)
	s.Equal(1, count)

	var aSchema schema
	err = s.db.QueryRow(`select * from minisql_schema;`).Scan(&aSchema.Type, &aSchema.Name, &aSchema.RootPage, &aSchema.SQL)
	s.Require().NoError(err)
	s.assertSchemaTable(aSchema)
}

var createUsersTableSQL = `create table "users" (
	id int8 primary key autoincrement,
	email varchar(255) unique,
	name text,
	created timestamp default now()
);`

var createUsersTableIfNotExistsSQL = `create table if not exists "users" (
	id int8 primary key autoincrement,
	email varchar(255) unique,
	name text,
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
		s.Equal(4, count)

		schemas := s.scanSchemas()
		s.Require().Equal(4, len(schemas))
		s.assertSchemaTable(schemas[0])

		// Check newly created rows for users table and its indexes
		s.assertUsersTable(schemas[1], schemas[2], schemas[3])
	})

	s.Run("create table fails if table already exists", func() {
		_, err := s.db.Exec(createUsersTableSQL)
		s.Require().Error(err)
		s.Equal("table already exists", err.Error())
	})

	s.Run("create table with IF NOT EXISTS does not fail if table exists", func() {
		aResult, err := s.db.Exec(createUsersTableIfNotExistsSQL)
		s.Require().NoError(err)

		rowsAffected, err := aResult.RowsAffected()
		s.Require().NoError(err)
		s.Require().Equal(int64(0), rowsAffected)

		// Nothing should have changed, still the same 3 rows
		var count int
		err = s.db.QueryRow(`select count(*) from minisql_schema;`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(4, count)

		schemas := s.scanSchemas()
		s.Require().Equal(4, len(schemas))
		s.assertSchemaTable(schemas[0])
		s.assertUsersTable(schemas[1], schemas[2], schemas[3])
	})

	s.Run("drop table", func() {
		_, err := s.db.Exec(`drop table "users";`)
		s.Require().NoError(err)

		var count int
		err = s.db.QueryRow(`select count(*) from minisql_schema;`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(1, count)

		schemas := s.scanSchemas()
		s.Require().Equal(1, len(schemas))
		s.assertSchemaTable(schemas[0])
	})
}

func (s *TestSuite) scanSchemas() []schema {
	var schemas []schema
	rows, err := s.db.Query(`select * from minisql_schema;`)
	s.Require().NoError(err)
	for rows.Next() {
		var aSchema schema
		err := rows.Scan(&aSchema.Type, &aSchema.Name, &aSchema.RootPage, &aSchema.SQL)
		s.Require().NoError(err)
		schemas = append(schemas, aSchema)
	}
	s.Require().NoError(rows.Err())
	return schemas
}

func (s *TestSuite) assertUsersTable(table, primaryKey, uniqueIndex schema) {
	s.Equal(minisql.SchemaTable, table.Type)
	s.Equal("users", table.Name)
	s.Equal(1, int(table.RootPage))
	s.Equal(createUsersTableSQL, *table.SQL)

	s.Equal(minisql.SchemaPrimaryKey, primaryKey.Type)
	s.Equal("pkey__users", primaryKey.Name)
	s.Equal(2, int(primaryKey.RootPage))
	s.Nil(primaryKey.SQL)

	s.Equal(minisql.SchemaUniqueIndex, uniqueIndex.Type)
	s.Equal("key__users__email", uniqueIndex.Name)
	s.Equal(3, int(uniqueIndex.RootPage))
	s.Nil(uniqueIndex.SQL)
}

func (s *TestSuite) assertSchemaTable(aTable schema) {
	s.Equal(minisql.SchemaTable, aTable.Type)
	s.Equal(minisql.SchemaTableName, aTable.Name)
	s.Equal(0, int(aTable.RootPage))
	s.Equal(minisql.MainTableSQL, *aTable.SQL)
}
