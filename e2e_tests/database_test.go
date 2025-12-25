package e2etests

import (
	"database/sql"
	"fmt"
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

func (s *TestSuite) TestCreateTable() {
	s.Run("Create users table", func() {
		aResult, err := s.db.Exec(createUsersTableSQL)
		s.Require().NoError(err)

		rowsAffected, err := aResult.RowsAffected()
		s.Require().NoError(err)
		s.Require().Equal(int64(0), rowsAffected)

		// There should be 4 rows one for minisql_schema and one for users table
		// plus one for the users table primary key index and one for the unique index
		var count int
		err = s.db.QueryRow(`select count(*) from minisql_schema;`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(4, count)

		schemas := s.scanSchemas()
		s.Require().Equal(4, len(schemas))
		s.assertSchemaTable(schemas[0])

		// Check newly created rows for users table and its indexes
		s.assertUsersTable(schemas[1], 1, schemas[2], 2, schemas[3], 3)
	})

	s.Run("Create table fails if table already exists", func() {
		_, err := s.db.Exec(createUsersTableSQL)
		s.Require().Error(err)
		s.Equal("table already exists", err.Error())
	})

	s.Run("Create table with IF NOT EXISTS does not fail if table exists", func() {
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
		s.assertUsersTable(schemas[1], 1, schemas[2], 2, schemas[3], 3)
	})

	s.Run("Drop table", func() {
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

	s.Run("Create multi table schema", func() {
		tx, err := s.db.Begin()
		s.Require().NoError(err)

		for _, tableSQL := range []string{createUsersTableSQL, createProductsTableSQL, createOrdersTableSQL} {
			aResult, err := tx.Exec(tableSQL)
			s.Require().NoError(err)
			rowsAffected, err := aResult.RowsAffected()
			s.Require().NoError(err)
			s.Require().Equal(int64(0), rowsAffected)
		}

		err = tx.Commit()
		s.Require().NoError(err)

		// There should be 8 rows now:
		// - 1 for minisql_schema and one for users table
		// - 3 for the users table, its primary key index and unique index
		// - 2 for products table and its primary key index
		// - 2 for orders table and its primary key index
		var count int
		err = s.db.QueryRow(`select count(*) from minisql_schema;`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(8, count)

		schemas := s.scanSchemas()
		s.Require().Equal(8, len(schemas))
		fmt.Println(schemas)
		s.assertSchemaTable(schemas[0])
		// Page numbers will be reversed because we dropped the table previously and
		// pages were freed up and now reused back from linked list of free pages.
		s.assertUsersTable(schemas[1], 3, schemas[2], 2, schemas[3], 1)
		// Products and orders tables should be created as well
		s.assertProductsTable(schemas[4], 4, schemas[5], 5)
		s.assertOrdersTable(schemas[6], 6, schemas[7], 7)
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

func (s *TestSuite) assertUsersTable(table schema, idx int, primaryKey schema, pkIdx int, uniqueIndex schema, keyIdx int) {
	s.Equal(minisql.SchemaTable, table.Type)
	s.Equal("users", table.Name)
	s.Equal(idx, int(table.RootPage))
	s.Equal(createUsersTableSQL, *table.SQL)

	s.Equal(minisql.SchemaPrimaryKey, primaryKey.Type)
	s.Equal("pkey__users", primaryKey.Name)
	s.Equal(pkIdx, int(primaryKey.RootPage))
	s.Nil(primaryKey.SQL)

	s.Equal(minisql.SchemaUniqueIndex, uniqueIndex.Type)
	s.Equal("key__users__email", uniqueIndex.Name)
	s.Equal(keyIdx, int(uniqueIndex.RootPage))
	s.Nil(uniqueIndex.SQL)
}

func (s *TestSuite) assertProductsTable(table schema, idx int, primaryKey schema, pkIdx int) {
	s.Equal(minisql.SchemaTable, table.Type)
	s.Equal("products", table.Name)
	s.Equal(idx, int(table.RootPage))
	s.Equal(createProductsTableSQL, *table.SQL)

	s.Equal(minisql.SchemaPrimaryKey, primaryKey.Type)
	s.Equal("pkey__products", primaryKey.Name)
	s.Equal(pkIdx, int(primaryKey.RootPage))
	s.Nil(primaryKey.SQL)
}

func (s *TestSuite) assertOrdersTable(table schema, idx int, primaryKey schema, pkIdx int) {
	s.Equal(minisql.SchemaTable, table.Type)
	s.Equal("orders", table.Name)
	s.Equal(idx, int(table.RootPage))
	s.Equal(createOrdersTableSQL, *table.SQL)

	s.Equal(minisql.SchemaPrimaryKey, primaryKey.Type)
	s.Equal("pkey__orders", primaryKey.Name)
	s.Equal(pkIdx, int(primaryKey.RootPage))
	s.Nil(primaryKey.SQL)
}

func (s *TestSuite) assertSchemaTable(aTable schema) {
	s.Equal(minisql.SchemaTable, aTable.Type)
	s.Equal(minisql.SchemaTableName, aTable.Name)
	s.Equal(0, int(aTable.RootPage))
	s.Equal(minisql.MainTableSQL, *aTable.SQL)
}
