package e2etests

import (
	_ "github.com/RichardKnop/minisql"
	"github.com/RichardKnop/minisql/internal/minisql"
)

type schema struct {
	Type     minisql.SchemaType
	Name     string
	TblName  *string
	RootPage int
	Sql      *string
}

func (s schema) TableName() string {
	if s.TblName == nil {
		return ""
	}
	return *s.TblName
}

func (s schema) SQL() string {
	if s.Sql == nil {
		return ""
	}
	return *s.Sql
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
	err = s.db.QueryRow(`select * from minisql_schema;`).Scan(&aSchema.Type, &aSchema.Name, &aSchema.TblName, &aSchema.RootPage, &aSchema.Sql)
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

	s.Run("Create index", func() {
		_, err := s.db.Exec(createUsersTimestampIndexSQL)
		s.Require().NoError(err)

		var count int
		err = s.db.QueryRow(`select count(*) from minisql_schema;`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(5, count)

		schemas := s.scanSchemas()
		s.assertSchemaTable(schemas[0])
		s.assertUsersTable(schemas[1], 1, schemas[2], 2, schemas[3], 3)
		s.assertIndex(schemas[4], "idx_created", "users", 4, createUsersTimestampIndexSQL)
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

		for _, tableSQL := range []string{
			createUsersTableSQL,
			createUsersTimestampIndexSQL,
			createProductsTableSQL,
			createOrdersTableSQL,
		} {
			aResult, err := tx.Exec(tableSQL)
			s.Require().NoError(err)
			rowsAffected, err := aResult.RowsAffected()
			s.Require().NoError(err)
			s.Require().Equal(int64(0), rowsAffected)
		}

		err = tx.Commit()
		s.Require().NoError(err)

		// There should be 9 rows now:
		// - 1 for minisql_schema and one for users table
		// - 3 for the users table, its primary key index and unique index
		// - 1 for secondary inex on users table
		// - 2 for products table and its primary key index
		// - 2 for orders table and its primary key index
		var count int
		err = s.db.QueryRow(`select count(*) from minisql_schema;`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(9, count)

		schemas := s.scanSchemas()
		s.Require().Equal(9, len(schemas))

		// System table should be unchanged
		s.assertSchemaTable(schemas[0])

		// Page numbers are coming from the free pages linked list from the database header,
		// remember we freed pages 1-4 when we dropped the users and then its index previously.
		s.assertUsersTable(schemas[1], 4, schemas[2], 3, schemas[3], 2)
		s.assertIndex(schemas[4], "idx_created", "users", 1, createUsersTimestampIndexSQL)

		// Products and orders tables should be created as well
		s.assertProductsTable(schemas[5], 5, schemas[6], 6)
		s.assertOrdersTable(schemas[7], 7, schemas[8], 8)
	})
}

func (s *TestSuite) scanSchemas() []schema {
	var schemas []schema
	rows, err := s.db.Query(`select * from minisql_schema;`)
	s.Require().NoError(err)
	for rows.Next() {
		var aSchema schema
		err := rows.Scan(&aSchema.Type, &aSchema.Name, &aSchema.TblName, &aSchema.RootPage, &aSchema.Sql)
		s.Require().NoError(err)
		schemas = append(schemas, aSchema)
	}
	s.Require().NoError(rows.Err())
	return schemas
}

func (s *TestSuite) assertSchemaTable(aSchema schema) {
	s.Equal(minisql.SchemaTable, aSchema.Type)
	s.Equal(minisql.SchemaTableName, aSchema.Name)
	s.Empty(aSchema.TableName())
	s.Equal(0, int(aSchema.RootPage))
	s.Equal(minisql.MainTableSQL, aSchema.SQL())
}

func (s *TestSuite) assertUsersTable(aTable schema, idx int, primaryKey schema, pkIdx int, uniqueIndex schema, keyIdx int) {
	s.Equal(minisql.SchemaTable, aTable.Type)
	s.Equal("users", aTable.Name)
	s.Empty(aTable.TableName())
	s.Equal(idx, int(aTable.RootPage))
	s.Equal(createUsersTableSQL, aTable.SQL())

	s.Equal(minisql.SchemaPrimaryKey, primaryKey.Type)
	s.Equal("pkey__users", primaryKey.Name)
	s.Equal("users", primaryKey.TableName())
	s.Equal(pkIdx, int(primaryKey.RootPage))
	s.Empty(uniqueIndex.SQL())

	s.Equal(minisql.SchemaUniqueIndex, uniqueIndex.Type)
	s.Equal("key__users__email", uniqueIndex.Name)
	s.Equal("users", uniqueIndex.TableName())
	s.Equal(keyIdx, int(uniqueIndex.RootPage))
	s.Empty(uniqueIndex.SQL())
}

func (s *TestSuite) assertIndex(aIndex schema, name, tableName string, idx int, sql string) {
	s.Equal(minisql.SchemaSecondaryIndex, aIndex.Type)
	s.Equal(name, aIndex.Name)
	s.Equal(tableName, aIndex.TableName())
	s.Equal(idx, int(aIndex.RootPage))
	s.Equal(sql, aIndex.SQL())
}

func (s *TestSuite) assertProductsTable(aTable schema, idx int, primaryKey schema, pkIdx int) {
	s.Equal(minisql.SchemaTable, aTable.Type)
	s.Equal("products", aTable.Name)
	s.Empty(aTable.TableName())
	s.Equal(idx, int(aTable.RootPage))
	s.Equal(createProductsTableSQL, aTable.SQL())

	s.Equal(minisql.SchemaPrimaryKey, primaryKey.Type)
	s.Equal("pkey__products", primaryKey.Name)
	s.Equal("products", primaryKey.TableName())
	s.Equal(pkIdx, int(primaryKey.RootPage))
	s.Empty(primaryKey.SQL())
}

func (s *TestSuite) assertOrdersTable(aTable schema, idx int, primaryKey schema, pkIdx int) {
	s.Equal(minisql.SchemaTable, aTable.Type)
	s.Equal("orders", aTable.Name)
	s.Empty(aTable.TableName())
	s.Equal(idx, int(aTable.RootPage))
	s.Equal(createOrdersTableSQL, aTable.SQL())

	s.Equal(minisql.SchemaPrimaryKey, primaryKey.Type)
	s.Equal("pkey__orders", primaryKey.Name)
	s.Equal("orders", primaryKey.TableName())
	s.Equal(pkIdx, int(primaryKey.RootPage))
	s.Empty(primaryKey.SQL())
}
