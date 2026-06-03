package e2etests

import (
	_ "github.com/RichardKnop/minisql"
	"github.com/RichardKnop/minisql/internal/minisql"
	minisqlErrors "github.com/RichardKnop/minisql/pkg/errors"
)

type schema struct {
	Type     minisql.SchemaType
	Name     string
	TblName  *string
	RootPage int
	sqlText  *string
}

func (s schema) TableName() string {
	if s.TblName == nil {
		return ""
	}
	return *s.TblName
}

// SQL returns the SQL definition stored in this schema row.
func (s schema) SQL() string {
	if s.sqlText == nil {
		return ""
	}
	return *s.sqlText
}

func (s *TestSuite) TestEmptyDatabase() {
	err := s.db.Ping()
	s.Require().NoError(err)

	// There should be only one row for the minisql_schema table
	s.countRowsInTable("minisql_schema", 1)

	var schema schema
	err = s.db.QueryRow(`select * from minisql_schema;`).Scan(&schema.Type, &schema.Name, &schema.TblName, &schema.RootPage, &schema.sqlText)
	s.Require().NoError(err)
	s.assertSchemaTable(schema)
}

func (s *TestSuite) TestCreateTable() {
	s.Run("Create users table", func() {
		result, err := s.db.Exec(createUsersTableSQL)
		s.Require().NoError(err)

		rowsAffected, err := result.RowsAffected()
		s.Require().NoError(err)
		s.Require().Equal(int64(0), rowsAffected)

		// There should be 4 rows one for minisql_schema and one for users table
		// plus one for the users table primary key index and one for the unique index
		s.countRowsInTable("minisql_schema", 4)

		schemas := s.scanSchemas()
		s.Require().Len(schemas, 4)
		s.assertSchemaTable(schemas[0])

		// Check newly created rows for users table and its indexes
		s.assertUsersTable(schemas[1], 1, schemas[2], 2, schemas[3], 3)
	})

	s.Run("Create table fails if table already exists", func() {
		_, err := s.db.Exec(createUsersTableSQL)
		s.Require().Error(err)
		var tblExistsErr minisqlErrors.ErrTableAlreadyExists
		s.Require().ErrorAs(err, &tblExistsErr)
		s.NotEmpty(tblExistsErr.Name)
	})

	s.Run("Create table with IF NOT EXISTS does not fail if table exists", func() {
		result, err := s.db.Exec(createUsersTableIfNotExistsSQL)
		s.Require().NoError(err)

		rowsAffected, err := result.RowsAffected()
		s.Require().NoError(err)
		s.Require().Equal(int64(0), rowsAffected)

		// Nothing should have changed, still the main system table and same 3 rows
		s.countRowsInTable("minisql_schema", 4)

		schemas := s.scanSchemas()
		s.Require().Len(schemas, 4)
		s.assertSchemaTable(schemas[0])
		s.assertUsersTable(schemas[1], 1, schemas[2], 2, schemas[3], 3)
	})

	s.Run("Create index", func() {
		_, err := s.db.Exec(createUsersTimestampIndexSQL)
		s.Require().NoError(err)

		s.countRowsInTable("minisql_schema", 5)

		schemas := s.scanSchemas()
		s.assertSchemaTable(schemas[0])
		s.assertUsersTable(schemas[1], 1, schemas[2], 2, schemas[3], 3)
		s.assertIndex(schemas[4], "idx_created", "users", 4, createUsersTimestampIndexSQL)
	})

	s.Run("Cannot drop system table", func() {
		_, err := s.db.Exec(`drop table "minisql_schema";`)
		s.Require().Error(err)
		s.ErrorContains(err, "cannot write to system table ")
	})

	s.Run("Drop table", func() {
		_, err := s.db.Exec(`drop table "users";`)
		s.Require().NoError(err)

		var count int
		err = s.db.QueryRow(`select count(*) from minisql_schema;`).Scan(&count)
		s.Require().NoError(err)
		s.Equal(1, count)

		schemas := s.scanSchemas()
		s.Require().Len(schemas, 1)
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
			result, err := tx.Exec(tableSQL)
			s.Require().NoError(err)
			rowsAffected, err := result.RowsAffected()
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
		s.countRowsInTable("minisql_schema", 9)

		schemas := s.scanSchemas()
		s.Require().Len(schemas, 9)

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
		var schema schema
		err := rows.Scan(&schema.Type, &schema.Name, &schema.TblName, &schema.RootPage, &schema.sqlText)
		s.Require().NoError(err)
		schemas = append(schemas, schema)
	}
	s.Require().NoError(rows.Err())
	return schemas
}

func (s *TestSuite) assertSchemaTable(schema schema) {
	s.Equal(minisql.SchemaTable, schema.Type)
	s.Equal(minisql.SchemaTableName, schema.Name)
	s.Empty(schema.TableName())
	s.Equal(0, int(schema.RootPage))
	s.Equal(minisql.MainTableSQL, schema.SQL())
}

func (s *TestSuite) assertUsersTable(table schema, idx int, primaryKey schema, pkIdx int, uniqueIndex schema, keyIdx int) {
	s.Equal(minisql.SchemaTable, table.Type)
	s.Equal("users", table.Name)
	s.Empty(table.TableName())
	s.Equal(idx, int(table.RootPage))
	s.Equal(createUsersTableSQL, table.SQL())

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

func (s *TestSuite) assertIndex(indexSchema schema, name, tableName string, idx int, sql string) {
	s.Equal(minisql.SchemaSecondaryIndex, indexSchema.Type)
	s.Equal(name, indexSchema.Name)
	s.Equal(tableName, indexSchema.TableName())
	s.Equal(idx, int(indexSchema.RootPage))
	s.Equal(sql, indexSchema.SQL())
}

func (s *TestSuite) assertProductsTable(table schema, idx int, primaryKey schema, pkIdx int) {
	s.Equal(minisql.SchemaTable, table.Type)
	s.Equal("products", table.Name)
	s.Empty(table.TableName())
	s.Equal(idx, int(table.RootPage))
	s.Equal(createProductsTableSQL, table.SQL())

	s.Equal(minisql.SchemaPrimaryKey, primaryKey.Type)
	s.Equal("pkey__products", primaryKey.Name)
	s.Equal("products", primaryKey.TableName())
	s.Equal(pkIdx, int(primaryKey.RootPage))
	s.Empty(primaryKey.SQL())
}

func (s *TestSuite) assertOrdersTable(table schema, idx int, primaryKey schema, pkIdx int) {
	s.Equal(minisql.SchemaTable, table.Type)
	s.Equal("orders", table.Name)
	s.Empty(table.TableName())
	s.Equal(idx, int(table.RootPage))
	s.Equal(createOrdersTableSQL, table.SQL())

	s.Equal(minisql.SchemaPrimaryKey, primaryKey.Type)
	s.Equal("pkey__orders", primaryKey.Name)
	s.Equal("orders", primaryKey.TableName())
	s.Equal(pkIdx, int(primaryKey.RootPage))
	s.Empty(primaryKey.SQL())
}
