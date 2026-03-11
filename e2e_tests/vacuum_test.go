package e2etests

import (
	"context"
	"fmt"
)

func (s *TestSuite) TestVacuum_EmptyDatabase() {
	_, err := s.db.ExecContext(context.Background(), `VACUUM`)
	s.Require().NoError(err)

	// Schema table should still be present and queryable
	s.countRowsInTable("minisql_schema", 1)
}

func (s *TestSuite) TestVacuum_DataPreservedAfterVacuum() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)

	users := gen.Users(20)
	for _, u := range users {
		_, err := stmt.Exec(u.Email, u.Name)
		s.Require().NoError(err)
	}

	s.countRowsInTable("users", 20)

	_, err = s.db.ExecContext(context.Background(), `VACUUM`)
	s.Require().NoError(err)

	s.countRowsInTable("users", 20)
}

func (s *TestSuite) TestVacuum_MultipleTablesDataPreserved() {
	for _, sql := range []string{
		createUsersTableSQL,
		createProductsTableSQL,
		createOrdersTableSQL,
	} {
		_, err := s.db.Exec(sql)
		s.Require().NoError(err)
	}

	userStmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)
	users := gen.Users(5)
	for _, u := range users {
		_, err := userStmt.Exec(u.Email, u.Name)
		s.Require().NoError(err)
	}

	for i := 1; i <= 3; i++ {
		_, err := s.db.Exec(
			fmt.Sprintf(`insert into "products" (name, price) values ('Product %d', %d)`, i, i*100),
		)
		s.Require().NoError(err)
	}

	s.countRowsInTable("users", 5)
	s.countRowsInTable("products", 3)

	_, err = s.db.ExecContext(context.Background(), `VACUUM`)
	s.Require().NoError(err)

	s.countRowsInTable("users", 5)
	s.countRowsInTable("products", 3)
}

func (s *TestSuite) TestVacuum_WritableAfterVacuum() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)

	users := gen.Users(5)
	for _, u := range users {
		_, err := stmt.Exec(u.Email, u.Name)
		s.Require().NoError(err)
	}

	_, err = s.db.ExecContext(context.Background(), `VACUUM`)
	s.Require().NoError(err)

	// Prepare a new statement after vacuum since the DB was reopened
	stmt2, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)
	moreUsers := gen.Users(3)
	for _, u := range moreUsers {
		_, err := stmt2.Exec(u.Email, u.Name)
		s.Require().NoError(err)
	}

	s.countRowsInTable("users", 8)
}

func (s *TestSuite) TestVacuum_WithSecondaryIndex() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)
	_, err = s.db.Exec(createUsersTimestampIndexSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)

	users := gen.Users(10)
	for _, u := range users {
		_, err := stmt.Exec(u.Email, u.Name)
		s.Require().NoError(err)
	}

	s.countRowsInTable("users", 10)

	_, err = s.db.ExecContext(context.Background(), `VACUUM`)
	s.Require().NoError(err)

	s.countRowsInTable("users", 10)

	// Secondary index must still be usable for writes after vacuum
	stmt2, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)
	moreUsers := gen.Users(2)
	for _, u := range moreUsers {
		_, err := stmt2.Exec(u.Email, u.Name)
		s.Require().NoError(err)
	}
	s.countRowsInTable("users", 12)
}

func (s *TestSuite) TestVacuum_SchemaPreservedAfterVacuum() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)
	_, err = s.db.Exec(createUsersTimestampIndexSQL)
	s.Require().NoError(err)

	schemasBefore := s.scanSchemas()

	_, err = s.db.ExecContext(context.Background(), `VACUUM`)
	s.Require().NoError(err)

	schemasAfter := s.scanSchemas()
	s.Require().Equal(len(schemasBefore), len(schemasAfter), "schema row count should not change")

	for i, before := range schemasBefore {
		after := schemasAfter[i]
		s.Equal(before.Type, after.Type, "schema[%d].Type mismatch", i)
		s.Equal(before.Name, after.Name, "schema[%d].Name mismatch", i)
		s.Equal(before.TableName(), after.TableName(), "schema[%d].TableName mismatch", i)
		s.Equal(before.SQL(), after.SQL(), "schema[%d].SQL mismatch", i)
	}
}

func (s *TestSuite) TestVacuum_RepeatedVacuums() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)

	users := gen.Users(5)
	for _, u := range users {
		_, err := stmt.Exec(u.Email, u.Name)
		s.Require().NoError(err)
	}

	for range 3 {
		_, err = s.db.ExecContext(context.Background(), `VACUUM`)
		s.Require().NoError(err)
	}

	s.countRowsInTable("users", 5)
}
