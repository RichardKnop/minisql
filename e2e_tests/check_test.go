package e2etests

import (
	"database/sql"
	"errors"

	"github.com/RichardKnop/minisql/internal/minisql"
)

func (s *TestSuite) TestCheckConstraints() {
	// Create one table for the whole test; subtests share it.
	_, err := s.db.Exec(`create table "products" (
		id    int8 primary key autoincrement,
		name  varchar(100) not null,
		price int8 not null check (price > 0),
		qty   int8 not null check (qty >= 0)
	);`)
	s.Require().NoError(err)

	s.Run("valid_insert_passes", func() {
		_, err := s.db.Exec(`insert into "products" (name, price, qty) values ('Widget', 10, 5)`)
		s.Require().NoError(err)
	})

	// Use a prepared statement so that negative int64 values can be passed
	// without relying on negative literal parsing (not supported by the parser).
	s.Run("insert_zero_price_violates_check", func() {
		stmt, err := s.db.Prepare(`insert into "products" (name, price, qty) values (?, ?, ?)`)
		s.Require().NoError(err)
		defer stmt.Close()

		_, err = stmt.Exec("Bad", int64(0), int64(5))
		s.Require().Error(err)
		var checkErr minisql.ErrCheckConstraintViolation
		s.True(errors.As(err, &checkErr), "expected ErrCheckConstraintViolation, got %T: %v", err, err)
		s.Equal("price", checkErr.ColumnName)
	})

	s.Run("insert_negative_qty_violates_check", func() {
		stmt, err := s.db.Prepare(`insert into "products" (name, price, qty) values (?, ?, ?)`)
		s.Require().NoError(err)
		defer stmt.Close()

		_, err = stmt.Exec("Bad", int64(5), int64(-1))
		s.Require().Error(err)
		var checkErr minisql.ErrCheckConstraintViolation
		s.True(errors.As(err, &checkErr), "expected ErrCheckConstraintViolation, got %T: %v", err, err)
		s.Equal("qty", checkErr.ColumnName)
	})

	s.Run("update_valid_change_passes", func() {
		_, err := s.db.Exec(`insert into "products" (name, price, qty) values ('Gadget', 15, 3)`)
		s.Require().NoError(err)

		_, err = s.db.Exec(`update "products" set price = 25 where name = 'Gadget'`)
		s.Require().NoError(err)
	})

	s.Run("update_violates_check_constraint", func() {
		_, err := s.db.Exec(`insert into "products" (name, price, qty) values ('Doomed', 10, 2)`)
		s.Require().NoError(err)

		stmt, err := s.db.Prepare(`update "products" set price = ? where name = 'Doomed'`)
		s.Require().NoError(err)
		defer stmt.Close()

		_, err = stmt.Exec(int64(0))
		s.Require().Error(err)
		var checkErr minisql.ErrCheckConstraintViolation
		s.True(errors.As(err, &checkErr), "expected ErrCheckConstraintViolation, got %T: %v", err, err)
		s.Equal("price", checkErr.ColumnName)
	})
}

func (s *TestSuite) TestCheckConstraints_DDLRoundTrip() {
	// Use CHECK (score > 10) so that inserting 5 (a valid positive literal) triggers the violation.
	_, err := s.db.Exec(`create table "scores" (
		id    int8 primary key autoincrement,
		score int8 not null check (score > 10)
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "scores" (score) values (50)`)
	s.Require().NoError(err)

	// Close and reopen: DDL must round-trip through the WAL / schema table.
	s.Require().NoError(s.db.Close())
	s.db, err = sql.Open("minisql", s.dbFile.Name())
	s.Require().NoError(err)
	s.db.SetMaxOpenConns(1)
	s.db.SetMaxIdleConns(1)

	// Valid insert still works after reopen.
	_, err = s.db.Exec(`insert into "scores" (score) values (99)`)
	s.Require().NoError(err)

	// Violating insert still fails after reopen.
	_, err = s.db.Exec(`insert into "scores" (score) values (5)`)
	s.Require().Error(err)
	var checkErr minisql.ErrCheckConstraintViolation
	s.True(errors.As(err, &checkErr), "expected ErrCheckConstraintViolation after reopen, got %T: %v", err, err)
	s.Equal("score", checkErr.ColumnName)
}

func (s *TestSuite) TestCheckConstraints_VarcharColumn() {
	_, err := s.db.Exec(`create table "items" (
		id   int8 primary key autoincrement,
		code varchar(10) not null check (code != '')
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "items" (code) values ('ABC')`)
	s.Require().NoError(err)

	// Empty string violates check (code != '').
	_, err = s.db.Exec(`insert into "items" (code) values ('')`)
	s.Require().Error(err)
	var checkErr minisql.ErrCheckConstraintViolation
	s.True(errors.As(err, &checkErr), "expected ErrCheckConstraintViolation, got %T: %v", err, err)
	s.Equal("code", checkErr.ColumnName)
}

func (s *TestSuite) TestCheckConstraints_DefaultAndCheck() {
	// Ensure DEFAULT and CHECK can coexist on the same column.
	_, err := s.db.Exec(`create table "counters" (
		id  int8 primary key autoincrement,
		cnt int8 not null default 1 check (cnt > 0)
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "counters" (cnt) values (5)`)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "counters" (cnt) values (?)`)
	s.Require().NoError(err)
	defer stmt.Close()

	_, err = stmt.Exec(int64(0))
	s.Require().Error(err)
	var checkErr minisql.ErrCheckConstraintViolation
	s.True(errors.As(err, &checkErr), "expected ErrCheckConstraintViolation, got %T: %v", err, err)
	s.Equal("cnt", checkErr.ColumnName)
}
