package e2etests

import (
	"errors"

	"github.com/RichardKnop/minisql/internal/minisql"
)

// createParentChildTables sets up a users (parent) + orders (child with FK) schema.
func (s *TestSuite) createParentChildTables() {
	_, err := s.db.Exec(`create table "users" (
		id int8 primary key autoincrement,
		email varchar(255) not null unique,
		name  varchar(100) not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "orders" (
		id      int8 primary key autoincrement,
		user_id int8 not null,
		amount  int8 not null,
		foreign key (user_id) references "users" (id)
	);`)
	s.Require().NoError(err)
}

// ─────────────────────────────────────────────────────────────────────────────
// PRAGMA foreign_keys
// ─────────────────────────────────────────────────────────────────────────────

func (s *TestSuite) TestForeignKey_PragmaDefaultOn() {
	rows, err := s.db.Query(`PRAGMA foreign_keys;`)
	s.Require().NoError(err)
	defer rows.Close()
	s.Require().True(rows.Next())
	var v int32
	s.Require().NoError(rows.Scan(&v))
	s.Equal(int32(1), v, "foreign_keys should be enabled by default")
}

func (s *TestSuite) TestForeignKey_PragmaToggle() {
	_, err := s.db.Exec(`PRAGMA foreign_keys = off;`)
	s.Require().NoError(err)
	rows, err := s.db.Query(`PRAGMA foreign_keys;`)
	s.Require().NoError(err)
	defer rows.Close()
	s.Require().True(rows.Next())
	var v int32
	s.Require().NoError(rows.Scan(&v))
	s.Equal(int32(0), v)
	rows.Close()

	_, err = s.db.Exec(`PRAGMA foreign_keys = on;`)
	s.Require().NoError(err)
}

// ─────────────────────────────────────────────────────────────────────────────
// CREATE TABLE validation
// ─────────────────────────────────────────────────────────────────────────────

func (s *TestSuite) TestForeignKey_CreateTable_ReferencesUnknownTable() {
	_, err := s.db.Exec(`create table "orders" (
		id      int8 primary key autoincrement,
		user_id int8 not null references "no_such_table" (id)
	);`)
	s.Require().Error(err)
	s.Contains(err.Error(), "no_such_table")
}

func (s *TestSuite) TestForeignKey_CreateTable_ReferencesNonIndexedColumn() {
	_, err := s.db.Exec(`create table "users" (
		id   int8 primary key autoincrement,
		name varchar(100) not null
	);`)
	s.Require().NoError(err)

	// "name" is not a PK or unique column, so FK is invalid.
	_, err = s.db.Exec(`create table "orders" (
		id      int8 primary key autoincrement,
		user_name varchar(100) not null references "users" (name)
	);`)
	s.Require().Error(err)
	s.Contains(err.Error(), "must be a primary key or unique index column")
}

func (s *TestSuite) TestForeignKey_CreateTable_ReferencesPKColumn_OK() {
	_, err := s.db.Exec(`create table "users" (
		id   int8 primary key autoincrement,
		name varchar(100) not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "orders" (
		id      int8 primary key autoincrement,
		user_id int8 not null references "users" (id)
	);`)
	s.Require().NoError(err)
}

func (s *TestSuite) TestForeignKey_CreateTable_ReferencesUniqueColumn_OK() {
	_, err := s.db.Exec(`create table "users" (
		id    int8 primary key autoincrement,
		email varchar(255) not null unique
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "orders" (
		id         int8 primary key autoincrement,
		user_email varchar(255) not null references "users" (email)
	);`)
	s.Require().NoError(err)
}

// ─────────────────────────────────────────────────────────────────────────────
// INSERT (child FK check)
// ─────────────────────────────────────────────────────────────────────────────

func (s *TestSuite) TestForeignKey_Insert_ValidFK() {
	s.createParentChildTables()

	_, err := s.db.Exec(`insert into "users" (email, name) values ('alice@example.com', 'Alice')`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "orders" (user_id, amount) values (1, 100)`)
	s.Require().NoError(err)
}

func (s *TestSuite) TestForeignKey_Insert_InvalidFK() {
	s.createParentChildTables()

	_, err := s.db.Exec(`insert into "orders" (user_id, amount) values (99, 100)`)
	s.Require().Error(err)
	var fkErr minisql.ErrForeignKeyViolation
	s.True(errors.As(err, &fkErr), "expected ErrForeignKeyViolation, got %T: %v", err, err)
	s.Equal("orders", fkErr.ChildTable)
	s.Equal("user_id", fkErr.ChildColumn)
	s.Equal("users", fkErr.ParentTable)
	s.Equal("id", fkErr.ParentColumn)
}

func (s *TestSuite) TestForeignKey_Insert_NullFK_Allowed() {
	_, err := s.db.Exec(`create table "users" (
		id   int8 primary key autoincrement,
		name varchar(100) not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "orders" (
		id      int8 primary key autoincrement,
		user_id int8,
		amount  int8 not null,
		foreign key (user_id) references "users" (id)
	);`)
	s.Require().NoError(err)

	// NULL FK is allowed (row without a linked user)
	stmt, err := s.db.Prepare(`insert into "orders" (user_id, amount) values (?, ?)`)
	s.Require().NoError(err)
	defer stmt.Close()

	_, err = stmt.Exec(nil, int64(50))
	s.Require().NoError(err)
}

func (s *TestSuite) TestForeignKey_Insert_DisabledByPragma() {
	s.createParentChildTables()

	_, err := s.db.Exec(`PRAGMA foreign_keys = off;`)
	s.Require().NoError(err)
	defer func() {
		_, _ = s.db.Exec(`PRAGMA foreign_keys = on;`)
	}()

	// Should succeed because FK checks are disabled.
	_, err = s.db.Exec(`insert into "orders" (user_id, amount) values (999, 100)`)
	s.Require().NoError(err)
}

// ─────────────────────────────────────────────────────────────────────────────
// DELETE (parent FK check)
// ─────────────────────────────────────────────────────────────────────────────

func (s *TestSuite) TestForeignKey_Delete_ParentReferencedRow_Blocked() {
	s.createParentChildTables()

	_, err := s.db.Exec(`insert into "users" (email, name) values ('alice@example.com', 'Alice')`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "orders" (user_id, amount) values (1, 100)`)
	s.Require().NoError(err)

	// Deleting the parent row must fail.
	_, err = s.db.Exec(`delete from "users" where id = 1`)
	s.Require().Error(err)
	var fkErr minisql.ErrForeignKeyParentViolation
	s.True(errors.As(err, &fkErr), "expected ErrForeignKeyParentViolation, got %T: %v", err, err)
	s.Equal("users", fkErr.ParentTable)
	s.Equal("id", fkErr.ParentColumn)
	s.Equal("orders", fkErr.ChildTable)
	s.Equal("user_id", fkErr.ChildColumn)
}

func (s *TestSuite) TestForeignKey_Delete_ParentUnreferencedRow_OK() {
	s.createParentChildTables()

	_, err := s.db.Exec(`insert into "users" (email, name) values ('alice@example.com', 'Alice')`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "users" (email, name) values ('bob@example.com', 'Bob')`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "orders" (user_id, amount) values (1, 100)`)
	s.Require().NoError(err)

	// Bob (id=2) has no orders — delete must succeed.
	_, err = s.db.Exec(`delete from "users" where id = 2`)
	s.Require().NoError(err)
}

func (s *TestSuite) TestForeignKey_Delete_ChildFirst_ThenParent_OK() {
	s.createParentChildTables()

	_, err := s.db.Exec(`insert into "users" (email, name) values ('alice@example.com', 'Alice')`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "orders" (user_id, amount) values (1, 100)`)
	s.Require().NoError(err)

	// Delete child row first, then parent.
	_, err = s.db.Exec(`delete from "orders" where id = 1`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`delete from "users" where id = 1`)
	s.Require().NoError(err)
}

// ─────────────────────────────────────────────────────────────────────────────
// UPDATE (child and parent FK checks)
// ─────────────────────────────────────────────────────────────────────────────

func (s *TestSuite) TestForeignKey_Update_ChildFKColumn_ValidTarget() {
	s.createParentChildTables()

	_, err := s.db.Exec(`insert into "users" (email, name) values ('alice@example.com', 'Alice')`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "users" (email, name) values ('bob@example.com', 'Bob')`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "orders" (user_id, amount) values (1, 100)`)
	s.Require().NoError(err)

	// Reassign order to Bob (id=2) — valid.
	_, err = s.db.Exec(`update "orders" set user_id = 2 where id = 1`)
	s.Require().NoError(err)
}

func (s *TestSuite) TestForeignKey_Update_ChildFKColumn_InvalidTarget() {
	s.createParentChildTables()

	_, err := s.db.Exec(`insert into "users" (email, name) values ('alice@example.com', 'Alice')`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "orders" (user_id, amount) values (1, 100)`)
	s.Require().NoError(err)

	// Reassign order to non-existent user — must fail.
	_, err = s.db.Exec(`update "orders" set user_id = 999 where id = 1`)
	s.Require().Error(err)
	var fkErr minisql.ErrForeignKeyViolation
	s.True(errors.As(err, &fkErr), "expected ErrForeignKeyViolation, got %T: %v", err, err)
}

func (s *TestSuite) TestForeignKey_Update_NonFKColumn_Unrestricted() {
	s.createParentChildTables()

	_, err := s.db.Exec(`insert into "users" (email, name) values ('alice@example.com', 'Alice')`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "orders" (user_id, amount) values (1, 100)`)
	s.Require().NoError(err)

	// Updating a non-FK column should not trigger FK checks.
	_, err = s.db.Exec(`update "orders" set amount = 200 where id = 1`)
	s.Require().NoError(err)
}

// ─────────────────────────────────────────────────────────────────────────────
// DROP TABLE (FK guard)
// ─────────────────────────────────────────────────────────────────────────────

func (s *TestSuite) TestForeignKey_DropTable_ReferencedTable_Blocked() {
	s.createParentChildTables()

	// Dropping the parent table while the child FK still references it must fail.
	_, err := s.db.Exec(`drop table "users"`)
	s.Require().Error(err)
	s.True(errors.Is(err, minisql.ErrDropTableReferencedByFK) || s.Contains(err.Error(), "referenced by a foreign key"),
		"expected ErrDropTableReferencedByFK, got: %v", err)
}

func (s *TestSuite) TestForeignKey_DropTable_DropChildFirst_ThenParent_OK() {
	s.createParentChildTables()

	_, err := s.db.Exec(`drop table "orders"`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`drop table "users"`)
	s.Require().NoError(err)
}

// ─────────────────────────────────────────────────────────────────────────────
// DDL round-trip: create → close → reopen → FK still enforced
// ─────────────────────────────────────────────────────────────────────────────

func (s *TestSuite) TestForeignKey_Reopen_StillEnforced() {
	s.createParentChildTables()

	_, err := s.db.Exec(`insert into "users" (email, name) values ('alice@example.com', 'Alice')`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "orders" (user_id, amount) values (1, 100)`)
	s.Require().NoError(err)

	// Close and reopen the database.
	s.Require().NoError(s.db.Close())
	s.db = s.reopenDB()

	// FK should still be enforced after reopen.
	_, err = s.db.Exec(`insert into "orders" (user_id, amount) values (99, 50)`)
	s.Require().Error(err)
	var fkErr minisql.ErrForeignKeyViolation
	s.True(errors.As(err, &fkErr), "expected ErrForeignKeyViolation after reopen, got %T: %v", err, err)
}

// ─────────────────────────────────────────────────────────────────────────────
// Self-referential FK
// ─────────────────────────────────────────────────────────────────────────────

func (s *TestSuite) TestForeignKey_SelfReferential() {
	_, err := s.db.Exec(`create table "categories" (
		id        int8 primary key autoincrement,
		name      varchar(100) not null,
		parent_id int8 references "categories" (id)
	);`)
	s.Require().NoError(err)

	// Root category (no parent).
	_, err = s.db.Exec(`insert into "categories" (name) values ('Root')`)
	s.Require().NoError(err)

	// Child referencing root (id=1).
	_, err = s.db.Exec(`insert into "categories" (name, parent_id) values ('Child', 1)`)
	s.Require().NoError(err)

	// Invalid parent reference.
	_, err = s.db.Exec(`insert into "categories" (name, parent_id) values ('Bad', 99)`)
	s.Require().Error(err)
	var fkErr minisql.ErrForeignKeyViolation
	s.True(errors.As(err, &fkErr))
}

// ─────────────────────────────────────────────────────────────────────────────
// Batch INSERT with mixed valid/invalid rows
// ─────────────────────────────────────────────────────────────────────────────

func (s *TestSuite) TestForeignKey_BatchInsert_OneInvalidRow() {
	s.createParentChildTables()

	_, err := s.db.Exec(`insert into "users" (email, name) values ('alice@example.com', 'Alice')`)
	s.Require().NoError(err)

	// Second row references non-existent user_id=99 — the whole batch should fail.
	_, err = s.db.Exec(`insert into "orders" (user_id, amount) values (1, 10), (99, 20)`)
	s.Require().Error(err)
	var fkErr minisql.ErrForeignKeyViolation
	s.True(errors.As(err, &fkErr))
}
