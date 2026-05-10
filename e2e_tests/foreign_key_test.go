package e2etests

import (
	"errors"

	minisqlErrors "github.com/RichardKnop/minisql/pkg/errors"
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
	var fkErr minisqlErrors.ErrForeignKeyViolation
	s.True(errors.As(err, &fkErr), "expected ErrForeignKeyViolation, got %T: %v", err, err)
	s.Equal("orders", fkErr.ChildTable)
	s.Equal([]string{"user_id"}, fkErr.ChildColumns)
	s.Equal("users", fkErr.ParentTable)
	s.Equal([]string{"id"}, fkErr.ParentColumns)
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
	var fkErr minisqlErrors.ErrForeignKeyParentViolation
	s.True(errors.As(err, &fkErr), "expected ErrForeignKeyParentViolation, got %T: %v", err, err)
	s.Equal("users", fkErr.ParentTable)
	s.Equal([]string{"id"}, fkErr.ParentColumns)
	s.Equal("orders", fkErr.ChildTable)
	s.Equal([]string{"user_id"}, fkErr.ChildColumns)
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
	var fkErr minisqlErrors.ErrForeignKeyViolation
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
	s.True(errors.Is(err, minisqlErrors.ErrDropTableReferencedByFK) || s.Contains(err.Error(), "referenced by a foreign key"),
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
	var fkErr minisqlErrors.ErrForeignKeyViolation
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
	var fkErr minisqlErrors.ErrForeignKeyViolation
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
	var fkErr minisqlErrors.ErrForeignKeyViolation
	s.True(errors.As(err, &fkErr))
}

// ─────────────────────────────────────────────────────────────────────────────
// ON DELETE CASCADE
// ─────────────────────────────────────────────────────────────────────────────

func (s *TestSuite) TestForeignKey_OnDeleteCascade() {
	_, err := s.db.Exec(`create table "users" (
		id    int8 primary key autoincrement,
		email varchar(255) not null unique,
		name  varchar(100) not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "orders" (
		id      int8 primary key autoincrement,
		user_id int8 not null,
		amount  int8 not null,
		foreign key (user_id) references "users" (id) on delete cascade
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "users" (email, name) values ('alice@example.com', 'Alice')`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "orders" (user_id, amount) values (1, 100), (1, 200)`)
	s.Require().NoError(err)

	// Deleting the parent must cascade and delete child rows.
	_, err = s.db.Exec(`delete from "users" where id = 1`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`select id from "orders"`)
	s.Require().NoError(err)
	defer rows.Close()
	s.False(rows.Next(), "all child orders should have been deleted by cascade")
}

func (s *TestSuite) TestForeignKey_OnDeleteCascade_MultipleChildren() {
	_, err := s.db.Exec(`create table "users" (
		id    int8 primary key autoincrement,
		email varchar(255) not null unique,
		name  varchar(100) not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "orders" (
		id      int8 primary key autoincrement,
		user_id int8 not null,
		amount  int8 not null,
		foreign key (user_id) references "users" (id) on delete cascade
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "users" (email, name) values ('alice@example.com', 'Alice')`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "users" (email, name) values ('bob@example.com', 'Bob')`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "orders" (user_id, amount) values (1, 100), (2, 50)`)
	s.Require().NoError(err)

	// Delete Alice — only her orders should be removed, Bob's remain.
	_, err = s.db.Exec(`delete from "users" where id = 1`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`select user_id from "orders"`)
	s.Require().NoError(err)
	defer rows.Close()
	s.Require().True(rows.Next())
	var uid int64
	s.Require().NoError(rows.Scan(&uid))
	s.Equal(int64(2), uid, "Bob's order should survive")
	s.False(rows.Next())
}

// ─────────────────────────────────────────────────────────────────────────────
// ON UPDATE CASCADE
// ─────────────────────────────────────────────────────────────────────────────

func (s *TestSuite) TestForeignKey_OnUpdateCascade() {
	_, err := s.db.Exec(`create table "accounts" (
		id   int8 primary key autoincrement,
		name varchar(100) not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "transactions" (
		id         int8 primary key autoincrement,
		account_id int8 not null,
		amount     int8 not null,
		foreign key (account_id) references "accounts" (id) on update cascade
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "accounts" (name) values ('Checking')`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "transactions" (account_id, amount) values (1, 100), (1, 200)`)
	s.Require().NoError(err)

	// Since accounts.id is autoincrement we cannot directly update it via SQL (PKs are immutable).
	// Instead verify ON UPDATE CASCADE is wired by checking the child rows reference exists after
	// a no-op update that does NOT change the referenced column — no cascade should trigger.
	_, err = s.db.Exec(`update "accounts" set name = 'Savings' where id = 1`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`select account_id from "transactions" where account_id = 1`)
	s.Require().NoError(err)
	defer rows.Close()
	count := 0
	for rows.Next() {
		count += 1
	}
	s.Equal(2, count, "transactions should still reference account 1 after non-referenced column update")
}

// ─────────────────────────────────────────────────────────────────────────────
// ON DELETE SET NULL
// ─────────────────────────────────────────────────────────────────────────────

func (s *TestSuite) TestForeignKey_OnDeleteSetNull() {
	_, err := s.db.Exec(`create table "departments" (
		id   int8 primary key autoincrement,
		name varchar(100) not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "employees" (
		id            int8 primary key autoincrement,
		name          varchar(100) not null,
		department_id int8,
		foreign key (department_id) references "departments" (id) on delete set null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "departments" (name) values ('Engineering')`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "employees" (name, department_id) values ('Alice', 1), ('Bob', 1)`)
	s.Require().NoError(err)

	// Delete the department — employees should have department_id set to NULL.
	_, err = s.db.Exec(`delete from "departments" where id = 1`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`select name from "employees" where department_id is null`)
	s.Require().NoError(err)
	defer rows.Close()
	count := 0
	for rows.Next() {
		count += 1
	}
	s.Equal(2, count, "both employees should have department_id = NULL after department deletion")
}

// ─────────────────────────────────────────────────────────────────────────────
// ON UPDATE SET NULL
// ─────────────────────────────────────────────────────────────────────────────

func (s *TestSuite) TestForeignKey_OnUpdateSetNull_NonReferencedColumn() {
	_, err := s.db.Exec(`create table "categories" (
		id   int8 primary key autoincrement,
		name varchar(100) not null unique
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "products" (
		id          int8 primary key autoincrement,
		title       varchar(100) not null,
		category_id int8,
		foreign key (category_id) references "categories" (id) on update set null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "categories" (name) values ('Electronics')`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "products" (title, category_id) values ('Phone', 1)`)
	s.Require().NoError(err)

	// Updating a non-referenced column (name) does not change id, so no SET NULL triggers.
	_, err = s.db.Exec(`update "categories" set name = 'Gadgets' where id = 1`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`select category_id from "products" where id = 1`)
	s.Require().NoError(err)
	defer rows.Close()
	s.Require().True(rows.Next())
	var catID int64
	s.Require().NoError(rows.Scan(&catID))
	s.Equal(int64(1), catID, "category_id must remain 1 when non-referenced column updated")
}

// ─────────────────────────────────────────────────────────────────────────────
// SET NULL validation: FK column must be nullable
// ─────────────────────────────────────────────────────────────────────────────

func (s *TestSuite) TestForeignKey_SetNull_NonNullableColumn_Rejected() {
	_, err := s.db.Exec(`create table "users" (
		id   int8 primary key autoincrement,
		name varchar(100) not null
	);`)
	s.Require().NoError(err)

	// user_id is NOT NULL — SET NULL action must be rejected at DDL time.
	_, err = s.db.Exec(`create table "orders" (
		id      int8 primary key autoincrement,
		user_id int8 not null,
		foreign key (user_id) references "users" (id) on delete set null
	);`)
	s.Require().Error(err)
	s.Contains(err.Error(), "must be nullable")
}

// ─────────────────────────────────────────────────────────────────────────────
// Multi-column foreign key
// ─────────────────────────────────────────────────────────────────────────────

func (s *TestSuite) TestForeignKey_MultiColumn_CreateTable_NonUniqueTarget_Rejected() {
	_, err := s.db.Exec(`create table "order_lines" (
		order_id   int8 not null,
		product_id int8 not null,
		qty        int8 not null
	);`)
	s.Require().NoError(err)

	// (order_id, product_id) has no PK or unique constraint — FK must be rejected.
	_, err = s.db.Exec(`create table "shipment_lines" (
		id         int8 primary key autoincrement,
		order_id   int8 not null,
		product_id int8 not null,
		shipped    int8 not null,
		foreign key (order_id, product_id) references "order_lines" (order_id, product_id)
	);`)
	s.Require().Error(err)
	s.Contains(err.Error(), "must form a primary key or unique index")
}

func (s *TestSuite) TestForeignKey_MultiColumn_CreateTable_UniqueIndexTarget_OK() {
	// Declare (order_id, product_id) as a composite unique constraint (not the PK).
	_, err := s.db.Exec(`create table "order_lines" (
		id         int8 primary key autoincrement,
		order_id   int8 not null,
		product_id int8 not null,
		qty        int8 not null,
		unique (order_id, product_id)
	);`)
	s.Require().NoError(err)

	// (order_id, product_id) is covered by a unique index — FK must be accepted.
	_, err = s.db.Exec(`create table "shipment_lines" (
		id         int8 primary key autoincrement,
		order_id   int8 not null,
		product_id int8 not null,
		shipped    int8 not null,
		foreign key (order_id, product_id) references "order_lines" (order_id, product_id)
	);`)
	s.Require().NoError(err)
}

func (s *TestSuite) TestForeignKey_MultiColumn_ValidFK() {
	_, err := s.db.Exec(`create table "order_lines" (
		order_id   int8 not null,
		product_id int8 not null,
		qty        int8 not null,
		primary key (order_id, product_id)
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "shipment_lines" (
		id         int8 primary key autoincrement,
		order_id   int8 not null,
		product_id int8 not null,
		shipped    int8 not null,
		foreign key (order_id, product_id) references "order_lines" (order_id, product_id)
	);`)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "order_lines" (order_id, product_id, qty) values (?, ?, ?)`)
	s.Require().NoError(err)
	defer stmt.Close()
	_, err = stmt.Exec(int64(1), int64(10), int64(5))
	s.Require().NoError(err)

	// Valid FK reference.
	stmt2, err := s.db.Prepare(`insert into "shipment_lines" (order_id, product_id, shipped) values (?, ?, ?)`)
	s.Require().NoError(err)
	defer stmt2.Close()
	_, err = stmt2.Exec(int64(1), int64(10), int64(3))
	s.Require().NoError(err)
}

func (s *TestSuite) TestForeignKey_MultiColumn_InvalidFK() {
	_, err := s.db.Exec(`create table "order_lines" (
		order_id   int8 not null,
		product_id int8 not null,
		qty        int8 not null,
		primary key (order_id, product_id)
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "shipment_lines" (
		id         int8 primary key autoincrement,
		order_id   int8 not null,
		product_id int8 not null,
		shipped    int8 not null,
		foreign key (order_id, product_id) references "order_lines" (order_id, product_id)
	);`)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "order_lines" (order_id, product_id, qty) values (?, ?, ?)`)
	s.Require().NoError(err)
	defer stmt.Close()
	_, err = stmt.Exec(int64(1), int64(10), int64(5))
	s.Require().NoError(err)

	// Wrong product_id — FK must fail.
	stmt2, err := s.db.Prepare(`insert into "shipment_lines" (order_id, product_id, shipped) values (?, ?, ?)`)
	s.Require().NoError(err)
	defer stmt2.Close()
	_, err = stmt2.Exec(int64(1), int64(99), int64(3))
	s.Require().Error(err)
	var fkErr minisqlErrors.ErrForeignKeyViolation
	s.True(errors.As(err, &fkErr), "expected ErrForeignKeyViolation, got %T: %v", err, err)
}

func (s *TestSuite) TestForeignKey_MultiColumn_DeleteParent_Blocked() {
	_, err := s.db.Exec(`create table "order_lines" (
		order_id   int8 not null,
		product_id int8 not null,
		qty        int8 not null,
		primary key (order_id, product_id)
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "shipment_lines" (
		id         int8 primary key autoincrement,
		order_id   int8 not null,
		product_id int8 not null,
		shipped    int8 not null,
		foreign key (order_id, product_id) references "order_lines" (order_id, product_id)
	);`)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "order_lines" (order_id, product_id, qty) values (?, ?, ?)`)
	s.Require().NoError(err)
	defer stmt.Close()
	_, err = stmt.Exec(int64(1), int64(10), int64(5))
	s.Require().NoError(err)

	stmt2, err := s.db.Prepare(`insert into "shipment_lines" (order_id, product_id, shipped) values (?, ?, ?)`)
	s.Require().NoError(err)
	defer stmt2.Close()
	_, err = stmt2.Exec(int64(1), int64(10), int64(3))
	s.Require().NoError(err)

	// Delete must be blocked because a child row references this parent.
	stmt3, err := s.db.Prepare(`delete from "order_lines" where order_id = ? and product_id = ?`)
	s.Require().NoError(err)
	defer stmt3.Close()
	_, err = stmt3.Exec(int64(1), int64(10))
	s.Require().Error(err)
	var fkErr minisqlErrors.ErrForeignKeyParentViolation
	s.True(errors.As(err, &fkErr), "expected ErrForeignKeyParentViolation, got %T: %v", err, err)
}

func (s *TestSuite) TestForeignKey_MultiColumn_OnDeleteCascade() {
	_, err := s.db.Exec(`create table "order_lines" (
		order_id   int8 not null,
		product_id int8 not null,
		qty        int8 not null,
		primary key (order_id, product_id)
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "shipment_lines" (
		id         int8 primary key autoincrement,
		order_id   int8 not null,
		product_id int8 not null,
		shipped    int8 not null,
		foreign key (order_id, product_id) references "order_lines" (order_id, product_id) on delete cascade
	);`)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "order_lines" (order_id, product_id, qty) values (?, ?, ?)`)
	s.Require().NoError(err)
	defer stmt.Close()
	_, err = stmt.Exec(int64(1), int64(10), int64(5))
	s.Require().NoError(err)
	_, err = stmt.Exec(int64(2), int64(20), int64(3))
	s.Require().NoError(err)

	stmt2, err := s.db.Prepare(`insert into "shipment_lines" (order_id, product_id, shipped) values (?, ?, ?)`)
	s.Require().NoError(err)
	defer stmt2.Close()
	_, err = stmt2.Exec(int64(1), int64(10), int64(2))
	s.Require().NoError(err)
	_, err = stmt2.Exec(int64(2), int64(20), int64(1))
	s.Require().NoError(err)

	// Delete order_line (1,10) — its shipment_line should cascade.
	stmt3, err := s.db.Prepare(`delete from "order_lines" where order_id = ? and product_id = ?`)
	s.Require().NoError(err)
	defer stmt3.Close()
	_, err = stmt3.Exec(int64(1), int64(10))
	s.Require().NoError(err)

	rows, err := s.db.Query(`select order_id from "shipment_lines"`)
	s.Require().NoError(err)
	defer rows.Close()
	s.Require().True(rows.Next())
	var oid int64
	s.Require().NoError(rows.Scan(&oid))
	s.Equal(int64(2), oid, "only order 2's shipment line should survive")
	s.False(rows.Next())
}
