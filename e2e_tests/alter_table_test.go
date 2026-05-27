package e2etests

import (
	"context"
	"database/sql"
)

// TestAlterTable_AddColumn verifies that a new column can be added to an existing
// table and that existing rows return the column's default value (nil for nullable
// columns without a default) while new rows return the inserted value.
func (s *TestSuite) TestAlterTable_AddColumn() {
	ctx := context.Background()

	_, err := s.db.ExecContext(ctx, `create table "items" (
		id int8 primary key autoincrement,
		name varchar(255) not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.ExecContext(ctx, `insert into "items" (name) values ('alpha'), ('beta');`)
	s.Require().NoError(err)

	_, err = s.db.ExecContext(ctx, `ALTER TABLE items ADD COLUMN score int4;`)
	s.Require().NoError(err)

	// Existing rows should have NULL for the new column (lazy ADD COLUMN).
	rows, err := s.db.QueryContext(ctx, `select id, name, score from "items" order by id;`)
	s.Require().NoError(err)
	defer rows.Close()

	var got []struct {
		id    int64
		name  string
		score sql.NullInt32
	}
	for rows.Next() {
		var row struct {
			id    int64
			name  string
			score sql.NullInt32
		}
		s.Require().NoError(rows.Scan(&row.id, &row.name, &row.score))
		got = append(got, row)
	}
	s.Require().NoError(rows.Err())
	s.Require().Len(got, 2)
	s.Equal(int64(1), got[0].id)
	s.Equal("alpha", got[0].name)
	s.False(got[0].score.Valid, "existing row should have NULL for new column")
	s.Equal(int64(2), got[1].id)
	s.Equal("beta", got[1].name)
	s.False(got[1].score.Valid, "existing row should have NULL for new column")

	// Newly inserted rows should carry the new column value.
	_, err = s.db.ExecContext(ctx, `insert into "items" (name, score) values ('gamma', 42);`)
	s.Require().NoError(err)

	var id int64
	var name string
	var score sql.NullInt32
	err = s.db.QueryRowContext(ctx, `select id, name, score from "items" where id = 3`).
		Scan(&id, &name, &score)
	s.Require().NoError(err)
	s.Equal(int64(3), id)
	s.Equal("gamma", name)
	s.True(score.Valid)
	s.Equal(int32(42), score.Int32)
}

// TestAlterTable_AddColumn_WithDefault verifies that when a default is declared the
// new column value is returned for existing rows.
func (s *TestSuite) TestAlterTable_AddColumn_WithDefault() {
	ctx := context.Background()

	_, err := s.db.ExecContext(ctx, `create table "items" (
		id int8 primary key autoincrement,
		name varchar(255) not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.ExecContext(ctx, `insert into "items" (name) values ('alpha');`)
	s.Require().NoError(err)

	_, err = s.db.ExecContext(ctx, `ALTER TABLE items ADD COLUMN score int4 NOT NULL DEFAULT 99;`)
	s.Require().NoError(err)

	var score sql.NullInt32
	err = s.db.QueryRowContext(ctx, `select score from "items" where id = 1`).Scan(&score)
	s.Require().NoError(err)
	s.True(score.Valid)
	s.Equal(int32(99), score.Int32)
}

// TestAlterTable_AddColumn_SchemaPersists verifies that the schema update survives
// a database reopen.
func (s *TestSuite) TestAlterTable_AddColumn_SchemaPersists() {
	ctx := context.Background()

	_, err := s.db.ExecContext(ctx, `create table "items" (
		id int8 primary key autoincrement,
		name varchar(255) not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.ExecContext(ctx, `insert into "items" (name) values ('alpha');`)
	s.Require().NoError(err)

	_, err = s.db.ExecContext(ctx, `ALTER TABLE items ADD COLUMN score int4;`)
	s.Require().NoError(err)

	// Reopen and verify the column is still there.
	s.db = s.reopenDB()

	_, err = s.db.ExecContext(ctx, `insert into "items" (name, score) values ('beta', 7);`)
	s.Require().NoError(err)

	var id int64
	var name string
	var score sql.NullInt32
	err = s.db.QueryRowContext(ctx, `select id, name, score from "items" where id = 2`).
		Scan(&id, &name, &score)
	s.Require().NoError(err)
	s.Equal(int64(2), id)
	s.Equal("beta", name)
	s.True(score.Valid)
	s.Equal(int32(7), score.Int32)
}

// TestAlterTable_DropColumn verifies that a column can be tombstoned and that
// subsequent queries no longer return it.
func (s *TestSuite) TestAlterTable_DropColumn() {
	ctx := context.Background()

	_, err := s.db.ExecContext(ctx, `create table "items" (
		id int8 primary key autoincrement,
		name varchar(255) not null,
		internal_note text
	);`)
	s.Require().NoError(err)

	_, err = s.db.ExecContext(ctx, `insert into "items" (name, internal_note) values ('alpha', 'secret');`)
	s.Require().NoError(err)

	_, err = s.db.ExecContext(ctx, `ALTER TABLE items DROP COLUMN internal_note;`)
	s.Require().NoError(err)

	// The column should no longer be selectable.
	rows, err := s.db.QueryContext(ctx, `select id, name from "items";`)
	s.Require().NoError(err)
	defer rows.Close()

	cols, err := rows.Columns()
	s.Require().NoError(err)
	s.Equal([]string{"id", "name"}, cols)

	var id int64
	var name string
	s.Require().True(rows.Next())
	s.Require().NoError(rows.Scan(&id, &name))
	s.Equal(int64(1), id)
	s.Equal("alpha", name)
}

// TestAlterTable_DropColumn_SchemaPersists verifies the tombstone survives a reopen.
func (s *TestSuite) TestAlterTable_DropColumn_SchemaPersists() {
	ctx := context.Background()

	_, err := s.db.ExecContext(ctx, `create table "items" (
		id int8 primary key autoincrement,
		name varchar(255) not null,
		obsolete text
	);`)
	s.Require().NoError(err)

	_, err = s.db.ExecContext(ctx, `insert into "items" (name, obsolete) values ('x', 'old');`)
	s.Require().NoError(err)

	_, err = s.db.ExecContext(ctx, `ALTER TABLE items DROP COLUMN obsolete;`)
	s.Require().NoError(err)

	s.db = s.reopenDB()

	// Insert a new row after reopen — it should not have the obsolete column.
	_, err = s.db.ExecContext(ctx, `insert into "items" (name) values ('y');`)
	s.Require().NoError(err)

	rows, err := s.db.QueryContext(ctx, `select id, name from "items" order by id;`)
	s.Require().NoError(err)
	defer rows.Close()

	var got [][2]string
	for rows.Next() {
		var id int64
		var name string
		s.Require().NoError(rows.Scan(&id, &name))
		got = append(got, [2]string{"id", name})
	}
	s.Require().NoError(rows.Err())
	s.Require().Len(got, 2)
}

// TestAlterTable_RenameColumn verifies that a column can be renamed.
func (s *TestSuite) TestAlterTable_RenameColumn() {
	ctx := context.Background()

	_, err := s.db.ExecContext(ctx, `create table "items" (
		id int8 primary key autoincrement,
		nm text not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.ExecContext(ctx, `insert into "items" (nm) values ('hello');`)
	s.Require().NoError(err)

	_, err = s.db.ExecContext(ctx, `ALTER TABLE items RENAME COLUMN nm TO full_name;`)
	s.Require().NoError(err)

	var id int64
	var fullName string
	err = s.db.QueryRowContext(ctx, `select id, full_name from "items" where id = 1`).
		Scan(&id, &fullName)
	s.Require().NoError(err)
	s.Equal(int64(1), id)
	s.Equal("hello", fullName)
}

// TestAlterTable_RenameTo verifies that a table can be renamed.
func (s *TestSuite) TestAlterTable_RenameTo() {
	ctx := context.Background()

	_, err := s.db.ExecContext(ctx, `create table "items" (
		id int8 primary key autoincrement,
		name text not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.ExecContext(ctx, `insert into "items" (name) values ('foo');`)
	s.Require().NoError(err)

	_, err = s.db.ExecContext(ctx, `ALTER TABLE items RENAME TO products;`)
	s.Require().NoError(err)

	// Old name should be gone.
	_, err = s.db.ExecContext(ctx, `select * from "items";`)
	s.Error(err)
	s.ErrorContains(err, `table does not exist`)

	// New name should work.
	var id int64
	var name string
	err = s.db.QueryRowContext(ctx, `select id, name from "products" where id = 1`).
		Scan(&id, &name)
	s.Require().NoError(err)
	s.Equal(int64(1), id)
	s.Equal("foo", name)
}

// TestAlterTable_AddColumn_DuplicateFails verifies that adding a column that
// already exists returns an error.
func (s *TestSuite) TestAlterTable_AddColumn_DuplicateFails() {
	ctx := context.Background()

	_, err := s.db.ExecContext(ctx, `create table "items" (
		id int8 primary key autoincrement,
		name text not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.ExecContext(ctx, `ALTER TABLE items ADD COLUMN name text;`)
	s.Error(err)
}

// TestAlterTable_DropColumn_PKFails verifies that dropping the primary key column
// returns an error.
func (s *TestSuite) TestAlterTable_DropColumn_PKFails() {
	ctx := context.Background()

	_, err := s.db.ExecContext(ctx, `create table "items" (
		id int8 primary key autoincrement,
		name text not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.ExecContext(ctx, `ALTER TABLE items DROP COLUMN id;`)
	s.Error(err)
}
