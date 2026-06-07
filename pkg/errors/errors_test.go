package errors

import (
	stderrors "errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPageChecksumError(t *testing.T) {
	t.Parallel()

	err := PageChecksumError{PageIndex: 7}

	require.Equal(t, "page 7: checksum mismatch (possible corruption)", err.Error())
	require.ErrorIs(t, err, ErrPageChecksumMismatch)
}

func TestConstraintErrors(t *testing.T) {
	t.Parallel()

	cause := stderrors.New("duplicate key")
	uniqueErr := NewUniqueViolation("users", "idx_email", []string{"email", "tenant_id"}, cause)
	require.Equal(
		t,
		`unique constraint violation on table "users" index "idx_email": duplicate value in column(s) (email, tenant_id)`,
		uniqueErr.Error(),
	)
	require.ErrorIs(t, uniqueErr, cause)

	require.Equal(
		t,
		`not null constraint violation on table "users": field "email" cannot be NULL`,
		ErrNotNullViolation{Table: "users", Column: "email"}.Error(),
	)
	require.Equal(
		t,
		`type mismatch on table "users" column "age": expected INT8`,
		ErrTypeMismatch{Table: "users", Column: "age", Expected: "INT8"}.Error(),
	)
	require.Equal(
		t,
		`type mismatch on table "users" column "age": cannot parse "old"`,
		ErrTypeMismatch{Table: "users", Column: "age", Detail: `cannot parse "old"`}.Error(),
	)
	require.Equal(
		t,
		`check constraint violation on table "users" column "age": age > 0`,
		ErrCheckConstraintViolation{Table: "users", ColumnName: "age", Expr: "age > 0"}.Error(),
	)
	require.Equal(
		t,
		`check constraint violation on column "age": age > 0`,
		ErrCheckConstraintViolation{ColumnName: "age", Expr: "age > 0"}.Error(),
	)
}

func TestForeignKeyErrors(t *testing.T) {
	t.Parallel()

	require.Equal(
		t,
		"foreign key constraint violation: orders.(user_id, tenant_id) references non-existent value in users.(id, tenant_id)",
		ErrForeignKeyViolation{
			ChildTable:    "orders",
			ChildColumns:  []string{"user_id", "tenant_id"},
			ParentTable:   "users",
			ParentColumns: []string{"id", "tenant_id"},
		}.Error(),
	)
	require.Equal(
		t,
		"foreign key constraint violation: users.(id) is still referenced by orders.(user_id)",
		ErrForeignKeyParentViolation{
			ParentTable:   "users",
			ParentColumns: []string{"id"},
			ChildTable:    "orders",
			ChildColumns:  []string{"user_id"},
		}.Error(),
	)
	require.Equal(t, "cannot drop table: it is still referenced by a foreign key constraint", ErrDropTableReferencedByFK.Error())
}

func TestSchemaErrors(t *testing.T) {
	t.Parallel()

	require.Equal(t, `table "users" does not exist`, ErrNoSuchTable{Name: "users"}.Error())
	require.Equal(t, `table "users" already exists`, ErrTableAlreadyExists{Name: "users"}.Error())
	require.Equal(t, `index "idx_users_email" does not exist`, ErrNoSuchIndex{Name: "idx_users_email"}.Error())
	require.Equal(t, `index "idx_users_email" already exists`, ErrIndexAlreadyExists{Name: "idx_users_email"}.Error())
}

func TestTransactionErrors(t *testing.T) {
	t.Parallel()

	require.Equal(t, "concurrent writer: only one write transaction allowed at a time", ErrConcurrentWriter.Error())
}
