package main

import (
	"bufio"
	"database/sql"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/RichardKnop/minisql"
)

// openTestDB opens a fresh temp-file database and returns it with a cleanup fn.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	f, err := os.CreateTemp("", "minisql_cli_test_*.db")
	require.NoError(t, err)
	path := f.Name()
	f.Close()
	t.Cleanup(func() {
		os.Remove(path)
		os.Remove(path + "-wal")
	})
	db, err := sql.Open("minisql", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

// newTestShell builds a shell that reads from input and writes to a buffer.
func newTestShell(db *sql.DB, input string) (*shell, *strings.Builder) {
	var out strings.Builder
	sh := &shell{
		db:      db,
		out:     &out,
		mode:    modeTable,
		scanner: bufio.NewScanner(strings.NewReader(input)),
		isatty:  false,
	}
	return sh, &out
}

// --- statementComplete ---

func TestStatementComplete(t *testing.T) {
	cases := []struct {
		input string
		want  bool
	}{
		{"select 1;", true},
		{"select 1", false},
		{"", false},
		{";", true},
		{"select 'it\\'s fine';", false}, // not valid escape, but tests quote tracking
		{"select 'hello;world';", true},   // inner ';' ignored; trailing ';' is the terminator
		{"select 'hello;world'", false},   // no terminator outside quotes
		{`select "col;name" from t;`, true},
		{"select 1;\nselect 2;", true},
		{"insert into t (name) values ('alice');\n", true},
		{"insert into t (name)\nvalues ('bob');\n", true},
		{"select '", false},  // unclosed quote
		{"select ';", false}, // semicolon inside unclosed quote
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			assert.Equal(t, tc.want, statementComplete(tc.input))
		})
	}
}

// --- formatValue ---

func TestFormatValue(t *testing.T) {
	assert.Equal(t, "NULL", formatValue(nil))
	assert.Equal(t, "hello", formatValue([]byte("hello")))
	assert.Equal(t, "42", formatValue(int64(42)))
	assert.Equal(t, "3.14", formatValue(float64(3.14)))
	assert.Equal(t, "1e+10", formatValue(float64(1e10)))
	assert.Equal(t, "true", formatValue(true))
	assert.Equal(t, "hello", formatValue("hello"))
}

// --- splitDotCommand ---

func TestSplitDotCommand(t *testing.T) {
	cases := []struct {
		input string
		want  []string
	}{
		{".quit", []string{".quit"}},
		{".mode csv", []string{".mode", "csv"}},
		{".schema users", []string{".schema", "users"}},
		{".schema 'my table'", []string{".schema", "my table"}},
		{"", nil},
		{".help  ", []string{".help"}},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			assert.Equal(t, tc.want, splitDotCommand(tc.input))
		})
	}
}

// --- quoteString ---

func TestQuoteString(t *testing.T) {
	assert.Equal(t, "'hello'", quoteString("hello"))
	assert.Equal(t, "'it''s'", quoteString("it's"))
	assert.Equal(t, "''", quoteString(""))
}

// --- onOff ---

func TestOnOff(t *testing.T) {
	assert.Equal(t, "on", onOff(true))
	assert.Equal(t, "off", onOff(false))
}

// --- printResult / printTable / printCSV ---

func TestPrintResult_Table(t *testing.T) {
	var buf strings.Builder
	printResult(&buf, []string{"id", "name"}, [][]string{{"1", "alice"}, {"2", "bob"}}, modeTable)
	out := buf.String()
	assert.Contains(t, out, "id")
	assert.Contains(t, out, "name")
	assert.Contains(t, out, "alice")
	assert.Contains(t, out, "bob")
	assert.Contains(t, out, "--") // separator
}

func TestPrintResult_CSV(t *testing.T) {
	var buf strings.Builder
	printResult(&buf, []string{"id", "name"}, [][]string{{"1", "alice"}, {"2", "bob"}}, modeCSV)
	out := buf.String()
	assert.Equal(t, "id,name\n1,alice\n2,bob\n", out)
}

func TestPrintResult_Empty(t *testing.T) {
	var buf strings.Builder
	printResult(&buf, nil, nil, modeTable)
	assert.Empty(t, buf.String())
}

func TestPrintResult_CSV_SpecialChars(t *testing.T) {
	var buf strings.Builder
	printResult(&buf, []string{"note"}, [][]string{{"hello, world"}, {"it's \"quoted\""}}, modeCSV)
	out := buf.String()
	assert.Contains(t, out, `"hello, world"`)
	assert.Contains(t, out, `"it's ""quoted"""`)
}

func TestPrintTable_UnicodeWidth(t *testing.T) {
	var buf strings.Builder
	printTable(&buf, []string{"name"}, [][]string{{"日本語"}, {"hi"}})
	out := buf.String()
	// All three rows (header, "日本語", "hi") must appear.
	assert.Contains(t, out, "name")
	assert.Contains(t, out, "日本語")
	assert.Contains(t, out, "hi")
}

// --- shell.exec ---

func TestShell_Exec_Select(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`create table "t" (id int8, name varchar(255))`)
	require.NoError(t, err)
	_, err = db.Exec(`insert into "t" (id, name) values (1, 'alice'), (2, 'bob')`)
	require.NoError(t, err)

	sh, out := newTestShell(db, "")
	sh.exec(`select * from "t"`)
	got := out.String()
	assert.Contains(t, got, "alice")
	assert.Contains(t, got, "bob")
}

func TestShell_Exec_Error(t *testing.T) {
	db := openTestDB(t)
	sh, out := newTestShell(db, "")
	sh.exec(`select * from "nonexistent"`)
	assert.Contains(t, out.String(), "Error:")
}

func TestShell_Exec_Timer(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`create table "t" (id int8)`)
	require.NoError(t, err)

	sh, out := newTestShell(db, "")
	sh.timer = true
	sh.exec(`select * from "t"`)
	assert.Contains(t, out.String(), "Time:")
}

func TestShell_Exec_CSV(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`create table "t" (id int8, name varchar(255))`)
	require.NoError(t, err)
	_, err = db.Exec(`insert into "t" (id, name) values (1, 'alice')`)
	require.NoError(t, err)

	sh, out := newTestShell(db, "")
	sh.mode = modeCSV
	sh.exec(`select * from "t"`)
	assert.Contains(t, out.String(), "id,name")
	assert.Contains(t, out.String(), "1,alice")
}

// --- shell.dotCommand ---

func TestShell_DotMode(t *testing.T) {
	db := openTestDB(t)
	sh, _ := newTestShell(db, "")

	sh.dotCommand(".mode csv")
	assert.Equal(t, modeCSV, sh.mode)

	sh.dotCommand(".mode table")
	assert.Equal(t, modeTable, sh.mode)
}

func TestShell_DotMode_Unknown(t *testing.T) {
	db := openTestDB(t)
	sh, out := newTestShell(db, "")
	sh.dotCommand(".mode ndjson")
	assert.Contains(t, out.String(), "Error:")
}

func TestShell_DotMode_NoArg(t *testing.T) {
	db := openTestDB(t)
	sh, out := newTestShell(db, "")
	sh.dotCommand(".mode")
	assert.Contains(t, out.String(), "table")
}

func TestShell_DotTimer(t *testing.T) {
	db := openTestDB(t)
	sh, _ := newTestShell(db, "")

	sh.dotCommand(".timer on")
	assert.True(t, sh.timer)

	sh.dotCommand(".timer off")
	assert.False(t, sh.timer)
}

func TestShell_DotTimer_NoArg(t *testing.T) {
	db := openTestDB(t)
	sh, out := newTestShell(db, "")
	sh.dotCommand(".timer")
	assert.Contains(t, out.String(), "off")
}

func TestShell_DotTimer_Unknown(t *testing.T) {
	db := openTestDB(t)
	sh, out := newTestShell(db, "")
	sh.dotCommand(".timer maybe")
	assert.Contains(t, out.String(), "Error:")
}

func TestShell_DotHelp(t *testing.T) {
	db := openTestDB(t)
	sh, out := newTestShell(db, "")
	sh.dotCommand(".help")
	assert.Contains(t, out.String(), ".tables")
	assert.Contains(t, out.String(), ".schema")
}

func TestShell_DotUnknown(t *testing.T) {
	db := openTestDB(t)
	sh, out := newTestShell(db, "")
	sh.dotCommand(".nosuchcommand")
	assert.Contains(t, out.String(), "Error:")
}

func TestShell_DotTables(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`create table "users" (id int8)`)
	require.NoError(t, err)

	sh, out := newTestShell(db, "")
	sh.dotCommand(".tables")
	got := out.String()
	assert.Contains(t, got, "users")
	assert.NotContains(t, got, "minisql_schema")
}

func TestShell_DotSchema_All(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`create table "users" (id int8)`)
	require.NoError(t, err)

	sh, out := newTestShell(db, "")
	sh.dotCommand(".schema")
	got := out.String()
	assert.Contains(t, got, "create table")
	assert.Contains(t, got, "users")
	assert.NotContains(t, got, "minisql_schema")
}

func TestShell_DotSchema_Specific(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`create table "users" (id int8, email varchar(255))`)
	require.NoError(t, err)

	sh, out := newTestShell(db, "")
	sh.dotCommand(".schema users")
	got := out.String()
	assert.Contains(t, got, "email")
}

// --- shell.run (integration) ---

func TestShell_Run_SelectAndQuit(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`create table "t" (id int8, name varchar(255))`)
	require.NoError(t, err)
	_, err = db.Exec(`insert into "t" (id, name) values (1, 'alice')`)
	require.NoError(t, err)

	sh, out := newTestShell(db, `select * from "t";`+"\n")
	sh.run()
	assert.Contains(t, out.String(), "alice")
}

func TestShell_Run_MultiLine(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`create table "t" (id int8)`)
	require.NoError(t, err)

	input := "select *\nfrom \"t\"\nwhere id = 1;\n"
	sh, out := newTestShell(db, input)
	sh.run()
	// No error expected — table exists, zero rows is fine.
	assert.NotContains(t, out.String(), "Error:")
}

func TestShell_Run_DotCommandInRun(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`create table "items" (id int8)`)
	require.NoError(t, err)

	sh, out := newTestShell(db, ".tables\n")
	sh.run()
	assert.Contains(t, out.String(), "items")
}

func TestShell_Run_ErrorContinues(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`create table "t" (id int8)`)
	require.NoError(t, err)

	// Bad query followed by good query — shell must continue after the error.
	input := "select * from \"missing\";\nselect * from \"t\";\n"
	sh, out := newTestShell(db, input)
	sh.run()
	got := out.String()
	assert.Contains(t, got, "Error:")
	// The id column header from the second query must still appear.
	assert.Contains(t, got, "id")
}

// --- isSelectLike ---

func TestIsSelectLike(t *testing.T) {
	cases := []struct {
		query string
		want  bool
	}{
		{"select * from t", true},
		{"SELECT 1", true},
		{"with cte as (select 1) select * from cte", true},
		{"explain select * from t", true},
		{"values (1),(2)", true},
		{"insert into t values (1)", false},
		{"update t set x=1", false},
		{"delete from t", false},
		{"create table t (id int8)", false},
		{"drop table t", false},
		// RETURNING makes DML produce rows
		{"insert into t (id) values (1) returning id", true},
		{"update t set x=1 returning x", true},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			assert.Equal(t, tc.want, isSelectLike(tc.query))
		})
	}
}

// --- DML feedback ---

func TestShell_Exec_Insert_RowsAffected(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`create table "t" (id int8, name varchar(255))`)
	require.NoError(t, err)

	sh, out := newTestShell(db, "")
	sh.exec(`insert into "t" (id, name) values (1, 'alice'), (2, 'bob')`)
	assert.Contains(t, out.String(), "2 row(s) affected")
}

func TestShell_Exec_Update_RowsAffected(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`create table "t" (id int8, name varchar(255))`)
	require.NoError(t, err)
	_, err = db.Exec(`insert into "t" (id, name) values (1, 'alice'), (2, 'bob')`)
	require.NoError(t, err)

	sh, out := newTestShell(db, "")
	sh.exec(`update "t" set name = 'x' where id = 1`)
	assert.Contains(t, out.String(), "1 row(s) affected")
}

func TestShell_Exec_Delete_RowsAffected(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`create table "t" (id int8)`)
	require.NoError(t, err)
	_, err = db.Exec(`insert into "t" (id) values (1),(2),(3)`)
	require.NoError(t, err)

	sh, out := newTestShell(db, "")
	sh.exec(`delete from "t"`)
	assert.Contains(t, out.String(), "3 row(s) affected")
}

func TestShell_Exec_DDL_Silent(t *testing.T) {
	db := openTestDB(t)

	sh, out := newTestShell(db, "")
	sh.exec(`create table "t" (id int8)`)
	// DDL succeeds silently — no "rows affected" output, no error.
	assert.NotContains(t, out.String(), "Error:")
	assert.NotContains(t, out.String(), "row(s)")
}

// --- newline escaping in table output ---

func TestEscapeForTable(t *testing.T) {
	assert.Equal(t, `hello\nworld`, escapeForTable("hello\nworld"))
	assert.Equal(t, `hello\r\nworld`, escapeForTable("hello\r\nworld"))
	assert.Equal(t, `col1\tcol2`, escapeForTable("col1\tcol2"))
	assert.Equal(t, "no specials", escapeForTable("no specials"))
}

func TestPrintTable_NewlineInCell(t *testing.T) {
	var buf strings.Builder
	printTable(&buf, []string{"note"}, [][]string{{"line1\nline2"}})
	out := buf.String()
	// The raw newline must not appear — it must be escaped.
	assert.NotContains(t, out, "line1\nline2")
	assert.Contains(t, out, `line1\nline2`)
}
