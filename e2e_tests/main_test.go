package e2etests

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"testing"

	"github.com/RichardKnop/minisql/internal/minisql"
	"github.com/RichardKnop/minisql/internal/parser"
	"github.com/RichardKnop/minisql/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	port             = 8082
	addr             = ":8082"
	createUsersTable = `create table "users" (
	id int8 primary key autoincrement,
	name varchar(255),
	email text,
	created timestamp
);`
)

func TestEndToEnd(t *testing.T) {
	err := startServer(t)
	require.NoError(t, err)

	aClient, err := protocol.NewClient(addr)
	require.NoError(t, err)
	defer aClient.Close()

	t.Run("Inspect schema table when database is empty", func(t *testing.T) {
		// Database should be empty except for the initial schema table itself
		assertEmptySchemaTable(t, aClient)
	})

	t.Run("Test meta commands", func(t *testing.T) {
		resp, err := aClient.SendMetaCommand("ping")
		require.NoError(t, err)
		assert.True(t, resp.Success)
		assert.Equal(t, "pong", resp.Message)

		assertTables(t, aClient, "minisql_schema")
	})

	t.Run("Test creating a table", func(t *testing.T) {
		resp, err := aClient.SendQuery(createUsersTable)
		require.NoError(t, err)

		assert.True(t, resp.Success)
		assert.Equal(t, minisql.CreateTable, resp.Kind)
		assert.Equal(t, "Table 'users' created successfully", resp.Message)

		// Database should now contain the users table in addition to the schema table
		assertTables(t, aClient, "minisql_schema", "users")

		// Check schema table contents, there should be three rows now:
		// schema table + users table + users pk
		rows := checkSchemaTable(t, aClient)
		assert.Len(t, rows, 3)
		assertSchemaTableRow(
			t,
			rows[1],
			minisql.SchemaTable,
			"users",
			1,
			createUsersTable,
		)
		assertSchemaTableRow(
			t,
			rows[2],
			minisql.SchemaPrimaryKey,
			"pk_users",
			2,
			"",
		)
	})

	t.Run("Test dropping a table", func(t *testing.T) {
		resp, err := aClient.SendQuery(`drop table users;`)
		require.NoError(t, err)
		assert.True(t, resp.Success)
		assert.Equal(t, minisql.DropTable, resp.Kind)
		assert.Equal(t, "Table 'users' dropped successfully", resp.Message)

		// Database should be empty except for the initial schema table itself
		assertTables(t, aClient, "minisql_schema")
		assertEmptySchemaTable(t, aClient)
	})

	t.Run("Create test tables and insert some data", func(t *testing.T) {
		_, err := aClient.SendQuery(createUsersTable)
		require.NoError(t, err)
		assertTables(t, aClient, "minisql_schema", "users")

		resp, err := aClient.SendQuery(`insert into users("name", "email", "created") values('Danny Mason', 'Danny_Mason2966@xqj6f.tech', '2024-01-01 12:00:00'),
('Johnathan Walker', 'Johnathan_Walker250@ptr6k.page', '2024-01-02 12:00:00'),
('Tyson Weldon', 'Tyson_Weldon2108@zynuu.video', '2024-01-03 12:00:00'),
('Mason Callan', 'Mason_Callan9524@bu2lo.edu', '2024-01-04 12:00:00'),
('Logan Flynn', 'Logan_Flynn9019@xtwt3.pro', '2024-01-05 12:00:00'),
('Beatrice Uttley', 'Beatrice_Uttley1670@1wa8o.org', '2024-01-06 12:00:00'),
('Harry Johnson', 'Harry_Johnson5515@jcf8v.video', '2024-01-07 12:00:00'),
('Carl Thomson', 'Carl_Thomson4218@kyb7t.host', '2024-01-08 12:00:00'),
('Kaylee Johnson', 'Kaylee_Johnson8112@c2nyu.design', '2024-01-09 12:00:00'),
('Cristal Duvall', 'Cristal_Duvall6639@yvu30.press', '2024-01-10 12:00:00');`)
		require.NoError(t, err)

		fmt.Printf("Insert Response: %+v\n", resp)

		assert.True(t, resp.Success)
		assert.Equal(t, minisql.Insert, resp.Kind)
		assert.Equal(t, 10, resp.RowsAffected)
	})

	t.Run("Basic select queries", func(t *testing.T) {
		resp, err := aClient.SendQuery(`select * from users order by id;`)
		require.NoError(t, err)

		assert.True(t, resp.Success)
		assert.Equal(t, minisql.Select, resp.Kind)
		assert.Len(t, resp.Columns, 4)
		assert.Equal(t, "id", resp.Columns[0].Name)
		assert.Equal(t, "name", resp.Columns[1].Name)
		assert.Equal(t, "email", resp.Columns[2].Name)
		assert.Equal(t, "created", resp.Columns[3].Name)

		require.NotEmpty(t, resp.Rows)
		assert.Len(t, resp.Rows, 10)
		assert.Equal(t, []minisql.OptionalValue{
			{Value: float64(1), Valid: true},
			{Value: "Danny Mason", Valid: true},
			{Value: "Danny_Mason2966@xqj6f.tech", Valid: true},
			{Value: "2024-01-01 12:00:00", Valid: true},
		}, resp.Rows[0])
	})
}

func assertEmptySchemaTable(t *testing.T, aClient *protocol.Client) {
	rows := checkSchemaTable(t, aClient)
	assert.Len(t, rows, 1)
}

func checkSchemaTable(t *testing.T, aClient *protocol.Client) [][]minisql.OptionalValue {
	query := fmt.Sprintf("select * from %s;", minisql.SchemaTableName)
	resp, err := aClient.SendQuery(query)
	require.NoError(t, err)

	assert.True(t, resp.Success)
	assert.Equal(t, minisql.Select, resp.Kind)
	assert.Len(t, resp.Columns, 4)
	assert.Equal(t, "type", resp.Columns[0].Name)
	assert.Equal(t, "name", resp.Columns[1].Name)
	assert.Equal(t, "root_page", resp.Columns[2].Name)
	assert.Equal(t, "sql", resp.Columns[3].Name)

	require.NotEmpty(t, resp.Rows)

	assertSchemaTableRow(
		t,
		resp.Rows[0],
		minisql.SchemaTable,     // Schema table type (1 == table, 2 == primary key)
		minisql.SchemaTableName, // Schema table name
		0,                       // Schema table root page is always 0
		minisql.MainTableSQL,    // SQL definition of the schema table
	)

	return resp.Rows
}

func assertTables(t *testing.T, aClient *protocol.Client, expectedTables ...string) {
	resp, err := aClient.SendMetaCommand("list_tables")
	require.NoError(t, err)
	assert.True(t, resp.Success)
	assert.ElementsMatch(t, expectedTables, strings.Split(resp.Message, "\n"))
}

func assertSchemaTableRow(t *testing.T, row []minisql.OptionalValue, expectedType minisql.SchemaType, expectedName string, expectedRootPage int, expectedSQL string) {
	assert.True(t, row[0].Valid)
	assert.Equal(t, expectedType, minisql.SchemaType(row[0].Value.(float64)))

	assert.True(t, row[1].Valid)
	assert.Equal(t, expectedName, row[1].Value.(string))

	assert.True(t, row[2].Valid)
	assert.Equal(t, expectedRootPage, int(row[2].Value.(float64)))

	if expectedSQL == "" {
		assert.False(t, row[3].Valid)
	} else {
		assert.True(t, row[3].Valid)
		assert.Equal(t, expectedSQL, row[3].Value.(string))
	}
}

func startServer(t *testing.T) error {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	tempFile, err := os.CreateTemp("", "testdb")
	if err != nil {
		return err
	}
	t.Cleanup(func() { os.Remove(tempFile.Name()) })

	aPager, err := minisql.NewPager(tempFile, minisql.PageSize)
	if err != nil {
		return err
	}

	logger := zap.NewNop()

	aDatabase, err := minisql.NewDatabase(ctx, logger, "testdb_e2e", parser.New(), aPager, aPager, aPager)
	if err != nil {
		return err
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	srv, err := protocol.NewServer(aDatabase, logger, port)
	if err != nil {
		return err
	}

	srv.Serve(ctx)

	t.Cleanup(func() {
		srv.Stop()
		if err := aDatabase.Close(ctx); err != nil {
			fmt.Printf("error closing database: %s\n", err)
		}
	})

	return nil
}
