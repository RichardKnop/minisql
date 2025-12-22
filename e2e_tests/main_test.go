package e2etests

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/RichardKnop/minisql/internal/minisql"
	"github.com/RichardKnop/minisql/internal/parser"
	"github.com/RichardKnop/minisql/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	port = 8082
	addr = ":8082"

	createUsersTable = `create table "users" (
	id int8 primary key autoincrement,
	name varchar(255),
	email text,
	created timestamp default now()
);`
)

func TestEndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempFile, err := os.CreateTemp("", "testdb")
	if err != nil {
		require.NoError(t, err)
	}
	defer os.Remove(tempFile.Name())

	db, stopServer, err := startServer(ctx, tempFile)
	require.NoError(t, err)
	defer stopServer()

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
		require.True(t, resp.Success)
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
		require.True(t, resp.Success)
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

		// First insert one row with explicitely set timestamp for created column
		resp, err := aClient.SendQuery(`insert into users("name", "email", "created") 
values('Danny Mason', 'Danny_Mason2966@xqj6f.tech', '2024-01-01 12:00:00');`)
		require.NoError(t, err)
		// fmt.Printf("Insert Response: %+v\n", resp)
		require.True(t, resp.Success)

		// Next try to specify primary key manually without using autoincrement
		resp, err = aClient.SendQuery(`insert into users("id", "name", "email", "created") 
values(100, 'Johnathan Walker', 'Johnathan_Walker250@ptr6k.page', '2024-01-02 15:30:27');`)
		require.NoError(t, err)
		// fmt.Printf("Insert Response: %+v\n", resp)
		require.True(t, resp.Success)

		// Next insert multiple rows without specifying created column (should default to now())
		resp, err = aClient.SendQuery(`insert into users("name", "email") values('Tyson Weldon', 'Tyson_Weldon2108@zynuu.video'),
('Mason Callan', 'Mason_Callan9524@bu2lo.edu'),
('Logan Flynn', 'Logan_Flynn9019@xtwt3.pro'),
('Beatrice Uttley', 'Beatrice_Uttley1670@1wa8o.org'),
('Harry Johnson', 'Harry_Johnson5515@jcf8v.video'),
('Carl Thomson', 'Carl_Thomson4218@kyb7t.host'),
('Kaylee Johnson', 'Kaylee_Johnson8112@c2nyu.design'),
('Cristal Duvall', 'Cristal_Duvall6639@yvu30.press');`)
		require.NoError(t, err)
		// fmt.Printf("Insert Response: %+v\n", resp)
		require.True(t, resp.Success)
		assert.Equal(t, minisql.Insert, resp.Kind)
		assert.Equal(t, 8, resp.RowsAffected)
	})

	t.Run("Basic select queries", func(t *testing.T) {
		resp, err := aClient.SendQuery(`select * from users order by id;`)
		require.NoError(t, err)
		require.True(t, resp.Success)

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

		require.NotEmpty(t, resp.Rows)
		assert.Len(t, resp.Rows, 10)
		assert.Equal(t, []minisql.OptionalValue{
			{Value: float64(100), Valid: true},
			{Value: "Johnathan Walker", Valid: true},
			{Value: "Johnathan_Walker250@ptr6k.page", Valid: true},
			{Value: "2024-01-02 15:30:27", Valid: true},
		}, resp.Rows[1])

		now := time.Now().UTC()
		for i := 2; i < 10; i++ {
			assert.Equal(t, float64(100+i-1), resp.Rows[i][0].Value.(float64)) // id should continue from 100
			timestamp, err := minisql.ParseTimestamp(resp.Rows[i][3].Value.(string))
			require.NoError(t, err)
			assert.Equal(t, now.Year(), int(timestamp.Year))
			assert.Equal(t, now.Month(), time.Month(timestamp.Month))
			assert.Equal(t, now.Day(), int(timestamp.Day))
			assert.Equal(t, now.Hour(), int(timestamp.Hour))
			assert.Equal(t, now.Minute(), int(timestamp.Minutes))
			assert.Equal(t, now.Second(), int(timestamp.Seconds))
		}
	})

	t.Run("Flush database and reinitialise to test unmarshaling from disk", func(t *testing.T) {
		// Flush database to ensure all pages are written to disk
		err := db.Flush(ctx)
		require.NoError(t, err)

		// Reinitialize database to clear pager cache, force read from disk
		stopServer()
		aClient.Close()
		db, stopServer, err = startServer(ctx, tempFile)
		require.NoError(t, err)
		defer stopServer()
		aClient, err := protocol.NewClient(addr)
		require.NoError(t, err)
		defer aClient.Close()

		resp, err := aClient.SendQuery(`select * from users order by id desc;`)
		require.NoError(t, err)
		require.True(t, resp.Success)

		expectedIDs := []float64{108, 107, 106, 105, 104, 103, 102, 101, 100, 1}
		for i := 9; i >= 0; i-- {
			assert.Equal(t, expectedIDs[i], resp.Rows[i][0].Value.(float64))
		}
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

func startServer(ctx context.Context, dbFile minisql.DBFile) (*minisql.Database, func(), error) {
	aPager, err := minisql.NewPager(dbFile, minisql.PageSize)
	if err != nil {
		return nil, nil, err
	}

	logger := zap.NewNop()

	aDatabase, err := minisql.NewDatabase(ctx, logger, "testdb_e2e", parser.New(), aPager, aPager, aPager)
	if err != nil {
		return nil, nil, err
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	srv, err := protocol.NewServer(aDatabase, logger, port)
	if err != nil {
		return nil, nil, err
	}

	srv.Serve(ctx)

	return aDatabase, sync.OnceFunc(srv.Stop), nil
}
