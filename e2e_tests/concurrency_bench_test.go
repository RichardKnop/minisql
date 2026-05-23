package e2etests

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/require"
)


func BenchmarkConcurrentReads(b *testing.B) {
	// Create temporary database
	tmpFile, err := os.CreateTemp("", "benchmark-*.db")
	require.NoError(b, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	db, err := sql.Open("minisql", tmpFile.Name()+"?log_level=warn")
	require.NoError(b, err)
	defer db.Close()

	// Embedded database - use single connection to avoid pool contention
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// Create table
	_, err = db.Exec(createUsersTableSQL)
	require.NoError(b, err)
	_, err = db.Exec(createUsersTimestampIndexSQL)
	require.NoError(b, err)

	// Insert test data
	faker := gofakeit.New(uint64(time.Now().Unix()))
	tx, err := db.Begin()
	require.NoError(b, err)

	stmt, err := tx.Prepare(`insert into users("email", "name") values(?, ?);`)
	require.NoError(b, err)

	for i := range 1000 {
		email := fmt.Sprintf("user%d@example.com", i)
		name := faker.Name()
		_, err := stmt.Exec(email, name)
		require.NoError(b, err)
	}

	err = stmt.Close()
	require.NoError(b, err)

	err = tx.Commit()
	require.NoError(b, err)

	// Prepare statement once and reuse it (avoids connection pool overhead)
	readStmt, err := db.Prepare(`select * from users where id = ?;`)
	require.NoError(b, err)
	defer readStmt.Close()

	b.ResetTimer()

	// Concurrent reads
	b.RunParallel(func(pb *testing.PB) {
		i := 1
		for pb.Next() {
			var id int64
			var email, name sql.NullString
			var created sql.NullTime

			err := readStmt.QueryRow(int64((i%1000)+1)).Scan(&id, &email, &name, &created)
			require.NoError(b, err)

			i += 1
		}
	})
}
