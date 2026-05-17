//go:build bench

package benchmarks

import (
	"fmt"
	"testing"
)

const subquerySeedN = 10_000

// BenchmarkCTE_Materialise measures the overhead of a non-recursive CTE that
// materialises a filtered subset before the outer query runs.
func BenchmarkCTE_Materialise(b *testing.B) {
	for _, d := range drivers {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openDB(b, d)
			defer cleanup()
			seedRows(b, db, d, subquerySeedN)

			// CTE selects roughly 20% of rows (age >= 80, values 0-99).
			var query string
			switch d.name {
			case "minisql":
				query = `with seniors as (select id, name from "bench_rows" where age >= 80)
				         select count(*) from seniors`
			default:
				query = `WITH seniors AS (SELECT id, name FROM bench_rows WHERE age >= 80)
				         SELECT count(*) FROM seniors`
			}

			b.ResetTimer()
			for range b.N {
				var count int64
				if err := db.QueryRow(query).Scan(&count); err != nil {
					b.Fatalf("query: %v", err)
				}
			}
		})
	}
}

// BenchmarkSubquery_InList measures a correlated-free IN (subquery) — minisql
// converts eligible subqueries to semi-joins; SQLite uses its own semi-join
// optimisation.  Uses two distinct tables to avoid the self-join conflict check.
func BenchmarkSubquery_InList(b *testing.B) {
	for _, d := range drivers {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openDB(b, d)
			defer cleanup()

			var (
				createUsers  string
				createOrders string
				insertUser   string
				insertOrder  string
				query        string
			)
			switch d.name {
			case "minisql":
				createUsers  = `create table "bench_sq_users" (id int8 primary key autoincrement, name varchar(100))`
				createOrders = `create table "bench_sq_orders" (id int8 primary key autoincrement, user_id int8, amount int8)`
				insertUser   = `insert into "bench_sq_users" (name) values (?)`
				insertOrder  = `insert into "bench_sq_orders" (user_id, amount) values (?, ?)`
				query        = `select name from "bench_sq_users" where id in (select user_id from "bench_sq_orders" where amount > 500)`
			default:
				createUsers  = `CREATE TABLE bench_sq_users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)`
				createOrders = `CREATE TABLE bench_sq_orders (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount INTEGER)`
				insertUser   = `INSERT INTO bench_sq_users (name) VALUES (?)`
				insertOrder  = `INSERT INTO bench_sq_orders (user_id, amount) VALUES (?, ?)`
				query        = `SELECT name FROM bench_sq_users WHERE id IN (SELECT user_id FROM bench_sq_orders WHERE amount > 500)`
			}

			mustExec(b, db, createUsers)
			mustExec(b, db, createOrders)

			// Seed users.
			tx, err := db.Begin()
			if err != nil {
				b.Fatalf("begin users: %v", err)
			}
			insU, err := tx.Prepare(insertUser)
			if err != nil {
				_ = tx.Rollback()
				b.Fatalf("prepare insert user: %v", err)
			}
			for i := range subquerySeedN {
				if _, err := insU.Exec(fmt.Sprintf("user-%06d", i)); err != nil {
					_ = tx.Rollback()
					b.Fatalf("insert user %d: %v", i, err)
				}
			}
			insU.Close()
			if err := tx.Commit(); err != nil {
				b.Fatalf("commit users: %v", err)
			}

			// Seed orders — 2 per user, amount cycles 0-999 so ~50% exceed 500.
			tx, err = db.Begin()
			if err != nil {
				b.Fatalf("begin orders: %v", err)
			}
			insO, err := tx.Prepare(insertOrder)
			if err != nil {
				_ = tx.Rollback()
				b.Fatalf("prepare insert order: %v", err)
			}
			for i := range subquerySeedN {
				userID := int64(i + 1)
				for j := range 2 {
					amount := int64((i*2+j)%1000 + 1)
					if _, err := insO.Exec(userID, amount); err != nil {
						_ = tx.Rollback()
						b.Fatalf("insert order %d/%d: %v", i, j, err)
					}
				}
			}
			insO.Close()
			if err := tx.Commit(); err != nil {
				b.Fatalf("commit orders: %v", err)
			}

			b.ResetTimer()
			for range b.N {
				rows, err := db.Query(query)
				if err != nil {
					b.Fatalf("query: %v", err)
				}
				n := 0
				for rows.Next() {
					var name string
					if err := rows.Scan(&name); err != nil {
						rows.Close()
						b.Fatalf("scan: %v", err)
					}
					n++
				}
				rows.Close()
				if err := rows.Err(); err != nil {
					b.Fatalf("rows err: %v", err)
				}
				b.ReportMetric(float64(n), "rows/op")
			}
		})
	}
}

// BenchmarkOnConflict_DoUpdate measures the upsert path (INSERT … ON CONFLICT
// DO UPDATE SET) — a read-modify-write cycle that updates an existing row when
// the primary key already exists.
func BenchmarkOnConflict_DoUpdate(b *testing.B) {
	for _, d := range drivers {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openDB(b, d)
			defer cleanup()

			const upsertRows = 1_000
			var (
				createT string
				insertT string
				upsertT string
			)
			switch d.name {
			case "minisql":
				createT = `create table "bench_upsert" (id int8 primary key, name varchar(100), value int8)`
				insertT = `insert into "bench_upsert" (id, name, value) values (?, ?, ?)`
				upsertT = `insert into "bench_upsert" (id, name, value) values (?, ?, ?) on conflict do update set value = ?`
			default:
				createT = `CREATE TABLE bench_upsert (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)`
				insertT = `INSERT INTO bench_upsert (id, name, value) VALUES (?, ?, ?)`
				upsertT = `INSERT INTO bench_upsert (id, name, value) VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET value = excluded.value`
			}

			mustExec(b, db, createT)

			// Pre-insert rows so every loop iteration hits the ON CONFLICT branch.
			tx, err := db.Begin()
			if err != nil {
				b.Fatalf("begin: %v", err)
			}
			ins, err := tx.Prepare(insertT)
			if err != nil {
				_ = tx.Rollback()
				b.Fatalf("prepare insert: %v", err)
			}
			for i := range upsertRows {
				if _, err := ins.Exec(int64(i+1), fmt.Sprintf("item-%04d", i), int64(i)); err != nil {
					_ = tx.Rollback()
					b.Fatalf("insert %d: %v", i, err)
				}
			}
			ins.Close()
			if err := tx.Commit(); err != nil {
				b.Fatalf("commit: %v", err)
			}

			upsertStmt, err := db.Prepare(upsertT)
			if err != nil {
				b.Fatalf("prepare upsert: %v", err)
			}
			defer upsertStmt.Close()

			b.ResetTimer()
			for i := range b.N {
				id := int64(i%upsertRows + 1)
				newVal := int64(i)
				var execErr error
				switch d.name {
				case "minisql":
					_, execErr = upsertStmt.Exec(id, fmt.Sprintf("item-%04d", id-1), newVal, newVal)
				default:
					_, execErr = upsertStmt.Exec(id, fmt.Sprintf("item-%04d", id-1), newVal)
				}
				if execErr != nil {
					b.Fatalf("upsert: %v", execErr)
				}
			}
		})
	}
}
