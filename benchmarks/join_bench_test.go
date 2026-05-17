//go:build bench

package benchmarks

import (
	"fmt"
	"testing"
)

const (
	joinDeptN = 100    // small table
	joinEmpN  = 10_000 // large table joining to departments
	joinPremN = 100    // premium subset for LEFT JOIN (1% match rate)
)

// BenchmarkJoin_Inner_SmallLarge measures an INNER JOIN between a large
// employees table (10K rows) and a small departments table (100 rows).
// Exercises the hash-join build+probe path.
func BenchmarkJoin_Inner_SmallLarge(b *testing.B) {
	for _, d := range drivers {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openDB(b, d)
			defer cleanup()

			var (
				createDept string
				insertDept string
				createEmp  string
				insertEmp  string
				query      string
			)
			switch d.name {
			case "minisql":
				createDept = `create table "bench_dept" (id int8 primary key autoincrement, name varchar(100))`
				insertDept = `insert into "bench_dept" (name) values (?)`
				createEmp  = `create table "bench_emp" (id int8 primary key autoincrement, dept_id int8, name varchar(100), salary int8)`
				insertEmp  = `insert into "bench_emp" (dept_id, name, salary) values (?, ?, ?)`
				query      = `select e.id, e.name, d.name from "bench_emp" AS e inner join "bench_dept" AS d on e.dept_id = d.id`
			default:
				createDept = `CREATE TABLE bench_dept (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)`
				insertDept = `INSERT INTO bench_dept (name) VALUES (?)`
				createEmp  = `CREATE TABLE bench_emp (id INTEGER PRIMARY KEY AUTOINCREMENT, dept_id INTEGER, name TEXT, salary INTEGER)`
				insertEmp  = `INSERT INTO bench_emp (dept_id, name, salary) VALUES (?, ?, ?)`
				query      = `SELECT e.id, e.name, d.name FROM bench_emp AS e INNER JOIN bench_dept AS d ON e.dept_id = d.id`
			}

			mustExec(b, db, createDept)
			mustExec(b, db, createEmp)

			for i := range joinDeptN {
				mustExec(b, db, insertDept, fmt.Sprintf("dept-%03d", i))
			}

			tx, err := db.Begin()
			if err != nil {
				b.Fatalf("begin: %v", err)
			}
			ins, err := tx.Prepare(insertEmp)
			if err != nil {
				_ = tx.Rollback()
				b.Fatalf("prepare insert emp: %v", err)
			}
			for i := range joinEmpN {
				deptID := (i % joinDeptN) + 1
				if _, err := ins.Exec(deptID, fmt.Sprintf("emp-%06d", i), int64(30_000+i%50_000)); err != nil {
					_ = tx.Rollback()
					b.Fatalf("insert emp %d: %v", i, err)
				}
			}
			ins.Close()
			if err := tx.Commit(); err != nil {
				b.Fatalf("commit: %v", err)
			}

			b.ResetTimer()
			for range b.N {
				rows, err := db.Query(query)
				if err != nil {
					b.Fatalf("query: %v", err)
				}
				n := 0
				for rows.Next() {
					var (
						id       int64
						empName  string
						deptName string
					)
					if err := rows.Scan(&id, &empName, &deptName); err != nil {
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

// BenchmarkJoin_Left_UnmatchedRows measures a LEFT JOIN where ~99% of outer
// rows have no matching inner row.  Exercises the null-row-emit path.
func BenchmarkJoin_Left_UnmatchedRows(b *testing.B) {
	for _, d := range drivers {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openDB(b, d)
			defer cleanup()
			seedRows(b, db, d, seedN)

			var (
				createPrem string
				insertPrem string
				query      string
			)
			switch d.name {
			case "minisql":
				createPrem = `create table "bench_premium" (emp_id int8 primary key, tier varchar(20))`
				insertPrem = `insert into "bench_premium" (emp_id, tier) values (?, ?)`
				query      = `select r.id, r.name, p.tier from "bench_rows" AS r left join "bench_premium" AS p on r.id = p.emp_id`
			default:
				createPrem = `CREATE TABLE bench_premium (emp_id INTEGER PRIMARY KEY, tier TEXT)`
				insertPrem = `INSERT INTO bench_premium (emp_id, tier) VALUES (?, ?)`
				query      = `SELECT r.id, r.name, p.tier FROM bench_rows AS r LEFT JOIN bench_premium AS p ON r.id = p.emp_id`
			}

			mustExec(b, db, createPrem)
			// Only 1% of bench_rows have a premium entry.
			for i := range joinPremN {
				mustExec(b, db, insertPrem, int64(i*100+1), "gold")
			}

			b.ResetTimer()
			for range b.N {
				rows, err := db.Query(query)
				if err != nil {
					b.Fatalf("query: %v", err)
				}
				n := 0
				for rows.Next() {
					var (
						id   int64
						name string
						tier *string
					)
					if err := rows.Scan(&id, &name, &tier); err != nil {
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
