package minisql

import (
	"database/sql"
	"os"
	"testing"
)

// FuzzExecute feeds arbitrary SQL through the full parse + execute stack against
// a real temp-file database via the registered database/sql driver.  The only
// invariant enforced is no-panic: errors at any stage are expected and fine;
// crashes are bugs.
//
// This goes beyond FuzzParser (which only checks the parser) by exercising
// plan building, index selection, expression evaluation, and row encoding.
//
// Run for a fixed duration during development:
//
//	go test -fuzz=FuzzExecute -fuzztime=60s .
//
// Seeds are replayed as ordinary unit tests on every `go test` invocation.
func FuzzExecute(f *testing.F) {
	seeds := []string{
		// SELECT shapes
		`select * from "t"`,
		`select id, name from "t"`,
		`select id from "t" where id = 1`,
		`select id from "t" where id > 0 and id < 100`,
		`select count(*) from "t"`,
		`select id from "t" order by id asc`,
		`select id from "t" order by id desc limit 5`,
		`select id from "t" limit 10 offset 5`,
		`select distinct name from "t"`,
		`select distinct name from "t" order by name`,
		`select id from "t" where name like 'a%'`,
		`select id from "t" where name ilike 'A%'`,
		`select id from "t" where name is null`,
		`select id from "t" where name is not null`,
		`select id from "t" where id between 1 and 10`,
		`select id from "t" where id in (1, 2, 3)`,
		`select id from "t" where id not in (1, 2, 3)`,
		`select id from "t" where id = ?`,

		// Aggregates and GROUP BY / HAVING
		`select name, count(*) from "t" group by name`,
		`select name, sum(id) from "t" group by name`,
		`select name, count(*) from "t" group by name having count(*) > 1`,
		`select name, min(id), max(id) from "t" group by name`,

		// INSERT
		`insert into "t" (name) values ('hello')`,
		`insert into "t" (id, name) values (99, 'fuzz')`,
		`insert into "t" (id, name) values (1, 'a'), (2, 'b')`,
		`insert into "t" (name) values (null)`,
		`insert into "t" (id, name) values (1, 'x') on conflict do nothing`,
		`insert into "t" (id, name) values (1, 'x') on conflict do update set name = excluded.name`,
		`insert into "t" (name) values ('ret') returning id`,

		// UPDATE
		`update "t" set name = 'updated' where id = 1`,
		`update "t" set name = null where id = 2`,
		`update "t" set id = id + 1 where id < 5`,
		`update "t" set name = 'x' where id = 1 returning id, name`,

		// DELETE
		`delete from "t" where id = 1`,
		`delete from "t" where id > 50`,
		`delete from "t"`,
		`delete from "t" where id = 1 returning id`,

		// Subqueries / CTEs
		`select id from "t" where id in (select id from "t" where id < 5)`,
		`with c as (select id from "t" where id < 3) select id from c`,

		// EXPLAIN
		`explain select * from "t" where id = 1`,
		`explain analyze select * from "t" where id = 1`,

		// Edge / degenerate inputs
		``,
		`select`,
		`select *`,
		`select * from`,
		`insert into`,
		`update`,
		`delete`,
		`;`,
		`' OR '1'='1`,
		`AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA`,
		`select * from "t"; drop table "t"`,
	}

	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, query string) {
		tmp, err := os.CreateTemp("", "fuzz_execute_*.db")
		if err != nil {
			return
		}
		name := tmp.Name()
		tmp.Close()
		defer os.Remove(name)
		defer os.Remove(name + "-wal")

		db, err := sql.Open("minisql", name)
		if err != nil {
			return
		}
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		defer db.Close()

		// Create a simple table with a few rows so most query shapes have data to work with.
		if _, err = db.Exec(`create table "t" (id int8 primary key autoincrement, name varchar(255))`); err != nil {
			return
		}
		if _, err = db.Exec(`insert into "t" (name) values ('alpha'), ('beta'), ('gamma')`); err != nil {
			return
		}

		// Execute the fuzz input. Errors are fine; panics are bugs.
		rows, err := db.Query(query)
		if err != nil {
			return
		}
		// Drain the result set — execution is lazy; bugs may surface during iteration.
		for rows.Next() {
		}
		_ = rows.Close()
		_ = rows.Err()
	})
}
