//go:build bench

package benchmarks

import (
	"fmt"
	"testing"
)

// BenchmarkForeignKey_Insert measures the per-insert overhead of a FOREIGN KEY
// parent-existence check.  100 parent rows are pre-seeded; each iteration
// inserts one child row and verifies the FK constraint is satisfied.
func BenchmarkForeignKey_Insert(b *testing.B) {
	for _, d := range drivers {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openDB(b, d)
			defer cleanup()

			// Enable FK enforcement — required for SQLite (off by default), a no-op for minisql.
			if d.name != "minisql" {
				mustExec(b, db, "PRAGMA foreign_keys = ON")
			}

			var (
				createParent string
				createChild  string
				insertParent string
				insertChild  string
			)
			switch d.name {
			case "minisql":
				createParent = `create table "bench_fk_parent" (id int8 primary key autoincrement, name varchar(100))`
				createChild  = `create table "bench_fk_child" (
					id int8 primary key autoincrement,
					parent_id int8 not null,
					data varchar(100),
					foreign key (parent_id) references "bench_fk_parent" (id)
				)`
				insertParent = `insert into "bench_fk_parent" (name) values (?)`
				insertChild  = `insert into "bench_fk_child" (parent_id, data) values (?, ?)`
			default:
				createParent = `CREATE TABLE bench_fk_parent (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)`
				createChild  = `CREATE TABLE bench_fk_child (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					parent_id INTEGER NOT NULL,
					data TEXT,
					FOREIGN KEY (parent_id) REFERENCES bench_fk_parent(id)
				)`
				insertParent = `INSERT INTO bench_fk_parent (name) VALUES (?)`
				insertChild  = `INSERT INTO bench_fk_child (parent_id, data) VALUES (?, ?)`
			}

			mustExec(b, db, createParent)
			mustExec(b, db, createChild)

			const parentN = 100
			for i := range parentN {
				mustExec(b, db, insertParent, fmt.Sprintf("parent-%03d", i))
			}

			childStmt, err := db.Prepare(insertChild)
			if err != nil {
				b.Fatalf("prepare insert child: %v", err)
			}
			defer childStmt.Close()

			b.ResetTimer()
			for i := range b.N {
				parentID := int64(i%parentN + 1)
				if _, err := childStmt.Exec(parentID, fmt.Sprintf("data-%d", i)); err != nil {
					b.Fatalf("insert child: %v", err)
				}
			}
		})
	}
}

// BenchmarkForeignKey_DeleteCascade measures the cost of deleting a parent row
// that has cascade-delete children.  Each iteration inserts a parent with 10
// children, then deletes the parent (triggering cascade deletion of all 10 children).
// The insert phase is excluded from timing via b.StopTimer / b.StartTimer.
func BenchmarkForeignKey_DeleteCascade(b *testing.B) {
	for _, d := range drivers {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openDB(b, d)
			defer cleanup()

			if d.name != "minisql" {
				mustExec(b, db, "PRAGMA foreign_keys = ON")
			}

			var (
				createParent string
				createChild  string
				insertParent string
				insertChild  string
				deleteParent string
			)
			switch d.name {
			case "minisql":
				// Explicit primary key (no autoincrement) so we control IDs directly.
				// LastInsertId() is not implemented in the minisql driver.
				createParent = `create table "bench_fkc_parent" (id int8 primary key, name varchar(100))`
				createChild  = `create table "bench_fkc_child" (
					id int8 primary key autoincrement,
					parent_id int8 not null,
					data varchar(100),
					foreign key (parent_id) references "bench_fkc_parent" (id) on delete cascade
				)`
				insertParent = `insert into "bench_fkc_parent" (id, name) values (?, ?)`
				insertChild  = `insert into "bench_fkc_child" (parent_id, data) values (?, ?)`
				deleteParent = `delete from "bench_fkc_parent" where id = ?`
			default:
				createParent = `CREATE TABLE bench_fkc_parent (id INTEGER PRIMARY KEY, name TEXT)`
				createChild  = `CREATE TABLE bench_fkc_child (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					parent_id INTEGER NOT NULL,
					data TEXT,
					FOREIGN KEY (parent_id) REFERENCES bench_fkc_parent(id) ON DELETE CASCADE
				)`
				insertParent = `INSERT INTO bench_fkc_parent (id, name) VALUES (?, ?)`
				insertChild  = `INSERT INTO bench_fkc_child (parent_id, data) VALUES (?, ?)`
				deleteParent = `DELETE FROM bench_fkc_parent WHERE id = ?`
			}

			mustExec(b, db, createParent)
			mustExec(b, db, createChild)

			insParent, err := db.Prepare(insertParent)
			if err != nil {
				b.Fatalf("prepare insert parent: %v", err)
			}
			defer insParent.Close()

			insChild, err := db.Prepare(insertChild)
			if err != nil {
				b.Fatalf("prepare insert child: %v", err)
			}
			defer insChild.Close()

			delParent, err := db.Prepare(deleteParent)
			if err != nil {
				b.Fatalf("prepare delete parent: %v", err)
			}
			defer delParent.Close()

			// Pre-seed all b.N parent rows with 10 children each so the timed
			// loop only measures the cascade delete, not the insert overhead.
			for i := range b.N {
				parentID := int64(i + 1)
				if _, err := insParent.Exec(parentID, fmt.Sprintf("parent-%d", i)); err != nil {
					b.Fatalf("pre-seed insert parent: %v", err)
				}
				for j := range 10 {
					if _, err := insChild.Exec(parentID, fmt.Sprintf("child-%d-%d", i, j)); err != nil {
						b.Fatalf("pre-seed insert child: %v", err)
					}
				}
			}

			b.ResetTimer()
			for i := range b.N {
				parentID := int64(i + 1)
				if _, err := delParent.Exec(parentID); err != nil {
					b.Fatalf("delete parent: %v", err)
				}
			}
		})
	}
}
