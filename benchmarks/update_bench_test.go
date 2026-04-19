//go:build bench

package benchmarks

import (
	"fmt"
	"testing"
)

// BenchmarkUpdate_ByPK measures updating a single row located by primary key.
func BenchmarkUpdate_ByPK(b *testing.B) {
	for _, d := range drivers {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openDB(b, d)
			defer cleanup()
			seedRows(b, db, d, seedN)

			var query string
			switch d.name {
			case "minisql":
				query = `update "bench_rows" set name = ?, age = ? where id = ?`
			default:
				query = `UPDATE bench_rows SET name = ?, age = ? WHERE id = ?`
			}

			stmt, err := db.Prepare(query)
			if err != nil {
				b.Fatalf("prepare: %v", err)
			}
			defer stmt.Close()

			b.ResetTimer()
			for i := range b.N {
				id := (i % seedN) + 1
				name := fmt.Sprintf("updated-%d", i)
				if _, err := stmt.Exec(name, i%100, id); err != nil {
					b.Fatalf("exec: %v", err)
				}
			}
		})
	}
}
