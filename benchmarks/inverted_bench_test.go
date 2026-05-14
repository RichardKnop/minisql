//go:build bench

package benchmarks

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/brianvoe/gofakeit/v7"
)

const invertedSeedN = 1_000

type invertedBenchDriver struct {
	name       string
	driverName string
	dsn        func(path string) string
	afterOpen  func(testing.TB, *sql.DB)
}

var invertedBenchDrivers = []invertedBenchDriver{
	{
		name:       "minisql",
		driverName: "minisql",
		dsn:        func(path string) string { return path },
	},
	{
		name:       "sqlite",
		driverName: "sqlite",
		dsn:        func(path string) string { return path + "?_pragma=journal_mode(WAL)" },
	},
}

var minisqlOnlyInvertedBenchDrivers = []invertedBenchDriver{invertedBenchDrivers[0]}

// BenchmarkFullText_BuildIndex measures building the full-text structure over
// an already populated document table.
func BenchmarkFullText_BuildIndex(b *testing.B) {
	for _, d := range fullTextComparableDrivers(b) {
		b.Run(d.name, func(b *testing.B) {
			for i := range b.N {
				db, cleanup := openInvertedBenchDB(b, d)
				createFullTextTable(b, db, d)
				seedFullTextRows(b, db, d, invertedSeedN, int64(i))

				b.StartTimer()
				createFullTextIndex(b, db, d)
				b.StopTimer()

				cleanup()
			}
			b.ReportMetric(float64(invertedSeedN), "docs/op")
		})
	}
}

// BenchmarkFullText_Insert_WithIndex measures full-text index maintenance for
// one inserted document per transaction.
func BenchmarkFullText_Insert_WithIndex(b *testing.B) {
	for _, d := range fullTextComparableDrivers(b) {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openInvertedBenchDB(b, d)
			defer cleanup()
			createFullTextTable(b, db, d)
			createFullTextIndex(b, db, d)
			stmt := prepareFullTextInsert(b, db, d)
			defer stmt.Close()

			faker := gofakeit.New(11)
			b.ResetTimer()
			for i := range b.N {
				title, body := generatedFullTextDoc(faker, i)
				if _, err := stmt.Exec(i+1, title, body); err != nil {
					b.Fatalf("insert full-text row: %v", err)
				}
			}
			b.ReportMetric(1, "docs/op")
		})
	}
}

// BenchmarkFullText_Search_SingleTerm measures indexed token lookup at rare,
// medium, and common selectivities.
func BenchmarkFullText_Search_SingleTerm(b *testing.B) {
	cases := []struct {
		name      string
		query     string
		wantRows  int
		sqliteFts string
	}{
		{name: "rare", query: "rare0001", sqliteFts: "rare0001", wantRows: 1},
		{name: "medium", query: "cohort17", sqliteFts: "cohort17", wantRows: invertedSeedN / 100},
		{name: "common", query: "common", sqliteFts: "common", wantRows: invertedSeedN},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			for _, d := range fullTextComparableDrivers(b) {
				b.Run(d.name, func(b *testing.B) {
					db, cleanup := openSeededFullTextBenchDB(b, d, invertedSeedN)
					defer cleanup()
					query := fullTextSearchSQL(d, tc.query, tc.sqliteFts)

					b.ResetTimer()
					for range b.N {
						n := queryCount(b, db, query)
						if n != tc.wantRows {
							b.Fatalf("expected %d rows, got %d", tc.wantRows, n)
						}
					}
					b.ReportMetric(float64(tc.wantRows), "matches/op")
				})
			}
		})
	}
}

// BenchmarkFullText_Search_MultiTermAND measures posting-list intersection.
func BenchmarkFullText_Search_MultiTermAND(b *testing.B) {
	for _, d := range fullTextComparableDrivers(b) {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openSeededFullTextBenchDB(b, d, invertedSeedN)
			defer cleanup()
			query := fullTextSearchSQL(d, "common cohort17", "common cohort17")

			b.ResetTimer()
			for range b.N {
				n := queryCount(b, db, query)
				if n != invertedSeedN/100 {
					b.Fatalf("expected %d rows, got %d", invertedSeedN/100, n)
				}
			}
			b.ReportMetric(float64(invertedSeedN/100), "matches/op")
		})
	}
}

// BenchmarkFullText_Search_Phrase measures positional phrase filtering.
func BenchmarkFullText_Search_Phrase(b *testing.B) {
	for _, d := range fullTextComparableDrivers(b) {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openSeededFullTextBenchDB(b, d, invertedSeedN)
			defer cleanup()
			query := fullTextSearchSQL(d, `"alpha beta"`, `"alpha beta"`)

			b.ResetTimer()
			for range b.N {
				n := queryCount(b, db, query)
				if n != invertedSeedN/10 {
					b.Fatalf("expected %d rows, got %d", invertedSeedN/10, n)
				}
			}
			b.ReportMetric(float64(invertedSeedN/10), "matches/op")
		})
	}
}

// BenchmarkFullText_Update_WithIndex measures replacing indexed text while the
// full-text index removes old postings and inserts new ones.
func BenchmarkFullText_Update_WithIndex(b *testing.B) {
	for _, d := range fullTextComparableDrivers(b) {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openSeededFullTextBenchDB(b, d, 1_000)
			defer cleanup()
			stmt := prepareFullTextUpdate(b, db, d)
			defer stmt.Close()
			faker := gofakeit.New(12)

			b.ResetTimer()
			for i := range b.N {
				id := (i % 1_000) + 1
				body := generatedUpdatedFullTextDoc(faker, i)
				if _, err := stmt.Exec(body, id); err != nil {
					b.Fatalf("update full-text row: %v", err)
				}
			}
		})
	}
}

// BenchmarkFullText_Delete_WithIndex measures deleting indexed documents.
func BenchmarkFullText_Delete_WithIndex(b *testing.B) {
	for _, d := range fullTextComparableDrivers(b) {
		b.Run(d.name, func(b *testing.B) {
			db, cleanup := openInvertedBenchDB(b, d)
			defer cleanup()
			createFullTextTable(b, db, d)
			seedFullTextRows(b, db, d, b.N, 13)
			createFullTextIndex(b, db, d)
			stmt := prepareFullTextDelete(b, db, d)
			defer stmt.Close()

			b.ResetTimer()
			for i := range b.N {
				if _, err := stmt.Exec(i + 1); err != nil {
					b.Fatalf("delete full-text row: %v", err)
				}
			}
		})
	}
}

// BenchmarkJSONInverted_BuildIndex measures building MiniSQL's JSON inverted
// index over an already populated JSON table.
func BenchmarkJSONInverted_BuildIndex(b *testing.B) {
	for _, d := range minisqlOnlyInvertedBenchDrivers {
		b.Run(d.name+"_indexed", func(b *testing.B) {
			for i := range b.N {
				db, cleanup := openInvertedBenchDB(b, d)
				createJSONTable(b, db, d)
				seedJSONRows(b, db, d, invertedSeedN, int64(i))

				b.StartTimer()
				mustExec(b, db, `create inverted index "idx_events_payload" on "events_json" (payload)`)
				b.StopTimer()

				cleanup()
			}
			b.ReportMetric(float64(invertedSeedN), "docs/op")
		})
	}
}

// BenchmarkJSONInverted_Insert_WithIndex measures JSON inverted-index
// maintenance for one inserted event per transaction.
func BenchmarkJSONInverted_Insert_WithIndex(b *testing.B) {
	for _, d := range minisqlOnlyInvertedBenchDrivers {
		b.Run(d.name+"_indexed", func(b *testing.B) {
			db, cleanup := openInvertedBenchDB(b, d)
			defer cleanup()
			createJSONTable(b, db, d)
			mustExec(b, db, `create inverted index "idx_events_payload" on "events_json" (payload)`)
			stmt := prepareJSONInsert(b, db, d)
			defer stmt.Close()
			faker := gofakeit.New(21)

			b.ResetTimer()
			for i := range b.N {
				if _, err := stmt.Exec(i+1, fmt.Sprintf("event-%06d", i), generatedJSONPayload(faker, i)); err != nil {
					b.Fatalf("insert JSON row: %v", err)
				}
			}
		})
	}
}

// BenchmarkJSONInverted_Contains_KeyValue compares indexed MiniSQL JSON
// containment with MiniSQL sequential JSON_CONTAINS and contextual SQLite JSON
// baselines.
func BenchmarkJSONInverted_Contains_KeyValue(b *testing.B) {
	benchJSONContains(b, "key_value", `{"type":"click"}`, `json_extract(payload, '$.type') = 'click'`, jsonClickRows(invertedSeedN))
}

// BenchmarkJSONInverted_Contains_ObjectSubset measures a multi-term JSON
// containment predicate.
func BenchmarkJSONInverted_Contains_ObjectSubset(b *testing.B) {
	benchJSONContains(b, "object_subset", `{"type":"click","tags":["web"]}`, `json_extract(payload, '$.type') = 'click' AND instr(payload, '"web"') > 0`, jsonClickRows(invertedSeedN))
}

// BenchmarkJSONInverted_Update_WithIndex measures replacing JSON payloads while
// the inverted index removes old terms and inserts new ones.
func BenchmarkJSONInverted_Update_WithIndex(b *testing.B) {
	for _, d := range minisqlOnlyInvertedBenchDrivers {
		b.Run(d.name+"_indexed", func(b *testing.B) {
			db, cleanup := openSeededJSONBenchDB(b, d, 1_000, true)
			defer cleanup()
			stmt := prepareJSONUpdate(b, db, d)
			defer stmt.Close()
			faker := gofakeit.New(22)

			b.ResetTimer()
			for i := range b.N {
				id := (i % 1_000) + 1
				if _, err := stmt.Exec(generatedUpdatedJSONPayload(faker, i), id); err != nil {
					b.Fatalf("update JSON row: %v", err)
				}
			}
		})
	}
}

// BenchmarkJSONInverted_Delete_WithIndex measures deleting rows from a table
// with a JSON inverted index.
func BenchmarkJSONInverted_Delete_WithIndex(b *testing.B) {
	for _, d := range minisqlOnlyInvertedBenchDrivers {
		b.Run(d.name+"_indexed", func(b *testing.B) {
			db, cleanup := openSeededJSONBenchDB(b, d, b.N, true)
			defer cleanup()
			stmt := prepareJSONDelete(b, db, d)
			defer stmt.Close()

			b.ResetTimer()
			for i := range b.N {
				if _, err := stmt.Exec(i + 1); err != nil {
					b.Fatalf("delete JSON row: %v", err)
				}
			}
		})
	}
}

func benchJSONContains(b *testing.B, name, minisqlPredicateJSON, sqlitePredicate string, wantRows int) {
	b.Run(name, func(b *testing.B) {
		for _, tc := range []struct {
			name    string
			driver  invertedBenchDriver
			indexed bool
			query   string
		}{
			{
				name:    "minisql_indexed",
				driver:  invertedBenchDrivers[0],
				indexed: true,
				query:   fmt.Sprintf(`select count(*) from "events_json" where JSON_CONTAINS(payload, '%s')`, minisqlPredicateJSON),
			},
			{
				name:    "minisql_sequential",
				driver:  invertedBenchDrivers[0],
				indexed: false,
				query:   fmt.Sprintf(`select count(*) from "events_json" where JSON_CONTAINS(payload, '%s')`, minisqlPredicateJSON),
			},
			{
				name:    "sqlite_json_scan",
				driver:  invertedBenchDrivers[1],
				indexed: false,
				query:   fmt.Sprintf(`SELECT count(*) FROM events_json WHERE %s`, sqlitePredicate),
			},
			{
				name:    "sqlite_json_expr_index",
				driver:  invertedBenchDrivers[1],
				indexed: true,
				query:   fmt.Sprintf(`SELECT count(*) FROM events_json WHERE %s`, sqlitePredicate),
			},
		} {
			b.Run(tc.name, func(b *testing.B) {
				db, cleanup := openSeededJSONBenchDB(b, tc.driver, invertedSeedN, tc.indexed)
				defer cleanup()
				b.ResetTimer()
				for range b.N {
					n := queryCount(b, db, tc.query)
					if n != wantRows {
						b.Fatalf("expected %d rows, got %d", wantRows, n)
					}
				}
				b.ReportMetric(float64(wantRows), "matches/op")
			})
		}
	})
}

func fullTextComparableDrivers(b *testing.B) []invertedBenchDriver {
	b.Helper()
	drivers := []invertedBenchDriver{invertedBenchDrivers[0]}
	sqliteDriver := invertedBenchDrivers[1]
	db, cleanup := openInvertedBenchDB(b, sqliteDriver)
	defer cleanup()
	if _, err := db.Exec(`CREATE VIRTUAL TABLE fts_probe USING fts5(body);`); err == nil {
		drivers = append(drivers, sqliteDriver)
	}
	return drivers
}

func openSeededFullTextBenchDB(b testing.TB, d invertedBenchDriver, n int) (*sql.DB, func()) {
	b.Helper()
	db, cleanup := openInvertedBenchDB(b, d)
	createFullTextTable(b, db, d)
	seedFullTextRows(b, db, d, n, 1)
	createFullTextIndex(b, db, d)
	return db, cleanup
}

func openSeededJSONBenchDB(b testing.TB, d invertedBenchDriver, n int, indexed bool) (*sql.DB, func()) {
	b.Helper()
	db, cleanup := openInvertedBenchDB(b, d)
	createJSONTable(b, db, d)
	seedJSONRows(b, db, d, n, 2)
	if indexed {
		switch d.name {
		case "minisql":
			mustExec(b, db, `create inverted index "idx_events_payload" on "events_json" (payload)`)
		case "sqlite":
			mustExec(b, db, `CREATE INDEX idx_events_payload_type ON events_json(json_extract(payload, '$.type'))`)
		}
	}
	return db, cleanup
}

func openInvertedBenchDB(t testing.TB, d invertedBenchDriver) (*sql.DB, func()) {
	t.Helper()
	dbDriver := dbDriver{
		name:        d.name,
		driverName:  d.driverName,
		dsn:         d.dsn,
		createTable: `create table "__bench_placeholder" (id int8 primary key)`,
	}
	db, cleanup := openDB(t, dbDriver)
	mustExec(t, db, `drop table "__bench_placeholder"`)
	if d.afterOpen != nil {
		d.afterOpen(t, db)
	}
	return db, cleanup
}

func createFullTextTable(t testing.TB, db *sql.DB, d invertedBenchDriver) {
	t.Helper()
	switch d.name {
	case "minisql":
		mustExec(t, db, `create table "articles_fts" (id int8 primary key, title varchar(255), body text not null)`)
	case "sqlite":
		mustExec(t, db, `CREATE TABLE articles_fts (id INTEGER PRIMARY KEY, title TEXT, body TEXT NOT NULL)`)
	}
}

func createFullTextIndex(t testing.TB, db *sql.DB, d invertedBenchDriver) {
	t.Helper()
	switch d.name {
	case "minisql":
		mustExec(t, db, `create fulltext index "idx_articles_body" on "articles_fts" (body) with (tokenizer = 'simple')`)
	case "sqlite":
		mustExec(t, db, `CREATE VIRTUAL TABLE articles_fts_index USING fts5(body, content='articles_fts', content_rowid='id')`)
		mustExec(t, db, `INSERT INTO articles_fts_index(articles_fts_index) VALUES ('rebuild')`)
		mustExec(t, db, `CREATE TRIGGER articles_fts_ai AFTER INSERT ON articles_fts BEGIN INSERT INTO articles_fts_index(rowid, body) VALUES (new.id, new.body); END`)
		mustExec(t, db, `CREATE TRIGGER articles_fts_ad AFTER DELETE ON articles_fts BEGIN INSERT INTO articles_fts_index(articles_fts_index, rowid, body) VALUES('delete', old.id, old.body); END`)
		mustExec(t, db, `CREATE TRIGGER articles_fts_au AFTER UPDATE ON articles_fts BEGIN INSERT INTO articles_fts_index(articles_fts_index, rowid, body) VALUES('delete', old.id, old.body); INSERT INTO articles_fts_index(rowid, body) VALUES (new.id, new.body); END`)
	}
}

func seedFullTextRows(t testing.TB, db *sql.DB, d invertedBenchDriver, n int, seed int64) {
	t.Helper()
	stmt := prepareFullTextInsert(t, db, d)
	defer stmt.Close()
	faker := gofakeit.New(uint64(seed))
	for i := range n {
		title, body := generatedFullTextDoc(faker, i)
		if _, err := stmt.Exec(i+1, title, body); err != nil {
			t.Fatalf("seed full-text row %d: %v", i, err)
		}
	}
}

func prepareFullTextInsert(t testing.TB, db *sql.DB, d invertedBenchDriver) *sql.Stmt {
	t.Helper()
	switch d.name {
	case "minisql":
		stmt, err := db.Prepare(`insert into "articles_fts" (id, title, body) values (?, ?, ?)`)
		if err != nil {
			t.Fatalf("prepare full-text insert: %v", err)
		}
		return stmt
	default:
		stmt, err := db.Prepare(`INSERT INTO articles_fts (id, title, body) VALUES (?, ?, ?)`)
		if err != nil {
			t.Fatalf("prepare full-text insert: %v", err)
		}
		return stmt
	}
}

func prepareFullTextUpdate(t testing.TB, db *sql.DB, d invertedBenchDriver) *sql.Stmt {
	t.Helper()
	switch d.name {
	case "minisql":
		stmt, err := db.Prepare(`update "articles_fts" set body = ? where id = ?`)
		if err != nil {
			t.Fatalf("prepare full-text update: %v", err)
		}
		return stmt
	default:
		stmt, err := db.Prepare(`UPDATE articles_fts SET body = ? WHERE id = ?`)
		if err != nil {
			t.Fatalf("prepare full-text update: %v", err)
		}
		return stmt
	}
}

func prepareFullTextDelete(t testing.TB, db *sql.DB, d invertedBenchDriver) *sql.Stmt {
	t.Helper()
	switch d.name {
	case "minisql":
		stmt, err := db.Prepare(`delete from "articles_fts" where id = ?`)
		if err != nil {
			t.Fatalf("prepare full-text delete: %v", err)
		}
		return stmt
	default:
		stmt, err := db.Prepare(`DELETE FROM articles_fts WHERE id = ?`)
		if err != nil {
			t.Fatalf("prepare full-text delete: %v", err)
		}
		return stmt
	}
}

func fullTextSearchSQL(d invertedBenchDriver, minisqlQuery, sqliteQuery string) string {
	switch d.name {
	case "minisql":
		return fmt.Sprintf(`select count(*) from "articles_fts" where MATCH(body, '%s')`, strings.ReplaceAll(minisqlQuery, `'`, `''`))
	default:
		return fmt.Sprintf(`SELECT count(*) FROM articles_fts_index WHERE articles_fts_index MATCH '%s'`, strings.ReplaceAll(sqliteQuery, `'`, `''`))
	}
}

func createJSONTable(t testing.TB, db *sql.DB, d invertedBenchDriver) {
	t.Helper()
	switch d.name {
	case "minisql":
		mustExec(t, db, `create table "events_json" (id int8 primary key, name varchar(255), payload json not null)`)
	case "sqlite":
		mustExec(t, db, `CREATE TABLE events_json (id INTEGER PRIMARY KEY, name TEXT, payload TEXT NOT NULL)`)
	}
}

func seedJSONRows(t testing.TB, db *sql.DB, d invertedBenchDriver, n int, seed int64) {
	t.Helper()
	stmt := prepareJSONInsert(t, db, d)
	defer stmt.Close()
	faker := gofakeit.New(uint64(seed))
	for i := range n {
		if _, err := stmt.Exec(i+1, fmt.Sprintf("event-%06d", i), generatedJSONPayload(faker, i)); err != nil {
			t.Fatalf("seed JSON row %d: %v", i, err)
		}
	}
}

func prepareJSONInsert(t testing.TB, db *sql.DB, d invertedBenchDriver) *sql.Stmt {
	t.Helper()
	switch d.name {
	case "minisql":
		stmt, err := db.Prepare(`insert into "events_json" (id, name, payload) values (?, ?, ?)`)
		if err != nil {
			t.Fatalf("prepare JSON insert: %v", err)
		}
		return stmt
	default:
		stmt, err := db.Prepare(`INSERT INTO events_json (id, name, payload) VALUES (?, ?, ?)`)
		if err != nil {
			t.Fatalf("prepare JSON insert: %v", err)
		}
		return stmt
	}
}

func prepareJSONUpdate(t testing.TB, db *sql.DB, d invertedBenchDriver) *sql.Stmt {
	t.Helper()
	switch d.name {
	case "minisql":
		stmt, err := db.Prepare(`update "events_json" set payload = ? where id = ?`)
		if err != nil {
			t.Fatalf("prepare JSON update: %v", err)
		}
		return stmt
	default:
		stmt, err := db.Prepare(`UPDATE events_json SET payload = ? WHERE id = ?`)
		if err != nil {
			t.Fatalf("prepare JSON update: %v", err)
		}
		return stmt
	}
}

func prepareJSONDelete(t testing.TB, db *sql.DB, d invertedBenchDriver) *sql.Stmt {
	t.Helper()
	switch d.name {
	case "minisql":
		stmt, err := db.Prepare(`delete from "events_json" where id = ?`)
		if err != nil {
			t.Fatalf("prepare JSON delete: %v", err)
		}
		return stmt
	default:
		stmt, err := db.Prepare(`DELETE FROM events_json WHERE id = ?`)
		if err != nil {
			t.Fatalf("prepare JSON delete: %v", err)
		}
		return stmt
	}
}

func generatedFullTextDoc(faker *gofakeit.Faker, i int) (string, string) {
	title := fmt.Sprintf("Doc %06d %s", i, faker.Word())
	terms := []string{
		"common",
		fmt.Sprintf("cohort%02d", i%100),
		fmt.Sprintf("rare%04d", i),
		"database",
		"storage",
		faker.Word(),
		faker.Word(),
	}
	if i%10 == 0 {
		terms = append(terms, "alpha", "beta")
	}
	return title, strings.Join(terms, " ")
}

func generatedUpdatedFullTextDoc(faker *gofakeit.Faker, i int) string {
	return fmt.Sprintf("updated common cohort%02d revision%04d %s %s", i%100, i, faker.Word(), faker.Word())
}

func generatedJSONPayload(faker *gofakeit.Faker, i int) string {
	eventType := "click"
	if i%3 == 1 {
		eventType = "view"
	} else if i%3 == 2 {
		eventType = "purchase"
	}
	tags := `["api","batch"]`
	if eventType == "click" {
		tags = `["web","batch"]`
	}
	return fmt.Sprintf(
		`{"type":"%s","status":"%s","tags":%s,"user":{"id":"u%06d","country":"%s"},"active":%t}`,
		eventType,
		[]string{"new", "queued", "done"}[i%3],
		tags,
		i%1_000,
		[]string{"GB", "US", "DE", "FR"}[i%4],
		i%2 == 0,
	)
}

func generatedUpdatedJSONPayload(faker *gofakeit.Faker, i int) string {
	return fmt.Sprintf(
		`{"type":"updated","status":"%s","tags":["maintenance"],"user":{"id":"u%06d"},"active":%t}`,
		[]string{"queued", "done"}[i%2],
		i%1_000,
		i%2 == 0,
	)
}

func queryCount(t testing.TB, db *sql.DB, query string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(query).Scan(&count); err != nil {
		t.Fatalf("query count %q: %v", query, err)
	}
	return count
}

func jsonClickRows(n int) int {
	return (n + 2) / 3
}
