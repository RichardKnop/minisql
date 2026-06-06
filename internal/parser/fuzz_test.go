package parser

import (
	"context"
	"testing"
)

// FuzzParser verifies that the SQL parser never panics on arbitrary input.
// The only invariant enforced is no-panic: the parser must return either a
// valid statement slice or an error, never crash.
//
// Run for a fixed time during development:
//
//	go test -fuzz=FuzzParser -fuzztime=60s ./internal/parser/
//
// Seeds are run as ordinary unit tests on every `go test` invocation.
func FuzzParser(f *testing.F) {
	seeds := []string{
		// SELECT — basic
		"SELECT * FROM users;",
		"SELECT id, name FROM users;",
		"SELECT DISTINCT id FROM users;",
		"SELECT * FROM users LIMIT 10;",
		"SELECT * FROM users LIMIT 10 OFFSET 20;",
		"SELECT * FROM users ORDER BY id ASC;",
		"SELECT * FROM users ORDER BY id DESC, name ASC;",

		// SELECT — WHERE
		"SELECT * FROM users WHERE id = 1;",
		"SELECT * FROM users WHERE id != 1;",
		"SELECT * FROM users WHERE id > 1 AND active = true;",
		"SELECT * FROM users WHERE status = 'active' OR status = 'pending';",
		"SELECT * FROM users WHERE id IN (1, 2, 3);",
		"SELECT * FROM users WHERE id NOT IN (1, 2, 3);",
		"SELECT * FROM users WHERE id BETWEEN 10 AND 20;",
		"SELECT * FROM users WHERE name LIKE 'Al%';",
		"SELECT * FROM users WHERE name ILIKE 'alice%';",
		"SELECT * FROM users WHERE nickname IS NULL;",
		"SELECT * FROM users WHERE email IS NOT NULL;",
		"SELECT * FROM users WHERE id = ?;",
		"SELECT * FROM users WHERE id > ? AND active = ?;",

		// SELECT — aggregates / GROUP BY / HAVING
		"SELECT COUNT(*) FROM orders;",
		"SELECT SUM(total_paid) FROM orders;",
		"SELECT user_id, COUNT(*) FROM orders GROUP BY user_id;",
		"SELECT user_id, SUM(total) FROM orders GROUP BY user_id HAVING SUM(total) > 100;",
		"SELECT user_id, COUNT(*) FROM orders GROUP BY user_id HAVING COUNT(*) >= ?;",

		// SELECT — JOINs
		"SELECT u.id, o.id FROM users AS u INNER JOIN orders AS o ON u.id = o.user_id;",
		"SELECT u.id, o.id FROM users AS u LEFT JOIN orders AS o ON u.id = o.user_id;",
		"SELECT u.id, o.id FROM users AS u RIGHT JOIN orders AS o ON u.id = o.user_id;",
		"SELECT u.id, o.id FROM users AS u FULL OUTER JOIN orders AS o ON u.id = o.user_id;",

		// SELECT — subqueries
		"SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total_paid > 100);",
		"SELECT * FROM (SELECT id, name FROM users) AS sub;",

		// SELECT — arithmetic and expressions
		"SELECT price * quantity AS total FROM order_lines;",
		"SELECT id, a + b * c FROM t;",
		"SELECT CAST(score AS INT8) FROM results;",

		// SELECT — JSON operators
		"SELECT payload -> 'name' FROM events;",
		"SELECT payload ->> 'uid' FROM events;",
		"SELECT * FROM events WHERE payload ->> 'type' = 'login';",

		// SELECT — CASE WHEN
		"SELECT CASE WHEN score >= 90 THEN 'A' ELSE 'B' END FROM results;",
		"SELECT CASE status WHEN 1 THEN 'active' WHEN 2 THEN 'pending' ELSE 'other' END FROM t;",

		// SELECT — CTEs
		"WITH cte AS (SELECT id, name FROM users) SELECT cte.name FROM cte;",
		"WITH a AS (SELECT id FROM t), b AS (SELECT id FROM a) SELECT * FROM b;",

		// SELECT — window functions
		"SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM users;",
		"SELECT id, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) FROM employees;",

		// SELECT — UNION
		"SELECT id FROM a UNION SELECT id FROM b;",
		"SELECT id FROM a UNION ALL SELECT id FROM b;",

		// SELECT — EXPLAIN
		"EXPLAIN SELECT * FROM users WHERE id = 1;",
		"EXPLAIN ANALYZE SELECT * FROM users WHERE id = 1;",

		// INSERT
		"INSERT INTO users (name, email) VALUES ('alice', 'alice@example.com');",
		"INSERT INTO users (id, name) VALUES (1, 'bob'), (2, 'carol');",
		"INSERT INTO users (name) VALUES (?);",
		"INSERT INTO users (name) VALUES (-42);",
		"INSERT INTO users (name) VALUES (-3.14);",
		"INSERT INTO users (name) VALUES (NULL);",
		"INSERT INTO users (id, name) VALUES (1, 'x') ON CONFLICT DO NOTHING;",
		"INSERT INTO users (id, name) VALUES (1, 'x') ON CONFLICT DO UPDATE SET name = EXCLUDED.name;",
		"INSERT INTO users (id, name) SELECT id, name FROM staging;",
		"INSERT INTO users (name) VALUES ('alice') RETURNING id;",

		// UPDATE
		"UPDATE users SET name = 'bob' WHERE id = 1;",
		"UPDATE users SET name = ?, active = ? WHERE id = ?;",
		"UPDATE users SET score = score + 1 WHERE active = true;",
		"UPDATE users SET name = 'x' WHERE id = 1 RETURNING id, name;",

		// DELETE
		"DELETE FROM users WHERE id = 1;",
		"DELETE FROM users WHERE id = ?;",
		"DELETE FROM users;",
		"DELETE FROM users WHERE id = 1 RETURNING id;",

		// CREATE TABLE
		"CREATE TABLE t (id INT8 PRIMARY KEY AUTOINCREMENT, name VARCHAR(255) NOT NULL);",
		"CREATE TABLE t (id INT8, val DOUBLE, ts TIMESTAMP DEFAULT NOW());",
		"CREATE TABLE t (id INT8 PRIMARY KEY, ref INT8 REFERENCES other(id));",
		"CREATE TABLE IF NOT EXISTS t (id INT8);",
		"CREATE TABLE t (id INT8, payload JSON, embedding VECTOR(128));",

		// CREATE INDEX
		"CREATE INDEX idx ON users (email);",
		"CREATE UNIQUE INDEX idx ON users (email);",
		"CREATE INDEX idx ON users (active) WHERE active = true;",
		"CREATE FULLTEXT INDEX idx ON articles (body);",
		"CREATE INVERTED INDEX idx ON events (payload);",
		"CREATE HNSW INDEX idx ON documents (embedding);",
		"CREATE HNSW INDEX idx ON documents (embedding) WITH (m = 16, ef_construction = 200);",

		// DROP
		"DROP TABLE users;",
		"DROP TABLE IF EXISTS users;",
		"DROP INDEX idx;",
		"DROP INDEX IF EXISTS idx;",

		// ALTER TABLE
		"ALTER TABLE users ADD COLUMN bio TEXT;",
		"ALTER TABLE users DROP COLUMN bio;",
		"ALTER TABLE users RENAME COLUMN bio TO biography;",
		"ALTER TABLE users RENAME TO members;",

		// TRUNCATE
		"TRUNCATE TABLE users;",

		// PRAGMA
		"PRAGMA quick_check;",
		"PRAGMA integrity_check;",
		"PRAGMA wal_checkpoint;",
		"PRAGMA synchronous = normal;",
		"PRAGMA parallel_scan = on;",
		"PRAGMA foreign_keys = on;",

		// Edge cases: empty-ish / degenerate inputs
		"",
		";",
		"   ",
		"SELECT",
		"SELECT *",
		"SELECT * FROM",
		"WHERE id = 1",
		"INSERT INTO",
		"DROP",
		"--",
		"' OR '1'='1",
		"SELECT * FROM users; DROP TABLE users;",
		"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
	}

	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, sql string) {
		p := New()
		// The only contract: Parse must never panic.
		// Errors are expected and fine; panics are bugs.
		_, _ = p.Parse(context.Background(), sql)
	})
}
