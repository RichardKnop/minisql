package e2etests

import (
	"strings"

	"github.com/RichardKnop/minisql/internal/minisql"
)

// overflowStr builds an ASCII string of exactly n bytes.
func overflowStr(n int) string {
	return strings.Repeat("x", n)
}

// TestOverflowPages verifies that large text/varchar values are correctly
// written to and read from overflow pages across the relevant size boundaries:
//
//   - inline   : ≤ 512 bytes stored directly in the leaf cell
//   - single-page overflow: 513 – 4 083 bytes (one overflow page)
//   - multi-page overflow : 4 084 – 65 328 bytes (two or more overflow pages)
func (s *TestSuite) TestOverflowPages() {
	_, err := s.db.Exec(`create table articles (
		id      int8 primary key autoincrement,
		title   varchar(600)  not null,
		body    text          not null,
		summary varchar(4200) not null
	);`)
	s.Require().NoError(err)

	// ── VARCHAR: just below inline threshold ────────────────────────────────

	s.Run("VARCHAR_inline_boundary_below", func() {
		// 512 bytes — stored fully inline, no overflow page.
		val := overflowStr(minisql.MaxInlineVarchar)
		_, err := s.db.Exec(`insert into articles (title, body, summary) values (?, ?, ?)`,
			val, "body", "summary")
		s.Require().NoError(err)

		rows, err := s.db.Query(`select title from articles where title = ?`, val)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var got string
		s.Require().NoError(rows.Scan(&got))
		s.Equal(val, got)
	})

	// ── VARCHAR: just above inline threshold ────────────────────────────────

	s.Run("VARCHAR_single_overflow_page", func() {
		// 513 bytes — crosses the inline threshold; written to one overflow page.
		val := overflowStr(minisql.MaxInlineVarchar + 1)
		_, err := s.db.Exec(`insert into articles (title, body, summary) values (?, ?, ?)`,
			val, "body", "summary")
		s.Require().NoError(err)

		rows, err := s.db.Query(`select title from articles where title = ?`, val)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var got string
		s.Require().NoError(rows.Scan(&got))
		s.Equal(val, got)
	})

	// ── TEXT: always uses overflow pages ────────────────────────────────────

	s.Run("TEXT_small_value", func() {
		// TEXT always goes through overflow, even for short strings.
		val := "hello overflow"
		_, err := s.db.Exec(`insert into articles (title, body, summary) values (?, ?, ?)`,
			"t", val, "s")
		s.Require().NoError(err)

		rows, err := s.db.Query(`select body from articles where body = ?`, val)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var got string
		s.Require().NoError(rows.Scan(&got))
		s.Equal(val, got)
	})

	s.Run("TEXT_single_overflow_page", func() {
		// 3 000 bytes — fits on one overflow page (capacity 4 083 bytes).
		val := overflowStr(3000)
		_, err := s.db.Exec(`insert into articles (title, body, summary) values (?, ?, ?)`,
			"t", val, "s")
		s.Require().NoError(err)

		rows, err := s.db.Query(`select body from articles where body = ?`, val)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var got string
		s.Require().NoError(rows.Scan(&got))
		s.Equal(val, got)
	})

	s.Run("TEXT_spans_two_overflow_pages", func() {
		// 5 000 bytes — exceeds one overflow page (4 083 bytes), needs two.
		val := overflowStr(5000)
		_, err := s.db.Exec(`insert into articles (title, body, summary) values (?, ?, ?)`,
			"t", val, "s")
		s.Require().NoError(err)

		rows, err := s.db.Query(`select body from articles where body = ?`, val)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var got string
		s.Require().NoError(rows.Scan(&got))
		s.Equal(val, got)
	})

	s.Run("TEXT_spans_many_overflow_pages", func() {
		// 40 000 bytes — spans ~10 overflow pages.
		val := overflowStr(40000)
		_, err := s.db.Exec(`insert into articles (title, body, summary) values (?, ?, ?)`,
			"t", val, "s")
		s.Require().NoError(err)

		rows, err := s.db.Query(`select body from articles where body = ?`, val)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var got string
		s.Require().NoError(rows.Scan(&got))
		s.Equal(val, got)
	})

	s.Run("TEXT_at_max_size", func() {
		// 65 328 bytes — exactly the maximum (16 overflow pages).
		val := overflowStr(minisql.MaxOverflowTextSize)
		_, err := s.db.Exec(`insert into articles (title, body, summary) values (?, ?, ?)`,
			"t", val, "s")
		s.Require().NoError(err)

		rows, err := s.db.Query(`select body from articles where body = ?`, val)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var got string
		s.Require().NoError(rows.Scan(&got))
		s.Equal(val, got)
	})

	s.Run("TEXT_exceeds_max_size_rejected", func() {
		// One byte over the maximum must be rejected.
		val := overflowStr(minisql.MaxOverflowTextSize + 1)
		_, err := s.db.Exec(`insert into articles (title, body, summary) values (?, ?, ?)`,
			"t", val, "s")
		s.Require().Error(err)
	})

	// ── VARCHAR(>512): large declared size ──────────────────────────────────

	s.Run("VARCHAR_multi_overflow_spans_two_pages", func() {
		// summary is VARCHAR(4200); 4 200 bytes > one overflow page (4 083 bytes).
		val := overflowStr(4200)
		_, err := s.db.Exec(`insert into articles (title, body, summary) values (?, ?, ?)`,
			"t", "body", val)
		s.Require().NoError(err)

		rows, err := s.db.Query(`select summary from articles where summary = ?`, val)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var got string
		s.Require().NoError(rows.Scan(&got))
		s.Equal(val, got)
	})

	// ── Multiple rows with overflow values ──────────────────────────────────

	s.Run("multiple_rows_with_overflow", func() {
		bodies := []string{
			overflowStr(600),
			overflowStr(5000),
			overflowStr(20000),
		}
		for i, b := range bodies {
			_, err := s.db.Exec(`insert into articles (title, body, summary) values (?, ?, ?)`,
				overflowStr(i+1), b, "s")
			s.Require().NoError(err)
		}

		rows, err := s.db.Query(`select body from articles where body = ? or body = ? or body = ?`,
			bodies[0], bodies[1], bodies[2])
		s.Require().NoError(err)
		defer rows.Close()

		got := map[int]bool{}
		for rows.Next() {
			var body string
			s.Require().NoError(rows.Scan(&body))
			for i, b := range bodies {
				if body == b {
					got[i] = true
				}
			}
		}
		s.Require().NoError(rows.Err())
		for i := range bodies {
			s.True(got[i], "row %d not returned", i)
		}
	})

	// ── UPDATE: overflow → inline and inline → overflow ──────────────────────

	s.Run("UPDATE_overflow_to_inline", func() {
		large := overflowStr(5000)
		_, err := s.db.Exec(`insert into articles (title, body, summary) values (?, ?, ?)`,
			"update-test", large, "s")
		s.Require().NoError(err)

		small := "short"
		_, err = s.db.Exec(`update articles set body = ? where title = ?`, small, "update-test")
		s.Require().NoError(err)

		rows, err := s.db.Query(`select body from articles where title = ?`, "update-test")
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var got string
		s.Require().NoError(rows.Scan(&got))
		s.Equal(small, got)
	})

	s.Run("UPDATE_inline_to_overflow", func() {
		_, err := s.db.Exec(`insert into articles (title, body, summary) values (?, ?, ?)`,
			"inline-to-overflow", "short", "s")
		s.Require().NoError(err)

		large := overflowStr(8000)
		_, err = s.db.Exec(`update articles set body = ? where title = ?`, large, "inline-to-overflow")
		s.Require().NoError(err)

		rows, err := s.db.Query(`select body from articles where title = ?`, "inline-to-overflow")
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var got string
		s.Require().NoError(rows.Scan(&got))
		s.Equal(large, got)
	})

	// ── DELETE removes overflow pages (no leak) ──────────────────────────────

	s.Run("DELETE_row_with_overflow", func() {
		large := overflowStr(10000)
		_, err := s.db.Exec(`insert into articles (title, body, summary) values (?, ?, ?)`,
			"delete-me", large, "s")
		s.Require().NoError(err)

		_, err = s.db.Exec(`delete from articles where title = ?`, "delete-me")
		s.Require().NoError(err)

		rows, err := s.db.Query(`select id from articles where title = ?`, "delete-me")
		s.Require().NoError(err)
		defer rows.Close()

		s.False(rows.Next(), "deleted row must not be returned")
	})

	// ── ORDER BY on overflow column ──────────────────────────────────────────

	s.Run("ORDER_BY_overflow_column", func() {
		_, err := s.db.Exec(`create table docs (
			id   int8 primary key autoincrement,
			body text not null
		);`)
		s.Require().NoError(err)

		vals := []string{
			overflowStr(600) + "c",
			overflowStr(600) + "a",
			overflowStr(600) + "b",
		}
		for _, v := range vals {
			_, err = s.db.Exec(`insert into docs (body) values (?)`, v)
			s.Require().NoError(err)
		}

		rows, err := s.db.Query(`select body from docs order by body asc`)
		s.Require().NoError(err)
		defer rows.Close()

		var got []string
		for rows.Next() {
			var b string
			s.Require().NoError(rows.Scan(&b))
			got = append(got, b)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(got, 3)
		// Last character determines order: a < b < c
		s.Equal(vals[1], got[0]) // "...a"
		s.Equal(vals[2], got[1]) // "...b"
		s.Equal(vals[0], got[2]) // "...c"
	})

	// ── Persistence across close/reopen ─────────────────────────────────────

	s.Run("overflow_survives_reopen", func() {
		_, err := s.db.Exec(`create table blobs (
			id   int8 primary key autoincrement,
			data text not null
		);`)
		s.Require().NoError(err)

		val := overflowStr(12000)
		_, err = s.db.Exec(`insert into blobs (data) values (?)`, val)
		s.Require().NoError(err)

		// Reopen the database by closing and reopening via the test suite helper.
		s.Require().NoError(s.db.Close())
		s.db = s.reopenDB()

		rows, err := s.db.Query(`select data from blobs`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var got string
		s.Require().NoError(rows.Scan(&got))
		s.Equal(val, got)
	})
}
