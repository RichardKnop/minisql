package e2etests

import (
	"time"
)

const tsLayout = "2006-01-02 15:04:05"

// scanTimestamps collects rows from a single-timestamp-column query into
// []string formatted as "YYYY-MM-DD HH:MM:SS".
func scanTimestamps(s *TestSuite, rows interface {
	Next() bool
	Scan(...any) error
	Err() error
	Close() error
}) []string {
	var results []string
	for rows.Next() {
		var v time.Time
		s.Require().NoError(rows.Scan(&v))
		results = append(results, v.UTC().Format(tsLayout))
	}
	s.Require().NoError(rows.Err())
	s.Require().NoError(rows.Close())
	return results
}

// ── INTERVAL arithmetic ───────────────────────────────────────────────────────

func (s *TestSuite) TestInterval_AddDaysToTimestamp() {
	_, err := s.db.Exec(`create table "events" (
		id         int8 primary key autoincrement,
		created_at timestamp not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "events" (created_at) values ('2024-03-15 10:00:00')`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`SELECT created_at + INTERVAL '3 days' FROM "events"`)
	s.Require().NoError(err)
	s.Equal([]string{"2024-03-18 10:00:00"}, scanTimestamps(s, rows))
}

func (s *TestSuite) TestInterval_SubtractDaysFromTimestamp() {
	_, err := s.db.Exec(`create table "orders" (
		id         int8 primary key autoincrement,
		placed_at  timestamp not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "orders" (placed_at) values ('2024-06-10 00:00:00')`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`SELECT placed_at - INTERVAL '5 days' FROM "orders"`)
	s.Require().NoError(err)
	s.Equal([]string{"2024-06-05 00:00:00"}, scanTimestamps(s, rows))
}

func (s *TestSuite) TestInterval_AddMonthsCalendarAware() {
	_, err := s.db.Exec(`create table "subs" (
		id         int8 primary key autoincrement,
		started_at timestamp not null
	)`)
	s.Require().NoError(err)

	// Jan 31 + 1 month = Feb 29 in 2024 (leap year).
	_, err = s.db.Exec(`insert into "subs" (started_at) values ('2024-01-31 00:00:00')`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`SELECT started_at + INTERVAL '1 month' FROM "subs"`)
	s.Require().NoError(err)
	s.Equal([]string{"2024-02-29 00:00:00"}, scanTimestamps(s, rows))
}

func (s *TestSuite) TestInterval_AddYearToTimestamp() {
	_, err := s.db.Exec(`create table "licenses" (
		id         int8 primary key autoincrement,
		issued_at  timestamp not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "licenses" (issued_at) values ('2023-07-04 12:00:00')`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`SELECT issued_at + INTERVAL '1 year' FROM "licenses"`)
	s.Require().NoError(err)
	s.Equal([]string{"2024-07-04 12:00:00"}, scanTimestamps(s, rows))
}

func (s *TestSuite) TestInterval_AddHoursMinutes() {
	_, err := s.db.Exec(`create table "sessions" (
		id         int8 primary key autoincrement,
		started_at timestamp not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "sessions" (started_at) values ('2024-05-01 22:30:00')`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`SELECT started_at + INTERVAL '2 hours 45 minutes' FROM "sessions"`)
	s.Require().NoError(err)
	// 22:30 + 2h45m = 01:15 next day
	s.Equal([]string{"2024-05-02 01:15:00"}, scanTimestamps(s, rows))
}

func (s *TestSuite) TestInterval_WithAlias() {
	_, err := s.db.Exec(`create table "trials" (
		id         int8 primary key autoincrement,
		started_at timestamp not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "trials" (started_at) values ('2024-09-01 00:00:00')`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`SELECT started_at + INTERVAL '14 days' AS expires_at FROM "trials"`)
	s.Require().NoError(err)
	s.Equal([]string{"2024-09-15 00:00:00"}, scanTimestamps(s, rows))
}

func (s *TestSuite) TestInterval_InWhereClause() {
	_, err := s.db.Exec(`create table "logs" (
		id         int8 primary key autoincrement,
		ts         timestamp not null
	)`)
	s.Require().NoError(err)

	for _, t := range []string{
		"2024-01-01 00:00:00",
		"2024-01-05 00:00:00",
		"2024-01-10 00:00:00",
	} {
		_, err = s.db.Exec(`insert into "logs" (ts) values ('` + t + `')`)
		s.Require().NoError(err)
	}

	// Select rows where ts > '2024-01-01 00:00:00' + 3 days = '2024-01-04 00:00:00'
	rows, err := s.db.Query(`SELECT ts FROM "logs" WHERE ts > '2024-01-01 00:00:00'`)
	s.Require().NoError(err)
	s.ElementsMatch([]string{"2024-01-05 00:00:00", "2024-01-10 00:00:00"}, scanTimestamps(s, rows))
}
