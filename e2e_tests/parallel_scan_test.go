package e2etests

import (
	"context"
	"sort"
)

func (s *TestSuite) TestParallelScan_PragmaReadDefault() {
	rows, err := s.db.QueryContext(context.Background(), `PRAGMA parallel_scan;`)
	s.Require().NoError(err)
	defer rows.Close()

	columns, err := rows.Columns()
	s.Require().NoError(err)
	s.Equal([]string{"parallel_scan"}, columns)

	s.Require().True(rows.Next())
	var v int32
	s.Require().NoError(rows.Scan(&v))
	s.Equal(int32(0), v, "parallel_scan should be off by default")
}

func (s *TestSuite) TestParallelScan_PragmaEnableDisable() {
	// Enable
	_, err := s.db.Exec(`PRAGMA parallel_scan = on;`)
	s.Require().NoError(err)

	rows, err := s.db.QueryContext(context.Background(), `PRAGMA parallel_scan;`)
	s.Require().NoError(err)
	defer rows.Close()
	s.Require().True(rows.Next())
	var v int32
	s.Require().NoError(rows.Scan(&v))
	s.Equal(int32(1), v)
	rows.Close()

	// Disable
	_, err = s.db.Exec(`PRAGMA parallel_scan = off;`)
	s.Require().NoError(err)

	rows2, err := s.db.QueryContext(context.Background(), `PRAGMA parallel_scan;`)
	s.Require().NoError(err)
	defer rows2.Close()
	s.Require().True(rows2.Next())
	var v2 int32
	s.Require().NoError(rows2.Scan(&v2))
	s.Equal(int32(0), v2)
}

func (s *TestSuite) TestParallelScan_ResultsMatchSequential() {
	// Create a table with enough rows to span multiple leaf pages.
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)

	users := gen.Users(200)
	for _, u := range users {
		_, err := stmt.Exec(u.Email, u.Name)
		s.Require().NoError(err)
	}

	// Collect rows with sequential scan (parallel_scan off by default).
	type row struct {
		id          int64
		email, name string
	}
	collectRows := func() []row {
		rs, err := s.db.QueryContext(context.Background(), `select id, email, name from "users";`)
		s.Require().NoError(err)
		defer rs.Close()
		var out []row
		for rs.Next() {
			var r row
			s.Require().NoError(rs.Scan(&r.id, &r.email, &r.name))
			out = append(out, r)
		}
		s.Require().NoError(rs.Err())
		return out
	}

	seqRows := collectRows()
	s.Require().Len(seqRows, 200)

	// Enable parallel scan and collect again.
	_, err = s.db.Exec(`PRAGMA parallel_scan = on;`)
	s.Require().NoError(err)

	parRows := collectRows()
	s.Require().Len(parRows, 200)

	// Parallel scan may return rows in a different order — sort by id before comparing.
	sort.Slice(seqRows, func(i, j int) bool { return seqRows[i].id < seqRows[j].id })
	sort.Slice(parRows, func(i, j int) bool { return parRows[i].id < parRows[j].id })
	s.Equal(seqRows, parRows)
}

func (s *TestSuite) TestParallelScan_FilterCorrect() {
	// Verify WHERE predicates are applied correctly under parallel scan.
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)

	users := gen.Users(100)
	for _, u := range users {
		_, err := stmt.Exec(u.Email, u.Name)
		s.Require().NoError(err)
	}

	_, err = s.db.Exec(`PRAGMA parallel_scan = on;`)
	s.Require().NoError(err)

	rows, err := s.db.QueryContext(context.Background(), `select count(*) from "users" where id > 50;`)
	s.Require().NoError(err)
	defer rows.Close()
	s.Require().True(rows.Next())
	var cnt int64
	s.Require().NoError(rows.Scan(&cnt))
	s.Equal(int64(50), cnt)
}
