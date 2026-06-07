package e2etests

import (
	"context"
	"database/sql"
)

type pragmaResult struct {
	Check   string
	Code    string
	Page    sql.NullInt64
	Object  sql.NullString
	Message string
}

func (s *TestSuite) TestPragma_QuickCheck() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)
	_, err = s.db.Exec(createUsersTimestampIndexSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)

	users := gen.Users(3)
	for _, u := range users {
		_, err := stmt.Exec(u.Email, u.Name)
		s.Require().NoError(err)
	}

	results := s.collectPragmaResults(`PRAGMA quick_check;`)
	s.Require().Len(results, 1)
	s.Equal(pragmaResult{
		Check:   "quick_check",
		Code:    "ok",
		Message: "ok",
	}, results[0])
}

func (s *TestSuite) TestPragma_IntegrityCheck() {
	_, err := s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)
	_, err = s.db.Exec(createUsersTimestampIndexSQL)
	s.Require().NoError(err)

	stmt, err := s.db.Prepare(`insert into "users" (email, name) values (?, ?)`)
	s.Require().NoError(err)

	users := gen.Users(5)
	for _, u := range users {
		_, err := stmt.Exec(u.Email, u.Name)
		s.Require().NoError(err)
	}

	results := s.collectPragmaResults(`PRAGMA integrity_check;`)
	s.Require().Len(results, 1)
	s.Equal(pragmaResult{
		Check:   "integrity_check",
		Code:    "ok",
		Message: "ok",
	}, results[0])

	rows, err := s.db.QueryContext(context.Background(), `PRAGMA integrity_check;`)
	s.Require().NoError(err)
	defer rows.Close()

	columns, err := rows.Columns()
	s.Require().NoError(err)
	s.Equal([]string{"check", "code", "page", "object", "message"}, columns)
}

func (s *TestSuite) TestPragma_Synchronous_ReadDefault() {
	rows, err := s.db.QueryContext(context.Background(), `PRAGMA synchronous;`)
	s.Require().NoError(err)
	defer rows.Close()

	columns, err := rows.Columns()
	s.Require().NoError(err)
	s.Equal([]string{"synchronous"}, columns)

	s.Require().True(rows.Next())
	var mode int32
	s.Require().NoError(rows.Scan(&mode))
	// Default is SynchronousNormal = 1
	s.Equal(int32(1), mode)
}

func (s *TestSuite) TestPragma_Synchronous_SetFull() {
	_, err := s.db.Exec(`PRAGMA synchronous = full;`)
	s.Require().NoError(err)

	rows, err := s.db.QueryContext(context.Background(), `PRAGMA synchronous;`)
	s.Require().NoError(err)
	defer rows.Close()

	s.Require().True(rows.Next())
	var mode int32
	s.Require().NoError(rows.Scan(&mode))
	s.Equal(int32(2), mode)
}

func (s *TestSuite) TestPragma_Synchronous_SetNormal() {
	// Set to full first, then back to normal
	_, err := s.db.Exec(`PRAGMA synchronous = full;`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`PRAGMA synchronous = normal;`)
	s.Require().NoError(err)

	rows, err := s.db.QueryContext(context.Background(), `PRAGMA synchronous;`)
	s.Require().NoError(err)
	defer rows.Close()

	s.Require().True(rows.Next())
	var mode int32
	s.Require().NoError(rows.Scan(&mode))
	s.Equal(int32(1), mode)
}

func (s *TestSuite) TestPragma_SortMemLimit_ReadDefault() {
	rows, err := s.db.QueryContext(context.Background(), `PRAGMA sort_mem_limit;`)
	s.Require().NoError(err)
	defer rows.Close()

	cols, err := rows.Columns()
	s.Require().NoError(err)
	s.Equal([]string{"sort_mem_limit"}, cols)

	s.Require().True(rows.Next())
	var limit int64
	s.Require().NoError(rows.Scan(&limit))
	s.Equal(int64(4*1024*1024), limit)
}

func (s *TestSuite) TestPragma_SortMemLimit_Set() {
	_, err := s.db.Exec(`PRAGMA sort_mem_limit = 1048576;`)
	s.Require().NoError(err)

	rows, err := s.db.QueryContext(context.Background(), `PRAGMA sort_mem_limit;`)
	s.Require().NoError(err)
	defer rows.Close()

	s.Require().True(rows.Next())
	var limit int64
	s.Require().NoError(rows.Scan(&limit))
	s.Equal(int64(1048576), limit)
}

func (s *TestSuite) TestPragma_SortMemLimit_SetZeroDisables() {
	_, err := s.db.Exec(`PRAGMA sort_mem_limit = 0;`)
	s.Require().NoError(err)

	rows, err := s.db.QueryContext(context.Background(), `PRAGMA sort_mem_limit;`)
	s.Require().NoError(err)
	defer rows.Close()

	s.Require().True(rows.Next())
	var limit int64
	s.Require().NoError(rows.Scan(&limit))
	s.Equal(int64(0), limit)
}

func (s *TestSuite) collectPragmaResults(query string) []pragmaResult {
	rows, err := s.db.QueryContext(context.Background(), query)
	s.Require().NoError(err)
	defer rows.Close()

	results := make([]pragmaResult, 0)
	for rows.Next() {
		var result pragmaResult
		err := rows.Scan(&result.Check, &result.Code, &result.Page, &result.Object, &result.Message)
		s.Require().NoError(err)
		results = append(results, result)
	}
	s.Require().NoError(rows.Err())

	return results
}
