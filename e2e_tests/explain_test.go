package e2etests

import (
	"context"
	"database/sql"
	"strings"
)

type explainResult struct {
	RowsEstimated sql.NullInt64
	RowsActual    sql.NullInt64
	DurationUS    sql.NullInt64
	Operation     string
	Detail        string
	Step          int64
}

func (s *TestSuite) TestExplainSelect() {
	s.execQuery(createUsersTableSQL, 0)
	s.execQuery(`insert into users("email", "name") values
('alice@example.com', 'Alice'),
('bob@example.com', 'Bob');`, 2)

	rows := s.collectExplain(`EXPLAIN SELECT * FROM users WHERE id = 1;`)
	s.Require().NotEmpty(rows)
	s.Equal(int64(1), rows[0].Step)
	s.Equal("index_point", rows[0].Operation)
	s.Contains(rows[0].Detail, "table=users")
	s.Contains(rows[0].Detail, "index=pkey__users")
	s.True(rows[0].RowsEstimated.Valid)
	s.False(rows[0].RowsActual.Valid)
	s.False(rows[0].DurationUS.Valid)
}

func (s *TestSuite) TestExplainAnalyzeSelect() {
	s.execQuery(createUsersTableSQL, 0)
	s.execQuery(`insert into users("email", "name") values
('alice@example.com', 'Alice'),
('bob@example.com', 'Bob');`, 2)

	rows := s.collectExplain(`EXPLAIN ANALYZE SELECT * FROM users WHERE id > 0 ORDER BY name;`)
	s.Require().Len(rows, 2)
	s.Equal("index_range", rows[0].Operation)
	s.True(rows[0].RowsActual.Valid)
	s.Equal(int64(2), rows[0].RowsActual.Int64)
	s.True(rows[0].DurationUS.Valid)
	s.Equal("sort", rows[1].Operation)
	s.True(rows[1].RowsActual.Valid)
	s.Equal(int64(2), rows[1].RowsActual.Int64)
	s.True(rows[1].DurationUS.Valid)
}

func (s *TestSuite) TestExplainUnsupportedStatement() {
	s.execQuery(createUsersTableSQL, 0)

	rows, err := s.db.QueryContext(context.Background(), `EXPLAIN INSERT INTO users("email", "name") VALUES ('x@example.com', 'X');`)
	s.Require().Error(err)
	s.Nil(rows)
	s.True(strings.Contains(err.Error(), "EXPLAIN currently supports SELECT statements only"))
	s.True(strings.Contains(err.Error(), "got INSERT"))
}

func (s *TestSuite) collectExplain(query string) []explainResult {
	rows, err := s.db.QueryContext(context.Background(), query)
	s.Require().NoError(err)
	defer rows.Close()

	var results []explainResult
	for rows.Next() {
		var result explainResult
		err := rows.Scan(
			&result.Step,
			&result.Operation,
			&result.Detail,
			&result.RowsEstimated,
			&result.RowsActual,
			&result.DurationUS,
		)
		s.Require().NoError(err)
		results = append(results, result)
	}
	s.Require().NoError(rows.Err())
	return results
}
