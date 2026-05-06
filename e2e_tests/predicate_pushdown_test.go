package e2etests

// TestPredicatePushdownJoin verifies that single-table WHERE conditions pushed
// into individual table scans within a JOIN use index scans when a matching
// index exists, rather than always falling back to a sequential scan.
func (s *TestSuite) TestPredicatePushdownJoin() {
	// departments has a PK on dept_id (autoincrement) and a secondary index on name.
	_, err := s.db.Exec(`create table "pd_departments" (
		dept_id  int8 primary key autoincrement,
		name     varchar(50) not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create index "idx_pd_dept_name" on "pd_departments" (name)`)
	s.Require().NoError(err)

	// pd_employees.dept_id has NO index — join on it forces hash join.
	// pd_employees.salary HAS an index — pushed-down WHERE on salary uses it.
	_, err = s.db.Exec(`create table "pd_employees" (
		emp_id   int8 primary key autoincrement,
		dept_id  int8 not null,
		name     varchar(50) not null,
		salary   int8 not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create index "idx_pd_emp_salary" on "pd_employees" (salary)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "pd_departments" (name) values
		('Engineering'),
		('Marketing'),
		('HR')`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "pd_employees" (dept_id, name, salary) values
		(1, 'Alice',   90000),
		(1, 'Bob',     85000),
		(2, 'Carol',   70000),
		(3, 'Dave',    60000),
		(1, 'Eve',     95000)`)
	s.Require().NoError(err)

	s.Run("base_table_index_pushdown_point", func() {
		// d.dept_id = 1 is an equality on the PK of pd_departments.
		// The planner should use an index_point scan on the base table, not sequential.
		rows := s.collectExplain(`
			EXPLAIN SELECT e.name
			FROM "pd_departments" AS d
			INNER JOIN "pd_employees" AS e ON d.dept_id = e.dept_id
			WHERE d.dept_id = 1`)
		var baseScanRow *explainResult
		for i := range rows {
			if rows[i].Operation == "index_point" || rows[i].Operation == "index_range" {
				baseScanRow = &rows[i]
				break
			}
		}
		s.Require().NotNil(baseScanRow, "expected an index scan for pushed-down PK condition on base table")
	})

	s.Run("base_table_index_pushdown_by_name", func() {
		// d.name = 'Engineering' matches idx_pd_dept_name on the base table.
		rows := s.collectExplain(`
			EXPLAIN SELECT e.name
			FROM "pd_departments" AS d
			INNER JOIN "pd_employees" AS e ON d.dept_id = e.dept_id
			WHERE d.name = 'Engineering'`)
		var baseScanRow *explainResult
		for i := range rows {
			if rows[i].Operation == "index_point" {
				baseScanRow = &rows[i]
				break
			}
		}
		s.Require().NotNil(baseScanRow, "expected an index_point scan for d.name pushed-down to base table")
	})

	s.Run("inner_table_index_pushdown_salary_range", func() {
		// e.salary > 80000 matches idx_pd_emp_salary on the hash-join build side.
		// The build scan should use an index_range scan instead of sequential.
		rows := s.collectExplain(`
			EXPLAIN SELECT e.name
			FROM "pd_departments" AS d
			INNER JOIN "pd_employees" AS e ON d.dept_id = e.dept_id
			WHERE e.salary > 80000`)
		var innerScanRow *explainResult
		for i := range rows {
			if rows[i].Operation == "index_range" {
				innerScanRow = &rows[i]
				break
			}
		}
		s.Require().NotNil(innerScanRow, "expected an index_range scan for e.salary pushed-down to inner table")
	})

	s.Run("pushed_down_base_condition_correct_results", func() {
		// Even with index pushdown, results must be identical to the unoptimized path.
		rows, err := s.db.Query(`
			select e.name
			from   "pd_departments" as d
			inner join "pd_employees" as e on d.dept_id = e.dept_id
			where  d.dept_id = 1
			order by e.name`)
		s.Require().NoError(err)
		defer rows.Close()

		var got []string
		for rows.Next() {
			var name string
			s.Require().NoError(rows.Scan(&name))
			got = append(got, name)
		}
		s.Require().NoError(rows.Err())
		s.Require().Equal([]string{"Alice", "Bob", "Eve"}, got)
	})

	s.Run("pushed_down_inner_condition_correct_results", func() {
		// e.salary > 80000 pushed down to the hash-join build side — only
		// high-earners should be joined.
		rows, err := s.db.Query(`
			select e.name, e.salary
			from   "pd_departments" as d
			inner join "pd_employees" as e on d.dept_id = e.dept_id
			where  e.salary > 80000
			order by e.name`)
		s.Require().NoError(err)
		defer rows.Close()

		type result struct {
			name   string
			salary int64
		}
		var got []result
		for rows.Next() {
			var r result
			s.Require().NoError(rows.Scan(&r.name, &r.salary))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(got, 3)
		s.Equal("Alice", got[0].name)
		s.Equal(int64(90000), got[0].salary)
		s.Equal("Bob", got[1].name)
		s.Equal(int64(85000), got[1].salary)
		s.Equal("Eve", got[2].name)
		s.Equal(int64(95000), got[2].salary)
	})

	s.Run("both_tables_index_pushdown_correct_results", func() {
		// Both base and inner table conditions pushed down with index acceleration.
		rows, err := s.db.Query(`
			select e.name
			from   "pd_departments" as d
			inner join "pd_employees" as e on d.dept_id = e.dept_id
			where  d.name = 'Engineering'
			and    e.salary >= 90000
			order by e.name`)
		s.Require().NoError(err)
		defer rows.Close()

		var got []string
		for rows.Next() {
			var name string
			s.Require().NoError(rows.Scan(&name))
			got = append(got, name)
		}
		s.Require().NoError(rows.Err())
		s.Require().Equal([]string{"Alice", "Eve"}, got)
	})
}
