package e2etests

// TestHashJoin exercises the hash-join execution path.
//
// Hash join is chosen when the right (inner/build) table has no index on its
// join column.  The queries below use "departments" as the outer (left) table
// and "employees" as the inner (right/build) table.  employees.dept_id has no
// secondary index, so the planner picks JoinAlgorithmHash instead of the
// indexed nested-loop path.
func (s *TestSuite) TestHashJoin() {
	_, err := s.db.Exec(`create table "departments" (
		dept_id  int8 primary key autoincrement,
		name     varchar(50) not null
	);`)
	s.Require().NoError(err)

	// dept_id on employees has NO secondary index — join on it must use hash join.
	_, err = s.db.Exec(`create table "employees" (
		emp_id   int8 primary key autoincrement,
		dept_id  int8 not null,
		name     varchar(50) not null,
		salary   int8 not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "departments" (name) values
		('Engineering'),
		('Marketing'),
		('HR')`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "employees" (dept_id, name, salary) values
		(1, 'Alice',   90000),
		(1, 'Bob',     85000),
		(2, 'Carol',   70000),
		(3, 'Dave',    60000),
		(1, 'Eve',     95000)`)
	s.Require().NoError(err)

	s.Run("inner_join_hash", func() {
		// departments is outer; employees is inner (build side, no index) → hash join.
		rows, err := s.db.Query(`
			select e.name, d.name
			from   "departments" as d
			inner join "employees" as e on d.dept_id = e.dept_id
			where  d.name = 'Engineering'
			order by e.name`)
		s.Require().NoError(err)
		defer rows.Close()

		type result struct{ emp, dept string }
		var got []result
		for rows.Next() {
			var r result
			s.Require().NoError(rows.Scan(&r.emp, &r.dept))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())

		s.Require().Len(got, 3)
		s.Equal("Alice", got[0].emp)
		s.Equal("Bob", got[1].emp)
		s.Equal("Eve", got[2].emp)
		for _, r := range got {
			s.Equal("Engineering", r.dept)
		}
	})

	s.Run("inner_join_hash_no_match", func() {
		rows, err := s.db.Query(`
			select e.name
			from   "departments" as d
			inner join "employees" as e on d.dept_id = e.dept_id
			where  d.name = 'Finance'`)
		s.Require().NoError(err)
		defer rows.Close()

		var count int
		for rows.Next() {
			count++
		}
		s.Require().NoError(rows.Err())
		s.Zero(count)
	})

	s.Run("left_join_hash_includes_unmatched_dept", func() {
		// Insert a department with no employees — left join should emit it with NULL emp.
		_, err := s.db.Exec(`insert into "departments" (name) values ('Legal')`)
		s.Require().NoError(err)

		rows, err := s.db.Query(`
			select d.name, e.name
			from   "departments" as d
			left join "employees" as e on d.dept_id = e.dept_id
			where  d.name = 'Legal'`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var deptName string
		var empName *string
		s.Require().NoError(rows.Scan(&deptName, &empName))
		s.Equal("Legal", deptName)
		s.Nil(empName) // no employees in Legal → NULL
		s.Require().NoError(rows.Err())
	})

	s.Run("inner_join_hash_all_employees_ordered_by_salary", func() {
		// Verify hash join returns all matching rows correctly, ordered by salary.
		rows, err := s.db.Query(`
			select e.name, e.salary, d.name
			from   "departments" as d
			inner join "employees" as e on d.dept_id = e.dept_id
			where  d.dept_id in (1, 2)
			order by e.salary`)
		s.Require().NoError(err)
		defer rows.Close()

		type result struct {
			emp    string
			salary int64
			dept   string
		}
		var got []result
		for rows.Next() {
			var r result
			s.Require().NoError(rows.Scan(&r.emp, &r.salary, &r.dept))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(got, 4)
		s.Equal("Carol", got[0].emp)
		s.Equal(int64(70000), got[0].salary)
		s.Equal("Marketing", got[0].dept)
		s.Equal("Bob", got[1].emp)
		s.Equal(int64(85000), got[1].salary)
		s.Equal("Engineering", got[1].dept)
		s.Equal("Alice", got[2].emp)
		s.Equal(int64(90000), got[2].salary)
		s.Equal("Engineering", got[2].dept)
		s.Equal("Eve", got[3].emp)
		s.Equal(int64(95000), got[3].salary)
		s.Equal("Engineering", got[3].dept)
	})

	s.Run("explain_shows_hash_algorithm", func() {
		rows := s.collectExplain(`
			EXPLAIN SELECT e.name
			FROM "departments" AS d
			INNER JOIN "employees" AS e ON d.dept_id = e.dept_id`)
		var joinRow *explainResult
		for i := range rows {
			if rows[i].Operation == "join" {
				joinRow = &rows[i]
				break
			}
		}
		s.Require().NotNil(joinRow, "expected a 'join' row in EXPLAIN output")
		s.Contains(joinRow.Detail, "algorithm=hash")
	})
}
