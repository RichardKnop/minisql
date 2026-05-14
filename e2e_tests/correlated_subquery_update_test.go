package e2etests

func (s *TestSuite) TestCorrelatedSubqueryUpdate() {
	// Schema: departments and employees tables.
	_, err := s.db.Exec(`create table "depts" (
		id     int8 primary key autoincrement,
		name   varchar(100) not null,
		budget int8 not null
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "emps" (
		id      int8 primary key autoincrement,
		name    varchar(100) not null,
		dept_id int8 not null,
		salary  int8 not null
	);`)
	s.Require().NoError(err)

	// Seed departments.
	_, err = s.db.Exec(`insert into "depts" (name, budget) values ('Engineering', 100000)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "depts" (name, budget) values ('Sales', 60000)`)
	s.Require().NoError(err)

	// Seed employees.
	_, err = s.db.Exec(`insert into "emps" (name, dept_id, salary) values ('Alice', 1, 0)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "emps" (name, dept_id, salary) values ('Bob', 2, 0)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "emps" (name, dept_id, salary) values ('Carol', 1, 0)`)
	s.Require().NoError(err)

	s.Run("correlated_set_from_dept_budget", func() {
		// SET salary = (SELECT budget FROM depts WHERE id = e.dept_id)
		_, err := s.db.Exec(`
			update emps e
			set salary = (select budget from depts where id = e.dept_id)
		`)
		s.Require().NoError(err)

		rows, err := s.db.Query(`select name, salary from emps order by id`)
		s.Require().NoError(err)
		defer rows.Close()

		type emp struct {
			name   string
			salary int64
		}
		var got []emp
		for rows.Next() {
			var e emp
			s.Require().NoError(rows.Scan(&e.name, &e.salary))
			got = append(got, e)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(got, 3)
		s.Equal("Alice", got[0].name)
		s.Equal(int64(100000), got[0].salary, "Alice is in Engineering (budget 100000)")
		s.Equal("Bob", got[1].name)
		s.Equal(int64(60000), got[1].salary, "Bob is in Sales (budget 60000)")
		s.Equal("Carol", got[2].name)
		s.Equal(int64(100000), got[2].salary, "Carol is in Engineering (budget 100000)")
	})

	s.Run("correlated_set_with_where_filter", func() {
		// Reset salaries.
		_, err := s.db.Exec(`update emps set salary = 0`)
		s.Require().NoError(err)

		// Only update Engineering employees.
		_, err = s.db.Exec(`
			update emps e
			set salary = (select budget from depts where id = e.dept_id)
			where e.dept_id = 1
		`)
		s.Require().NoError(err)

		rows, err := s.db.Query(`select name, salary from emps order by id`)
		s.Require().NoError(err)
		defer rows.Close()

		type emp struct {
			name   string
			salary int64
		}
		var got []emp
		for rows.Next() {
			var e emp
			s.Require().NoError(rows.Scan(&e.name, &e.salary))
			got = append(got, e)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(got, 3)
		s.Equal(int64(100000), got[0].salary, "Alice updated (Engineering)")
		s.Equal(int64(0), got[1].salary, "Bob not updated (Sales)")
		s.Equal(int64(100000), got[2].salary, "Carol updated (Engineering)")
	})

	s.Run("non_correlated_set_subquery", func() {
		// Reset salaries.
		_, err := s.db.Exec(`update emps set salary = 0`)
		s.Require().NoError(err)

		// Set all salaries to the max budget (non-correlated).
		_, err = s.db.Exec(`
			update emps
			set salary = (select max(budget) from depts)
		`)
		s.Require().NoError(err)

		rows, err := s.db.Query(`select salary from emps order by id`)
		s.Require().NoError(err)
		defer rows.Close()

		var salaries []int64
		for rows.Next() {
			var sal int64
			s.Require().NoError(rows.Scan(&sal))
			salaries = append(salaries, sal)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(salaries, 3)
		for _, sal := range salaries {
			s.Equal(int64(100000), sal, "all employees should get the max budget")
		}
	})

	s.Run("correlated_set_subquery_no_match_yields_null", func() {
		// dept_id=99 doesn't exist → subquery returns 0 rows → NULL.
		_, err := s.db.Exec(`insert into "emps" (name, dept_id, salary) values ('Dave', 99, 9999)`)
		s.Require().NoError(err)

		_, err = s.db.Exec(`
			update emps e
			set salary = (select budget from depts where id = e.dept_id)
			where e.name = 'Dave'
		`)
		s.Require().NoError(err)

		var salary *int64
		err = s.db.QueryRow(`select salary from emps where name = 'Dave'`).Scan(&salary)
		s.Require().NoError(err)
		s.Nil(salary, "unmatched correlated subquery should set column to NULL")
	})

	s.Run("correlated_subquery_more_than_one_row_errors", func() {
		// All depts match "id > 0" → subquery returns multiple rows → error.
		_, err := s.db.Exec(`
			update emps e
			set salary = (select budget from depts where id > 0)
			where e.name = 'Alice'
		`)
		s.Require().Error(err, "multi-row scalar subquery in SET must error")
	})
}
