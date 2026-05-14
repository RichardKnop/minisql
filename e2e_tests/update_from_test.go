package e2etests

func (s *TestSuite) TestUpdateFrom() {
	// Schema: employees and departments.
	_, err := s.db.Exec(`create table "departments" (
		id   int8 primary key autoincrement,
		name varchar(100) not null,
		budget int8
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "employees" (
		id      int8 primary key autoincrement,
		name    varchar(100) not null,
		dept_id int8,
		salary  int8
	);`)
	s.Require().NoError(err)

	// Seed departments.
	_, err = s.db.Exec(`insert into "departments" (name, budget) values ('Engineering', 100000)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "departments" (name, budget) values ('Marketing', 50000)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "departments" (name, budget) values ('HR', 30000)`)
	s.Require().NoError(err)

	// Seed employees (dept_id 1 = Engineering, 2 = Marketing, 3 = HR).
	_, err = s.db.Exec(`insert into "employees" (name, dept_id, salary) values ('Alice', 1, 80000)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "employees" (name, dept_id, salary) values ('Bob', 1, 90000)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "employees" (name, dept_id, salary) values ('Carol', 2, 60000)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "employees" (name, dept_id, salary) values ('Dave', 3, 40000)`)
	s.Require().NoError(err)

	s.Run("basic_update_from", func() {
		// Set each employee's salary to the budget of their department divided by 10.
		_, err := s.db.Exec(`
			update employees e
			set salary = d.budget / 10
			from departments d
			where e.dept_id = d.id
		`)
		s.Require().NoError(err)

		rows, err := s.db.Query(`select name, salary from employees order by id`)
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

		s.Require().Len(got, 4)
		s.Equal("Alice", got[0].name)
		s.Equal(int64(10000), got[0].salary) // 100000 / 10
		s.Equal("Bob", got[1].name)
		s.Equal(int64(10000), got[1].salary)
		s.Equal("Carol", got[2].name)
		s.Equal(int64(5000), got[2].salary) // 50000 / 10
		s.Equal("Dave", got[3].name)
		s.Equal(int64(3000), got[3].salary) // 30000 / 10
	})

	// Reset salaries for subsequent subtests.
	_, err = s.db.Exec(`update employees set salary = 0`)
	s.Require().NoError(err)

	s.Run("update_from_with_where_filter", func() {
		// Only update Engineering employees (dept_id = 1).
		_, err := s.db.Exec(`
			update employees e
			set salary = d.budget / 5
			from departments d
			where e.dept_id = d.id and d.id = 1
		`)
		s.Require().NoError(err)

		rows, err := s.db.Query(`select name, salary from employees order by id`)
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

		s.Require().Len(got, 4)
		s.Equal("Alice", got[0].name)
		s.Equal(int64(20000), got[0].salary) // 100000 / 5
		s.Equal("Bob", got[1].name)
		s.Equal(int64(20000), got[1].salary)
		s.Equal("Carol", got[2].name)
		s.Equal(int64(0), got[2].salary) // unaffected
		s.Equal("Dave", got[3].name)
		s.Equal(int64(0), got[3].salary) // unaffected
	})

	// Reset.
	_, err = s.db.Exec(`update employees set salary = 0`)
	s.Require().NoError(err)

	s.Run("update_from_set_literal", func() {
		// Set a literal value for employees in Engineering.
		_, err := s.db.Exec(`
			update employees e
			set salary = 99999
			from departments d
			where e.dept_id = d.id and d.name = 'Engineering'
		`)
		s.Require().NoError(err)

		rows, err := s.db.Query(`select name, salary from employees where dept_id = 1 order by id`)
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

		s.Require().Len(got, 2)
		s.Equal(int64(99999), got[0].salary)
		s.Equal(int64(99999), got[1].salary)
	})

	// Reset.
	_, err = s.db.Exec(`update employees set salary = 0`)
	s.Require().NoError(err)

	s.Run("update_from_table_alias_with_AS", func() {
		// Use explicit AS for both aliases.
		_, err := s.db.Exec(`
			update employees as emp
			set salary = dept.budget / 10
			from departments as dept
			where emp.dept_id = dept.id
		`)
		s.Require().NoError(err)

		rows, err := s.db.Query(`select name, salary from employees order by id`)
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

		s.Require().Len(got, 4)
		s.Equal(int64(10000), got[0].salary)
		s.Equal(int64(10000), got[1].salary)
		s.Equal(int64(5000), got[2].salary)
		s.Equal(int64(3000), got[3].salary)
	})

	// Reset.
	_, err = s.db.Exec(`update employees set salary = 0`)
	s.Require().NoError(err)

	s.Run("update_from_subquery", func() {
		// Use a subquery as the FROM source — select only Engineering.
		_, err := s.db.Exec(`
			update employees e
			set salary = d.budget
			from (select id, budget from departments where id = 1) as d
			where e.dept_id = d.id
		`)
		s.Require().NoError(err)

		rows, err := s.db.Query(`select name, salary from employees order by id`)
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

		s.Require().Len(got, 4)
		s.Equal("Alice", got[0].name)
		s.Equal(int64(100000), got[0].salary) // Engineering budget
		s.Equal("Bob", got[1].name)
		s.Equal(int64(100000), got[1].salary)
		s.Equal("Carol", got[2].name)
		s.Equal(int64(0), got[2].salary) // not in subquery result
		s.Equal("Dave", got[3].name)
		s.Equal(int64(0), got[3].salary)
	})

	// Reset.
	_, err = s.db.Exec(`update employees set salary = 0`)
	s.Require().NoError(err)

	s.Run("update_from_no_alias", func() {
		// Without explicit alias — use table name as qualifier in WHERE.
		_, err := s.db.Exec(`
			update employees
			set salary = departments.budget / 10
			from departments
			where employees.dept_id = departments.id
		`)
		s.Require().NoError(err)

		rows, err := s.db.Query(`select name, salary from employees order by id`)
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

		s.Require().Len(got, 4)
		s.Equal(int64(10000), got[0].salary)
		s.Equal(int64(10000), got[1].salary)
		s.Equal(int64(5000), got[2].salary)
		s.Equal(int64(3000), got[3].salary)
	})
}
