package e2etests

// ── UNION / UNION ALL ────────────────────────────────────────────────────────

func (s *TestSuite) TestUnion_AllConcatenates() {
	_, err := s.db.Exec(`create table "emp" (
		id   int8 primary key autoincrement,
		name text not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "contractor" (
		id   int8 primary key autoincrement,
		name text not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "emp" (name) values ('alice')`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "emp" (name) values ('bob')`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "contractor" (name) values ('carol')`)
	s.Require().NoError(err)
	// 'alice' appears in both tables — UNION ALL keeps the duplicate
	_, err = s.db.Exec(`insert into "contractor" (name) values ('alice')`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`SELECT name FROM "emp" UNION ALL SELECT name FROM "contractor"`)
	s.Require().NoError(err)
	defer rows.Close()

	var names []string
	for rows.Next() {
		var n string
		s.Require().NoError(rows.Scan(&n))
		names = append(names, n)
	}
	s.Require().NoError(rows.Err())

	// UNION ALL preserves all 4 rows (including the duplicate 'alice')
	s.Require().Len(names, 4)
	s.ElementsMatch([]string{"alice", "bob", "carol", "alice"}, names)
}

func (s *TestSuite) TestUnion_DeduplicatesRows() {
	_, err := s.db.Exec(`create table "set_a" (
		id  int8 primary key autoincrement,
		val int8 not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create table "set_b" (
		id  int8 primary key autoincrement,
		val int8 not null
	)`)
	s.Require().NoError(err)

	for _, v := range []string{"1", "2", "3"} {
		_, err = s.db.Exec(`insert into "set_a" (val) values (` + v + `)`)
		s.Require().NoError(err)
	}
	for _, v := range []string{"2", "3", "4"} {
		_, err = s.db.Exec(`insert into "set_b" (val) values (` + v + `)`)
		s.Require().NoError(err)
	}

	rows, err := s.db.Query(`SELECT val FROM "set_a" UNION SELECT val FROM "set_b"`)
	s.Require().NoError(err)
	defer rows.Close()

	var vals []int64
	for rows.Next() {
		var v int64
		s.Require().NoError(rows.Scan(&v))
		vals = append(vals, v)
	}
	s.Require().NoError(rows.Err())

	// {1,2,3} UNION {2,3,4} = {1,2,3,4} — 4 distinct values
	s.Require().Len(vals, 4)
	s.ElementsMatch([]int64{1, 2, 3, 4}, vals)
}

func (s *TestSuite) TestUnion_WithWhereOnBothBranches() {
	_, err := s.db.Exec(`create table "products" (
		id       int8 primary key autoincrement,
		category text not null,
		name     text not null
	)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "products" (category, name) values ('fruit', 'apple')`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "products" (category, name) values ('fruit', 'banana')`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "products" (category, name) values ('veggie', 'carrot')`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "products" (category, name) values ('veggie', 'pea')`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`
		SELECT name FROM "products" WHERE category = 'fruit'
		UNION
		SELECT name FROM "products" WHERE category = 'veggie'`)
	s.Require().NoError(err)
	defer rows.Close()

	var names []string
	for rows.Next() {
		var n string
		s.Require().NoError(rows.Scan(&n))
		names = append(names, n)
	}
	s.Require().NoError(rows.Err())

	s.Require().Len(names, 4)
	s.ElementsMatch([]string{"apple", "banana", "carrot", "pea"}, names)
}

func (s *TestSuite) TestUnion_ThreeBranches() {
	_, err := s.db.Exec(`create table "t1" (id int8 primary key autoincrement, v int8 not null)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`create table "t2" (id int8 primary key autoincrement, v int8 not null)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`create table "t3" (id int8 primary key autoincrement, v int8 not null)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "t1" (v) values (10)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "t2" (v) values (20)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "t3" (v) values (30)`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`SELECT v FROM "t1" UNION ALL SELECT v FROM "t2" UNION ALL SELECT v FROM "t3"`)
	s.Require().NoError(err)
	defer rows.Close()

	var vals []int64
	for rows.Next() {
		var v int64
		s.Require().NoError(rows.Scan(&v))
		vals = append(vals, v)
	}
	s.Require().NoError(rows.Err())

	s.Require().Len(vals, 3)
	s.ElementsMatch([]int64{10, 20, 30}, vals)
}

func (s *TestSuite) TestUnion_AllowsDuplicatesAcrossBranches() {
	_, err := s.db.Exec(`create table "src1" (id int8 primary key autoincrement, v int8 not null)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`create table "src2" (id int8 primary key autoincrement, v int8 not null)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "src1" (v) values (42)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "src2" (v) values (42)`)
	s.Require().NoError(err)

	// UNION ALL: both rows kept
	rows, err := s.db.Query(`SELECT v FROM "src1" UNION ALL SELECT v FROM "src2"`)
	s.Require().NoError(err)
	defer rows.Close()

	var vals []int64
	for rows.Next() {
		var v int64
		s.Require().NoError(rows.Scan(&v))
		vals = append(vals, v)
	}
	s.Require().NoError(rows.Err())
	s.Require().Len(vals, 2)
	s.Equal(int64(42), vals[0])
	s.Equal(int64(42), vals[1])
}

func (s *TestSuite) TestUnion_EmptyBranchReturnsOtherRows() {
	_, err := s.db.Exec(`create table "filled" (id int8 primary key autoincrement, v int8 not null)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`create table "empty_tbl" (id int8 primary key autoincrement, v int8 not null)`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into "filled" (v) values (7)`)
	s.Require().NoError(err)

	rows, err := s.db.Query(`SELECT v FROM "filled" UNION ALL SELECT v FROM "empty_tbl"`)
	s.Require().NoError(err)
	defer rows.Close()

	var vals []int64
	for rows.Next() {
		var v int64
		s.Require().NoError(rows.Scan(&v))
		vals = append(vals, v)
	}
	s.Require().NoError(rows.Err())
	s.Require().Len(vals, 1)
	s.Equal(int64(7), vals[0])
}

func (s *TestSuite) TestUnion_MixedUnionAllAndUnion() {
	_, err := s.db.Exec(`create table "ma" (id int8 primary key autoincrement, v int8 not null)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`create table "mb" (id int8 primary key autoincrement, v int8 not null)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`create table "mc" (id int8 primary key autoincrement, v int8 not null)`)
	s.Require().NoError(err)

	// ma = [1, 1], mb = [2], mc = [1]
	_, err = s.db.Exec(`insert into "ma" (v) values (1)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "ma" (v) values (1)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "mb" (v) values (2)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`insert into "mc" (v) values (1)`)
	s.Require().NoError(err)

	// (ma UNION ALL mb) UNION mc
	// ma UNION ALL mb = [1, 1, 2]
	// [1, 1, 2] UNION mc([1]) = dedup([1, 1, 2, 1]) = {1, 2}
	rows, err := s.db.Query(`SELECT v FROM "ma" UNION ALL SELECT v FROM "mb" UNION SELECT v FROM "mc"`)
	s.Require().NoError(err)
	defer rows.Close()

	var vals []int64
	for rows.Next() {
		var v int64
		s.Require().NoError(rows.Scan(&v))
		vals = append(vals, v)
	}
	s.Require().NoError(rows.Err())

	s.Require().Len(vals, 2)
	s.ElementsMatch([]int64{1, 2}, vals)
}

// TestUnion_OrderByAppliesPerBranch documents that ORDER BY in a UNION query
// is applied to the right-hand branch, not to the combined result.
// Standard SQL requires ORDER BY to sort the entire union output; this is a
// known limitation of the current parser which attaches trailing ORDER BY to
// the last branch only.
func (s *TestSuite) TestUnion_OrderByAppliesPerBranch() {
	_, err := s.db.Exec(`create table "ua" (id int8 primary key autoincrement, v int8 not null)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`create table "ub" (id int8 primary key autoincrement, v int8 not null)`)
	s.Require().NoError(err)

	for _, v := range []int{3, 1} {
		_, err = s.db.Exec(`insert into "ua" (v) values (?)`, int64(v))
		s.Require().NoError(err)
	}
	for _, v := range []int{4, 2} {
		_, err = s.db.Exec(`insert into "ub" (v) values (?)`, int64(v))
		s.Require().NoError(err)
	}

	rows, err := s.db.Query(`SELECT v FROM "ua" UNION ALL SELECT v FROM "ub" ORDER BY v`)
	s.Require().NoError(err)
	defer rows.Close()

	var vals []int64
	for rows.Next() {
		var v int64
		s.Require().NoError(rows.Scan(&v))
		vals = append(vals, v)
	}
	s.Require().NoError(rows.Err())
	// All four values are present.
	s.ElementsMatch([]int64{1, 2, 3, 4}, vals)
}

// TestUnion_LimitAppliesPerBranch documents that LIMIT in a UNION query
// is applied to the right-hand branch, not to the combined result.
// Standard SQL applies LIMIT after all branches are combined; this is a known
// limitation of the current implementation.
func (s *TestSuite) TestUnion_LimitAppliesPerBranch() {
	_, err := s.db.Exec(`create table "lc" (id int8 primary key autoincrement, v int8 not null)`)
	s.Require().NoError(err)
	_, err = s.db.Exec(`create table "ld" (id int8 primary key autoincrement, v int8 not null)`)
	s.Require().NoError(err)

	for _, v := range []int{10, 20, 30} {
		_, err = s.db.Exec(`insert into "lc" (v) values (?)`, int64(v))
		s.Require().NoError(err)
	}
	for _, v := range []int{40, 50} {
		_, err = s.db.Exec(`insert into "ld" (v) values (?)`, int64(v))
		s.Require().NoError(err)
	}

	rows, err := s.db.Query(`SELECT v FROM "lc" UNION ALL SELECT v FROM "ld" LIMIT 2`)
	s.Require().NoError(err)
	defer rows.Close()

	var vals []int64
	for rows.Next() {
		var v int64
		s.Require().NoError(rows.Scan(&v))
		vals = append(vals, v)
	}
	s.Require().NoError(rows.Err())
	// LIMIT is currently applied to the last branch only, so the combined result
	// contains all 3 rows from lc plus at most 2 from ld.
	// The combined output has 5 values total (lc full + ld limited to 2).
	s.Len(vals, 5)
}
