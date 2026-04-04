package e2etests

import (
	"context"
	"math"
	"sort"
)

func (s *TestSuite) TestAggregateWithoutGroupBy() {
	_, err := s.db.Exec(createOrdersTableSQL)
	s.Require().NoError(err)

	// Insert rows with known values so we can assert exact results.
	// total_paid values: 10, 20, 30, 40, 50  (sum=150, avg=30, min=10, max=50)
	s.execQuery(`insert into orders(user_id, product_id, total_paid) values
(1, 1, 10),
(1, 2, 20),
(2, 1, 30),
(2, 3, 40),
(3, 2, 50);`, 5)

	s.Run("SUM of integer column", func() {
		rows, err := s.db.QueryContext(context.Background(), `select SUM(total_paid) from orders;`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var sum int64
		s.Require().NoError(rows.Scan(&sum))
		s.Equal(int64(150), sum)
		s.False(rows.Next())
		s.Require().NoError(rows.Err())
	})

	s.Run("MIN of integer column", func() {
		rows, err := s.db.QueryContext(context.Background(), `select MIN(total_paid) from orders;`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var minVal int64
		s.Require().NoError(rows.Scan(&minVal))
		s.Equal(int64(10), minVal)
		s.False(rows.Next())
		s.Require().NoError(rows.Err())
	})

	s.Run("MAX of integer column", func() {
		rows, err := s.db.QueryContext(context.Background(), `select MAX(total_paid) from orders;`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var maxVal int64
		s.Require().NoError(rows.Scan(&maxVal))
		s.Equal(int64(50), maxVal)
		s.False(rows.Next())
		s.Require().NoError(rows.Err())
	})

	s.Run("AVG of integer column", func() {
		rows, err := s.db.QueryContext(context.Background(), `select AVG(total_paid) from orders;`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var avg float64
		s.Require().NoError(rows.Scan(&avg))
		s.InDelta(30.0, avg, 0.001)
		s.False(rows.Next())
		s.Require().NoError(rows.Err())
	})

	s.Run("Multiple aggregates in one query", func() {
		rows, err := s.db.QueryContext(context.Background(), `select MIN(total_paid), MAX(total_paid), SUM(total_paid), AVG(total_paid) from orders;`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var minV, maxV int64
		var sumV int64
		var avgV float64
		s.Require().NoError(rows.Scan(&minV, &maxV, &sumV, &avgV))
		s.Equal(int64(10), minV)
		s.Equal(int64(50), maxV)
		s.Equal(int64(150), sumV)
		s.InDelta(30.0, avgV, 0.001)
		s.False(rows.Next())
		s.Require().NoError(rows.Err())
	})

	s.Run("Aggregate with WHERE filter", func() {
		// Only rows where total_paid > 20: 30, 40, 50 → sum=120, min=30, max=50
		rows, err := s.db.QueryContext(context.Background(), `select SUM(total_paid), MIN(total_paid), MAX(total_paid) from orders where total_paid > 20;`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var sumV, minV, maxV int64
		s.Require().NoError(rows.Scan(&sumV, &minV, &maxV))
		s.Equal(int64(120), sumV)
		s.Equal(int64(30), minV)
		s.Equal(int64(50), maxV)
		s.False(rows.Next())
		s.Require().NoError(rows.Err())
	})

	s.Run("Aggregate with WHERE that matches no rows returns NULL", func() {
		rows, err := s.db.QueryContext(context.Background(), `select SUM(total_paid), MIN(total_paid), MAX(total_paid), AVG(total_paid) from orders where total_paid > 9999;`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		// All aggregates on an empty set return NULL
		var sumV, minV, maxV *int64
		var avgV *float64
		s.Require().NoError(rows.Scan(&sumV, &minV, &maxV, &avgV))
		s.Nil(sumV)
		s.Nil(minV)
		s.Nil(maxV)
		s.Nil(avgV)
		s.False(rows.Next())
		s.Require().NoError(rows.Err())
	})

	s.Run("Non-aggregate column without GROUP BY is rejected", func() {
		_, err := s.db.Exec(`select user_id, SUM(total_paid) from orders;`)
		s.Require().Error(err)
		s.Contains(err.Error(), "non-aggregate column")
	})

	s.Run("SUM on non-numeric column is rejected", func() {
		_, err := s.db.Exec(createUsersTableSQL)
		s.Require().NoError(err)

		_, err = s.db.Exec(`select SUM(name) from users;`)
		s.Require().Error(err)
		s.Contains(err.Error(), "must be numeric")
	})
}

func (s *TestSuite) TestAggregateOnFloatColumn() {
	_, err := s.db.Exec(createProductsTableSQL)
	s.Require().NoError(err)

	// price is int4 in createProductsTableSQL; let's use a table with a DOUBLE column.
	_, err = s.db.Exec(`create table "metrics" (
	id int8 primary key autoincrement,
	value double not null
);`)
	s.Require().NoError(err)

	s.execQuery(`insert into metrics(value) values(1.5),(2.5),(3.5),(4.5),(8.5);`, 5)
	// sum=20.5, avg=4.1, min=1.5, max=8.5

	s.Run("SUM of double column", func() {
		rows, err := s.db.QueryContext(context.Background(), `select SUM(value) from metrics;`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var sum float64
		s.Require().NoError(rows.Scan(&sum))
		s.InDelta(20.5, sum, 0.001)
		s.False(rows.Next())
		s.Require().NoError(rows.Err())
	})

	s.Run("AVG of double column", func() {
		rows, err := s.db.QueryContext(context.Background(), `select AVG(value) from metrics;`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var avg float64
		s.Require().NoError(rows.Scan(&avg))
		s.InDelta(4.1, avg, 0.001)
		s.False(rows.Next())
		s.Require().NoError(rows.Err())
	})

	s.Run("MIN and MAX of double column", func() {
		rows, err := s.db.QueryContext(context.Background(), `select MIN(value), MAX(value) from metrics;`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var minV, maxV float64
		s.Require().NoError(rows.Scan(&minV, &maxV))
		s.InDelta(1.5, minV, 0.001)
		s.InDelta(8.5, maxV, 0.001)
		s.False(rows.Next())
		s.Require().NoError(rows.Err())
	})
}

// TestAggregateMinMaxWithIndex verifies that MIN/MAX on an indexed column produces
// correct results via the index endpoint optimisation (ScanTypeIndexFirst/Last).
func (s *TestSuite) TestAggregateMinMaxWithIndex() {
	// Create a table where the column being aggregated has a secondary index.
	_, err := s.db.Exec(`create table "scores" (
	id    int8 primary key autoincrement,
	value int4 not null
);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`create index "idx_scores_value" on "scores" (value);`)
	s.Require().NoError(err)

	// Insert rows in non-sorted order to ensure we're not just reading insertion order.
	s.execQuery(`insert into scores(value) values(30),(10),(50),(20),(40);`, 5)

	s.Run("MIN uses index endpoint and returns smallest value", func() {
		rows, err := s.db.QueryContext(context.Background(), `select MIN(value) from scores;`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var minVal int64
		s.Require().NoError(rows.Scan(&minVal))
		s.Equal(int64(10), minVal)
		s.False(rows.Next())
		s.Require().NoError(rows.Err())
	})

	s.Run("MAX uses index endpoint and returns largest value", func() {
		rows, err := s.db.QueryContext(context.Background(), `select MAX(value) from scores;`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var maxVal int64
		s.Require().NoError(rows.Scan(&maxVal))
		s.Equal(int64(50), maxVal)
		s.False(rows.Next())
		s.Require().NoError(rows.Err())
	})

	s.Run("MIN on primary key uses index endpoint", func() {
		rows, err := s.db.QueryContext(context.Background(), `select MIN(id) from scores;`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var minVal int64
		s.Require().NoError(rows.Scan(&minVal))
		s.Equal(int64(1), minVal)
		s.False(rows.Next())
		s.Require().NoError(rows.Err())
	})

	s.Run("MAX on primary key uses index endpoint", func() {
		rows, err := s.db.QueryContext(context.Background(), `select MAX(id) from scores;`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var maxVal int64
		s.Require().NoError(rows.Scan(&maxVal))
		s.Equal(int64(5), maxVal)
		s.False(rows.Next())
		s.Require().NoError(rows.Err())
	})

	s.Run("MIN on empty table via index returns NULL", func() {
		_, err := s.db.Exec(`create table "empty_scores" (
		id    int8 primary key autoincrement,
		value int4 not null
	);`)
		s.Require().NoError(err)

		_, err = s.db.Exec(`create index "idx_empty_scores_value" on "empty_scores" (value);`)
		s.Require().NoError(err)

		rows, err := s.db.QueryContext(context.Background(), `select MIN(value) from empty_scores;`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var minVal *int64
		s.Require().NoError(rows.Scan(&minVal))
		s.Nil(minVal)
		s.False(rows.Next())
		s.Require().NoError(rows.Err())
	})
}

func (s *TestSuite) TestAggregateAVGFractional() {
	_, err := s.db.Exec(`create table "nums" (
	id int8 primary key autoincrement,
	n int4 not null
);`)
	s.Require().NoError(err)

	// 1 + 2 = 3, avg = 1.5
	s.execQuery(`insert into nums(n) values(1),(2);`, 2)

	rows, err := s.db.QueryContext(context.Background(), `select AVG(n) from nums;`)
	s.Require().NoError(err)
	defer rows.Close()

	s.Require().True(rows.Next())
	var avg float64
	s.Require().NoError(rows.Scan(&avg))
	// AVG(1,2) = 1.5, not 1 (integer truncation must not occur)
	s.InDelta(1.5, avg, 0.001)
	s.False(math.IsNaN(avg))
}

func (s *TestSuite) TestGroupBy() {
	_, err := s.db.Exec(createOrdersTableSQL)
	s.Require().NoError(err)

	// user_id=1: total_paid 10, 20  → sum=30, count=2
	// user_id=2: total_paid 30, 40  → sum=70, count=2
	// user_id=3: total_paid 50      → sum=50, count=1
	s.execQuery(`insert into orders(user_id, product_id, total_paid) values
(1, 1, 10),
(1, 2, 20),
(2, 1, 30),
(2, 3, 40),
(3, 2, 50);`, 5)

	s.Run("SUM grouped by user_id", func() {
		rows, err := s.db.QueryContext(context.Background(), `select user_id, SUM(total_paid) from orders GROUP BY user_id;`)
		s.Require().NoError(err)
		defer rows.Close()

		type row struct {
			userID int64
			sum    int64
		}
		var got []row
		for rows.Next() {
			var r row
			s.Require().NoError(rows.Scan(&r.userID, &r.sum))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(got, 3)

		// Sort by user_id for deterministic comparison.
		sort.Slice(got, func(i, j int) bool { return got[i].userID < got[j].userID })
		s.Equal(int64(1), got[0].userID)
		s.Equal(int64(30), got[0].sum)
		s.Equal(int64(2), got[1].userID)
		s.Equal(int64(70), got[1].sum)
		s.Equal(int64(3), got[2].userID)
		s.Equal(int64(50), got[2].sum)
	})

	s.Run("COUNT grouped by user_id", func() {
		rows, err := s.db.QueryContext(context.Background(), `select user_id, COUNT(*) from orders GROUP BY user_id;`)
		s.Require().NoError(err)
		defer rows.Close()

		type row struct {
			userID int64
			count  int64
		}
		var got []row
		for rows.Next() {
			var r row
			s.Require().NoError(rows.Scan(&r.userID, &r.count))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(got, 3)

		sort.Slice(got, func(i, j int) bool { return got[i].userID < got[j].userID })
		s.Equal(int64(1), got[0].userID)
		s.Equal(int64(2), got[0].count)
		s.Equal(int64(2), got[1].userID)
		s.Equal(int64(2), got[1].count)
		s.Equal(int64(3), got[2].userID)
		s.Equal(int64(1), got[2].count)
	})

	s.Run("MIN and MAX grouped by user_id", func() {
		rows, err := s.db.QueryContext(context.Background(), `select user_id, MIN(total_paid), MAX(total_paid) from orders GROUP BY user_id;`)
		s.Require().NoError(err)
		defer rows.Close()

		type row struct {
			userID int64
			min    int64
			max    int64
		}
		var got []row
		for rows.Next() {
			var r row
			s.Require().NoError(rows.Scan(&r.userID, &r.min, &r.max))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(got, 3)

		sort.Slice(got, func(i, j int) bool { return got[i].userID < got[j].userID })
		s.Equal(int64(10), got[0].min)
		s.Equal(int64(20), got[0].max)
		s.Equal(int64(30), got[1].min)
		s.Equal(int64(40), got[1].max)
		s.Equal(int64(50), got[2].min)
		s.Equal(int64(50), got[2].max)
	})

	s.Run("AVG grouped by user_id", func() {
		rows, err := s.db.QueryContext(context.Background(), `select user_id, AVG(total_paid) from orders GROUP BY user_id;`)
		s.Require().NoError(err)
		defer rows.Close()

		type row struct {
			userID int64
			avg    float64
		}
		var got []row
		for rows.Next() {
			var r row
			s.Require().NoError(rows.Scan(&r.userID, &r.avg))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(got, 3)

		sort.Slice(got, func(i, j int) bool { return got[i].userID < got[j].userID })
		s.InDelta(15.0, got[0].avg, 0.001) // (10+20)/2
		s.InDelta(35.0, got[1].avg, 0.001) // (30+40)/2
		s.InDelta(50.0, got[2].avg, 0.001) // 50/1
	})

	s.Run("GROUP BY with WHERE filter", func() {
		// Only rows with total_paid >= 20: user_id=1 has 20, user_id=2 has 30+40, user_id=3 has 50
		rows, err := s.db.QueryContext(context.Background(), `select user_id, SUM(total_paid) from orders where total_paid >= 20 GROUP BY user_id;`)
		s.Require().NoError(err)
		defer rows.Close()

		type row struct {
			userID int64
			sum    int64
		}
		var got []row
		for rows.Next() {
			var r row
			s.Require().NoError(rows.Scan(&r.userID, &r.sum))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(got, 3)

		sort.Slice(got, func(i, j int) bool { return got[i].userID < got[j].userID })
		s.Equal(int64(20), got[0].sum)  // user 1: only 20 passes filter
		s.Equal(int64(70), got[1].sum)  // user 2: 30+40
		s.Equal(int64(50), got[2].sum)  // user 3: 50
	})

	s.Run("GROUP BY with ORDER BY", func() {
		rows, err := s.db.QueryContext(context.Background(), `select user_id, SUM(total_paid) from orders GROUP BY user_id ORDER BY user_id DESC;`)
		s.Require().NoError(err)
		defer rows.Close()

		var userIDs []int64
		for rows.Next() {
			var uid int64
			var sum int64
			s.Require().NoError(rows.Scan(&uid, &sum))
			userIDs = append(userIDs, uid)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(userIDs, 3)
		// Should be ordered DESC: 3, 2, 1
		s.Equal(int64(3), userIDs[0])
		s.Equal(int64(2), userIDs[1])
		s.Equal(int64(1), userIDs[2])
	})

	s.Run("GROUP BY with LIMIT", func() {
		rows, err := s.db.QueryContext(context.Background(), `select user_id, SUM(total_paid) from orders GROUP BY user_id ORDER BY user_id ASC LIMIT 2;`)
		s.Require().NoError(err)
		defer rows.Close()

		type row struct {
			userID int64
			sum    int64
		}
		var got []row
		for rows.Next() {
			var r row
			s.Require().NoError(rows.Scan(&r.userID, &r.sum))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(got, 2)
		s.Equal(int64(1), got[0].userID)
		s.Equal(int64(2), got[1].userID)
	})

	s.Run("non-aggregate column without GROUP BY is rejected", func() {
		_, err := s.db.Exec(`select user_id, SUM(total_paid) from orders;`)
		s.Require().Error(err)
		s.Contains(err.Error(), "non-aggregate column")
	})
}

func (s *TestSuite) TestHaving() {
	_, err := s.db.Exec(createOrdersTableSQL)
	s.Require().NoError(err)

	// user_id=1: total_paid 10, 20  → sum=30, count=2
	// user_id=2: total_paid 30, 40  → sum=70, count=2
	// user_id=3: total_paid 50      → sum=50, count=1
	s.execQuery(`insert into orders(user_id, product_id, total_paid) values
(1, 1, 10),
(1, 2, 20),
(2, 1, 30),
(2, 3, 40),
(3, 2, 50);`, 5)

	s.Run("HAVING filters groups by aggregate", func() {
		// Only groups where SUM(total_paid) > 40: user_id=2 (70) and user_id=3 (50).
		rows, err := s.db.QueryContext(context.Background(), `select user_id, SUM(total_paid) from orders GROUP BY user_id HAVING SUM(total_paid) > 40;`)
		s.Require().NoError(err)
		defer rows.Close()

		type row struct {
			userID int64
			sum    int64
		}
		var got []row
		for rows.Next() {
			var r row
			s.Require().NoError(rows.Scan(&r.userID, &r.sum))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(got, 2)

		sort.Slice(got, func(i, j int) bool { return got[i].userID < got[j].userID })
		s.Equal(int64(2), got[0].userID)
		s.Equal(int64(70), got[0].sum)
		s.Equal(int64(3), got[1].userID)
		s.Equal(int64(50), got[1].sum)
	})

	s.Run("HAVING filters groups by COUNT", func() {
		// Only groups with COUNT(*) >= 2: user_id=1 and user_id=2.
		rows, err := s.db.QueryContext(context.Background(), `select user_id, COUNT(*) from orders GROUP BY user_id HAVING COUNT(*) >= 2;`)
		s.Require().NoError(err)
		defer rows.Close()

		type row struct {
			userID int64
			count  int64
		}
		var got []row
		for rows.Next() {
			var r row
			s.Require().NoError(rows.Scan(&r.userID, &r.count))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(got, 2)

		sort.Slice(got, func(i, j int) bool { return got[i].userID < got[j].userID })
		s.Equal(int64(1), got[0].userID)
		s.Equal(int64(2), got[0].count)
		s.Equal(int64(2), got[1].userID)
		s.Equal(int64(2), got[1].count)
	})

	s.Run("HAVING on GROUP BY column", func() {
		// Only groups where user_id > 1.
		rows, err := s.db.QueryContext(context.Background(), `select user_id, SUM(total_paid) from orders GROUP BY user_id HAVING user_id > 1;`)
		s.Require().NoError(err)
		defer rows.Close()

		type row struct {
			userID int64
			sum    int64
		}
		var got []row
		for rows.Next() {
			var r row
			s.Require().NoError(rows.Scan(&r.userID, &r.sum))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(got, 2)

		sort.Slice(got, func(i, j int) bool { return got[i].userID < got[j].userID })
		s.Equal(int64(2), got[0].userID)
		s.Equal(int64(3), got[1].userID)
	})

	s.Run("WHERE and HAVING together", func() {
		// WHERE removes total_paid=10 first; then GROUP BY:
		// user_id=1: 20 → sum=20, user_id=2: 30+40 → sum=70, user_id=3: 50 → sum=50
		// HAVING SUM > 30 keeps user_id=2 (70) and user_id=3 (50).
		rows, err := s.db.QueryContext(context.Background(), `select user_id, SUM(total_paid) from orders where total_paid > 10 GROUP BY user_id HAVING SUM(total_paid) > 30;`)
		s.Require().NoError(err)
		defer rows.Close()

		type row struct {
			userID int64
			sum    int64
		}
		var got []row
		for rows.Next() {
			var r row
			s.Require().NoError(rows.Scan(&r.userID, &r.sum))
			got = append(got, r)
		}
		s.Require().NoError(rows.Err())
		s.Require().Len(got, 2)

		sort.Slice(got, func(i, j int) bool { return got[i].userID < got[j].userID })
		s.Equal(int64(2), got[0].userID)
		s.Equal(int64(70), got[0].sum)
		s.Equal(int64(3), got[1].userID)
		s.Equal(int64(50), got[1].sum)
	})

	s.Run("HAVING that matches no groups returns empty result", func() {
		rows, err := s.db.QueryContext(context.Background(), `select user_id, SUM(total_paid) from orders GROUP BY user_id HAVING SUM(total_paid) > 9999;`)
		s.Require().NoError(err)
		defer rows.Close()
		s.False(rows.Next())
		s.Require().NoError(rows.Err())
	})

	s.Run("HAVING without GROUP BY is rejected", func() {
		_, err := s.db.Exec(`select SUM(total_paid) from orders HAVING SUM(total_paid) > 10;`)
		s.Require().Error(err)
		s.Contains(err.Error(), "HAVING requires GROUP BY")
	})
}

func (s *TestSuite) TestGroupByWithJoinRejected() {
	_, err := s.db.Exec(createOrdersTableSQL)
	s.Require().NoError(err)

	_, err = s.db.Exec(createUsersTableSQL)
	s.Require().NoError(err)

	_, err = s.db.Exec(`
		select o.user_id, SUM(o.total_paid)
		from orders as o
		inner join users as u on u.id = o.user_id
		GROUP BY o.user_id;`)
	s.Require().Error(err)
	s.Contains(err.Error(), "GROUP BY cannot be combined with JOIN")
}
