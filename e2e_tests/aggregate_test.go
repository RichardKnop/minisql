package e2etests

import (
	"context"
	"math"
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
		var min int64
		s.Require().NoError(rows.Scan(&min))
		s.Equal(int64(10), min)
		s.False(rows.Next())
		s.Require().NoError(rows.Err())
	})

	s.Run("MAX of integer column", func() {
		rows, err := s.db.QueryContext(context.Background(), `select MAX(total_paid) from orders;`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var max int64
		s.Require().NoError(rows.Scan(&max))
		s.Equal(int64(50), max)
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
		_, err := s.db.QueryContext(context.Background(), `select user_id, SUM(total_paid) from orders;`)
		s.Require().Error(err)
		s.Contains(err.Error(), "non-aggregate column")
	})

	s.Run("SUM on non-numeric column is rejected", func() {
		_, err := s.db.Exec(createUsersTableSQL)
		s.Require().NoError(err)

		_, err = s.db.QueryContext(context.Background(), `select SUM(name) from users;`)
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
