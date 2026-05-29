package e2etests

import (
	"context"
	"fmt"
)

var createEmbeddingsTableSQL = `create table "embeddings" (
	id int8 primary key autoincrement,
	label varchar(100) not null,
	vec vector(3) not null
);`

// TestHNSWIndex_CreateAndSearch verifies the end-to-end HNSW index lifecycle:
// CREATE HNSW INDEX, planner selection, and approximate nearest-neighbor results.
func (s *TestSuite) TestHNSWIndex_CreateAndSearch() {
	_, err := s.db.Exec(createEmbeddingsTableSQL)
	s.Require().NoError(err)

	// Insert a set of 3-D unit vectors at known positions.
	rows := []struct {
		label string
		vec   string
	}{
		{"origin", "[0.0, 0.0, 0.0]"},
		{"x1", "[1.0, 0.0, 0.0]"},
		{"x2", "[2.0, 0.0, 0.0]"},
		{"y1", "[0.0, 1.0, 0.0]"},
		{"z1", "[0.0, 0.0, 1.0]"},
		{"diag", "[1.0, 1.0, 1.0]"},
		{"far", "[10.0, 10.0, 10.0]"},
	}
	for _, r := range rows {
		_, err := s.db.Exec(
			`insert into embeddings (label, vec) values (?, ?)`,
			r.label, r.vec,
		)
		s.Require().NoError(err)
	}

	s.Run("create_hnsw_index_default_params", func() {
		_, err := s.db.Exec(`CREATE HNSW INDEX "idx_vec" ON "embeddings" (vec);`)
		s.Require().NoError(err)
	})

	s.Run("hnsw_index_rejects_non_vector_column", func() {
		_, err := s.db.Exec(`CREATE HNSW INDEX "idx_label" ON "embeddings" (label);`)
		s.Require().Error(err)
		s.Contains(err.Error(), `must be VECTOR(n)`)
	})

	s.Run("top1_nearest_to_x1_returns_x1", func() {
		res, err := s.db.QueryContext(context.Background(),
			`SELECT id, label, VEC_L2(vec, '[1.0, 0.0, 0.0]') AS dist FROM embeddings ORDER BY dist LIMIT 1;`)
		s.Require().NoError(err)
		defer res.Close()

		s.Require().True(res.Next())
		var id int64
		var label string
		var dist float64
		s.Require().NoError(res.Scan(&id, &label, &dist))
		s.Equal("x1", label, "nearest to [1,0,0] should be x1 (distance 0)")
		s.InDelta(0.0, dist, 1e-6)
		s.False(res.Next())
		s.Require().NoError(res.Err())
	})

	s.Run("top3_nearest_to_origin", func() {
		res, err := s.db.QueryContext(context.Background(),
			`SELECT id, label, VEC_L2(vec, '[0.0, 0.0, 0.0]') AS dist FROM embeddings ORDER BY dist LIMIT 3;`)
		s.Require().NoError(err)
		defer res.Close()

		var labels []string
		for res.Next() {
			var id int64
			var label string
			var dist float64
			s.Require().NoError(res.Scan(&id, &label, &dist))
			labels = append(labels, label)
		}
		s.Require().NoError(res.Err())
		// The first result must be "origin" (distance 0).
		s.Require().NotEmpty(labels)
		s.Equal("origin", labels[0], "nearest to [0,0,0] should be 'origin'")
		s.LessOrEqual(len(labels), 3)
	})

	s.Run("explain_shows_hnsw_scan", func() {
		rows := s.collectExplain(`EXPLAIN SELECT id, VEC_L2(vec, '[1.0, 0.0, 0.0]') AS dist FROM embeddings ORDER BY dist LIMIT 1;`)
		s.Require().NotEmpty(rows)
		ops := make([]string, 0, len(rows))
		for _, r := range rows {
			ops = append(ops, r.Operation)
		}
		s.Contains(ops, "hnsw_scan", "EXPLAIN should show hnsw_scan for vector ANN query")
	})
}

func (s *TestSuite) TestHNSWIndex_WithParams() {
	_, err := s.db.Exec(`create table "vecs_params" (
		id int8 primary key autoincrement,
		v vector(2) not null
	);`)
	s.Require().NoError(err)

	// Insert simple 2-D vectors.
	for i := 1; i <= 10; i++ {
		_, err := s.db.Exec(
			`insert into vecs_params (v) values (?)`,
			fmt.Sprintf("[%d.0, 0.0]", i),
		)
		s.Require().NoError(err)
	}

	s.Run("create_with_custom_m_and_ef", func() {
		_, err := s.db.Exec(`CREATE HNSW INDEX "idx_v" ON "vecs_params" (v) WITH (m = 8, ef_construction = 50);`)
		s.Require().NoError(err)
	})

	s.Run("returns_correct_nearest", func() {
		res, err := s.db.QueryContext(context.Background(),
			`SELECT id, VEC_L2(v, '[5.0, 0.0]') AS dist FROM vecs_params ORDER BY dist LIMIT 1;`)
		s.Require().NoError(err)
		defer res.Close()

		s.Require().True(res.Next())
		var id int64
		var dist float64
		s.Require().NoError(res.Scan(&id, &dist))
		s.Equal(int64(5), id, "nearest to [5.0, 0.0] should be row 5")
		s.InDelta(0.0, dist, 1e-6)
		s.Require().NoError(res.Err())
	})

	s.Run("if_not_exists_does_not_fail", func() {
		_, err := s.db.Exec(`CREATE HNSW INDEX IF NOT EXISTS "idx_v" ON "vecs_params" (v);`)
		s.Require().NoError(err)
	})
}
