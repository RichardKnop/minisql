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

// TestHNSWIndex_OnlineMaintenance verifies that INSERT, DELETE and UPDATE keep
// the HNSW index consistent with the underlying table.
func (s *TestSuite) TestHNSWIndex_OnlineMaintenance() {
	_, err := s.db.Exec(`create table "om_vecs" (
		id   int8 primary key autoincrement,
		tag  varchar(50) not null,
		v    vector(2) not null
	);`)
	s.Require().NoError(err)

	// Seed a few rows so the index is non-trivial at creation time.
	seed := []string{"[1.0, 0.0]", "[2.0, 0.0]", "[3.0, 0.0]"}
	for _, vec := range seed {
		_, err := s.db.Exec(`insert into om_vecs (tag, v) values (?, ?)`, vec, vec)
		s.Require().NoError(err)
	}

	_, err = s.db.Exec(`CREATE HNSW INDEX "idx_om" ON "om_vecs" (v);`)
	s.Require().NoError(err)

	s.Run("online_insert_is_searchable", func() {
		// Insert a new row AFTER index creation — it must be reachable via ANN search.
		_, err := s.db.Exec(
			`insert into om_vecs (tag, v) values (?, ?)`,
			"new", "[10.0, 0.0]",
		)
		s.Require().NoError(err)

		res, err := s.db.QueryContext(context.Background(),
			`SELECT tag, VEC_L2(v, '[10.0, 0.0]') AS dist FROM om_vecs ORDER BY dist LIMIT 1;`)
		s.Require().NoError(err)
		defer res.Close()

		s.Require().True(res.Next())
		var tag string
		var dist float64
		s.Require().NoError(res.Scan(&tag, &dist))
		s.Equal("new", tag, "online-inserted row should be found as nearest")
		s.InDelta(0.0, dist, 1e-6)
	})

	s.Run("online_delete_is_not_returned", func() {
		// Insert a distinctive row and verify it's found.
		_, err := s.db.Exec(
			`insert into om_vecs (tag, v) values (?, ?)`,
			"todelete", "[50.0, 0.0]",
		)
		s.Require().NoError(err)

		// Confirm it's the nearest to its own position.
		res, err := s.db.QueryContext(context.Background(),
			`SELECT tag FROM om_vecs WHERE tag = ? LIMIT 1;`, "todelete")
		s.Require().NoError(err)
		s.Require().True(res.Next())
		s.Require().NoError(res.Close())

		// Delete it.
		_, err = s.db.Exec(`DELETE FROM om_vecs WHERE tag = ?;`, "todelete")
		s.Require().NoError(err)

		// After deletion, the row must not appear in any result.
		res2, err := s.db.QueryContext(context.Background(),
			`SELECT tag FROM om_vecs WHERE tag = ? LIMIT 1;`, "todelete")
		s.Require().NoError(err)
		s.False(res2.Next(), "deleted row should not be returned by table scan")
		s.Require().NoError(res2.Close())

		// ANN result should not return "todelete".
		res3, err := s.db.QueryContext(context.Background(),
			`SELECT tag, VEC_L2(v, '[50.0, 0.0]') AS dist FROM om_vecs ORDER BY dist LIMIT 1;`)
		s.Require().NoError(err)
		defer res3.Close()
		if res3.Next() {
			var tag string
			var dist float64
			s.Require().NoError(res3.Scan(&tag, &dist))
			s.NotEqual("todelete", tag, "deleted row must not be returned by ANN search")
		}
	})

	s.Run("online_update_vector_is_findable", func() {
		// Insert a row and then move its vector far away.
		_, err := s.db.Exec(
			`insert into om_vecs (tag, v) values (?, ?)`,
			"tomove", "[100.0, 0.0]",
		)
		s.Require().NoError(err)

		// Move it.
		_, err = s.db.Exec(
			`UPDATE om_vecs SET v = ? WHERE tag = ?;`,
			"[200.0, 0.0]", "tomove",
		)
		s.Require().NoError(err)

		// It should now be nearest to [200, 0], not [100, 0].
		res, err := s.db.QueryContext(context.Background(),
			`SELECT tag, VEC_L2(v, '[200.0, 0.0]') AS dist FROM om_vecs ORDER BY dist LIMIT 1;`)
		s.Require().NoError(err)
		defer res.Close()

		s.Require().True(res.Next())
		var tag string
		var dist float64
		s.Require().NoError(res.Scan(&tag, &dist))
		s.Equal("tomove", tag, "updated row should be found at its new position")
		s.InDelta(0.0, dist, 1e-6)
	})
}
