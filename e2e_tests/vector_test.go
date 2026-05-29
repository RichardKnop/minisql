package e2etests

import (
	"context"
	"math"
)

var createDocumentsVectorTableSQL = `create table "documents" (
	id int8 primary key autoincrement,
	body text not null,
	embedding vector(3) not null
);`

func (s *TestSuite) TestVectorColumn() {
	_, err := s.db.Exec(createDocumentsVectorTableSQL)
	s.Require().NoError(err)

	s.Run("INSERT_and_SELECT", func() {
		_, err := s.db.Exec(
			`insert into documents (body, embedding) values (?, ?)`,
			"hello world", "[0.1, 0.2, 0.3]",
		)
		s.Require().NoError(err)

		rows, err := s.db.QueryContext(context.Background(),
			`select id, body, embedding from documents where id = 1;`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var id int64
		var body, embedding string
		s.Require().NoError(rows.Scan(&id, &body, &embedding))
		s.Equal(int64(1), id)
		s.Equal("hello world", body)
		s.Equal("[0.1, 0.2, 0.3]", embedding)
		s.False(rows.Next())
		s.Require().NoError(rows.Err())
	})

	s.Run("INSERT_multiple_rows", func() {
		vecs := [][2]string{
			{"doc A", "[1.0, 0.0, 0.0]"},
			{"doc B", "[0.0, 1.0, 0.0]"},
			{"doc C", "[0.0, 0.0, 1.0]"},
		}
		for _, v := range vecs {
			_, err := s.db.Exec(
				`insert into documents (body, embedding) values (?, ?)`,
				v[0], v[1],
			)
			s.Require().NoError(err)
		}

		rows, err := s.db.QueryContext(context.Background(),
			`select count(*) from documents;`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var count int64
		s.Require().NoError(rows.Scan(&count))
		s.Equal(int64(4), count) // 1 from previous sub-test + 3
	})
}

func (s *TestSuite) TestVecL2Distance() {
	_, err := s.db.Exec(createDocumentsVectorTableSQL)
	s.Require().NoError(err)

	// Insert docs with known embeddings.
	docs := []struct {
		body      string
		embedding string
	}{
		{"origin", "[0.0, 0.0, 0.0]"},
		{"point a", "[1.0, 0.0, 0.0]"},
		{"point b", "[3.0, 4.0, 0.0]"}, // L2 distance from origin = 5
		{"point c", "[0.0, 0.0, 1.0]"},
	}
	for _, d := range docs {
		_, err := s.db.Exec(
			`insert into documents (body, embedding) values (?, ?)`,
			d.body, d.embedding,
		)
		s.Require().NoError(err)
	}

	s.Run("VEC_L2_distance_computed_correctly", func() {
		rows, err := s.db.QueryContext(context.Background(),
			`select body, VEC_L2(embedding, '[0.0, 0.0, 0.0]') AS dist FROM documents ORDER BY id;`)
		s.Require().NoError(err)
		defer rows.Close()

		expected := []struct {
			body string
			dist float64
		}{
			{"origin", 0.0},
			{"point a", 1.0},
			{"point b", 5.0},
			{"point c", 1.0},
		}
		for _, exp := range expected {
			s.Require().True(rows.Next())
			var body string
			var dist float64
			s.Require().NoError(rows.Scan(&body, &dist))
			s.Equal(exp.body, body)
			s.InDelta(exp.dist, dist, 0.0001)
		}
		s.False(rows.Next())
		s.Require().NoError(rows.Err())
	})

	s.Run("VEC_L2_nearest_neighbor", func() {
		// Find the nearest neighbor to [1.0, 0.0, 0.0].
		rows, err := s.db.QueryContext(context.Background(),
			`select body, VEC_L2(embedding, '[1.0, 0.0, 0.0]') AS dist FROM documents ORDER BY dist LIMIT 1;`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var body string
		var dist float64
		s.Require().NoError(rows.Scan(&body, &dist))
		s.Equal("point a", body)
		s.InDelta(0.0, dist, 0.0001)
		s.False(rows.Next())
		s.Require().NoError(rows.Err())
	})
}

func (s *TestSuite) TestVecCosineDistance() {
	_, err := s.db.Exec(createDocumentsVectorTableSQL)
	s.Require().NoError(err)

	docs := []struct {
		body      string
		embedding string
	}{
		{"x-axis", "[1.0, 0.0, 0.0]"},
		{"y-axis", "[0.0, 1.0, 0.0]"},
		{"diagonal", "[1.0, 1.0, 0.0]"},
	}
	for _, d := range docs {
		_, err := s.db.Exec(
			`insert into documents (body, embedding) values (?, ?)`,
			d.body, d.embedding,
		)
		s.Require().NoError(err)
	}

	s.Run("VEC_COSINE_identical_direction", func() {
		// x-axis vs x-axis → cosine distance 0
		rows, err := s.db.QueryContext(context.Background(),
			`select body, VEC_COSINE(embedding, '[1.0, 0.0, 0.0]') AS dist FROM documents WHERE id = 1;`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var body string
		var dist float64
		s.Require().NoError(rows.Scan(&body, &dist))
		s.Equal("x-axis", body)
		s.InDelta(0.0, dist, 0.0001)
	})

	s.Run("VEC_COSINE_orthogonal", func() {
		// x-axis vs y-axis → cosine distance 1
		rows, err := s.db.QueryContext(context.Background(),
			`select body, VEC_COSINE(embedding, '[0.0, 1.0, 0.0]') AS dist FROM documents WHERE id = 1;`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var body string
		var dist float64
		s.Require().NoError(rows.Scan(&body, &dist))
		s.Equal("x-axis", body)
		s.InDelta(1.0, dist, 0.0001)
	})

	s.Run("VEC_COSINE_top_k_search", func() {
		// Find top-2 most similar to [1.0, 0.0, 0.0].
		rows, err := s.db.QueryContext(context.Background(),
			`select body, VEC_COSINE(embedding, '[1.0, 0.0, 0.0]') AS dist FROM documents ORDER BY dist LIMIT 2;`)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var body string
		var dist float64
		s.Require().NoError(rows.Scan(&body, &dist))
		// x-axis is identical → distance 0
		s.Equal("x-axis", body)
		s.InDelta(0.0, dist, 0.0001)

		s.Require().True(rows.Next())
		s.Require().NoError(rows.Scan(&body, &dist))
		// diagonal is 45° off → cosine distance = 1 - 1/sqrt(2)
		s.Equal("diagonal", body)
		s.InDelta(1.0-1.0/math.Sqrt2, dist, 0.001)

		s.False(rows.Next())
		s.Require().NoError(rows.Err())
	})
}

func (s *TestSuite) TestVectorColumnNullable() {
	_, err := s.db.Exec(`create table "items" (
		id int8 primary key autoincrement,
		name varchar(100) not null,
		embedding vector(2)
	);`)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into items (name, embedding) values (?, ?)`, "no-vec", nil)
	s.Require().NoError(err)

	_, err = s.db.Exec(`insert into items (name, embedding) values (?, ?)`, "with-vec", "[1.0, 0.0]")
	s.Require().NoError(err)

	rows, err := s.db.QueryContext(context.Background(),
		`select name, embedding from items order by id;`)
	s.Require().NoError(err)
	defer rows.Close()

	s.Require().True(rows.Next())
	var name string
	var emb *string
	s.Require().NoError(rows.Scan(&name, &emb))
	s.Equal("no-vec", name)
	s.Nil(emb)

	s.Require().True(rows.Next())
	s.Require().NoError(rows.Scan(&name, &emb))
	s.Equal("with-vec", name)
	s.Require().NotNil(emb)
	s.Equal("[1, 0]", *emb)

	s.False(rows.Next())
	s.Require().NoError(rows.Err())
}

func (s *TestSuite) TestVectorDDLRoundTrip() {
	_, err := s.db.Exec(`create table "vecs" (
		id int8 primary key autoincrement,
		embedding vector(1536) not null
	);`)
	s.Require().NoError(err)

	// Build a 1536-dim vector literal.
	data := make([]float32, 1536)
	for i := range data {
		data[i] = float32(i) * 0.001
	}

	// Insert a 1536-dim vector.
	vec := make([]byte, 0, 1536*6)
	vec = append(vec, '[')
	for i, f := range data {
		if i > 0 {
			vec = append(vec, ',')
		}
		vec = append(vec, []byte(floatStr(float64(f)))...)
	}
	vec = append(vec, ']')

	_, err = s.db.Exec(`insert into vecs (embedding) values (?)`, string(vec))
	s.Require().NoError(err)

	rows, err := s.db.QueryContext(context.Background(),
		`select embedding from vecs where id = 1;`)
	s.Require().NoError(err)
	defer rows.Close()

	s.Require().True(rows.Next())
	var got string
	s.Require().NoError(rows.Scan(&got))
	s.NotEmpty(got)
	s.False(rows.Next())
	s.Require().NoError(rows.Err())
}

func (s *TestSuite) TestVectorUpdate() {
	_, err := s.db.Exec(createDocumentsVectorTableSQL)
	s.Require().NoError(err)

	_, err = s.db.Exec(
		`insert into documents (body, embedding) values (?, ?)`,
		"doc1", "[1.0, 0.0, 0.0]",
	)
	s.Require().NoError(err)

	// Update the vector.
	_, err = s.db.Exec(
		`update documents set embedding = ? where id = 1`,
		"[0.0, 1.0, 0.0]",
	)
	s.Require().NoError(err)

	rows, err := s.db.QueryContext(context.Background(),
		`select embedding from documents where id = 1;`)
	s.Require().NoError(err)
	defer rows.Close()

	s.Require().True(rows.Next())
	var emb string
	s.Require().NoError(rows.Scan(&emb))
	s.Equal("[0, 1, 0]", emb)
	s.False(rows.Next())
	s.Require().NoError(rows.Err())
}

func (s *TestSuite) TestVectorWithWhereFilter() {
	_, err := s.db.Exec(`create table "tagged_docs" (
		id int8 primary key autoincrement,
		category varchar(50) not null,
		embedding vector(2) not null
	);`)
	s.Require().NoError(err)

	inserts := []struct {
		cat string
		vec string
	}{
		{"tech", "[1.0, 0.0]"},
		{"food", "[0.0, 1.0]"},
		{"tech", "[0.9, 0.1]"},
		{"food", "[0.1, 0.9]"},
	}
	for _, ins := range inserts {
		_, err := s.db.Exec(
			`insert into tagged_docs (category, embedding) values (?, ?)`,
			ins.cat, ins.vec,
		)
		s.Require().NoError(err)
	}

	// Nearest tech docs to [1.0, 0.0].
	rows, err := s.db.QueryContext(context.Background(),
		`select id, VEC_L2(embedding, '[1.0, 0.0]') AS dist
		FROM tagged_docs WHERE category = ?
		ORDER BY dist LIMIT 1;`, "tech")
	s.Require().NoError(err)
	defer rows.Close()

	s.Require().True(rows.Next())
	var id int64
	var dist float64
	s.Require().NoError(rows.Scan(&id, &dist))
	s.Equal(int64(1), id)    // "[1.0, 0.0]" is exact match
	s.InDelta(0.0, dist, 0.001)
	s.False(rows.Next())
	s.Require().NoError(rows.Err())
}

// floatStr converts a float64 to a compact string for vector literal construction.
func floatStr(f float64) string {
	if f == 0 {
		return "0"
	}
	// Use strconv-compatible format
	s := ""
	if f < 0 {
		s = "-"
		f = -f
	}
	// Simple integer part + decimal
	intPart := int64(f)
	fracPart := f - float64(intPart)
	s += itoa(intPart)
	if fracPart > 0 {
		s += "." + fracStr(fracPart, 3)
	}
	return s
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	digits := ""
	for n > 0 {
		digits = string(rune('0'+n%10)) + digits
		n /= 10
	}
	return digits
}

func fracStr(f float64, places int) string {
	s := ""
	for range places {
		f *= 10
		d := int(f)
		s += string(rune('0' + d))
		f -= float64(d)
	}
	// Trim trailing zeros
	for len(s) > 1 && s[len(s)-1] == '0' {
		s = s[:len(s)-1]
	}
	return s
}
