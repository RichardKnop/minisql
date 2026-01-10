package minisql

import (
	"testing"
)

func TestRowHeap_Basic(t *testing.T) {
	orderBy := []OrderBy{
		{Field: Field{Name: "score"}, Direction: Asc},
	}

	h := newRowHeap(orderBy, 3)

	// Add 5 rows, but heap should only keep top 3
	rows := []Row{
		NewRowWithValues([]Column{{Name: "score", Kind: Int4}}, []OptionalValue{{Value: int32(10), Valid: true}}),
		NewRowWithValues([]Column{{Name: "score", Kind: Int4}}, []OptionalValue{{Value: int32(50), Valid: true}}),
		NewRowWithValues([]Column{{Name: "score", Kind: Int4}}, []OptionalValue{{Value: int32(30), Valid: true}}),
		NewRowWithValues([]Column{{Name: "score", Kind: Int4}}, []OptionalValue{{Value: int32(20), Valid: true}}),
		NewRowWithValues([]Column{{Name: "score", Kind: Int4}}, []OptionalValue{{Value: int32(40), Valid: true}}),
	}

	for _, row := range rows {
		h.PushRow(row)
	}

	result := h.ExtractSorted()

	if len(result) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(result))
	}

	// Should have the 3 smallest (10, 20, 30) in ascending order
	expectedScores := []int32{10, 20, 30}
	for i, row := range result {
		val, _ := row.GetValue("score")
		score := val.Value.(int32)
		if score != expectedScores[i] {
			t.Errorf("Row %d: expected score %d, got %d", i, expectedScores[i], score)
		}
	}
}

func TestRowHeap_Descending(t *testing.T) {
	orderBy := []OrderBy{
		{Field: Field{Name: "score"}, Direction: Desc},
	}

	h := newRowHeap(orderBy, 3)

	rows := []Row{
		NewRowWithValues([]Column{{Name: "score", Kind: Int4}}, []OptionalValue{{Value: int32(10), Valid: true}}),
		NewRowWithValues([]Column{{Name: "score", Kind: Int4}}, []OptionalValue{{Value: int32(50), Valid: true}}),
		NewRowWithValues([]Column{{Name: "score", Kind: Int4}}, []OptionalValue{{Value: int32(30), Valid: true}}),
		NewRowWithValues([]Column{{Name: "score", Kind: Int4}}, []OptionalValue{{Value: int32(20), Valid: true}}),
		NewRowWithValues([]Column{{Name: "score", Kind: Int4}}, []OptionalValue{{Value: int32(40), Valid: true}}),
	}

	for _, row := range rows {
		h.PushRow(row)
	}

	result := h.ExtractSorted()

	if len(result) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(result))
	}

	// Should have the 3 largest (50, 40, 30) in descending order
	expectedScores := []int32{50, 40, 30}
	for i, row := range result {
		val, _ := row.GetValue("score")
		score := val.Value.(int32)
		if score != expectedScores[i] {
			t.Errorf("Row %d: expected score %d, got %d", i, expectedScores[i], score)
		}
	}
}

func TestRowHeap_LessThanMaxSize(t *testing.T) {
	orderBy := []OrderBy{
		{Field: Field{Name: "score"}, Direction: Asc},
	}

	h := newRowHeap(orderBy, 10) // Max size larger than input

	rows := []Row{
		NewRowWithValues([]Column{{Name: "score", Kind: Int4}}, []OptionalValue{{Value: int32(30), Valid: true}}),
		NewRowWithValues([]Column{{Name: "score", Kind: Int4}}, []OptionalValue{{Value: int32(10), Valid: true}}),
		NewRowWithValues([]Column{{Name: "score", Kind: Int4}}, []OptionalValue{{Value: int32(20), Valid: true}}),
	}

	for _, row := range rows {
		h.PushRow(row)
	}

	result := h.ExtractSorted()

	if len(result) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(result))
	}

	// Should have all 3 rows in ascending order
	expectedScores := []int32{10, 20, 30}
	for i, row := range result {
		val, _ := row.GetValue("score")
		score := val.Value.(int32)
		if score != expectedScores[i] {
			t.Errorf("Row %d: expected score %d, got %d", i, expectedScores[i], score)
		}
	}
}

func BenchmarkRowHeap_TopN(b *testing.B) {
	orderBy := []OrderBy{
		{Field: Field{Name: "score"}, Direction: Asc},
	}

	// Simulate keeping top 10 from 10000 rows
	const totalRows = 10000
	const topN = 10

	rows := make([]Row, totalRows)
	for i := range totalRows {
		rows[i] = NewRowWithValues(
			[]Column{{Name: "score", Kind: Int4}},
			[]OptionalValue{{Value: int32(i % 1000), Valid: true}},
		)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		h := newRowHeap(orderBy, topN)
		for _, row := range rows {
			h.PushRow(row)
		}
		_ = h.ExtractSorted()
	}
}

func BenchmarkRowHeap_FullSort(b *testing.B) {
	orderBy := []OrderBy{
		{Field: Field{Name: "score"}, Direction: Asc},
	}

	// Simulate collecting and sorting all 10000 rows
	const totalRows = 10000

	rows := make([]Row, totalRows)
	for i := 0; i < totalRows; i++ {
		rows[i] = NewRowWithValues(
			[]Column{{Name: "score", Kind: Int4}},
			[]OptionalValue{{Value: int32(i % 1000), Valid: true}},
		)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		allRows := make([]Row, len(rows))
		copy(allRows, rows)

		// This simulates the old approach
		table := &Table{}
		_ = table.sortRows(allRows, orderBy)
		_ = allRows[:10] // Take top 10
	}
}
