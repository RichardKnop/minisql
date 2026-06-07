package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	t.Parallel()

	input := strings.NewReader(`
BenchmarkInsert_SingleRow/minisql-10          100  1000 ns/op  64 B/op  3 allocs/op
BenchmarkInsert_SingleRow/minisql-10          100  3000 ns/op  128 B/op  4 allocs/op
BenchmarkInsert_SingleRow/sqlite-10           100  4000 ns/op  32 B/op  1 allocs/op
BenchmarkSelect_FullScan/minisql-10           100  2000000 ns/op  1000 rows/op  2048 B/op  10 allocs/op
not a benchmark line
BenchmarkNoDriver-10                          100  1 ns/op
`)

	got := parse(input)

	require.Len(t, got, 2)
	require.Equal(t, "BenchmarkInsert_SingleRow", got[0].name)
	require.Equal(t, row{nsPerOp: 2000, bPerOp: 96}, got[0].drivers["minisql"])
	require.Equal(t, row{nsPerOp: 4000, bPerOp: 32}, got[0].drivers["sqlite"])
	require.Equal(t, "BenchmarkSelect_FullScan", got[1].name)
	require.Equal(t, row{nsPerOp: 2000000, bPerOp: 2048}, got[1].drivers["minisql"])
}

func TestSplitBenchmarkPath(t *testing.T) {
	t.Parallel()

	bench, driver, ok := splitBenchmarkPath("BenchmarkFoo/case/minisql")
	require.True(t, ok)
	require.Equal(t, "BenchmarkFoo/case", bench)
	require.Equal(t, "minisql", driver)

	_, _, ok = splitBenchmarkPath("BenchmarkFoo")
	require.False(t, ok)
	_, _, ok = splitBenchmarkPath("BenchmarkFoo/")
	require.False(t, ok)
}

func TestMeanRow(t *testing.T) {
	t.Parallel()

	require.Equal(t, row{}, meanRow(nil))
	require.Equal(t, row{nsPerOp: 15, bPerOp: 8}, meanRow([]sample{
		{nsPerOp: 10},
		{nsPerOp: 20, bPerOp: 8},
	}))
}

func TestRenderMarkdown(t *testing.T) {
	t.Parallel()

	md := renderMarkdown([]benchData{
		{
			name: "BenchmarkInsert_SingleRow",
			drivers: map[string]row{
				"sqlite":  {nsPerOp: 2000, bPerOp: 512},
				"minisql": {nsPerOp: 1000, bPerOp: 2048},
			},
		},
		{
			name: "BenchmarkSelect_PointScan",
			drivers: map[string]row{
				"minisql": {nsPerOp: 2_000_000},
			},
		},
	})

	require.Contains(t, md, "#### Timing")
	require.Contains(t, md, "| Benchmark | minisql | sqlite | ratio |")
	require.Contains(t, md, "| Insert_SingleRow | 1.00 µs/op | 2.00 µs/op | 0.5× |")
	require.Contains(t, md, "| Select_PointScan | 2.00 ms/op | — | — |")
	require.Contains(t, md, "#### Memory (B/op)")
	require.Contains(t, md, "| Insert_SingleRow | 2.0 KiB | 512 B |")
}

func TestRenderMarkdownWithoutMemory(t *testing.T) {
	t.Parallel()

	md := renderMarkdown([]benchData{
		{name: "BenchmarkTiny", drivers: map[string]row{"minisql": {nsPerOp: 12}}},
	})

	require.Contains(t, md, "| Tiny | 12 ns/op |")
	require.NotContains(t, md, "#### Memory")
}

func TestFormatHelpers(t *testing.T) {
	t.Parallel()

	require.Equal(t, "2.00 s/op", fmtDuration(2_000_000_000))
	require.Equal(t, "3.00 ms/op", fmtDuration(3_000_000))
	require.Equal(t, "4.00 µs/op", fmtDuration(4_000))
	require.Equal(t, "5 ns/op", fmtDuration(5))

	require.Equal(t, "2.0 MiB", fmtBytes(2<<20))
	require.Equal(t, "3.0 KiB", fmtBytes(3<<10))
	require.Equal(t, "6 B", fmtBytes(5.6))
}

func TestReorder(t *testing.T) {
	t.Parallel()

	drivers := []string{"sqlite", "minisql", "other"}
	reorder(drivers, "minisql")
	require.Equal(t, []string{"minisql", "sqlite", "other"}, drivers)

	reorder(drivers, "missing")
	require.Equal(t, []string{"minisql", "sqlite", "other"}, drivers)
}
