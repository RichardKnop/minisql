// chart reads Go benchmark output from stdin (or a file) and produces PNG bar
// charts comparing minisql vs sqlite for each benchmark group.
//
// Usage:
//
//	go run ./benchmarks/cmd/chart/ [flags] [input-file]
//
// Flags:
//
//	-out <dir>   directory to write PNG files into (default: benchmarks/charts)
//
// If no input file is given, stdin is read.
// Output files are written to benchmarks/charts/ by default.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"image/color"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

// result holds the parsed ns/op value for one benchmark/driver combination.
type result struct {
	benchmark string // e.g. "BenchmarkInsert_SingleRow"
	driver    string // "minisql" or "sqlite"
	nsPerOp   float64
}

// benchLine matches lines like:
//
//	BenchmarkInsert_SingleRow/minisql-10    1234    56789 ns/op
//
// Nested sub-benchmark paths are supported; the final path segment is treated
// as the driver, e.g. BenchmarkFullText_Search/rare/minisql-10.
var benchLine = regexp.MustCompile(`^(Benchmark\S+)-\d+\s+\d+\s+([\d.]+)\s+ns/op`)

// driver display order and colours.
var (
	driverOrder  = []string{"minisql", "sqlite"}
	driverColors = map[string]color.RGBA{
		"minisql": {R: 66, G: 133, B: 244, A: 255}, // blue
		"sqlite":  {R: 219, G: 68, B: 55, A: 255},  // red
	}
)

func main() {
	outDir := flag.String("out", filepath.Join("benchmarks", "charts"), "directory to write PNG files into")
	flag.Parse()

	var (
		r         io.Reader
		closeFunc = func() {}
	)
	if flag.NArg() > 0 {
		inputFile := flag.Arg(0)
		f, err := os.Open(inputFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "open %s: %v\n", inputFile, err)
			os.Exit(1)
		}
		closeFunc = func() {
			if err := f.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "close %s: %v\n", inputFile, err)
			}
		}
		r = f
	} else {
		r = os.Stdin
	}

	results := parse(r)
	if len(results) == 0 {
		closeFunc()
		fmt.Fprintln(os.Stderr, "no benchmark results found in input")
		os.Exit(1)
	}

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		closeFunc()
		fmt.Fprintf(os.Stderr, "mkdir %s: %v\n", *outDir, err)
		os.Exit(1)
	}

	defer closeFunc()

	// Group by benchmark name.
	byBench := map[string][]result{}
	for _, res := range results {
		byBench[res.benchmark] = append(byBench[res.benchmark], res)
	}

	var benchNames []string
	for name := range byBench {
		benchNames = append(benchNames, name)
	}
	sort.Strings(benchNames)

	for _, name := range benchNames {
		if err := renderChart(name, byBench[name], *outDir); err != nil {
			fmt.Fprintf(os.Stderr, "render %s: %v\n", name, err)
		}
	}

	fmt.Printf("Charts written to %s/\n", *outDir)
}

func parse(r io.Reader) []result {
	var results []result
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		m := benchLine.FindStringSubmatch(sc.Text())
		if m == nil {
			continue
		}
		benchName, driver, ok := splitBenchmarkPath(m[1])
		if !ok {
			continue
		}
		ns, err := strconv.ParseFloat(m[2], 64)
		if err != nil {
			continue
		}
		results = append(results, result{benchmark: benchName, driver: driver, nsPerOp: ns})
	}
	return results
}

func splitBenchmarkPath(path string) (benchName, driver string, ok bool) {
	idx := strings.LastIndex(path, "/")
	if idx < 0 || idx == len(path)-1 {
		return "", "", false
	}
	return path[:idx], path[idx+1:], true
}

// renderChart writes a grouped bar chart PNG for one benchmark to outDir.
//
// gonum/plot grouped bar chart model:
//   - Each BarChart adds one "series" of bars.  Bar i in a series is placed at
//     DATA x = i.  NominalX labels each integer x position.
//   - BarChart.Offset shifts every bar in that series left/right in SCREEN space
//     (vg.Length = points).  This is how bars within the same data group are
//     spread side-by-side without overlapping.
//
// For an N-driver, 1-category-per-chart layout:
//   - Use a single NominalX label (the benchmark name).
//   - Give each driver's BarChart Offset = -(N-1)*half + i*barWidth so that the
//     bars fan out symmetrically around the single x tick at data position 0.
func renderChart(benchName string, results []result, outDir string) error {
	nsTotals := map[string]float64{}
	nsCounts := map[string]int{}
	for _, res := range results {
		nsTotals[res.driver] += res.nsPerOp
		nsCounts[res.driver]++
	}
	nsMap := map[string]float64{}
	for driver, total := range nsTotals {
		nsMap[driver] = total / float64(nsCounts[driver])
	}

	// Collect present drivers in stable order.
	var present []string
	for _, d := range driverOrder {
		if _, ok := nsMap[d]; ok {
			present = append(present, d)
		}
	}
	if len(present) == 0 {
		return nil
	}

	// Choose unit based on the largest value.
	maxNS := 0.0
	for _, d := range present {
		if v := nsMap[d]; v > maxNS {
			maxNS = v
		}
	}
	unit, divisor := unitAndDivisor(maxNS)

	title := strings.TrimPrefix(benchName, "Benchmark")

	p := plot.New()
	p.Title.Text = title
	p.Title.TextStyle.Font.Size = 14
	p.Y.Label.Text = unit
	p.Y.Label.TextStyle.Font.Size = 11
	p.X.Tick.Label.Font.Size = 11

	// Bar width in screen space and total group width.
	const barWidth = 30 * vg.Millimeter
	n := vg.Length(len(present))
	// Centre the group: first bar starts at -groupWidth/2 + barWidth/2.
	groupStart := -(n * barWidth / 2) + barWidth/2

	for i, d := range present {
		val := roundTo(nsMap[d]/divisor, 4)

		bars, err := plotter.NewBarChart(plotter.Values{val}, barWidth)
		if err != nil {
			return fmt.Errorf("new bar chart for %s: %w", d, err)
		}
		bars.Color = driverColors[d]
		bars.LineStyle.Width = 0
		// Offset fans bars left/right of the single data tick at x=0.
		bars.Offset = groupStart + vg.Length(i)*barWidth

		p.Add(bars)
		p.Legend.Add(d, bars)
	}

	// Single nominal label for the x axis (benchmark name in the title is enough).
	p.NominalX("")
	p.Legend.Top = true
	p.Legend.Left = true

	outPath := filepath.Join(outDir, title+".png")
	if err := p.Save(16*vg.Centimeter, 10*vg.Centimeter, outPath); err != nil {
		return fmt.Errorf("save PNG: %w", err)
	}
	fmt.Printf("  wrote %s\n", outPath)
	return nil
}

// unitAndDivisor picks a human-readable time unit for the y-axis.
func unitAndDivisor(maxNS float64) (unit string, divisor float64) {
	switch {
	case maxNS >= 1e9:
		return "s/op", 1e9
	case maxNS >= 1e6:
		return "ms/op", 1e6
	default:
		return "µs/op", 1e3
	}
}

func roundTo(v float64, decimals int) float64 {
	factor := math.Pow(10, float64(decimals))
	return math.Round(v*factor) / factor
}
