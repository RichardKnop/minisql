// report reads Go benchmark output from stdin (or a file) and appends a
// formatted Markdown table to benchmarks/RESULTS.md (or the file given by
// the -out flag).
//
// Usage:
//
//	go run ./benchmarks/cmd/report/ [flags] [input-file]
//
// Flags:
//
//	-out <file>   Markdown file to append results to (default: benchmarks/RESULTS.md)
//
// If no input file is given, stdin is read.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// row holds all parsed metrics for one benchmark/driver pair.
type row struct {
	nsPerOp float64
	bPerOp  float64 // 0 if not reported
}

// benchData holds all driver rows for a single benchmark.
type benchData struct {
	name    string
	drivers map[string]row // driver name → metrics
}

// benchLine matches the benchmark name, driver, and ns/op value.
// B/op is extracted separately because a custom metric (e.g. "rows/op") may
// appear between ns/op and B/op in the output line.
var (
	benchLine   = regexp.MustCompile(`^(Benchmark\w+)/(\w+)-\d+\s+\d+\s+([\d.]+)\s+ns/op`)
	bPerOpField = regexp.MustCompile(`([\d.]+)\s+B/op`)
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	outFile := flag.String("out", "benchmarks/RESULTS.md", "Markdown file to append results to")
	flag.Parse()

	var r io.Reader
	if flag.NArg() > 0 {
		f, err := os.Open(flag.Arg(0))
		if err != nil {
			return fmt.Errorf("open %s: %w", flag.Arg(0), err)
		}
		defer f.Close()
		r = f
	} else {
		r = os.Stdin
	}

	benches := parse(r)
	if len(benches) == 0 {
		return fmt.Errorf("no benchmark results found in input")
	}

	md := renderMarkdown(benches)

	f, err := os.OpenFile(*outFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open %s: %w", *outFile, err)
	}
	defer f.Close()

	if _, err := fmt.Fprintln(f, md); err != nil {
		return fmt.Errorf("write %s: %w", *outFile, err)
	}

	fmt.Printf("Results appended to %s\n", *outFile)
	return nil
}

func parse(r io.Reader) []benchData {
	// Preserve insertion order so benchmarks appear in the order they ran.
	var order []string
	byName := map[string]*benchData{}

	sc := bufio.NewScanner(r)
	for sc.Scan() {
		m := benchLine.FindStringSubmatch(sc.Text())
		if m == nil {
			continue
		}
		benchName := m[1]
		driver := m[2]
		ns, err := strconv.ParseFloat(m[3], 64)
		if err != nil {
			continue
		}
		// Extract B/op from anywhere in the line — a custom metric such as
		// "rows/op" may appear between ns/op and B/op.
		var bop float64
		if mb := bPerOpField.FindStringSubmatch(sc.Text()); mb != nil {
			bop, _ = strconv.ParseFloat(mb[1], 64)
		}

		if _, ok := byName[benchName]; !ok {
			byName[benchName] = &benchData{name: benchName, drivers: map[string]row{}}
			order = append(order, benchName)
		}
		byName[benchName].drivers[driver] = row{nsPerOp: ns, bPerOp: bop}
	}

	result := make([]benchData, 0, len(order))
	for _, name := range order {
		result = append(result, *byName[name])
	}
	return result
}

func renderMarkdown(benches []benchData) string {
	// Collect driver names present across all benchmarks (stable order).
	driverSet := map[string]struct{}{}
	for _, b := range benches {
		for d := range b.drivers {
			driverSet[d] = struct{}{}
		}
	}
	drivers := make([]string, 0, len(driverSet))
	for d := range driverSet {
		drivers = append(drivers, d)
	}
	sort.Strings(drivers)

	// Reorder so minisql comes first.
	reorder(drivers, "minisql")

	var sb strings.Builder

	// Heading with timestamp.
	fmt.Fprintf(&sb, "### %s\n\n", time.Now().UTC().Format("2006-01-02 15:04 UTC"))

	// ── Timing table ──────────────────────────────────────────────────────────
	sb.WriteString("#### Timing\n\n")

	// Header row.
	sb.WriteString("| Benchmark |")
	for _, d := range drivers {
		fmt.Fprintf(&sb, " %s |", d)
	}
	// Ratio column only makes sense when we have exactly minisql vs sqlite.
	hasRatio := slices.Contains(drivers, "minisql") && slices.Contains(drivers, "sqlite")
	if hasRatio {
		sb.WriteString(" ratio |")
	}
	sb.WriteByte('\n')

	// Separator row.
	sb.WriteString("|---|")
	for range drivers {
		sb.WriteString("---|")
	}
	if hasRatio {
		sb.WriteString("---|")
	}
	sb.WriteByte('\n')

	// Data rows.
	for _, b := range benches {
		title := strings.TrimPrefix(b.name, "Benchmark")
		fmt.Fprintf(&sb, "| %s |", title)
		for _, d := range drivers {
			r, ok := b.drivers[d]
			if !ok {
				sb.WriteString(" — |")
				continue
			}
			fmt.Fprintf(&sb, " %s |", fmtDuration(r.nsPerOp))
		}
		if hasRatio {
			ms, mok := b.drivers["minisql"]
			sq, sok := b.drivers["sqlite"]
			if mok && sok && sq.nsPerOp > 0 {
				ratio := ms.nsPerOp / sq.nsPerOp
				fmt.Fprintf(&sb, " %.1f× |", ratio)
			} else {
				sb.WriteString(" — |")
			}
		}
		sb.WriteByte('\n')
	}

	sb.WriteByte('\n')

	// ── Memory table ──────────────────────────────────────────────────────────
	// Only emit if at least one benchmark has B/op data.
	hasMem := false
	for _, b := range benches {
		for _, r := range b.drivers {
			if r.bPerOp > 0 {
				hasMem = true
				break
			}
		}
		if hasMem {
			break
		}
	}

	if hasMem {
		sb.WriteString("#### Memory (B/op)\n\n")

		sb.WriteString("| Benchmark |")
		for _, d := range drivers {
			fmt.Fprintf(&sb, " %s |", d)
		}
		sb.WriteByte('\n')

		sb.WriteString("|---|")
		for range drivers {
			sb.WriteString("---|")
		}
		sb.WriteByte('\n')

		for _, b := range benches {
			title := strings.TrimPrefix(b.name, "Benchmark")
			fmt.Fprintf(&sb, "| %s |", title)
			for _, d := range drivers {
				r, ok := b.drivers[d]
				if !ok {
					sb.WriteString(" — |")
					continue
				}
				fmt.Fprintf(&sb, " %s |", fmtBytes(r.bPerOp))
			}
			sb.WriteByte('\n')
		}
		sb.WriteByte('\n')
	}

	return sb.String()
}

// fmtDuration formats nanoseconds as a human-readable duration string.
func fmtDuration(ns float64) string {
	switch {
	case ns >= 1e9:
		return fmt.Sprintf("%.2f s/op", ns/1e9)
	case ns >= 1e6:
		return fmt.Sprintf("%.2f ms/op", ns/1e6)
	case ns >= 1e3:
		return fmt.Sprintf("%.2f µs/op", ns/1e3)
	default:
		return fmt.Sprintf("%.0f ns/op", ns)
	}
}

// fmtBytes formats a byte count with a sensible unit.
func fmtBytes(b float64) string {
	switch {
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MiB", b/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KiB", b/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", int(math.Round(b)))
	}
}

// reorder moves the named element to the front of the slice if present.
func reorder(s []string, first string) {
	for i, v := range s {
		if v == first {
			copy(s[1:i+1], s[:i])
			s[0] = first
			return
		}
	}
}
