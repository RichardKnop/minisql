package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

type outputMode int

const (
	modeTable outputMode = iota
	modeCSV
)

// printResult writes query results to w in the configured mode.
func printResult(w io.Writer, cols []string, rows [][]string, mode outputMode) {
	if len(rows) == 0 && len(cols) == 0 {
		return
	}
	switch mode {
	case modeCSV:
		printCSV(w, cols, rows)
	default:
		printTable(w, cols, rows)
	}
}

func printTable(w io.Writer, cols []string, rows [][]string) {
	if len(cols) == 0 {
		return
	}

	// Compute column widths: max of header and all cell values.
	widths := make([]int, len(cols))
	for i, c := range cols {
		widths[i] = utf8.RuneCountInString(c)
	}
	for _, row := range rows {
		for i, cell := range row {
			if i < len(widths) {
				widths[i] = max(widths[i], utf8.RuneCountInString(cell))
			}
		}
	}

	// Header.
	printTableRow(w, cols, widths)

	// Separator.
	parts := make([]string, len(widths))
	for i, w := range widths {
		parts[i] = strings.Repeat("-", w)
	}
	fmt.Fprintln(w, strings.Join(parts, "  "))

	// Data rows.
	for _, row := range rows {
		printTableRow(w, row, widths)
	}
}

func printTableRow(w io.Writer, cells []string, widths []int) {
	parts := make([]string, len(widths))
	for i, width := range widths {
		var cell string
		if i < len(cells) {
			cell = cells[i]
		}
		pad := max(0, width-utf8.RuneCountInString(cell))
		parts[i] = cell + strings.Repeat(" ", pad)
	}
	fmt.Fprintln(w, strings.Join(parts, "  "))
}

func printCSV(w io.Writer, cols []string, rows [][]string) {
	cw := csv.NewWriter(w)
	_ = cw.Write(cols)
	for _, row := range rows {
		_ = cw.Write(row)
	}
	cw.Flush()
}
