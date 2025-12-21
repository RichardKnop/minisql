package protocol

import (
	"fmt"
	"io"
	"strings"

	"github.com/RichardKnop/minisql/internal/minisql"
)

const (
	truncatedStringEnd = " ..."
	nonVarCharLength   = 20
	timestampLength    = 29
	maxLength          = 40
	maxMultiLineLength = 500
)

func PrintTableHeader(w io.Writer, columns []minisql.Column) {
	columnSize := computeTableSize(columns)

	for i, aColumn := range columns {
		// pad with columnSize[j] spaces on the right rather than the left (left-justify the field)
		// an asterisk * in the format specifies that the padding size should be given as an argument
		fmt.Fprintf(w, " %-*s ", columnSize[i], any(aColumn.Name))
		if i != len(columns)-1 {
			fmt.Fprint(w, "|")
		}
	}
	fmt.Fprintf(w, "\n")

	// add horizontal border bellow the header row
	for i, size := range columnSize {
		fmt.Fprintf(w, "%s", strings.Repeat("-", size+2))
		if i != len(columnSize)-1 {
			fmt.Fprint(w, "+")
		}
	}
	fmt.Fprint(w, "\n")
}

func PrintTableRow(w io.Writer, columns []minisql.Column, values []minisql.OptionalValue) {
	columnSize := computeTableSize(columns)

	rows := make([][]string, 0, 1)
	rows = append(rows, make([]string, len(values)))
	for i, aValue := range values {
		aStringValue := "NULL"
		if aValue.Valid {
			aStringValue = fmt.Sprint(aValue.Value)
		}

		r := []rune(aStringValue)
		if len(r) >= maxLength {
			if len(r) >= maxMultiLineLength {
				aStringValue = string(r[0:maxMultiLineLength-len(truncatedStringEnd)]) + truncatedStringEnd
			}
			lines := splitStringIntoLines(aStringValue, maxLength)
			for _, line := range lines {
				added := false
				for _, aRow := range rows {
					if aRow[i] == "" {
						aRow[i] = line
						added = true
						break
					}
				}
				if !added {
					rows = append(rows, make([]string, len(values)))
					rows[len(rows)-1][i] = line
				}
			}
		} else {
			rows[0][i] = aStringValue
		}
	}

	for _, aRow := range rows {
		for j, aCell := range aRow {
			fmt.Fprintf(w, " %-*s ", columnSize[j], aCell)
			if j != len(columns)-1 {
				fmt.Fprint(w, "|")
			}
		}
		fmt.Fprintf(w, "\n")
	}
}

func splitStringIntoLines(text string, maxWidth int) []string {
	if len(text) == 0 {
		return []string{""}
	}

	lines := strings.Split(text, "\n")
	finalLines := make([]string, 0, len(lines))

	for _, line := range lines {
		runes := []rune(line)
		if len(runes) <= maxWidth {
			finalLines = append(finalLines, line)
			continue
		}
		for i := 0; i < len(runes); i += maxWidth {
			end := i + maxWidth
			if end > len(runes) {
				end = len(runes)
			}
			finalLines = append(finalLines, string(runes[i:end]))
		}
	}

	return finalLines
}

func computeTableSize(columns []minisql.Column) []int {
	// find max width for each column
	columnSize := make([]int, len(columns))
	for i, aColumn := range columns {
		if aColumn.Kind.IsText() {
			columnSize[i] = maxLength
		} else if aColumn.Kind == minisql.Timestamp {
			columnSize[i] = timestampLength
		} else {
			columnSize[i] = nonVarCharLength
		}
	}

	return columnSize
}
