package util

import (
	"fmt"
	"io"
	"strings"

	"github.com/RichardKnop/minisql/internal/core/minisql"
)

const (
	truncatedStringEnd = " ..."
	nonVarCharLength   = 20
	maxLength          = 40
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

	for i, aValue := range values {
		aStringValue := fmt.Sprint(aValue)
		r := []rune(aStringValue)
		if len(r) >= maxLength-len(truncatedStringEnd) {
			aStringValue = string(r[0:maxLength-len(truncatedStringEnd)]) + truncatedStringEnd
		}
		fmt.Fprintf(w, " %-*s ", columnSize[i], aStringValue)
		if i != len(columns)-1 {
			fmt.Fprint(w, "|")
		}
	}
	fmt.Fprintf(w, "\n")
}

func computeTableSize(columns []minisql.Column) []int {
	// find max width for each column
	columnSize := make([]int, len(columns))
	for i, aColumn := range columns {
		if aColumn.Kind == minisql.Varchar {
			columnSize[i] = maxLength
		} else {
			columnSize[i] = nonVarCharLength
		}
	}

	return columnSize
}
