package util

import (
	"fmt"
	"io"
	"strings"

	"github.com/RichardKnop/minisql/internal/pkg/minisql"
)

const (
	truncatedStringEnd = " ..."
	nonVarCharLength   = 20
	maxLength          = 40
)

func PrintTableHeader(w io.Writer, columns []minisql.Column) {
	columnSize, tableWidth := computeTableSize(columns)

	// add top horizontal header
	fmt.Fprintf(w, "+%s+\n", strings.Repeat("-", tableWidth-2))

	for i, aColumn := range columns {
		// pad with columnSize[j] spaces on the right rather than the left (left-justify the field)
		// an asterisk * in the format specifies that the padding size should be given as an argument
		fmt.Fprintf(w, "| %-*s ", columnSize[i], any(aColumn.Name))
		// new line after last cell in a row
		if i == len(columns)-1 {
			fmt.Fprintf(w, "|\n")
		}
	}

	// add horizontal border bellow the header row
	fmt.Fprintf(w, "+%s+\n", strings.Repeat("-", tableWidth-2))
}

func PrintTableRow(w io.Writer, columns []minisql.Column, values []any) {
	columnSize, _ := computeTableSize(columns)

	for i, aValue := range values {
		aStringValue := fmt.Sprint(aValue)
		r := []rune(aStringValue)
		if len(r) >= maxLength-len(truncatedStringEnd) {
			aStringValue = string(r[0:maxLength-len(truncatedStringEnd)]) + truncatedStringEnd
		}
		fmt.Fprintf(w, "| %-*s ", columnSize[i], aStringValue)
	}
	fmt.Fprintf(w, "|\n")
}

func PrintTableEnd(w io.Writer, columns []minisql.Column) {
	_, tableWidth := computeTableSize(columns)

	fmt.Fprintf(w, "+%s+\n", strings.Repeat("-", tableWidth-2))
}

func computeTableSize(columns []minisql.Column) ([]int, int) {
	// find max width for each column
	columnSize := make([]int, len(columns))
	for i, aColumn := range columns {
		if aColumn.Kind == minisql.Varchar {
			columnSize[i] = maxLength
		} else {
			columnSize[i] = nonVarCharLength
		}
	}

	// left border is | followed by a space, right border is space followed by | (2+2=4)
	// then between each column we have space, |, space (3)
	tableWidth := 4 + (len(columnSize)-1)*3
	for _, columnWidth := range columnSize {
		tableWidth += columnWidth
	}

	return columnSize, tableWidth
}
