package minisql

import (
	"context"
	"fmt"
	"unsafe"
)

var (
	errUnrecognizedStatementType = fmt.Errorf("Unrecognised statement type")
)

type Operator int

const (
	// Eq -> "="
	Eq Operator = iota + 1
	// Ne -> "!="
	Ne
	// Gt -> ">"
	Gt
	// Lt -> "<"
	Lt
	// Gte -> ">="
	Gte
	// Lte -> "<="
	Lte
)

type Condition struct {
	// Operand1 is the left hand side operand
	Operand1 string
	// Operand1IsField determines if Operand1 is a literal or a field name
	Operand1IsField bool
	// Operator is e.g. "=", ">"
	Operator Operator
	// Operand1 is the right hand side operand
	Operand2 string
	// Operand2IsField determines if Operand2 is a literal or a field name
	Operand2IsField bool
}

type StatementKind int

const (
	CreateTable StatementKind = iota + 1
	DropTable
	Insert
	Select
	Update
	Delete
)

type ColumnKind int

const (
	Int4 ColumnKind = iota + 1
	Int8
	Varchar
)

type Column struct {
	Kind   ColumnKind
	Size   int
	Offset int
	Name   string
}

type Statement struct {
	Kind       StatementKind
	TableName  string
	Conditions []Condition
	Updates    map[string]string
	Inserts    [][]string
	Fields     []string // Used for SELECT (i.e. SELECTed field names) and INSERT (INSERTEDed field names)
	Aliases    map[string]string
	Columns    []Column
}

// Execute will eventually become virtual machine
func (s *Statement) Execute(ctx context.Context) error {
	switch s.Kind {
	case Insert:
		return s.executeInsert(ctx)
	case Select:
		return s.executeSelect(ctx)
	}
	return errUnrecognizedStatementType
}

func (stmt *Statement) executeInsert(ctx context.Context) error {
	fmt.Println("This is where we would do insert")
	return nil
}

func (stmt *Statement) executeSelect(ctx context.Context) error {
	fmt.Println("This is where we would do select")
	return nil
}

type Table struct {
}

type Row struct {
	Columns []Column
	Values  []interface{}
}

func (r Row) Size() int {
	size := 0
	for _, aColumn := range r.Columns {
		size += aColumn.Size
	}
	return size
}

func (r Row) Marshal() ([]byte, error) {
	buf := make([]byte, r.Size())

	for i, aColumn := range r.Columns {
		switch aColumn.Kind {
		case Int4:
			value := r.Values[i].(int32)
			serializeInt4(value, buf, aColumn.Offset)
		case Int8:
			value := r.Values[i].(int64)
			serializeInt8(value, buf, aColumn.Offset)
		case Varchar:
			value := r.Values[i].(string)
			serializeString(value, buf, aColumn.Offset)
		}
	}

	return buf, nil
}

func UnmarshalRow(buf []byte, aRow *Row) error {
	aRow.Values = make([]interface{}, 0, len(aRow.Columns))
	for _, aColumn := range aRow.Columns {
		switch aColumn.Kind {
		case Int4:
			value := deserializeToInt4(buf, aColumn.Offset)
			aRow.Values = append(aRow.Values, value)
		case Int8:
			value := deserializeToInt8(buf, aColumn.Offset)
			aRow.Values = append(aRow.Values, value)
		case Varchar:
			value := deserializeToString(buf, aColumn.Offset, aColumn.Size)
			aRow.Values = append(aRow.Values, value)
		}
	}

	return nil
}

func serializeInt4(value int32, buf []byte, offset int) {
	src := unsafe.Pointer(&value)
	theSrc := *((*[4]byte)(src))
	copy(buf[offset:], theSrc[:])
}

func deserializeToInt4(buf []byte, offset int) int32 {
	destValue := int32(0)
	dest := unsafe.Pointer(&destValue)
	theDest := ((*[4]byte)(dest))

	copy(theDest[:], buf[offset:offset+4])

	return destValue
}

func serializeInt8(value int64, buf []byte, offset int) {
	src := unsafe.Pointer(&value)
	theSrc := *((*[8]byte)(src))
	copy(buf[offset:], theSrc[:])
}

func deserializeToInt8(buf []byte, offset int) int64 {
	destValue := int64(0)
	dest := unsafe.Pointer(&destValue)
	theDest := ((*[8]byte)(dest))

	copy(theDest[:], buf[offset:offset+8])

	return destValue
}

func serializeString(value string, buf []byte, offset int) {
	const size = unsafe.Sizeof(value)
	src := unsafe.Pointer(&value)
	theSrc := *((*[size]byte)(src))
	copy(buf[offset:], theSrc[:])
}

func deserializeToString(buf []byte, offset int, length int) string {
	destValue := ""
	const size = unsafe.Sizeof(destValue)
	dest := unsafe.Pointer(&destValue)
	theDest := ((*[size]byte)(dest))

	copy(theDest[:], buf[offset:offset+length])

	return string(destValue)
}
