package minisql

import (
	"bytes"
	"context"
	"fmt"

	"github.com/RichardKnop/minisql/pkg/bitwise"
)

// RowView is a lazy view over a leaf cell's encoded row payload.
//
// It is the migration target for read paths: filters, projections, joins, and
// database/sql delivery should pull typed values from RowView instead of first
// materialising a Row with a []OptionalValue backing array.
type RowView struct {
	columns     []Column
	value       []byte
	nullBitmask uint64
	key         RowID
}

// NewRowView returns a lazy row view over cell using columns as the schema.
func NewRowView(columns []Column, cell Cell) RowView {
	return RowView{
		columns:     columns,
		value:       cell.Value,
		nullBitmask: cell.NullBitmask,
		key:         cell.Key,
	}
}

// Key returns the internal row id for this view.
func (rv RowView) Key() RowID {
	return rv.key
}

// Columns returns the schema used by this view.
func (rv RowView) Columns() []Column {
	return rv.columns
}

// IsNull reports whether column idx is SQL NULL.
func (rv RowView) IsNull(idx int) (bool, error) {
	if idx < 0 || idx >= len(rv.columns) {
		return false, fmt.Errorf("column index %d out of bounds", idx)
	}
	return bitwise.IsSet(rv.nullBitmask, idx), nil
}

// ValueByName lazily decodes a value by column name.
func (rv RowView) ValueByName(name string) (OptionalValue, bool, error) {
	for i, col := range rv.columns {
		if col.Name != name {
			continue
		}
		value, err := rv.ValueAt(i)
		if err != nil {
			return OptionalValue{}, false, err
		}
		return value, true, nil
	}
	return OptionalValue{}, false, nil
}

// ValueAt lazily decodes a single column value.
//
// This is a compatibility bridge for code that still expects OptionalValue.
// New read paths should prefer the typed accessors below to avoid interface
// boxing on text/UUID values.
func (rv RowView) ValueAt(idx int) (OptionalValue, error) {
	if idx < 0 || idx >= len(rv.columns) {
		return OptionalValue{}, fmt.Errorf("column index %d out of bounds", idx)
	}
	isNull, err := rv.IsNull(idx)
	if err != nil {
		return OptionalValue{}, err
	}
	if isNull {
		return OptionalValue{}, nil
	}

	offset, err := rv.offsetOf(idx)
	if err != nil {
		return OptionalValue{}, err
	}

	col := rv.columns[idx]
	switch col.Kind {
	case Boolean:
		return OptionalValue{Value: unmarshalBool(rv.value, uint64(offset)), Valid: true}, nil
	case Int4:
		return OptionalValue{Value: unmarshalInt32(rv.value, uint64(offset)), Valid: true}, nil
	case Int8:
		return OptionalValue{Value: unmarshalInt64(rv.value, uint64(offset)), Valid: true}, nil
	case Real:
		return OptionalValue{Value: unmarshalFloat32(rv.value, uint64(offset)), Valid: true}, nil
	case Double:
		return OptionalValue{Value: unmarshalFloat64(rv.value, uint64(offset)), Valid: true}, nil
	case Varchar, Text, JSON:
		textPointer, err := rv.TextAt(idx)
		if err != nil {
			return OptionalValue{}, err
		}
		return OptionalValue{Value: textPointer, Valid: true}, nil
	case Timestamp:
		return OptionalValue{Value: TimestampMicros(unmarshalInt64(rv.value, uint64(offset))), Valid: true}, nil
	case UUID:
		var value UUIDValue
		copy(value[:], rv.value[offset:offset+16])
		return OptionalValue{Value: value, Valid: true}, nil
	default:
		return OptionalValue{}, fmt.Errorf("unsupported column kind %s", col.Kind)
	}
}

// BoolAt lazily decodes a BOOLEAN column.
func (rv RowView) BoolAt(idx int) (bool, bool, error) {
	if null, err := rv.IsNull(idx); err != nil || null {
		return false, false, err
	}
	if err := rv.requireKind(idx, Boolean); err != nil {
		return false, false, err
	}
	offset, err := rv.offsetOf(idx)
	if err != nil {
		return false, false, err
	}
	return unmarshalBool(rv.value, uint64(offset)), true, nil
}

// Int64At lazily decodes an INT4, INT8, or TIMESTAMP column as int64.
func (rv RowView) Int64At(idx int) (int64, bool, error) {
	if null, err := rv.IsNull(idx); err != nil || null {
		return 0, false, err
	}
	col := rv.columns[idx]
	if col.Kind != Int4 && col.Kind != Int8 && col.Kind != Timestamp {
		return 0, false, fmt.Errorf("column %s is %s, not int-like", col.Name, col.Kind)
	}
	offset, err := rv.offsetOf(idx)
	if err != nil {
		return 0, false, err
	}
	if col.Kind == Int4 {
		return int64(unmarshalInt32(rv.value, uint64(offset))), true, nil
	}
	return unmarshalInt64(rv.value, uint64(offset)), true, nil
}

// Float64At lazily decodes a REAL or DOUBLE column as float64.
func (rv RowView) Float64At(idx int) (float64, bool, error) {
	if null, err := rv.IsNull(idx); err != nil || null {
		return 0, false, err
	}
	col := rv.columns[idx]
	if col.Kind != Real && col.Kind != Double {
		return 0, false, fmt.Errorf("column %s is %s, not float-like", col.Name, col.Kind)
	}
	offset, err := rv.offsetOf(idx)
	if err != nil {
		return 0, false, err
	}
	if col.Kind == Real {
		return float64(unmarshalFloat32(rv.value, uint64(offset))), true, nil
	}
	return unmarshalFloat64(rv.value, uint64(offset)), true, nil
}

// TextAt lazily decodes a TEXT/VARCHAR/JSON column as a TextPointer.
func (rv RowView) TextAt(idx int) (TextPointer, error) {
	if null, err := rv.IsNull(idx); err != nil || null {
		return TextPointer{}, err
	}
	if idx < 0 || idx >= len(rv.columns) {
		return TextPointer{}, fmt.Errorf("column index %d out of bounds", idx)
	}
	col := rv.columns[idx]
	if !col.Kind.IsText() {
		return TextPointer{}, fmt.Errorf("column %s is %s, not text", col.Name, col.Kind)
	}
	offset, err := rv.offsetOf(idx)
	if err != nil {
		return TextPointer{}, err
	}
	textPointer := TextPointer{}
	if err := textPointer.Unmarshal(rv.value, uint64(offset)); err != nil {
		return TextPointer{}, err
	}
	if textPointer.IsInline() {
		textPointer.Data = bytes.Trim(textPointer.Data, "\x00")
	}
	return textPointer, nil
}

// TextAtWithOverflow lazily decodes a text column and reads overflow pages when needed.
func (rv RowView) TextAtWithOverflow(ctx context.Context, pager TxPager, idx int) (TextPointer, error) {
	textPointer, err := rv.TextAt(idx)
	if err != nil || textPointer.IsInline() {
		return textPointer, err
	}
	if pager == nil {
		return TextPointer{}, fmt.Errorf("overflow text column %d requires a pager", idx)
	}
	return textPointer.readOverflowText(ctx, pager)
}

// UUIDAt lazily decodes a UUID column.
func (rv RowView) UUIDAt(idx int) (UUIDValue, bool, error) {
	if null, err := rv.IsNull(idx); err != nil || null {
		return UUIDValue{}, false, err
	}
	if err := rv.requireKind(idx, UUID); err != nil {
		return UUIDValue{}, false, err
	}
	offset, err := rv.offsetOf(idx)
	if err != nil {
		return UUIDValue{}, false, err
	}
	var value UUIDValue
	copy(value[:], rv.value[offset:offset+16])
	return value, true, nil
}

// Materialize decodes selected columns into the legacy Row representation.
//
// selectedMask follows Row.UnmarshalWithMask semantics: a nil/empty mask means
// no values are selected, while a true entry decodes the corresponding column.
func (rv RowView) Materialize(selectedMask []bool) (Row, error) {
	row := Row{
		Columns: rv.columns,
		Values:  make([]OptionalValue, len(rv.columns)),
		Key:     rv.key,
	}
	if len(selectedMask) == 0 {
		return row, nil
	}
	if len(selectedMask) != len(rv.columns) {
		return Row{}, fmt.Errorf("selected mask length %d does not match columns length %d", len(selectedMask), len(rv.columns))
	}
	for i, selected := range selectedMask {
		if !selected {
			continue
		}
		value, err := rv.ValueAt(i)
		if err != nil {
			return Row{}, err
		}
		row.Values[i] = value
	}
	return row, nil
}

// MaterializeWithOverflow decodes selected columns and loads overflow text.
func (rv RowView) MaterializeWithOverflow(ctx context.Context, pager TxPager, selectedMask []bool) (Row, error) {
	row, err := rv.Materialize(selectedMask)
	if err != nil {
		return Row{}, err
	}
	return row.readOverflowTexts(ctx, pager)
}

func (rv RowView) requireKind(idx int, kind ColumnKind) error {
	if idx < 0 || idx >= len(rv.columns) {
		return fmt.Errorf("column index %d out of bounds", idx)
	}
	if rv.columns[idx].Kind != kind {
		return fmt.Errorf("column %s is %s, not %s", rv.columns[idx].Name, rv.columns[idx].Kind, kind)
	}
	return nil
}

func (rv RowView) offsetOf(targetIdx int) (int, error) {
	offset := 0
	for i := 0; i < targetIdx; i++ {
		if bitwise.IsSet(rv.nullBitmask, i) {
			continue
		}
		size, err := encodedColumnSize(rv.columns[i], rv.value, offset)
		if err != nil {
			return 0, err
		}
		offset += size
	}
	if offset > len(rv.value) {
		return 0, fmt.Errorf("column offset %d exceeds encoded row length %d", offset, len(rv.value))
	}
	return offset, nil
}

func encodedColumnSize(col Column, data []byte, offset int) (int, error) {
	switch col.Kind {
	case Boolean:
		return 1, nil
	case Int4, Real:
		return 4, nil
	case Int8, Double, Timestamp:
		return 8, nil
	case Varchar, Text, JSON:
		if offset+varcharLengthPrefixSize > len(data) {
			return 0, fmt.Errorf("text column %s offset %d exceeds encoded row length %d", col.Name, offset, len(data))
		}
		length := unmarshalUint32(data, uint64(offset))
		return int(TextPointer{Length: length}.Size()), nil
	case UUID:
		return 16, nil
	default:
		return 0, fmt.Errorf("unsupported column kind %s", col.Kind)
	}
}
