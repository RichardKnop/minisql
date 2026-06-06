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
//
// For combined join views, inner != nil and splitIdx separates outer columns
// (indexes 0..splitIdx-1) from inner columns (indexes splitIdx..N-1).
// All typed accessors transparently route inner-column reads through inner.
//
// typeCodes and columnCount come from the underlying Cell and drive byte-offset
// calculation independently of the current schema (self-describing format).
// When idx >= columnCount the column was added after this row was written;
// ValueAt returns the schema default for that position (lazy ADD COLUMN).
type RowView struct {
	columns     []Column
	value       []byte
	typeCodes   []byte // Cell.TypeCodes
	nullBitmask uint64
	key         RowID
	columnCount int     // Cell.ColumnCount; columns beyond this are lazily added
	inner       *RowView // non-nil for combined join views; routes inner-column reads
	innerPager  TxPager  // pager for inner table (overflow text in inner columns)
	splitIdx    int      // outer column count; columns[splitIdx:] belong to inner
	innerIsNull bool     // true → all inner columns are NULL (LEFT JOIN miss)
}

// NewRowView returns a lazy row view over cell using columns as the schema.
func NewRowView(columns []Column, cell Cell) RowView {
	return RowView{
		columns:     columns,
		value:       cell.Value,
		typeCodes:   cell.TypeCodes,
		columnCount: int(cell.ColumnCount),
		nullBitmask: cell.NullBitmask,
		key:         cell.Key,
	}
}

// NewCombinedRowView constructs a join RowView that routes column accesses to
// the outer or inner side based on splitIdx.  outerView supplies the outer cell
// bytes; innerView (a pre-allocated pointer reused per inner match) supplies the
// inner cell bytes.  innerPager is the inner table's pager, used when an inner
// text column requires overflow-page reads.  If innerIsNull is true all inner
// column accesses return NULL (LEFT JOIN miss path).
func NewCombinedRowView(combinedCols []Column, outerView RowView, innerView *RowView, innerPager TxPager, splitIdx int, innerIsNull bool) RowView {
	return RowView{
		columns:     combinedCols,
		value:       outerView.value,
		typeCodes:   outerView.typeCodes,
		columnCount: outerView.columnCount,
		nullBitmask: outerView.nullBitmask,
		key:         outerView.key,
		inner:       innerView,
		innerPager:  innerPager,
		splitIdx:    splitIdx,
		innerIsNull: innerIsNull,
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
// Returns true for lazily-added columns (idx >= columnCount) and for
// tombstoned columns (col.Deleted == true).
func (rv RowView) IsNull(idx int) (bool, error) {
	if rv.splitIdx > 0 && idx >= rv.splitIdx {
		if rv.innerIsNull {
			return true, nil
		}
		return rv.inner.IsNull(idx - rv.splitIdx)
	}
	if idx < 0 || idx >= len(rv.columns) {
		return false, fmt.Errorf("column index %d out of bounds", idx)
	}
	// Column added after this row was written → NULL unless a default is set.
	if idx >= rv.columnCount {
		return !rv.columns[idx].DefaultValue.Valid, nil
	}
	// Tombstoned column → treat as NULL.
	if rv.columns[idx].Deleted {
		return true, nil
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
	if rv.splitIdx > 0 && idx >= rv.splitIdx {
		if rv.innerIsNull {
			return OptionalValue{}, nil
		}
		return rv.inner.ValueAt(idx - rv.splitIdx)
	}
	if idx < 0 || idx >= len(rv.columns) {
		return OptionalValue{}, fmt.Errorf("column index %d out of bounds", idx)
	}
	// Lazy ADD COLUMN: column was added after this row was written.
	if idx >= rv.columnCount {
		col := rv.columns[idx]
		if col.DefaultValue.Valid {
			return coerceDefaultToColumnKind(col), nil
		}
		return OptionalValue{}, nil
	}
	// Tombstone DROP COLUMN: column is logically deleted.
	if rv.columns[idx].Deleted {
		return OptionalValue{}, nil
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

	// Guard against a truncated value buffer: verify enough bytes remain for
	// this column's fixed-width data before doing any direct index reads.
	if idx < len(rv.typeCodes) {
		sz := typeCodeFixedSize(TypeCode(rv.typeCodes[idx]))
		if sz > 0 && offset+sz > len(rv.value) {
			return OptionalValue{}, fmt.Errorf("column %d (%s): value data truncated (need %d bytes at offset %d, have %d)",
				idx, col.Name, sz, offset, len(rv.value))
		}
	}

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
		textPointer, err := rv.textAtOffset(idx, offset)
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
	case Vector:
		var vp VectorPointer
		vp.Unmarshal(rv.value, uint64(offset))
		return OptionalValue{Value: vp, Valid: true}, nil
	default:
		return OptionalValue{}, fmt.Errorf("unsupported column kind %s", col.Kind)
	}
}

// ValueAtWithOverflow lazily decodes a single column and reads overflow text when needed.
func (rv RowView) ValueAtWithOverflow(ctx context.Context, pager TxPager, idx int) (OptionalValue, error) {
	if rv.splitIdx > 0 && idx >= rv.splitIdx {
		if rv.innerIsNull {
			return OptionalValue{}, nil
		}
		innerPager := pager
		if rv.innerPager != nil {
			innerPager = rv.innerPager
		}
		return rv.inner.ValueAtWithOverflow(ctx, innerPager, idx-rv.splitIdx)
	}
	value, err := rv.ValueAt(idx)
	if err != nil || !value.Valid {
		return value, err
	}
	col := rv.columns[idx]
	if col.MayUseOverflowText() {
		textPointer, ok := value.Value.(TextPointer)
		if !ok {
			return OptionalValue{}, fmt.Errorf("expected TextPointer value for text column %s", col.Name)
		}
		if !textPointer.IsInline() && pager == nil {
			return OptionalValue{}, fmt.Errorf("overflow text column %d requires a pager", idx)
		}
		textPointer, err = textPointer.readOverflowText(ctx, pager)
		if err != nil {
			return OptionalValue{}, err
		}
		return OptionalValue{Valid: true, Value: textPointer}, nil
	}
	if col.MayUseOverflowVector() {
		vp, ok := value.Value.(VectorPointer)
		if !ok {
			return OptionalValue{}, fmt.Errorf("expected VectorPointer value for vector column %s", col.Name)
		}
		if pager == nil {
			return OptionalValue{}, fmt.Errorf("vector overflow column %d requires a pager", idx)
		}
		vp, err = vp.readOverflow(ctx, pager)
		if err != nil {
			return OptionalValue{}, err
		}
		return OptionalValue{Valid: true, Value: vp}, nil
	}
	return value, nil
}

// BoolAt lazily decodes a BOOLEAN column.
func (rv RowView) BoolAt(idx int) (bool, bool, error) {
	if rv.splitIdx > 0 && idx >= rv.splitIdx {
		if rv.innerIsNull {
			return false, false, nil
		}
		return rv.inner.BoolAt(idx - rv.splitIdx)
	}
	if null, err := rv.IsNull(idx); err != nil || null {
		return false, false, err
	}
	if idx >= rv.columnCount {
		dv := coerceDefaultToColumnKind(rv.columns[idx])
		if b, ok := dv.Value.(bool); ok {
			return b, dv.Valid, nil
		}
		return false, false, nil
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
	if rv.splitIdx > 0 && idx >= rv.splitIdx {
		if rv.innerIsNull {
			return 0, false, nil
		}
		return rv.inner.Int64At(idx - rv.splitIdx)
	}
	if null, err := rv.IsNull(idx); err != nil || null {
		return 0, false, err
	}
	// Lazy ADD COLUMN with default: no bytes in this row for this column.
	if idx >= rv.columnCount {
		dv := coerceDefaultToColumnKind(rv.columns[idx])
		switch n := dv.Value.(type) {
		case int32:
			return int64(n), dv.Valid, nil
		case int64:
			return n, dv.Valid, nil
		}
		return 0, false, nil
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
	if rv.splitIdx > 0 && idx >= rv.splitIdx {
		if rv.innerIsNull {
			return 0, false, nil
		}
		return rv.inner.Float64At(idx - rv.splitIdx)
	}
	if null, err := rv.IsNull(idx); err != nil || null {
		return 0, false, err
	}
	if idx >= rv.columnCount {
		dv := coerceDefaultToColumnKind(rv.columns[idx])
		switch n := dv.Value.(type) {
		case float32:
			return float64(n), dv.Valid, nil
		case float64:
			return n, dv.Valid, nil
		}
		return 0, false, nil
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
	if rv.splitIdx > 0 && idx >= rv.splitIdx {
		if rv.innerIsNull {
			return TextPointer{}, nil
		}
		return rv.inner.TextAt(idx - rv.splitIdx)
	}
	if null, err := rv.IsNull(idx); err != nil || null {
		return TextPointer{}, err
	}
	if idx < 0 || idx >= len(rv.columns) {
		return TextPointer{}, fmt.Errorf("column index %d out of bounds", idx)
	}
	if idx >= rv.columnCount {
		dv := coerceDefaultToColumnKind(rv.columns[idx])
		if tp, ok := dv.Value.(TextPointer); ok {
			return tp, nil
		}
		return TextPointer{}, nil
	}
	col := rv.columns[idx]
	if !col.Kind.IsText() {
		return TextPointer{}, fmt.Errorf("column %s is %s, not text", col.Name, col.Kind)
	}
	offset, err := rv.offsetOf(idx)
	if err != nil {
		return TextPointer{}, err
	}
	return rv.textAtOffset(idx, offset)
}

func (rv RowView) textAtOffset(idx, offset int) (TextPointer, error) {
	if idx < 0 || idx >= len(rv.columns) {
		return TextPointer{}, fmt.Errorf("column index %d out of bounds", idx)
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
	if rv.splitIdx > 0 && idx >= rv.splitIdx {
		if rv.innerIsNull {
			return TextPointer{}, nil
		}
		innerPager := pager
		if rv.innerPager != nil {
			innerPager = rv.innerPager
		}
		return rv.inner.TextAtWithOverflow(ctx, innerPager, idx-rv.splitIdx)
	}
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
	if rv.splitIdx > 0 && idx >= rv.splitIdx {
		if rv.innerIsNull {
			return UUIDValue{}, false, nil
		}
		return rv.inner.UUIDAt(idx - rv.splitIdx)
	}
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
	if offset+16 > len(rv.value) {
		return UUIDValue{}, false, fmt.Errorf("UUID column %d: value data truncated (need 16 bytes at offset %d, have %d)", idx, offset, len(rv.value))
	}
	var value UUIDValue
	copy(value[:], rv.value[offset:offset+16])
	return value, true, nil
}

// Materialize decodes selected columns into the legacy Row representation.
//
// selectedMask is column-aligned: a nil/empty mask means no values are selected,
// while a true entry decodes the corresponding column.
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

// MaterializeWithOverflow decodes selected columns and loads overflow text and vector data.
func (rv RowView) MaterializeWithOverflow(ctx context.Context, pager TxPager, selectedMask []bool) (Row, error) {
	row, err := rv.Materialize(selectedMask)
	if err != nil {
		return Row{}, err
	}
	row, err = row.readOverflowTexts(ctx, pager)
	if err != nil {
		return Row{}, err
	}
	return row.readOverflowVectors(ctx, pager)
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

// offsetOf computes the byte offset within rv.value where column targetIdx starts.
// It uses the cell's TypeCodes (self-describing format) rather than the schema,
// so it works correctly for rows written with an older or newer schema version.
// Columns beyond rv.columnCount contribute 0 bytes (lazy ADD COLUMN).
func (rv RowView) offsetOf(targetIdx int) (int, error) {
	offset := 0
	for i := 0; i < targetIdx; i++ {
		if i >= rv.columnCount {
			// Column was added after this row; no bytes present.
			break
		}
		if bitwise.IsSet(rv.nullBitmask, i) {
			continue // NULL: no bytes in value area
		}
		tc := TypeCode(rv.typeCodes[i])
		sz := typeCodeFixedSize(tc)
		if sz >= 0 {
			offset += sz
		} else {
			// TypeCodeText: 4-byte length prefix + data.
			if offset+varcharLengthPrefixSize > len(rv.value) {
				return 0, fmt.Errorf("text column %d offset %d exceeds encoded row length %d", i, offset, len(rv.value))
			}
			length := unmarshalUint32(rv.value, uint64(offset))
			offset += int(TextPointer{Length: length}.Size())
		}
	}
	if offset > len(rv.value) {
		return 0, fmt.Errorf("column offset %d exceeds encoded row length %d", offset, len(rv.value))
	}
	return offset, nil
}

// coerceDefaultToColumnKind converts a column's DefaultValue to the native Go
// type expected by the driver for that column kind.  The parser always stores
// integer defaults as int64; numeric columns narrower than int64 must be
// narrowed so that database/sql Scan calls work correctly.
func coerceDefaultToColumnKind(col Column) OptionalValue {
	v := col.DefaultValue
	switch col.Kind {
	case Int4:
		if i, ok := v.Value.(int64); ok {
			return OptionalValue{Value: int32(i), Valid: true}
		}
	case Boolean:
		if i, ok := v.Value.(int64); ok {
			return OptionalValue{Value: i != 0, Valid: true}
		}
	case Real:
		switch n := v.Value.(type) {
		case int64:
			return OptionalValue{Value: float32(n), Valid: true}
		case float64:
			return OptionalValue{Value: float32(n), Valid: true}
		}
	}
	return v
}
