package minisql

import (
	"math"
	"unsafe"
)

// OptionalValue stores a nullable column value with zero heap allocations for
// the hot-path types (scalars, text, UUID).
//
// Memory layout (64 bytes on 64-bit):
//
//	kind  uint8          — discriminant: 0=null, 1-10=ColumnKind value, 251-255=special
//	[7]byte              — alignment padding
//	num   uint64         — scalar bits: bool(0/1), int32, int64, float32bits, float64bits,
//	                        timestamp; UUID first 8 bytes (little-endian); text: Length
//	hi    uint64         — UUID last 8 bytes; text: FirstPage
//	data  []byte         — text/varchar/json data; ExcludedRef column name; Function name
//	extra any            — *Expr (kind=251) or *Statement (kind=252) only
type OptionalValue struct {
	kind  uint8
	_     [7]byte
	num   uint64
	hi    uint64
	data  []byte
	extra any
}

// oval* constants are the discriminant values for OptionalValue.kind.
// Values 1-10 intentionally match the ColumnKind iota values.
const (
	ovalNull      uint8 = 0
	ovalBoolean   uint8 = 1
	ovalInt4      uint8 = 2
	ovalInt8      uint8 = 3
	ovalReal      uint8 = 4
	ovalDouble    uint8 = 5
	ovalVarchar   uint8 = 6
	ovalText      uint8 = 7
	ovalTimestamp uint8 = 8
	ovalJSON      uint8 = 9
	ovalUUID      uint8 = 10
	ovalExpr        uint8 = 251
	ovalStatement   uint8 = 252
	ovalFunction    uint8 = 253
	ovalExcludedRef uint8 = 254
	ovalPlaceholder uint8 = 255
)

// MakeNull returns a null OptionalValue.
func MakeNull() OptionalValue { return OptionalValue{} }

// OptionalValueFromParserAny converts a parser-produced any value (int64, float64,
// bool, string, TextPointer, TimestampMicros, UUIDValue, Placeholder, etc.)
// to an OptionalValue. Used by the parser when the column kind is not yet known.
func OptionalValueFromParserAny(v any) OptionalValue {
	if v == nil {
		return MakeNull()
	}
	switch n := v.(type) {
	case bool:
		return MakeBool(n)
	case int32:
		return MakeInt4(n)
	case int64:
		return MakeInt8(n)
	case float32:
		return MakeReal(n)
	case float64:
		return MakeDouble(n)
	case TextPointer:
		return MakeVarchar(n)
	case TimestampMicros:
		return MakeTimestamp(n)
	case UUIDValue:
		return MakeUUID(n)
	case Placeholder:
		return MakePlaceholder()
	case ExcludedRef:
		return MakeExcludedRef(n)
	case Function:
		return MakeFunction(n)
	case *Expr:
		return MakeExpr(n)
	case *Statement:
		return MakeStatement(n)
	}
	return MakeNull()
}

// MakeBool constructs a non-null boolean OptionalValue.
func MakeBool(v bool) OptionalValue {
	ov := OptionalValue{kind: ovalBoolean}
	if v {
		ov.num = 1
	}
	return ov
}

// MakeInt4 constructs a non-null int32 OptionalValue.
func MakeInt4(v int32) OptionalValue {
	return OptionalValue{kind: ovalInt4, num: uint64(uint32(v))}
}

// MakeInt8 constructs a non-null int64 OptionalValue.
func MakeInt8(v int64) OptionalValue {
	return OptionalValue{kind: ovalInt8, num: uint64(v)}
}

// MakeReal constructs a non-null float32 OptionalValue.
func MakeReal(v float32) OptionalValue {
	return OptionalValue{kind: ovalReal, num: uint64(math.Float32bits(v))}
}

// MakeDouble constructs a non-null float64 OptionalValue.
func MakeDouble(v float64) OptionalValue {
	return OptionalValue{kind: ovalDouble, num: math.Float64bits(v)}
}

// MakeTimestamp constructs a non-null TimestampMicros OptionalValue.
func MakeTimestamp(v TimestampMicros) OptionalValue {
	return OptionalValue{kind: ovalTimestamp, num: uint64(v)}
}

// MakeUUID constructs a non-null UUIDValue OptionalValue.
func MakeUUID(v UUIDValue) OptionalValue {
	lo := *(*uint64)(unsafe.Pointer(&v[0]))
	hi := *(*uint64)(unsafe.Pointer(&v[8]))
	return OptionalValue{kind: ovalUUID, num: lo, hi: hi}
}

// makeTextKind is the internal constructor for text-like kinds.
func makeTextKind(kind uint8, tp TextPointer) OptionalValue {
	return OptionalValue{kind: kind, num: uint64(tp.Length), hi: uint64(tp.FirstPage), data: tp.Data}
}

// MakeText constructs a non-null TEXT OptionalValue.
func MakeText(tp TextPointer) OptionalValue {
	return makeTextKind(ovalText, tp)
}

// MakeVarchar constructs a non-null VARCHAR OptionalValue.
func MakeVarchar(tp TextPointer) OptionalValue {
	return makeTextKind(ovalVarchar, tp)
}

// MakeJSON constructs a non-null JSON OptionalValue.
func MakeJSON(tp TextPointer) OptionalValue {
	return makeTextKind(ovalJSON, tp)
}

// MakeTextByColumnKind dispatches to the right text constructor based on ColumnKind.
func MakeTextByColumnKind(k ColumnKind, tp TextPointer) OptionalValue {
	switch k {
	case Varchar:
		return MakeVarchar(tp)
	case JSON:
		return MakeJSON(tp)
	default: // Text
		return MakeText(tp)
	}
}

// MakeExpr constructs an OptionalValue holding an *Expr (for expression update paths).
func MakeExpr(e *Expr) OptionalValue {
	return OptionalValue{kind: ovalExpr, extra: e}
}

// MakeStatement constructs an OptionalValue holding a *Statement (for correlated subquery paths).
func MakeStatement(s *Statement) OptionalValue {
	return OptionalValue{kind: ovalStatement, extra: s}
}

// MakePlaceholder constructs an OptionalValue representing an unbound ? placeholder.
func MakePlaceholder() OptionalValue {
	return OptionalValue{kind: ovalPlaceholder}
}

// MakeExcludedRef constructs an OptionalValue holding an ExcludedRef for ON CONFLICT DO UPDATE SET EXCLUDED.col.
func MakeExcludedRef(ref ExcludedRef) OptionalValue {
	return OptionalValue{kind: ovalExcludedRef, data: []byte(ref.Column)}
}

// MakeFunction constructs an OptionalValue holding a Function sentinel (e.g. NOW()).
func MakeFunction(fn Function) OptionalValue {
	return OptionalValue{kind: ovalFunction, data: []byte(fn.Name)}
}

// IsNull reports whether the value is SQL NULL.
func (ov OptionalValue) IsNull() bool { return ov.kind == ovalNull }

// IsValid reports whether the value is not SQL NULL.
func (ov OptionalValue) IsValid() bool { return ov.kind != ovalNull }

// Kind returns the raw kind discriminant byte.
func (ov OptionalValue) Kind() uint8 { return ov.kind }

// ColumnKind returns the ColumnKind for values 1-10; returns 0 for null/special kinds.
func (ov OptionalValue) ColumnKind() ColumnKind {
	if ov.kind >= 1 && ov.kind <= 10 {
		return ColumnKind(ov.kind)
	}
	return 0
}

// AsBool extracts a boolean value. Only valid when Kind() == ovalBoolean.
func (ov OptionalValue) AsBool() bool { return ov.num != 0 }

// AsInt4 extracts an int32 value. Only valid when Kind() == ovalInt4.
func (ov OptionalValue) AsInt4() int32 { return int32(ov.num) }

// AsInt8 extracts an int64 value. Only valid when Kind() == ovalInt8.
func (ov OptionalValue) AsInt8() int64 { return int64(ov.num) }

// AsReal extracts a float32 value. Only valid when Kind() == ovalReal.
func (ov OptionalValue) AsReal() float32 { return math.Float32frombits(uint32(ov.num)) }

// AsDouble extracts a float64 value. Only valid when Kind() == ovalDouble.
func (ov OptionalValue) AsDouble() float64 { return math.Float64frombits(ov.num) }

// AsTimestamp extracts a TimestampMicros value. Only valid when Kind() == ovalTimestamp.
func (ov OptionalValue) AsTimestamp() TimestampMicros { return TimestampMicros(ov.num) }

// AsUUID extracts a UUIDValue. Only valid when Kind() == ovalUUID.
func (ov OptionalValue) AsUUID() UUIDValue {
	var v UUIDValue
	*(*uint64)(unsafe.Pointer(&v[0])) = ov.num
	*(*uint64)(unsafe.Pointer(&v[8])) = ov.hi
	return v
}

// AsTextPointer extracts a TextPointer. Only valid when Kind() is ovalVarchar, ovalText, or ovalJSON.
func (ov OptionalValue) AsTextPointer() TextPointer {
	return TextPointer{Data: ov.data, Length: uint32(ov.num), FirstPage: PageIndex(ov.hi)}
}

// AsExpr extracts the *Expr pointer. Only valid when Kind() == ovalExpr.
func (ov OptionalValue) AsExpr() *Expr { return ov.extra.(*Expr) }

// AsStatement extracts the *Statement pointer. Only valid when Kind() == ovalStatement.
func (ov OptionalValue) AsStatement() *Statement { return ov.extra.(*Statement) }

// IsTextLike reports whether this value holds a text-like value (varchar, text, or JSON).
func (ov OptionalValue) IsTextLike() bool {
	return ov.kind == ovalVarchar || ov.kind == ovalText || ov.kind == ovalJSON
}

// IsExpr reports whether this value holds an *Expr.
func (ov OptionalValue) IsExpr() bool { return ov.kind == ovalExpr }

// IsStatement reports whether this value holds a *Statement.
func (ov OptionalValue) IsStatement() bool { return ov.kind == ovalStatement }

// IsPlaceholder reports whether this value is an unbound ? placeholder.
func (ov OptionalValue) IsPlaceholder() bool { return ov.kind == ovalPlaceholder }

// IsExcludedRef reports whether this value holds an ExcludedRef.
func (ov OptionalValue) IsExcludedRef() bool { return ov.kind == ovalExcludedRef }

// IsFunction reports whether this value holds a Function sentinel.
func (ov OptionalValue) IsFunction() bool { return ov.kind == ovalFunction }

// AsExcludedRef extracts the ExcludedRef. Only valid when IsExcludedRef() is true.
func (ov OptionalValue) AsExcludedRef() ExcludedRef { return ExcludedRef{Column: string(ov.data)} }

// AsFunction extracts the Function. Only valid when IsFunction() is true.
func (ov OptionalValue) AsFunction() Function { return Function{Name: string(ov.data)} }

// AsAny extracts the stored value as the native Go type (for interop with legacy
// any-typed APIs such as castKeyValue). Returns nil for null values.
func (ov OptionalValue) AsAny() any {
	switch ov.kind {
	case ovalNull:
		return nil
	case ovalBoolean:
		return ov.AsBool()
	case ovalInt4:
		return ov.AsInt4()
	case ovalInt8:
		return ov.AsInt8()
	case ovalReal:
		return ov.AsReal()
	case ovalDouble:
		return ov.AsDouble()
	case ovalVarchar, ovalText, ovalJSON:
		return ov.AsTextPointer()
	case ovalTimestamp:
		return ov.AsTimestamp()
	case ovalUUID:
		return ov.AsUUID()
	case ovalExpr:
		return ov.AsExpr()
	case ovalStatement:
		return ov.AsStatement()
	case ovalPlaceholder:
		return Placeholder{}
	case ovalExcludedRef:
		return ov.AsExcludedRef()
	case ovalFunction:
		return ov.AsFunction()
	}
	return nil
}
