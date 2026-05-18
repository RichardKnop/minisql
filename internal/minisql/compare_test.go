package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEqualAny(t *testing.T) {
	t.Parallel()

	assert.True(t, equalAny(int64(5), int64(5)))
	assert.False(t, equalAny(int64(5), int64(6)))
	assert.True(t, equalAny(int32(3), int64(3)))
	assert.True(t, equalAny(int64(3), int32(3)))
	assert.True(t, equalAny(float64(1.5), float64(1.5)))
	assert.True(t, equalAny(true, true))
	assert.False(t, equalAny(true, false))
	assert.False(t, equalAny(nil, nil))
	assert.False(t, equalAny(nil, int64(0)))
	assert.False(t, equalAny(int64(0), nil))
	// type mismatch
	assert.False(t, equalAny(int64(1), "1"))

	tp1 := NewTextPointer([]byte("hello"))
	tp2 := NewTextPointer([]byte("hello"))
	tp3 := NewTextPointer([]byte("world"))
	assert.True(t, equalAny(tp1, tp2))
	assert.False(t, equalAny(tp1, tp3))
}

func TestCompareAny(t *testing.T) {
	t.Parallel()

	t.Run("bool", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 0, compareAny(true, true))
		assert.Equal(t, 0, compareAny(false, false))
		assert.Equal(t, 1, compareAny(true, false))
		assert.Equal(t, -1, compareAny(false, true))
	})

	t.Run("int32 vs int32", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 0, compareAny(int32(5), int32(5)))
		assert.Equal(t, -1, compareAny(int32(3), int32(7)))
		assert.Equal(t, 1, compareAny(int32(7), int32(3)))
	})

	t.Run("int32 vs int64 cross-type", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 0, compareAny(int32(5), int64(5)))
		assert.Equal(t, -1, compareAny(int32(1), int64(2)))
		assert.Equal(t, 1, compareAny(int32(9), int64(8)))
	})

	t.Run("int64 vs int64", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 0, compareAny(int64(100), int64(100)))
		assert.Equal(t, -1, compareAny(int64(0), int64(1)))
		assert.Equal(t, 1, compareAny(int64(1), int64(0)))
	})

	t.Run("int64 vs int32 cross-type", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 0, compareAny(int64(5), int32(5)))
		assert.Equal(t, -1, compareAny(int64(2), int32(3)))
		assert.Equal(t, 1, compareAny(int64(4), int32(3)))
	})

	t.Run("float32 vs float32", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 0, compareAny(float32(1.5), float32(1.5)))
		assert.Equal(t, -1, compareAny(float32(1.0), float32(2.0)))
		assert.Equal(t, 1, compareAny(float32(2.0), float32(1.0)))
	})

	t.Run("float32 vs float64 cross-type", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 0, compareAny(float32(1.5), float64(1.5)))
		assert.Equal(t, -1, compareAny(float32(1.0), float64(2.0)))
		assert.Equal(t, 1, compareAny(float32(2.0), float64(1.0)))
	})

	t.Run("float64 vs float64", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 0, compareAny(float64(3.14), float64(3.14)))
		assert.Equal(t, -1, compareAny(float64(1.0), float64(1.1)))
		assert.Equal(t, 1, compareAny(float64(1.1), float64(1.0)))
	})

	t.Run("float64 vs float32 cross-type", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 0, compareAny(float64(1.5), float32(1.5)))
		assert.Equal(t, -1, compareAny(float64(0.5), float32(1.0)))
		assert.Equal(t, 1, compareAny(float64(2.0), float32(1.0)))
	})

	t.Run("string", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 0, compareAny("abc", "abc"))
		assert.Equal(t, -1, compareAny("abc", "abd"))
		assert.Equal(t, 1, compareAny("b", "a"))
	})

	t.Run("TextPointer", func(t *testing.T) {
		t.Parallel()
		hello1 := NewTextPointer([]byte("hello"))
		hello2 := NewTextPointer([]byte("hello"))
		world := NewTextPointer([]byte("world"))
		assert.Equal(t, 0, compareAny(hello1, hello2))
		assert.Equal(t, -1, compareAny(hello1, world))
		assert.Equal(t, 1, compareAny(world, hello1))
	})

	t.Run("TimestampMicros", func(t *testing.T) {
		t.Parallel()
		t1 := MustParseTimestampMicros("2020-01-01 00:00:00")
		t2 := MustParseTimestampMicros("2021-01-01 00:00:00")
		assert.Equal(t, 0, compareAny(t1, t1))
		assert.Equal(t, -1, compareAny(t1, t2))
		assert.Equal(t, 1, compareAny(t2, t1))
	})

	t.Run("CompositeKey", func(t *testing.T) {
		t.Parallel()
		cols := []Column{{Name: "id", Kind: Int8}}
		ck1 := NewCompositeKey(cols, int64(1))
		ck2 := NewCompositeKey(cols, int64(2))
		ck3 := NewCompositeKey(cols, int64(1))
		assert.Equal(t, 0, compareAny(ck1, ck3))
		assert.Equal(t, -1, compareAny(ck1, ck2))
		assert.Equal(t, 1, compareAny(ck2, ck1))
	})

	t.Run("type mismatch returns -1", func(t *testing.T) {
		t.Parallel()
		// unknown type falls through to default (returns 0), but mismatched
		// known types (e.g. int32 with non-int b) return -1
		assert.Equal(t, -1, compareAny(int32(1), "not an int"))
		assert.Equal(t, -1, compareAny(int64(1), "not an int"))
		assert.Equal(t, -1, compareAny(float32(1.0), "not a float"))
		assert.Equal(t, -1, compareAny(float64(1.0), "not a float"))
	})
}

func TestCompareValues(t *testing.T) {
	t.Parallel()

	t.Run("both NULL are equal", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 0, compareValues(MakeNull(), MakeNull()))
	})

	t.Run("NULL is less than any value", func(t *testing.T) {
		t.Parallel()
		null := MakeNull()
		nonNull := MakeInt8(int64(0))
		assert.Equal(t, -1, compareValues(null, nonNull))
	})

	t.Run("any value is greater than NULL", func(t *testing.T) {
		t.Parallel()
		null := MakeNull()
		nonNull := MakeInt8(int64(0))
		assert.Equal(t, 1, compareValues(nonNull, null))
	})

	t.Run("delegates to compareAny for non-NULL values", func(t *testing.T) {
		t.Parallel()
		lo := MakeInt8(int64(1))
		hi := MakeInt8(int64(2))
		eq := MakeInt8(int64(1))
		assert.Equal(t, -1, compareValues(lo, hi))
		assert.Equal(t, 1, compareValues(hi, lo))
		assert.Equal(t, 0, compareValues(lo, eq))
	})

	t.Run("string values", func(t *testing.T) {
		t.Parallel()
		a := MakeVarchar(NewTextPointer([]byte("apple")))
		b := MakeVarchar(NewTextPointer([]byte("banana")))
		assert.Equal(t, -1, compareValues(a, b))
		assert.Equal(t, 1, compareValues(b, a))
		assert.Equal(t, 0, compareValues(a, a))
	})
}
