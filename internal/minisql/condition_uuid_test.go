package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustParseUUID(t *testing.T, s string) UUIDValue {
	t.Helper()
	uv, err := ParseUUID(s)
	require.NoError(t, err)
	return uv
}

func TestUUIDCompare(t *testing.T) {
	t.Parallel()

	lo := mustParseUUID(t, "00000000-0000-0000-0000-000000000001")
	hi := mustParseUUID(t, "ffffffff-ffff-ffff-ffff-ffffffffffff")
	same := mustParseUUID(t, "00000000-0000-0000-0000-000000000001")

	assert.Equal(t, -1, uuidCompare(lo, hi))
	assert.Equal(t, 1, uuidCompare(hi, lo))
	assert.Equal(t, 0, uuidCompare(lo, same))
}

func TestCompareUUID(t *testing.T) {
	t.Parallel()

	lo := mustParseUUID(t, "550e8400-e29b-41d4-a716-446655440000")
	hi := mustParseUUID(t, "6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	same := mustParseUUID(t, "550e8400-e29b-41d4-a716-446655440000")

	tests := []struct {
		name     string
		v1, v2   UUIDValue
		op       Operator
		expected bool
		wantErr  bool
	}{
		{"eq equal", lo, same, Eq, true, false},
		{"eq not equal", lo, hi, Eq, false, false},
		{"ne equal", lo, same, Ne, false, false},
		{"ne not equal", lo, hi, Ne, true, false},
		{"lt less", lo, hi, Lt, true, false},
		{"lt greater", hi, lo, Lt, false, false},
		{"lte less", lo, hi, Lte, true, false},
		{"lte equal", lo, same, Lte, true, false},
		{"gt greater", hi, lo, Gt, true, false},
		{"gt less", lo, hi, Gt, false, false},
		{"gte greater", hi, lo, Gte, true, false},
		{"gte equal", lo, same, Gte, true, false},
		{"unknown operator", lo, same, 0, false, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := compareUUID(tc.v1, tc.v2, tc.op)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestIsInListUUID(t *testing.T) {
	t.Parallel()

	u1 := mustParseUUID(t, "550e8400-e29b-41d4-a716-446655440000")
	u2 := mustParseUUID(t, "6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	u3 := mustParseUUID(t, "6ba7b811-9dad-11d1-80b4-00c04fd430c8")

	t.Run("found in list", func(t *testing.T) {
		t.Parallel()
		found, err := isInListUUID(u1, []any{u2, u1, u3})
		require.NoError(t, err)
		assert.True(t, found)
	})

	t.Run("not found in list", func(t *testing.T) {
		t.Parallel()
		found, err := isInListUUID(u3, []any{u1, u2})
		require.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("empty list", func(t *testing.T) {
		t.Parallel()
		found, err := isInListUUID(u1, []any{})
		require.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("invalid value", func(t *testing.T) {
		t.Parallel()
		_, err := isInListUUID("not-a-uuid", []any{u1})
		require.Error(t, err)
	})

	t.Run("list not slice", func(t *testing.T) {
		t.Parallel()
		_, err := isInListUUID(u1, "not a list")
		require.Error(t, err)
	})

	t.Run("string value in list", func(t *testing.T) {
		t.Parallel()
		found, err := isInListUUID("550e8400-e29b-41d4-a716-446655440000", []any{u1, u2})
		require.NoError(t, err)
		assert.True(t, found)
	})
}

func TestToUUIDValue(t *testing.T) {
	t.Parallel()

	uv := mustParseUUID(t, "550e8400-e29b-41d4-a716-446655440000")

	t.Run("UUIDValue passthrough", func(t *testing.T) {
		t.Parallel()
		got, err := toUUIDValue(uv)
		require.NoError(t, err)
		assert.Equal(t, uv, got)
	})

	t.Run("string conversion", func(t *testing.T) {
		t.Parallel()
		got, err := toUUIDValue("550e8400-e29b-41d4-a716-446655440000")
		require.NoError(t, err)
		assert.Equal(t, uv, got)
	})

	t.Run("TextPointer conversion", func(t *testing.T) {
		t.Parallel()
		tp := NewTextPointer([]byte("550e8400-e29b-41d4-a716-446655440000"))
		got, err := toUUIDValue(tp)
		require.NoError(t, err)
		assert.Equal(t, uv, got)
	})

	t.Run("invalid type error", func(t *testing.T) {
		t.Parallel()
		_, err := toUUIDValue(int64(42))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot convert int64 to UUID")
	})

	t.Run("invalid string error", func(t *testing.T) {
		t.Parallel()
		_, err := toUUIDValue("not-a-uuid")
		require.Error(t, err)
	})
}
