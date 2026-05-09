package minisql_test

import (
	"testing"

	"github.com/RichardKnop/minisql/internal/minisql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseUUID(t *testing.T) {
	t.Parallel()

	t.Run("valid UUID", func(t *testing.T) {
		t.Parallel()
		uv, err := minisql.ParseUUID("550e8400-e29b-41d4-a716-446655440000")
		require.NoError(t, err)
		assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", uv.String())
	})

	t.Run("uppercase accepted", func(t *testing.T) {
		t.Parallel()
		uv, err := minisql.ParseUUID("550E8400-E29B-41D4-A716-446655440000")
		require.NoError(t, err)
		assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", uv.String())
	})

	t.Run("nil UUID", func(t *testing.T) {
		t.Parallel()
		uv, err := minisql.ParseUUID("00000000-0000-0000-0000-000000000000")
		require.NoError(t, err)
		assert.Equal(t, "00000000-0000-0000-0000-000000000000", uv.String())
	})

	t.Run("wrong length", func(t *testing.T) {
		t.Parallel()
		_, err := minisql.ParseUUID("too-short")
		require.Error(t, err)
	})

	t.Run("missing hyphens", func(t *testing.T) {
		t.Parallel()
		_, err := minisql.ParseUUID("550e8400Xe29bX41d4Xa716X446655440000")
		require.Error(t, err)
	})

	t.Run("invalid hex", func(t *testing.T) {
		t.Parallel()
		_, err := minisql.ParseUUID("zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz")
		require.Error(t, err)
	})
}

func TestUUIDValueString(t *testing.T) {
	t.Parallel()
	raw := minisql.UUIDValue{
		0x55, 0x0e, 0x84, 0x00,
		0xe2, 0x9b,
		0x41, 0xd4,
		0xa7, 0x16,
		0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
	}
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", raw.String())
}

func TestNewRandomUUID(t *testing.T) {
	t.Parallel()
	u1, err := minisql.NewRandomUUID()
	require.NoError(t, err)
	u2, err := minisql.NewRandomUUID()
	require.NoError(t, err)

	// Version 4: high nibble of byte 6 == 0x4
	assert.Equal(t, byte(0x40), u1[6]&0xf0)
	// Variant: high two bits of byte 8 == 0b10
	assert.Equal(t, byte(0x80), u1[8]&0xc0)

	// Two random UUIDs are distinct
	assert.NotEqual(t, u1, u2)

	// Round-trips through string format
	parsed, err := minisql.ParseUUID(u1.String())
	require.NoError(t, err)
	assert.Equal(t, u1, parsed)
}

func TestUUIDRowMarshalUnmarshal(t *testing.T) {
	t.Parallel()
	cols := []minisql.Column{
		{Name: "id", Kind: minisql.UUID, Size: 16},
	}
	uv, err := minisql.ParseUUID("550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)

	row := minisql.NewRowWithValues(cols, []minisql.OptionalValue{
		{Value: uv, Valid: true},
	})

	b, err := row.Marshal()
	require.NoError(t, err)
	assert.Len(t, b, 16)

	row2 := minisql.NewRow(cols)
	row2, err = row2.Unmarshal(minisql.Cell{Value: b}, minisql.Field{Name: "id"})
	require.NoError(t, err)

	got, ok := row2.Values[0].Value.(minisql.UUIDValue)
	require.True(t, ok)
	assert.Equal(t, uv, got)
}

func TestUUIDRowMarshalNull(t *testing.T) {
	t.Parallel()
	cols := []minisql.Column{
		{Name: "id", Kind: minisql.UUID, Size: 16},
	}

	row := minisql.NewRowWithValues(cols, []minisql.OptionalValue{
		{Valid: false},
	})

	b, err := row.Marshal()
	require.NoError(t, err)
	assert.Empty(t, b) // NULL occupies no bytes
}
