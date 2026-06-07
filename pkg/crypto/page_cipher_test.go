package crypto

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewPageCipher_RejectsEmptyInputs(t *testing.T) {
	t.Parallel()

	_, err := NewPageCipher(nil, []byte("salt"))
	require.EqualError(t, err, "encryption key must not be empty")

	_, err = NewPageCipher([]byte("key"), nil)
	require.EqualError(t, err, "encryption salt must not be empty")
}

func TestPageCipher_XORKeyStreamRoundTrip(t *testing.T) {
	t.Parallel()

	cipher, err := NewPageCipher([]byte("secret"), []byte("database salt"))
	require.NoError(t, err)

	plaintext := []byte("page payload")
	buf := append([]byte(nil), plaintext...)
	cipher.XORKeyStream(buf, 42)
	require.NotEqual(t, plaintext, buf)

	cipher.XORKeyStream(buf, 42)
	require.Equal(t, plaintext, buf)
}

func TestPageCipher_PageIndexChangesKeystream(t *testing.T) {
	t.Parallel()

	cipher, err := NewPageCipher([]byte("secret"), []byte("database salt"))
	require.NoError(t, err)

	pageA := []byte("same payload")
	pageB := []byte("same payload")
	cipher.XORKeyStream(pageA, 1)
	cipher.XORKeyStream(pageB, 2)

	require.False(t, bytes.Equal(pageA, pageB))
}
