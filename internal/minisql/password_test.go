package minisql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── Argon2id ─────────────────────────────────────────────────────────────────

func TestArgon2idHash_ProducesPhcFormat(t *testing.T) {
	t.Parallel()

	h, err := argon2idHash("correct horse battery staple")
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(h, "$argon2id$"), "hash should start with $argon2id$, got %q", h)
	// PHC format has exactly 5 $ separators → 6 parts when split on $.
	assert.Equal(t, 6, len(strings.Split(h, "$")))
}

func TestArgon2idHash_UniquePerCall(t *testing.T) {
	t.Parallel()

	h1, err := argon2idHash("password")
	require.NoError(t, err)
	h2, err := argon2idHash("password")
	require.NoError(t, err)
	assert.NotEqual(t, h1, h2, "each call must produce a different salt → different hash")
}

func TestArgon2idVerify_CorrectPassword(t *testing.T) {
	t.Parallel()

	h, err := argon2idHash("s3cr3t")
	require.NoError(t, err)

	ok, err := argon2idVerify("s3cr3t", h)
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestArgon2idVerify_WrongPassword(t *testing.T) {
	t.Parallel()

	h, err := argon2idHash("s3cr3t")
	require.NoError(t, err)

	ok, err := argon2idVerify("wrong", h)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestArgon2idVerify_InvalidFormat(t *testing.T) {
	t.Parallel()

	_, err := argon2idVerify("password", "not-a-valid-hash")
	assert.Error(t, err)
}

func TestArgon2idVerify_EmptyPassword(t *testing.T) {
	t.Parallel()

	h, err := argon2idHash("")
	require.NoError(t, err)

	ok, err := argon2idVerify("", h)
	require.NoError(t, err)
	assert.True(t, ok)

	ok, err = argon2idVerify("notempty", h)
	require.NoError(t, err)
	assert.False(t, ok)
}

// ── bcrypt ────────────────────────────────────────────────────────────────────

func TestBcryptHash_ProducesBcryptPrefix(t *testing.T) {
	t.Parallel()

	h, err := bcryptHash("correct horse battery staple", 0)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(h, "$2a$") || strings.HasPrefix(h, "$2b$"),
		"bcrypt hash should start with $2a$ or $2b$, got %q", h)
}

func TestBcryptHash_UniquePerCall(t *testing.T) {
	t.Parallel()

	h1, err := bcryptHash("password", 0)
	require.NoError(t, err)
	h2, err := bcryptHash("password", 0)
	require.NoError(t, err)
	assert.NotEqual(t, h1, h2, "bcrypt embeds a random salt so each call differs")
}

func TestBcryptHash_CustomCost(t *testing.T) {
	t.Parallel()

	// Cost 4 is the minimum bcrypt allows; use it in tests so they run fast.
	h, err := bcryptHash("password", 4)
	require.NoError(t, err)
	assert.Contains(t, h, "$04$", "hash should encode cost 4")
}

func TestBcryptVerify_CorrectPassword(t *testing.T) {
	t.Parallel()

	h, err := bcryptHash("s3cr3t", 4)
	require.NoError(t, err)

	ok, err := bcryptVerify("s3cr3t", h)
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestBcryptVerify_WrongPassword(t *testing.T) {
	t.Parallel()

	h, err := bcryptHash("s3cr3t", 4)
	require.NoError(t, err)

	ok, err := bcryptVerify("wrong", h)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestBcryptVerify_InvalidHash(t *testing.T) {
	t.Parallel()

	_, err := bcryptVerify("password", "not-a-bcrypt-hash")
	assert.Error(t, err)
}

func TestBcryptVerify_EmptyPassword(t *testing.T) {
	t.Parallel()

	h, err := bcryptHash("", 4)
	require.NoError(t, err)

	ok, err := bcryptVerify("", h)
	require.NoError(t, err)
	assert.True(t, ok)

	ok, err = bcryptVerify("notempty", h)
	require.NoError(t, err)
	assert.False(t, ok)
}

// ── parseArgon2idHash ─────────────────────────────────────────────────────────

func TestParseArgon2idHash_RoundTrip(t *testing.T) {
	t.Parallel()

	encoded, err := argon2idHash("round-trip")
	require.NoError(t, err)

	m, iters, p, salt, hash, err := parseArgon2idHash(encoded)
	require.NoError(t, err)
	assert.Equal(t, argon2Memory, m)
	assert.Equal(t, argon2Iterations, iters)
	assert.Equal(t, argon2Parallelism, p)
	assert.Len(t, salt, argon2SaltLength)
	assert.Len(t, hash, int(argon2KeyLength))
}

func TestParseArgon2idHash_MalformedInputs(t *testing.T) {
	t.Parallel()

	bad := []string{
		"",
		"$bcrypt$something",
		"$argon2id$v=19$m=65536",          // too few parts
		"$argon2id$v=19$m=65536,t=3,p=4$", // missing hash segment
	}
	for _, b := range bad {
		_, _, _, _, _, err := parseArgon2idHash(b)
		assert.Error(t, err, "expected error for input %q", b)
	}
}
