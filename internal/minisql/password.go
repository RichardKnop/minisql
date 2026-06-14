package minisql

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/bcrypt"
)

// Argon2id defaults follow OWASP recommendations (as of 2024):
// memory 64 MiB, 3 iterations, 4 lanes, 32-byte output.
const (
	argon2Memory      uint32 = 64 * 1024 // 64 MiB in KiB
	argon2Iterations  uint32 = 3
	argon2Parallelism uint8  = 4
	argon2KeyLength   uint32 = 32
	argon2SaltLength         = 16

	bcryptDefaultCost = 12 // OWASP recommends ≥10; 12 gives a good safety margin
)

// argon2idHash hashes password using Argon2id and returns a PHC-format string:
//
//	$argon2id$v=19$m=65536,t=3,p=4$<base64salt>$<base64hash>
func argon2idHash(password string) (string, error) {
	salt := make([]byte, argon2SaltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("argon2id: generate salt: %w", err)
	}
	hash := argon2.IDKey([]byte(password), salt,
		argon2Iterations, argon2Memory, argon2Parallelism, argon2KeyLength)

	b64Salt := base64.RawStdEncoding.EncodeToString(salt)
	b64Hash := base64.RawStdEncoding.EncodeToString(hash)
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, argon2Memory, argon2Iterations, argon2Parallelism,
		b64Salt, b64Hash), nil
}

// argon2idVerify checks password against a PHC-format Argon2id hash string.
// Returns true when the password matches.
func argon2idVerify(password, encoded string) (bool, error) {
	m, t, p, salt, storedHash, err := parseArgon2idHash(encoded)
	if err != nil {
		return false, err
	}
	keyLen := uint32(len(storedHash))
	candidate := argon2.IDKey([]byte(password), salt, t, m, p, keyLen)

	// Constant-time comparison.
	if len(candidate) != len(storedHash) {
		return false, nil
	}
	var diff byte
	for i := range candidate {
		diff |= candidate[i] ^ storedHash[i]
	}
	return diff == 0, nil
}

// parseArgon2idHash decodes a PHC-format Argon2id string produced by argon2idHash.
func parseArgon2idHash(encoded string) (memory, time uint32, parallelism uint8, salt, hash []byte, err error) {
	// Expected format: $argon2id$v=19$m=65536,t=3,p=4$<salt>$<hash>
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" {
		return 0, 0, 0, nil, nil, fmt.Errorf("ARGON2ID_VERIFY: invalid hash format")
	}
	// parts[2] = "v=19"  (version — we accept any value)
	// parts[3] = "m=65536,t=3,p=4"
	params := parts[3]
	for _, kv := range strings.Split(params, ",") {
		key, val, ok := strings.Cut(kv, "=")
		if !ok {
			return 0, 0, 0, nil, nil, fmt.Errorf("ARGON2ID_VERIFY: malformed param %q", kv)
		}
		n, nerr := strconv.ParseUint(val, 10, 64)
		if nerr != nil {
			return 0, 0, 0, nil, nil, fmt.Errorf("ARGON2ID_VERIFY: param %q: %w", key, nerr)
		}
		switch key {
		case "m":
			memory = uint32(n)
		case "t":
			time = uint32(n)
		case "p":
			parallelism = uint8(n)
		}
	}
	if salt, err = base64.RawStdEncoding.DecodeString(parts[4]); err != nil {
		return 0, 0, 0, nil, nil, fmt.Errorf("ARGON2ID_VERIFY: decode salt: %w", err)
	}
	if hash, err = base64.RawStdEncoding.DecodeString(parts[5]); err != nil {
		return 0, 0, 0, nil, nil, fmt.Errorf("ARGON2ID_VERIFY: decode hash: %w", err)
	}
	return memory, time, parallelism, salt, hash, nil
}

// bcryptHash hashes password using bcrypt at the given cost (0 = default).
func bcryptHash(password string, cost int) (string, error) {
	if cost <= 0 {
		cost = bcryptDefaultCost
	}
	h, err := bcrypt.GenerateFromPassword([]byte(password), cost)
	if err != nil {
		return "", fmt.Errorf("BCRYPT_HASH: %w", err)
	}
	return string(h), nil
}

// bcryptVerify checks password against a stored bcrypt hash.
// Returns true when the password matches.
func bcryptVerify(password, hash string) (bool, error) {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	if err == bcrypt.ErrMismatchedHashAndPassword {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("BCRYPT_VERIFY: %w", err)
	}
	return true, nil
}
