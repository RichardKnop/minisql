package minisql

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// UUIDValue is a 16-byte UUID stored inline in B-tree pages.
type UUIDValue [16]byte

// ParseUUID parses a standard hyphenated UUID string
// (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx) into a UUIDValue.
func ParseUUID(s string) (UUIDValue, error) {
	if len(s) != 36 {
		return UUIDValue{}, fmt.Errorf("invalid UUID length %d (expected 36)", len(s))
	}
	if s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return UUIDValue{}, fmt.Errorf("invalid UUID format: missing hyphens at expected positions")
	}
	// Decode without allocating by stripping hyphens into a 32-char hex string.
	var b [32]byte
	dst := b[:0]
	for i, c := range []byte(s) {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		dst = append(dst, c)
	}
	if len(dst) != 32 {
		return UUIDValue{}, fmt.Errorf("invalid UUID: expected 32 hex characters after removing hyphens")
	}
	var out [16]byte
	if _, err := hex.Decode(out[:], dst); err != nil {
		return UUIDValue{}, fmt.Errorf("invalid UUID: %w", err)
	}
	return UUIDValue(out), nil
}

// String formats a UUIDValue as a standard hyphenated UUID string.
func (u UUIDValue) String() string {
	var buf [36]byte
	hex.Encode(buf[0:8], u[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], u[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], u[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], u[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], u[10:16])
	return string(buf[:])
}

// NewRandomUUID generates a random UUID v4.
func NewRandomUUID() (UUIDValue, error) {
	var u UUIDValue
	if _, err := rand.Read(u[:]); err != nil {
		return UUIDValue{}, fmt.Errorf("uuid: failed to read random bytes: %w", err)
	}
	// Set version 4 bits (bits 12-15 of time_hi_and_version).
	u[6] = (u[6] & 0x0f) | 0x40
	// Set variant bits (bits 6-7 of clock_seq_hi_and_reserved).
	u[8] = (u[8] & 0x3f) | 0x80
	return u, nil
}

// toUUIDValue converts any value (UUIDValue or TextPointer/string) to UUIDValue.
func toUUIDValue(v any) (UUIDValue, error) {
	switch val := v.(type) {
	case UUIDValue:
		return val, nil
	case TextPointer:
		return ParseUUID(val.String())
	case string:
		return ParseUUID(val)
	default:
		return UUIDValue{}, fmt.Errorf("cannot convert %T to UUID", v)
	}
}
