package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

// PageCipher encrypts and decrypts individual database pages using AES-256-CTR.
// Each page has its own independent keystream derived from its page index, so
// pages can be encrypted or decrypted independently and in any order.
//
// The key is derived from the caller-supplied key material and a per-database
// salt using HKDF (RFC 5869) with HMAC-SHA256, so the raw user key is never
// used directly for AES.
type PageCipher struct {
	key [32]byte
}

// NewPageCipher derives a 256-bit AES key from userKey and salt using
// HKDF-Extract + HKDF-Expand (RFC 5869, HMAC-SHA256) and returns a
// PageCipher ready for use.
func NewPageCipher(userKey, salt []byte) (*PageCipher, error) {
	if len(userKey) == 0 {
		return nil, fmt.Errorf("encryption key must not be empty")
	}
	if len(salt) == 0 {
		return nil, fmt.Errorf("encryption salt must not be empty")
	}

	// HKDF-Extract: PRK = HMAC-SHA256(salt, userKey)
	mac := hmac.New(sha256.New, salt)
	mac.Write(userKey)
	prk := mac.Sum(nil)

	// HKDF-Expand: OKM = HMAC-SHA256(PRK, info || 0x01)
	mac = hmac.New(sha256.New, prk)
	mac.Write([]byte("minisql-page-enc-v1"))
	mac.Write([]byte{0x01})

	var key [32]byte
	copy(key[:], mac.Sum(nil))

	return &PageCipher{key: key}, nil
}

// XORKeyStream encrypts or decrypts buf in-place using AES-256-CTR.
// The nonce is deterministic: 8 zero bytes followed by the 8-byte big-endian
// encoding of pageIdx. Since each page has a unique index the keystream is
// unique per page, which is acceptable for a data-at-rest threat model where
// the on-disk content is fixed for a given (key, pageIdx) pair.
//
// For page 0, callers must pass pageIdx=0 but only supply the sub-slice that
// excludes the plaintext database header (bytes 0-99 of page 0 are never
// encrypted so the salt can be bootstrapped on open).
func (c *PageCipher) XORKeyStream(buf []byte, pageIdx uint32) {
	iv := [aes.BlockSize]byte{}
	binary.BigEndian.PutUint64(iv[8:], uint64(pageIdx))
	block, _ := aes.NewCipher(c.key[:])
	stream := cipher.NewCTR(block, iv[:])
	stream.XORKeyStream(buf, buf)
}
