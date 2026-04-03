package bitwise

// Unset clears bit k in n and returns the result.
func Unset(n uint64, k int) uint64 {
	return (n & ^(1 << k)) // AND NOT
}

// Set sets bit k in n and returns the result.
func Set(n uint64, k int) uint64 {
	return (n | (1 << k)) // OR
}

// Toggle flips bit k in n and returns the result.
func Toggle(n uint64, k int) uint64 {
	return (n ^ (1 << k)) // XOR
}

// IsSet reports whether bit k is set in n.
func IsSet(n uint64, k int) bool {
	return n&(1<<k) > 0
}
