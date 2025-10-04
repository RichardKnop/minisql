package bitwise

func Unset(n uint64, k int) uint64 {
	return (n & ^(1 << k)) // AND NOT
}

func Set(n uint64, k int) uint64 {
	return (n | (1 << k)) // OR
}

func Toggle(n uint64, k int) uint64 {
	return (n ^ (1 << k)) // XOR
}

func IsSet(n uint64, k int) bool {
	return n&(1<<k) > 0
}
