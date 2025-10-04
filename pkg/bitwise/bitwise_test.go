package bitwise

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Unset(t *testing.T) {
	t.Parallel()

	n := bin2uint("00001111")

	// We will be turning off the 3rd bit (index starts at 0)
	k := 2

	expected := strings.Repeat("0", 56) + "00001011"

	actual := Unset(n, k)

	assert.Equal(t, expected, fmt.Sprintf("%.64b", actual))
}

func Test_Set(t *testing.T) {
	t.Parallel()

	n := bin2uint("00001111")

	// We will be turning on the 8th bit (index starts at 0)
	k := 7

	expected := strings.Repeat("0", 56) + "10001111"

	actual := Set(n, k)

	assert.Equal(t, expected, fmt.Sprintf("%.64b", actual))
}

func Test_Toggle(t *testing.T) {
	t.Parallel()

	// We will be toggling on the 8th bit (index starts at 0)
	k := 7

	// Toogle on
	expected := strings.Repeat("0", 56) + "10001111"
	n := bin2uint("00001111")
	actual := Toggle(n, k)
	assert.Equal(t, expected, fmt.Sprintf("%.64b", actual))

	// Toggle off
	expected = strings.Repeat("0", 56) + "00001111"
	n = bin2uint("10001111")
	actual = Toggle(n, k)
	assert.Equal(t, expected, fmt.Sprintf("%.64b", actual))
}

func Test_IsSet(t *testing.T) {
	t.Parallel()

	n := bin2uint("10001111")

	assert.True(t, IsSet(n, 0))
	assert.True(t, IsSet(n, 1))
	assert.True(t, IsSet(n, 2))
	assert.True(t, IsSet(n, 3))
	assert.False(t, IsSet(n, 4))
	assert.False(t, IsSet(n, 5))
	assert.False(t, IsSet(n, 6))
	assert.True(t, IsSet(n, 7))

	for k := 8; k < 64; k++ {
		assert.False(t, IsSet(n, k))
	}
}

func bin2uint(binStr string) uint64 {
	// base 2 for binary
	result, _ := strconv.ParseUint(binStr, 2, 64)
	return uint64(result)
}
