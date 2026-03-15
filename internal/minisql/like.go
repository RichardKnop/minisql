package minisql

// likeMatch reports whether str matches the SQL LIKE pattern.
// '%' matches any sequence of zero or more characters.
// '_' matches exactly one character.
// Matching is case-sensitive and byte-level (consistent with compareText).
func likeMatch(pattern, str string) bool {
	for len(pattern) > 0 {
		switch pattern[0] {
		case '%':
			// Collapse consecutive '%' wildcards.
			for len(pattern) > 0 && pattern[0] == '%' {
				pattern = pattern[1:]
			}
			if len(pattern) == 0 {
				return true // trailing '%' matches anything
			}
			// Try to match the remaining pattern at every position in str.
			for i := 0; i <= len(str); i++ {
				if likeMatch(pattern, str[i:]) {
					return true
				}
			}
			return false
		case '_':
			if len(str) == 0 {
				return false // '_' requires exactly one character
			}
			pattern = pattern[1:]
			str = str[1:]
		default:
			if len(str) == 0 || pattern[0] != str[0] {
				return false
			}
			pattern = pattern[1:]
			str = str[1:]
		}
	}
	return len(str) == 0
}
