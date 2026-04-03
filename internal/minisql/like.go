package minisql

// likeMatch reports whether str matches the SQL LIKE pattern.
// '%' matches any sequence of zero or more characters.
// '_' matches exactly one character.
// Matching is case-sensitive and byte-level (consistent with compareText).
func likeMatch(pattern, str string) bool {
	for pattern != "" {
		switch pattern[0] {
		case '%':
			// Collapse consecutive '%' wildcards.
			for pattern != "" && pattern[0] == '%' {
				pattern = pattern[1:]
			}
			if pattern == "" {
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
			if str == "" {
				return false // '_' requires exactly one character
			}
			pattern = pattern[1:]
			str = str[1:]
		default:
			if str == "" || pattern[0] != str[0] {
				return false
			}
			pattern = pattern[1:]
			str = str[1:]
		}
	}
	return str == ""
}
