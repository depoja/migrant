package migrant

func stripLast(str string, ch byte) string {
	l := len(str)
	if l > 0 && str[l-1] == ch {
		return str[:l-1]
	}
	return str
}

// contains checks whether an array of strings contains a string
func contains(s string, a []string) bool {
	for _, k := range a {
		if s == k {
			return true
		}
	}
	return false
}
