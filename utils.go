package migrant

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"sort"
)

// stripLast removes the last given character from a given string (if present)
func stripLast(str string, ch byte) string {
	l := len(str)
	if l > 0 && str[l-1] == ch {
		return str[:l-1]
	}
	return str
}

// included checks whether a given string is included in an array of strings
func included(s string, a []string) bool {
	for _, k := range a {
		if s == k {
			return true
		}
	}
	return false
}

// getFiles reads and returns the migration file names at a given path
func getFiles(path string) ([]string, error) {
	// Read the migration files
	files, err := filepath.Glob(fmt.Sprintf("%s/*.sql", stripLast(path, '/')))

	// Sort the files in alphabetical order
	sort.Strings(files)

	return files, err
}

func readFile(path string, filename string) (string, error) {
	b, err := ioutil.ReadFile(fmt.Sprintf("%s/%s", stripLast(path, '/'), filename))
	return string(b), err
}
