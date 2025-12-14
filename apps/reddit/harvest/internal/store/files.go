package store

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func HasHashFile(dir, hash string) (bool, string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return false, "", nil
		}
		return false, "", err
	}
	var cand []string
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		n := e.Name()
		if strings.HasSuffix(n, ".jsonl") && strings.Contains(n, "_"+hash) {
			cand = append(cand, n)
		}
	}
	if len(cand) == 0 {
		return false, "", nil
	}
	sort.Strings(cand)
	return true, filepath.Join(dir, cand[len(cand)-1]), nil
}
