package store

import (
	"bufio"
	"encoding/json"
	"os"
)

func WriteJSONLLines(path string, rows []any) (int, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	n := 0
	for _, r := range rows {
		b, err := json.Marshal(r)
		if err != nil {
			return n, err
		}
		if _, err := w.Write(b); err != nil {
			return n, err
		}
		if err := w.WriteByte('\n'); err != nil {
			return n, err
		}
		n++
	}
	if err := w.Flush(); err != nil {
		return n, err
	}
	return n, nil
}
