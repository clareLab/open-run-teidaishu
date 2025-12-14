package env

import (
	"fmt"
	"os"
)

func Must(k string) string {
	v := os.Getenv(k)
	if v == "" {
		fmt.Fprintln(os.Stderr, k+" not set")
		os.Exit(1)
	}
	return v
}

func Maybe(k string) string { return os.Getenv(k) }
