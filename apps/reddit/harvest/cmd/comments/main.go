package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/open-run-org/teidaishu/apps/reddit/harvest/internal/env"
	"github.com/open-run-org/teidaishu/apps/reddit/harvest/internal/reddit"
	"github.com/open-run-org/teidaishu/apps/reddit/harvest/internal/store"
)

type submissionLite struct {
	ID         string  `json:"id"`
	CreatedUTC float64 `json:"created_utc"`
}

func readOneSubmission(path string) (*submissionLite, string, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, "", "", err
	}
	defer f.Close()
	rd := bufio.NewReader(f)
	line, err := rd.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return nil, "", "", err
	}
	var m submissionLite
	if err := json.Unmarshal(bytes.TrimSpace(line), &m); err != nil {
		return nil, "", "", err
	}
	createdID := filepath.Base(filepath.Dir(path))
	capture := strings.TrimSuffix(filepath.Base(path), ".jsonl")
	return &m, createdID, capture, nil
}

func threadHash(rows []map[string]any) (string, error) {
	type mini struct {
		ID            string `json:"id"`
		Parent        string `json:"parent_id"`
		Author        string `json:"author"`
		Body          string `json:"body"`
		BodyHTML      string `json:"body_html"`
		Edited        any    `json:"edited"`
		Stickied      bool   `json:"stickied"`
		Distinguished any    `json:"distinguished"`
		IsSubmitter   bool   `json:"is_submitter"`
		Permalink     string `json:"permalink"`
	}
	arr := make([]mini, 0, len(rows))
	for _, r := range rows {
		st := false
		if b, ok := r["stickied"].(bool); ok {
			st = b
		}
		isSub := false
		if b, ok := r["is_submitter"].(bool); ok {
			isSub = b
		}
		arr = append(arr, mini{
			ID:            getS(r["id"]),
			Parent:        getS(r["parent_id"]),
			Author:        getS(r["author"]),
			Body:          getS(r["body"]),
			BodyHTML:      getS(r["body_html"]),
			Edited:        r["edited"],
			Stickied:      st,
			Distinguished: r["distinguished"],
			IsSubmitter:   isSub,
			Permalink:     getS(r["permalink"]),
		})
	}
	b, err := json.Marshal(arr)
	if err != nil {
		return "", err
	}
	return store.SHA256Hex(b), nil
}

func getS(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func main() {
	var sub string
	var days int
	var root string
	flag.StringVar(&sub, "sub", "", "subreddit")
	flag.IntVar(&days, "days", 7, "days")
	flag.StringVar(&root, "root", "data/reddit/00_raw", "root")
	flag.Parse()

	if sub == "" {
		fmt.Fprintln(os.Stderr, "usage: comments -sub <name> [-days N] [-root PATH]")
		os.Exit(2)
	}

	ua := env.Must("REDDIT_USER_AGENT")
	c := reddit.NewClient(ua)

	tp := reddit.TokenProvider{
		ClientID:     env.Must("REDDIT_CLIENT_ID"),
		ClientSecret: env.Must("REDDIT_CLIENT_SECRET"),
		Username:     env.Maybe("REDDIT_USERNAME"),
		Password:     env.Maybe("REDDIT_PASSWORD"),
	}

	ctx := context.Background()
	tk, err := tp.Token(ctx, c)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	cutoff := time.Now().Add(-time.Duration(days) * 24 * time.Hour).Unix()
	subRoot := filepath.Join(root, "r_"+sub, "submissions")
	glob := filepath.Join(subRoot, "*", "*.jsonl")
	matches, _ := filepath.Glob(glob)
	sort.Strings(matches)

	type capInfo struct {
		Path        string
		CreatedUnix int64
		CreatedID   string
		Capture     string
	}

	byPost := map[string]capInfo{}
	for _, f := range matches {
		sl, createdID, capture, err := readOneSubmission(f)
		if err != nil {
			continue
		}
		created := int64(sl.CreatedUTC)
		if created < cutoff {
			continue
		}
		postID := sl.ID
		old, ok := byPost[postID]
		if !ok || capture > old.Capture {
			byPost[postID] = capInfo{Path: f, CreatedUnix: created, CreatedID: createdID, Capture: capture}
		}
	}

	keys := make([]string, 0, len(byPost))
	for k := range byPost {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	totalPosts := len(keys)
	writes := 0
	skips := 0
	empties := 0

	fmt.Fprintf(os.Stderr, "[%s] start days=%d posts=%d\n", sub, days, totalPosts)

	for _, pid := range keys {
		info := byPost[pid]
		dir := reddit.CommentsDir(root, sub, info.CreatedID)
		_ = os.MkdirAll(dir, 0o755)

		rows, err := reddit.FetchCommentsFlat(ctx, c, tk, pid)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[%s] %s fetch_error: %v\n", sub, pid, err)
			continue
		}
		if len(rows) == 0 {
			empties++
			_ = os.WriteFile(filepath.Join(dir, "EMPTY.txt"), []byte("no_comments"), 0o644)
			fmt.Fprintf(os.Stderr, "[%s] %s no_comments\n", sub, pid)
			continue
		}

		h, err := threadHash(rows)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[%s] %s hash_error: %v\n", sub, pid, err)
			continue
		}

		if ok, _, _ := store.HasHashFile(dir, h); ok {
			skips++
			fmt.Fprintf(os.Stderr, "[%s] %s skip hash=%s\n", sub, pid, h[:16])
			continue
		}

		nowStr := time.Now().UTC().Format("060102150405")
		out := filepath.Join(dir, nowStr+"_"+h+".jsonl")

		anyRows := make([]any, 0, len(rows))
		for _, r := range rows {
			anyRows = append(anyRows, r)
		}

		n, err := store.WriteJSONLLines(out, anyRows)
		if err != nil {
			if os.IsExist(err) {
				skips++
				fmt.Fprintf(os.Stderr, "[%s] %s skip exists\n", sub, pid)
				continue
			}
			fmt.Fprintln(os.Stderr, err)
			continue
		}

		writes++
		fmt.Fprintf(os.Stderr, "[%s] %s write %d lines hash=%s\n", sub, pid, n, h[:16])
	}

	fmt.Fprintf(os.Stderr, "[%s] done posts_scanned=%d write=%d skip=%d empty=%d\n", sub, totalPosts, writes, skips, empties)
}
