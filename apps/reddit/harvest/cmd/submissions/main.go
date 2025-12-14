package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/open-run-org/teidaishu/apps/reddit/harvest/internal/env"
	"github.com/open-run-org/teidaishu/apps/reddit/harvest/internal/reddit"
	"github.com/open-run-org/teidaishu/apps/reddit/harvest/internal/store"
)

func main() {
	var sub string
	var days int
	var root string
	var limit int
	flag.StringVar(&sub, "sub", "", "subreddit")
	flag.IntVar(&days, "days", 7, "days")
	flag.StringVar(&root, "root", "data/reddit/00_raw", "root")
	flag.IntVar(&limit, "limit", 100, "limit")
	flag.Parse()

	if sub == "" {
		fmt.Fprintln(os.Stderr, "usage: submissions -sub <name> [-days N] [-root PATH] [-limit N]")
		os.Exit(2)
	}
	if limit <= 0 || limit > 100 {
		limit = 100
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

	after := ""
	wrote := 0
	page := 0

	for {
		page++
		u := fmt.Sprintf("https://oauth.reddit.com/r/%s/new.json?limit=%d&raw_json=1", url.PathEscape(sub), limit)
		if after != "" {
			u += "&after=" + url.QueryEscape(after)
		}

		_, b, err := c.DoJSON(ctx, "GET", u, "Bearer "+tk, nil, "")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		var lst struct {
			Data struct {
				Children []struct {
					Data json.RawMessage `json:"data"`
				} `json:"children"`
				After string `json:"after"`
			} `json:"data"`
		}
		if err := json.Unmarshal(b, &lst); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		if len(lst.Data.Children) == 0 {
			fmt.Fprintf(os.Stderr, "[%s] %s page=%d empty\n", sub, time.Now().UTC().Format(time.RFC3339), page)
			break
		}

		stop := false
		nextAfter := ""
		nowStr := time.Now().UTC().Format("060102150405")
		batchW := 0

		for _, ch := range lst.Data.Children {
			created, id, name, err := reddit.CreatedIDName(ch.Data)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			if name != "" {
				nextAfter = name
			}
			if created < cutoff {
				stop = true
				break
			}

			dir := reddit.SubmissionDir(root, sub, created, id)
			if err := os.MkdirAll(dir, 0o755); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}

			h, err := reddit.SubsetHash(ch.Data)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}

			if ok, _, _ := store.HasHashFile(dir, h); ok {
				continue
			}

			out := filepath.Join(dir, nowStr+"_"+h+".jsonl")
			f, err := os.OpenFile(out, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
			if err != nil {
				if os.IsExist(err) {
					continue
				}
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			if _, err := f.Write(append(ch.Data, '\n')); err != nil {
				f.Close()
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			f.Close()
			wrote++
			batchW++
		}

		fmt.Fprintf(os.Stderr, "[%s] %s page=%d wrote=%d after=%q stop=%v\n", sub, time.Now().UTC().Format(time.RFC3339), page, batchW, nextAfter, stop)

		if stop || nextAfter == "" {
			break
		}
		after = nextAfter
	}

	fmt.Fprintf(os.Stderr, "[%s] done days=%d total=%d\n", sub, days, wrote)
}
