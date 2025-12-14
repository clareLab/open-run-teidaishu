package reddit

import (
	"encoding/json"
	"fmt"

	"github.com/open-run-org/teidaishu/apps/reddit/harvest/internal/model"
	"github.com/open-run-org/teidaishu/apps/reddit/harvest/internal/store"
)

func CreatedIDName(raw json.RawMessage) (int64, string, string, error) {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return 0, "", "", err
	}
	cv, ok := m["created_utc"]
	if !ok {
		return 0, "", "", fmt.Errorf("no created_utc")
	}
	var created int64
	switch t := cv.(type) {
	case float64:
		created = int64(t)
	case json.Number:
		v, err := t.Int64()
		if err != nil {
			return 0, "", "", err
		}
		created = v
	default:
		return 0, "", "", fmt.Errorf("bad created_utc")
	}
	id, _ := m["id"].(string)
	name, _ := m["name"].(string)
	if id == "" || name == "" {
		return 0, "", "", fmt.Errorf("no id/name")
	}
	return created, id, name, nil
}

func SubsetHash(raw json.RawMessage) (string, error) {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return "", err
	}
	g := model.SubmissionSubset{
		ID: getS(m["id"]), Name: getS(m["name"]), Subreddit: getS(m["subreddit"]), Author: getS(m["author"]),
		IsSelf: getB(m["is_self"]), Domain: getS(m["domain"]),
		Title: getS(m["title"]), SelftextHTML: getS(m["selftext_html"]), Selftext: getS(m["selftext"]),
		URL: getS(m["url"]), Permalink: getS(m["permalink"]), Edited: m["edited"],
		Over18: getB(m["over_18"]), Spoiler: getB(m["spoiler"]), Locked: getB(m["locked"]), Stickied: getB(m["stickied"]),
		LinkFlairText: getS(m["link_flair_text"]), LinkFlairCSSClass: getS(m["link_flair_css_class"]),
	}
	b, err := json.Marshal(g)
	if err != nil {
		return "", err
	}
	return store.SHA256Hex(b), nil
}

func getB(v any) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return false
}
