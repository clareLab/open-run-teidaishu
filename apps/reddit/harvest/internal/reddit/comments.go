package reddit

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
)

func FetchCommentsFlat(ctx context.Context, c *Client, token, postID string) ([]map[string]any, error) {
	u := fmt.Sprintf("https://oauth.reddit.com/comments/%s.json?depth=1000&limit=500&raw_json=1&sort=confidence", url.PathEscape(postID))
	_, b, err := c.DoJSON(ctx, "GET", u, "Bearer "+token, nil, "")
	if err != nil {
		return nil, err
	}

	var arr []any
	if err := json.Unmarshal(b, &arr); err != nil {
		return nil, err
	}
	if len(arr) < 2 {
		return nil, fmt.Errorf("unexpected comments payload")
	}

	root, _ := arr[1].(map[string]any)
	data, _ := root["data"].(map[string]any)
	children, _ := data["children"].([]any)

	var flat []map[string]any
	for _, ch := range children {
		fwalkFlatten(ch, &flat)
	}

	sort.SliceStable(flat, func(i, j int) bool {
		pi := getS(flat[i]["parent_id"])
		pj := getS(flat[j]["parent_id"])
		if pi != pj {
			return pi < pj
		}
		ci := getF(flat[i]["created_utc"])
		cj := getF(flat[j]["created_utc"])
		if ci != cj {
			return ci < cj
		}
		return getS(flat[i]["id"]) < getS(flat[j]["id"])
	})

	return flat, nil
}

func fwalkFlatten(node any, out *[]map[string]any) {
	obj, _ := node.(map[string]any)
	if obj == nil {
		return
	}
	kind, _ := obj["kind"].(string)
	data, _ := obj["data"].(map[string]any)
	if kind == "t1" && data != nil {
		*out = append(*out, data)
		if rep, ok := data["replies"].(map[string]any); ok {
			if rdata, ok := rep["data"].(map[string]any); ok {
				if ch, ok := rdata["children"].([]any); ok {
					for _, c := range ch {
						fwalkFlatten(c, out)
					}
				}
			}
		}
		return
	}
	if kind == "Listing" && data != nil {
		if ch, ok := data["children"].([]any); ok {
			for _, c := range ch {
				fwalkFlatten(c, out)
			}
		}
	}
}

func getS(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func getF(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case json.Number:
		f, _ := t.Float64()
		return f
	default:
		return 0
	}
}
