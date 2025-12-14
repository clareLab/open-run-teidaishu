package reddit

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	UA   string
	HTTP *http.Client
}

func NewClient(ua string) *Client {
	return &Client{
		UA: ua,
		HTTP: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *Client) DoJSON(ctx context.Context, method, u, auth string, body io.Reader, contentType string) (*http.Response, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("User-Agent", c.UA)
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, nil, err
	}

	b, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	resp.Body.Close()

	ct := resp.Header.Get("Content-Type")
	if !(resp.StatusCode >= 200 && resp.StatusCode < 300) || !strings.Contains(ct, "application/json") {
		if len(b) > 1024 {
			b = b[:1024]
		}
		return nil, nil, fmt.Errorf("%s %d %s %q", u, resp.StatusCode, ct, string(b))
	}

	return resp, bytes.TrimSpace(b), nil
}
