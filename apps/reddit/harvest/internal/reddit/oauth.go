package reddit

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
)

type TokenProvider struct {
	ClientID     string
	ClientSecret string
	Username     string
	Password     string
}

func (tp TokenProvider) Token(ctx context.Context, c *Client) (string, error) {
	form := url.Values{}
	var auth string

	if tp.Username != "" && tp.Password != "" {
		form.Set("grant_type", "password")
		form.Set("username", tp.Username)
		form.Set("password", tp.Password)
		form.Set("scope", "read")
		auth = "Basic " + base64.StdEncoding.EncodeToString([]byte(tp.ClientID+":"+tp.ClientSecret))
	} else {
		form.Set("grant_type", "client_credentials")
		form.Set("scope", "read")
		auth = "Basic " + base64.StdEncoding.EncodeToString([]byte(tp.ClientID+":"+tp.ClientSecret))
	}

	_, b, err := c.DoJSON(ctx, "POST", "https://www.reddit.com/api/v1/access_token", auth, strings.NewReader(form.Encode()), "application/x-www-form-urlencoded")
	if err != nil {
		return "", err
	}

	var tr struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(b, &tr); err != nil {
		return "", err
	}
	if tr.AccessToken == "" {
		return "", fmt.Errorf("empty access_token")
	}
	return tr.AccessToken, nil
}
