package model

type SubmissionSubset struct {
	ID                string `json:"id"`
	Name              string `json:"name"`
	Subreddit         string `json:"subreddit"`
	Author            string `json:"author"`
	IsSelf            bool   `json:"is_self"`
	Domain            string `json:"domain"`
	Title             string `json:"title"`
	SelftextHTML      string `json:"selftext_html"`
	Selftext          string `json:"selftext"`
	URL               string `json:"url"`
	Permalink         string `json:"permalink"`
	Edited            any    `json:"edited"`
	Over18            bool   `json:"over_18"`
	Spoiler           bool   `json:"spoiler"`
	Locked            bool   `json:"locked"`
	Stickied          bool   `json:"stickied"`
	LinkFlairText     string `json:"link_flair_text"`
	LinkFlairCSSClass string `json:"link_flair_css_class"`
}
