package reddit

import "encoding/json"

type listing struct {
	Data struct {
		Children []struct {
			Data json.RawMessage `json:"data"`
		} `json:"children"`
		After string `json:"after"`
	} `json:"data"`
}
