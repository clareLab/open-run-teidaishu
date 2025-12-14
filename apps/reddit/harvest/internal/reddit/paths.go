package reddit

import (
	"path/filepath"
	"time"
)

func TsYYMMDDHHMMSS(unix int64) string { return time.Unix(unix, 0).UTC().Format("060102150405") }

func SubmissionDir(root, sub string, createdUTC int64, postID string) string {
	return filepath.Join(root, "r_"+sub, "submissions", TsYYMMDDHHMMSS(createdUTC)+"_"+postID)
}

func CommentsDir(root, sub string, createdID string) string {
	return filepath.Join(root, "r_"+sub, "comments", createdID)
}
