package cmdshared

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
)

// OidIndexName returns the sharded index name for a Git LFS OID.
// The format is "<oid[0:2]>/<oid[2:4]>/<oid[4:]>.caibx", matching
// Forgejo's Pointer.RelativePath() convention (2-char/2-char/rest).
func OidIndexName(oid string) string {
	return oid[0:2] + "/" + oid[2:4] + "/" + oid[4:] + ".caibx"
}

// DeriveIndexURL derives an index store location from a chunk store location.
// For URLs (scheme length > 1): replaces the last path segment with "index/".
//
//	e.g. s3+https://host/bucket/chunks/ -> s3+https://host/bucket/index/
//
// For plain filesystem paths: returns a sibling "index" directory.
//
//	e.g. /path/to/chunks -> /path/to/index,  chunks -> index
func DeriveIndexURL(storeURL string) (string, error) {
	u, err := url.Parse(storeURL)
	if err != nil {
		return "", fmt.Errorf("invalid store URL %q: %w", storeURL, err)
	}
	// len(Scheme) <= 1 catches empty scheme (plain paths) and Windows drive letters (e.g. "C").
	// filepath.Clean normalises the path before Dir so a trailing slash is stripped first.
	if len(u.Scheme) <= 1 {
		return filepath.Join(filepath.Dir(filepath.Clean(storeURL)), "index"), nil
	}
	p := strings.TrimSuffix(u.Path, "/")
	idx := strings.LastIndex(p, "/")
	if idx < 0 {
		return "", fmt.Errorf("cannot derive index URL from %q: no path separator", storeURL)
	}
	u.Path = p[:idx+1] + "index/"
	return u.String(), nil
}
