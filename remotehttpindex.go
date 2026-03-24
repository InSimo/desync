package desync

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"time"
)

// RemoteHTTPIndex is a remote index store accessed via HTTP.
type RemoteHTTPIndex struct {
	*RemoteHTTPBase
}

// NewRemoteHTTPIndexStore initializes a new store that pulls the specified index file via HTTP(S) from
// a remote web server.
func NewRemoteHTTPIndexStore(location *url.URL, opt StoreOptions) (*RemoteHTTPIndex, error) {
	b, err := NewRemoteHTTPStoreBase(location, opt)
	if err != nil {
		return nil, err
	}
	return &RemoteHTTPIndex{b}, nil
}

// GetIndexReader returns an index reader from an HTTP store. Fails if the specified index
// file does not exist.
func (r RemoteHTTPIndex) GetIndexReader(name string) (rdr io.ReadCloser, e error) {
	b, err := r.GetObject(name)
	if err != nil {
		return rdr, err
	}
	rc := io.NopCloser(bytes.NewReader(b))
	return rc, nil
}

// GetIndex returns an Index structure from the store
func (r *RemoteHTTPIndex) GetIndex(name string) (i Index, e error) {
	ir, err := r.GetIndexReader(name)
	if err != nil {
		return i, err
	}
	return IndexFromReader(ir)
}

// HasIndex returns true if an index with the given name exists in the store.
func (r RemoteHTTPIndex) HasIndex(name string) (bool, error) {
	u, _ := r.location.Parse(name)
	statusCode, _, err := r.IssueRetryableHttpRequest("HEAD", u, func() io.Reader { return nil })
	if err != nil {
		return false, err
	}
	switch statusCode {
	case 200:
		return true, nil
	case 404:
		return false, nil
	default:
		return false, fmt.Errorf("unexpected status code: %d", statusCode)
	}
}

// StatIndex returns metadata about the named index. For HTTP stores the
// modification time is not reliably available, and the original content
// size is computed by parsing the index.
func (r *RemoteHTTPIndex) StatIndex(name string) (IndexInfo, error) {
	idx, err := r.GetIndex(name)
	if err != nil {
		return IndexInfo{}, err
	}
	return IndexInfo{
		Name:    name,
		Size:    idx.TotalSize(),
		ModTime: time.Time{},
	}, nil
}

// StoreIndex adds a new chunk to the store
func (r *RemoteHTTPIndex) StoreIndex(name string, idx Index) error {

	getReader := func() io.Reader {

		rdr, w := io.Pipe()
		go func() {
			defer w.Close()
			idx.WriteTo(w)
		}()
		return rdr
	}

	return r.StoreObject(name, getReader)
}
