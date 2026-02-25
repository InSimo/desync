package main

import (
	"fmt"
	"net/url"
	"runtime"

	"github.com/folbricht/desync"
	minio "github.com/minio/minio-go/v6"
)

// chunkStoreFromURL creates a WriteStore from a URL or filesystem path.
// It supports all desync store backends: local filesystem, SFTP, HTTP/HTTPS, S3, and GCS.
// The global cfg is used for S3 credentials.
func chunkStoreFromURL(location string, opt desync.StoreOptions) (desync.WriteStore, error) {
	loc, err := url.Parse(location)
	if err != nil {
		return nil, fmt.Errorf("invalid store URL %q: %w", location, err)
	}

	switch loc.Scheme {
	case "ssh":
		return nil, fmt.Errorf("SSH stores are read-only and cannot be used as a chunk store")
	case "sftp":
		s, err := desync.NewSFTPStore(loc, opt)
		if err != nil {
			return nil, fmt.Errorf("creating SFTP chunk store: %w", err)
		}
		return s, nil
	case "http", "https":
		s, err := desync.NewRemoteHTTPStore(loc, opt)
		if err != nil {
			return nil, fmt.Errorf("creating HTTP chunk store: %w", err)
		}
		return s, nil
	case "s3+http", "s3+https":
		s3Creds, region := cfg.GetS3CredentialsFor(loc)
		lookup := minio.BucketLookupAuto
		ls := loc.Query().Get("lookup")
		switch ls {
		case "dns":
			lookup = minio.BucketLookupDNS
		case "path":
			lookup = minio.BucketLookupPath
		case "", "auto":
		default:
			return nil, fmt.Errorf("unknown S3 bucket lookup type: %q", ls)
		}
		s, err := desync.NewS3Store(loc, s3Creds, region, opt, lookup)
		if err != nil {
			return nil, fmt.Errorf("creating S3 chunk store: %w", err)
		}
		return s, nil
	case "gs":
		s, err := desync.NewGCStore(loc, opt)
		if err != nil {
			return nil, fmt.Errorf("creating GCS chunk store: %w", err)
		}
		return s, nil
	default:
		local, err := desync.NewLocalStore(location, opt)
		if err != nil {
			return nil, fmt.Errorf("creating local chunk store: %w", err)
		}
		// On Windows, wrap with a dedup queue to serialize concurrent writes.
		if runtime.GOOS == "windows" {
			return desync.NewWriteDedupQueue(local), nil
		}
		return local, nil
	}
}

// indexStoreFromURL creates an IndexWriteStore from a directory URL or filesystem path.
// Unlike cmd/desync's indexStoreFromLocation, the location is a directory, not a file path,
// because the index filename is always derived from the LFS OID by the agent.
// The global cfg is used for S3 credentials.
func indexStoreFromURL(location string, opt desync.StoreOptions) (desync.IndexWriteStore, error) {
	loc, err := url.Parse(location)
	if err != nil {
		return nil, fmt.Errorf("invalid index store URL %q: %w", location, err)
	}

	switch loc.Scheme {
	case "ssh":
		return nil, fmt.Errorf("SSH stores do not support index storage")
	case "sftp":
		s, err := desync.NewSFTPIndexStore(loc, opt)
		if err != nil {
			return nil, fmt.Errorf("creating SFTP index store: %w", err)
		}
		return s, nil
	case "http", "https":
		s, err := desync.NewRemoteHTTPIndexStore(loc, opt)
		if err != nil {
			return nil, fmt.Errorf("creating HTTP index store: %w", err)
		}
		return s, nil
	case "s3+http", "s3+https":
		s3Creds, region := cfg.GetS3CredentialsFor(loc)
		lookup := minio.BucketLookupAuto
		ls := loc.Query().Get("lookup")
		switch ls {
		case "dns":
			lookup = minio.BucketLookupDNS
		case "path":
			lookup = minio.BucketLookupPath
		case "", "auto":
		default:
			return nil, fmt.Errorf("unknown S3 bucket lookup type: %q", ls)
		}
		s, err := desync.NewS3IndexStore(loc, s3Creds, region, opt, lookup)
		if err != nil {
			return nil, fmt.Errorf("creating S3 index store: %w", err)
		}
		return s, nil
	case "gs":
		s, err := desync.NewGCIndexStore(loc, opt)
		if err != nil {
			return nil, fmt.Errorf("creating GCS index store: %w", err)
		}
		return s, nil
	default:
		s, err := desync.NewLocalIndexStore(location)
		if err != nil {
			return nil, fmt.Errorf("creating local index store: %w", err)
		}
		return s, nil
	}
}
