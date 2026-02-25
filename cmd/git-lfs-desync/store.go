package main

import (
	"fmt"
	"net/url"
	"runtime"
	"time"

	"github.com/folbricht/desync"
	minio "github.com/minio/minio-go/v6"
	"github.com/spf13/cobra"
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

// buildReadStore returns the store used for chunk reads during downloads.
// If cacheLocation is empty, it returns chunkStore directly. Otherwise it
// creates a cache store from cacheLocation and wraps chunkStore in a
// desync.Cache so that downloads hit the local cache before the remote store.
// TLS/retry flag values are forwarded to the cache store options unchanged;
// the cmd parameter is used to detect which flags were explicitly passed.
func buildReadStore(
	cmd *cobra.Command,
	chunkStore desync.WriteStore,
	cacheLocation string,
	cacheRepair bool,
	concurrency int,
	errorRetry int,
	clientCert, clientKey, caCert string,
	trustInsecure bool,
	errorRetryInterval time.Duration,
) (desync.Store, error) {
	if cacheLocation == "" {
		return chunkStore, nil
	}

	cacheOpt, err := cfg.GetStoreOptionsFor(cacheLocation)
	if err != nil {
		return nil, err
	}
	cacheOpt.N = concurrency
	cacheOpt.ErrorRetry = errorRetry
	if cmd.Flags().Changed("client-cert") {
		cacheOpt.ClientCert = clientCert
	}
	if cmd.Flags().Changed("client-key") {
		cacheOpt.ClientKey = clientKey
	}
	if cmd.Flags().Changed("ca-cert") {
		cacheOpt.CACert = caCert
	}
	if cmd.Flags().Changed("trust-insecure") {
		cacheOpt.TrustInsecure = trustInsecure
	}
	if cmd.Flags().Changed("error-retry-base-interval") {
		cacheOpt.ErrorRetryBaseInterval = errorRetryInterval
	}

	cacheStore, err := chunkStoreFromURL(cacheLocation, cacheOpt)
	if err != nil {
		return nil, fmt.Errorf("creating cache store: %w", err)
	}

	var cacheLayer desync.WriteStore = cacheStore
	if cacheRepair {
		cacheLayer = desync.NewRepairableCache(cacheStore)
	}
	return desync.NewCache(chunkStore, cacheLayer), nil
}
