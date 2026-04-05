package cmdshared

import (
	"fmt"
	"net/url"
	"runtime"
	"strings"

	"github.com/folbricht/desync"
	minio "github.com/minio/minio-go/v6"
)

// parseS3BucketLookup extracts the "lookup" query parameter from an S3 URL
// and returns the corresponding minio BucketLookupType.
func parseS3BucketLookup(loc *url.URL) (minio.BucketLookupType, error) {
	lookup := minio.BucketLookupAuto
	ls := loc.Query().Get("lookup")
	switch ls {
	case "dns":
		lookup = minio.BucketLookupDNS
	case "path":
		lookup = minio.BucketLookupPath
	case "", "auto":
	default:
		return lookup, fmt.Errorf("unknown S3 bucket lookup type: %q", ls)
	}
	return lookup, nil
}

// StoreFromLocation parses a store location and returns an initialized Store.
// cfg is used for per-location store options and S3 credentials lookup.
// cmdOpt CLI overrides are merged on top of config options for the location.
func StoreFromLocation(location string, cfg Config, cmdOpt CmdStoreOptions) (desync.Store, error) {
	configOpt, err := cfg.GetStoreOptionsFor(location)
	if err != nil {
		return nil, err
	}
	opt := cmdOpt.MergedWith(configOpt)

	loc, err := url.Parse(location)
	if err != nil {
		return nil, fmt.Errorf("unable to parse store location %s : %s", location, err)
	}

	var s desync.Store
	switch loc.Scheme {
	case "ssh":
		s, err = desync.NewRemoteSSHStore(loc, opt)
		if err != nil {
			return nil, err
		}
	case "sftp":
		s, err = desync.NewSFTPStore(loc, opt)
		if err != nil {
			return nil, err
		}
	case "http", "https":
		s, err = desync.NewRemoteHTTPStore(loc, opt)
		if err != nil {
			return nil, err
		}
	case "s3+http", "s3+https":
		s3Creds, region := cfg.GetS3CredentialsFor(loc)
		lookup, err := parseS3BucketLookup(loc)
		if err != nil {
			return nil, err
		}
		s, err = desync.NewS3Store(loc, s3Creds, region, opt, lookup)
		if err != nil {
			return nil, err
		}
	case "gs":
		s, err = desync.NewGCStore(loc, opt)
		if err != nil {
			return nil, err
		}
	default:
		local, err := desync.NewLocalStore(location, opt)
		if err != nil {
			return nil, err
		}
		s = local
		// On Windows, it's not safe to operate on files concurrently. Wrap all
		// local stores in a dedup queue that serializes writes to the same chunk.
		if runtime.GOOS == "windows" {
			s = desync.NewWriteDedupQueue(local)
		}
	}
	return s, nil
}

// WritableStore parses a store location and returns an initialized WriteStore.
// Returns an error if the backend does not support writing.
func WritableStore(location string, cfg Config, cmdOpt CmdStoreOptions) (desync.WriteStore, error) {
	s, err := StoreFromLocation(location, cfg, cmdOpt)
	if err != nil {
		return nil, err
	}
	store, ok := s.(desync.WriteStore)
	if !ok {
		return nil, fmt.Errorf("store '%s' does not support writing", location)
	}
	return store, nil
}

// IndexStoreFromLocation parses a store location (treated as a store root, not
// a file path) and returns an initialized IndexStore.
// cfg is used for per-location store options and S3 credentials lookup.
// cmdOpt CLI overrides are merged on top of config options for the location.
func IndexStoreFromLocation(location string, cfg Config, cmdOpt CmdStoreOptions) (desync.IndexStore, error) {
	configOpt, err := cfg.GetStoreOptionsFor(location)
	if err != nil {
		return nil, err
	}
	opt := cmdOpt.MergedWith(configOpt)

	loc, err := url.Parse(location)
	if err != nil {
		return nil, fmt.Errorf("unable to parse store location %s : %s", location, err)
	}

	var s desync.IndexStore
	switch loc.Scheme {
	case "ssh":
		return nil, fmt.Errorf("index storage is not supported by ssh remote stores")
	case "sftp":
		s, err = desync.NewSFTPIndexStore(loc, opt)
		if err != nil {
			return nil, err
		}
	case "http", "https":
		s, err = desync.NewRemoteHTTPIndexStore(loc, opt)
		if err != nil {
			return nil, err
		}
	case "s3+http", "s3+https":
		s3Creds, region := cfg.GetS3CredentialsFor(loc)
		lookup, err := parseS3BucketLookup(loc)
		if err != nil {
			return nil, err
		}
		s, err = desync.NewS3IndexStore(loc, s3Creds, region, opt, lookup)
		if err != nil {
			return nil, err
		}
	case "gs":
		s, err = desync.NewGCIndexStore(loc, opt)
		if err != nil {
			return nil, err
		}
	default:
		s, err = desync.NewLocalIndexStore(location)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

// WritableIndexStore parses a store location and returns an initialized
// IndexWriteStore. Returns an error if the backend does not support writing.
func WritableIndexStore(location string, cfg Config, cmdOpt CmdStoreOptions) (desync.IndexWriteStore, error) {
	s, err := IndexStoreFromLocation(location, cfg, cmdOpt)
	if err != nil {
		return nil, err
	}
	store, ok := s.(desync.IndexWriteStore)
	if !ok {
		return nil, fmt.Errorf("index store '%s' does not support writing", location)
	}
	return store, nil
}

// storeGroup opens a single store-location string. If the string contains "|",
// each member is opened individually and wrapped in a FailoverGroup.
func storeGroup(location string, cfg Config, cmdOpt CmdStoreOptions) (desync.Store, error) {
	if !strings.ContainsAny(location, "|") {
		return StoreFromLocation(location, cfg, cmdOpt)
	}
	var stores []desync.Store
	for m := range strings.SplitSeq(location, "|") {
		s, err := StoreFromLocation(m, cfg, cmdOpt)
		if err != nil {
			return nil, err
		}
		stores = append(stores, s)
	}
	return desync.NewFailoverGroup(stores...), nil
}

// MultiStoreWithRouter opens each store location and returns a StoreRouter over
// them. Each location may contain "|" to form a FailoverGroup.
func MultiStoreWithRouter(cfg Config, cmdOpt CmdStoreOptions, storeLocations ...string) (desync.Store, error) {
	var stores []desync.Store
	for _, location := range storeLocations {
		s, err := storeGroup(location, cfg, cmdOpt)
		if err != nil {
			return nil, err
		}
		stores = append(stores, s)
	}
	return desync.NewStoreRouter(stores...), nil
}

// MultiStoreWithCache combines store locations into a router and optionally
// wraps it with a local cache. cmdOpt is used for all store locations including
// the cache (each gets its own per-location config options merged on top).
// Each store location may contain "|" to form a FailoverGroup.
func MultiStoreWithCache(cfg Config, cmdOpt CmdStoreOptions, cacheLocation string, storeLocations ...string) (desync.Store, error) {
	cacheLocation = cfg.ResolveCache(cacheLocation)
	store, err := MultiStoreWithRouter(cfg, cmdOpt, storeLocations...)
	if err != nil {
		return nil, err
	}
	if cacheLocation == "" {
		return store, nil
	}
	cache, err := WritableStore(cacheLocation, cfg, cmdOpt)
	if err != nil {
		store.Close()
		return nil, err
	}
	// Wrap the local cache store in a SizeLimitStore if a size or file limit is configured.
	maxSizeStr := cfg.ResolveCacheMaxSize(cmdOpt.CacheMaxSize)
	maxFiles := cfg.ResolveCacheMaxFiles(cmdOpt.CacheMaxFiles)
	var maxSize int64
	if maxSizeStr != "" {
		var err error
		maxSize, err = ParseByteSize(maxSizeStr)
		if err != nil {
			store.Close()
			cache.Close()
			return nil, fmt.Errorf("invalid cache-max-size: %w", err)
		}
	}
	if maxSize > 0 || maxFiles > 0 {
		partitions := cfg.ResolveCachePartitions(cmdOpt.CachePartitions)
		var ls desync.LocalStore
		switch c := cache.(type) {
		case desync.LocalStore:
			ls = c
		case *desync.WriteDedupQueue:
			if inner, ok := c.S.(desync.LocalStore); ok {
				ls = inner
			}
		}
		if ls.Base != "" {
			sls, err := desync.NewSizeLimitStore(ls, maxSize, maxFiles, partitions)
			if err != nil {
				store.Close()
				cache.Close()
				return nil, fmt.Errorf("init size-limited cache: %w", err)
			}
			switch c := cache.(type) {
			case desync.LocalStore:
				cache = sls
			case *desync.WriteDedupQueue:
				c.S = sls
			}
		}
	}
	var cacheLayer desync.WriteStore = cache
	if cmdOpt.CacheRepair {
		cacheLayer = desync.NewRepairableCache(cache)
	}
	return desync.NewCache(store, cacheLayer), nil
}
