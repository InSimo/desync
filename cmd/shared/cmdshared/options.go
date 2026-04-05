package cmdshared

import (
	"errors"
	"time"

	"github.com/folbricht/desync"
	"github.com/spf13/pflag"
)

// CmdStoreOptions holds command-line overrides for store initialization.
// These generally override settings read from the config file.
type CmdStoreOptions struct {
	N                      int
	ConnPoolSize           int // overrides N for S3 HTTP connection pool; 0 = use N
	ClientCert             string
	ClientKey              string
	CACert                 string
	SkipVerify             bool
	TrustInsecure          bool
	CacheRepair            bool
	CacheMaxSize           string
	CacheMaxFiles          int64
	CachePartitions        int
	ErrorRetry             int
	ErrorRetryBaseInterval time.Duration
	SafePruning            bool
	SafePropagationTime    time.Duration
	pflag.FlagSet
}

// MergedWith takes store options as read from the config, applies command-line
// provided options on top, and returns the merged result.
// Safe to call on a zero-value CmdStoreOptions (unregistered FlagSet).
func (o CmdStoreOptions) MergedWith(opt desync.StoreOptions) desync.StoreOptions {
	opt.N = o.N
	opt.ConnPoolSize = o.ConnPoolSize

	if f := o.FlagSet.Lookup("client-cert"); f != nil && f.Changed {
		opt.ClientCert = o.ClientCert
	}
	if f := o.FlagSet.Lookup("client-key"); f != nil && f.Changed {
		opt.ClientKey = o.ClientKey
	}
	if f := o.FlagSet.Lookup("ca-cert"); f != nil && f.Changed {
		opt.CACert = o.CACert
	}
	if o.SkipVerify {
		opt.SkipVerify = true
	}
	if f := o.FlagSet.Lookup("trust-insecure"); f != nil && f.Changed {
		opt.TrustInsecure = true
	}
	if f := o.FlagSet.Lookup("error-retry"); f != nil && f.Changed {
		opt.ErrorRetry = o.ErrorRetry
	}
	if f := o.FlagSet.Lookup("error-retry-base-interval"); f != nil && f.Changed {
		opt.ErrorRetryBaseInterval = o.ErrorRetryBaseInterval
	}
	if f := o.FlagSet.Lookup("safe-pruning"); f != nil && f.Changed {
		opt.SafePruning = true
	}
	if f := o.FlagSet.Lookup("safe-propagation-time"); f != nil && f.Changed {
		opt.SafePropagationTime = o.SafePropagationTime
	}
	return opt
}

// Validate checks that the command-line options are sensible.
func (o CmdStoreOptions) Validate() error {
	if (o.ClientKey == "") != (o.ClientCert == "") {
		return errors.New("--client-key and --client-cert options need to be provided together")
	}
	return nil
}

// AddStoreOptions registers common store option flags on f and links them to o.
func AddStoreOptions(o *CmdStoreOptions, f *pflag.FlagSet) {
	f.IntVarP(&o.N, "concurrency", "n", 10, "number of concurrent goroutines")
	f.StringVar(&o.ClientCert, "client-cert", "", "path to client certificate for TLS authentication")
	f.StringVar(&o.ClientKey, "client-key", "", "path to client key for TLS authentication")
	f.StringVar(&o.CACert, "ca-cert", "", "trust authorities in this file, instead of OS trust store")
	f.BoolVarP(&o.TrustInsecure, "trust-insecure", "t", false, "trust invalid certificates")
	f.BoolVarP(&o.CacheRepair, "cache-repair", "r", true, "replace invalid chunks in the cache from source")
	f.StringVar(&o.CacheMaxSize, "cache-max-size", "", "maximum cache size (e.g. '10G', '500M', 0=unlimited)")
	f.Int64Var(&o.CacheMaxFiles, "cache-max-files", 0, "maximum number of cached files (0=unlimited)")
	f.IntVar(&o.CachePartitions, "cache-partitions", 0, "number of cache eviction partitions, power of 2 (default 256)")
	f.IntVarP(&o.ErrorRetry, "error-retry", "e", desync.DefaultErrorRetry, "number of times to retry in case of network error")
	f.DurationVarP(&o.ErrorRetryBaseInterval, "error-retry-base-interval", "b", desync.DefaultErrorRetryBaseInterval, "initial retry delay, increases linearly with each subsequent attempt")
	f.BoolVar(&o.SafePruning, "safe-pruning", false, "enable safe concurrent pruning protocol (see doc/safe-pruning.md)")
	f.DurationVar(&o.SafePropagationTime, "safe-propagation-time", desync.DefaultSafePropagationTime, "max store write propagation delay for safe-pruning protocol")
	f.IntVar(&o.ConnPoolSize, "conn-pool-size", 0, "S3/HTTP connection pool size per store (default: same as concurrency)")

	o.FlagSet = *f
}
