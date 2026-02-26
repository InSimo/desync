package main

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/folbricht/desync"
	"github.com/folbricht/desync/cmd/internal/desyncconfig"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var cfg desyncconfig.Config
var cfgFile string
var cfgFromGit string
var digestAlgorithm string

func initConfig() error {
	if cfgFile != "" && cfgFromGit != "" {
		return fmt.Errorf("--config and --config-from-git are mutually exclusive")
	}
	if cfgFromGit != "" {
		out, err := exec.Command("git", "cat-file", "--text-conv", cfgFromGit).Output()
		if err != nil {
			return fmt.Errorf("reading config from git object %q: %w", cfgFromGit, err)
		}
		cfg, err = desyncconfig.LoadConfigFromReader(bytes.NewReader(out))
		return err
	}
	var err error
	cfg, cfgFile, err = desyncconfig.LoadConfig(cfgFile)
	return err
}

// deriveIndexURL derives an index store location from a chunk store location.
// For URLs (scheme length > 1): replaces the last path segment with "index/".
//   e.g. s3+https://host/bucket/chunks/ → s3+https://host/bucket/index/
// For plain filesystem paths: returns a sibling "index" directory.
//   e.g. /path/to/chunks → /path/to/index,  chunks → index
func deriveIndexURL(storeURL string) (string, error) {
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

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()

	var (
		storeURL            string
		indexURL            string
		cache               string
		cacheRepair         bool
		concurrency         int
		chunkSize           string
		errorRetry          int
		clientCert          string
		clientKey           string
		caCert              string
		trustInsecure       bool
		errorRetryInterval  time.Duration
	)

	cmd := &cobra.Command{
		Use:   "git-lfs-desync",
		Short: "Git LFS custom transfer agent using desync chunking",
		Long: `git-lfs-desync is a Git LFS custom transfer agent that chunks large files
using content-defined chunking (SipHash rolling hash), stores deduplicated
compressed chunks in a desync store, and stores resulting indexes keyed by
LFS OID. Supported store backends: local filesystem, S3-compatible, SFTP,
HTTP/HTTPS, and Google Cloud Storage.

Configure Git LFS to use this agent:

  [lfs "customtransfer.desync"]
    path = /usr/local/bin/git-lfs-desync
    args = --store s3+https://s3.amazonaws.com/my-bucket/lfs/chunks/
    concurrent = true
    concurrenttransfers = 5

  [lfs]
    standalonetransferagent = desync`,
		SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if err := initConfig(); err != nil {
				return err
			}
			return desyncconfig.SetDigestAlgorithm(cfg.ResolveDigest(digestAlgorithm))
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			storeURL = cfg.ResolveStore(storeURL)
			indexURL = cfg.ResolveIndexStore(indexURL)
			if storeURL == "" {
				return fmt.Errorf("--store is required")
			}

			opt, err := cfg.GetStoreOptionsFor(storeURL)
			if err != nil {
				return err
			}
			opt.N = concurrency
			opt.ErrorRetry = errorRetry
			if cmd.Flags().Changed("client-cert") {
				opt.ClientCert = clientCert
			}
			if cmd.Flags().Changed("client-key") {
				opt.ClientKey = clientKey
			}
			if cmd.Flags().Changed("ca-cert") {
				opt.CACert = caCert
			}
			if cmd.Flags().Changed("trust-insecure") {
				opt.TrustInsecure = trustInsecure
			}
			if cmd.Flags().Changed("error-retry-base-interval") {
				opt.ErrorRetryBaseInterval = errorRetryInterval
			}

			chunkStore, err := chunkStoreFromURL(storeURL, opt)
			if err != nil {
				return err
			}

			// Build the read store used for downloads, optionally wrapping the
			// chunk store with a local cache tier. readStore.Close() closes the
			// full chain (chunkStore and, if present, the cache store).
			readStore, err := buildReadStore(cmd, chunkStore, cache, cacheRepair,
				concurrency, errorRetry, clientCert, clientKey, caCert, trustInsecure, errorRetryInterval)
			if err != nil {
				chunkStore.Close()
				return err
			}
			defer readStore.Close()

			if indexURL == "" {
				indexURL, err = deriveIndexURL(storeURL)
				if err != nil {
					return err
				}
			}

			idxOpt, err := cfg.GetStoreOptionsFor(indexURL)
			if err != nil {
				return err
			}
			idxOpt.N = concurrency
			idxOpt.ErrorRetry = errorRetry
			if cmd.Flags().Changed("client-cert") {
				idxOpt.ClientCert = clientCert
			}
			if cmd.Flags().Changed("client-key") {
				idxOpt.ClientKey = clientKey
			}
			if cmd.Flags().Changed("ca-cert") {
				idxOpt.CACert = caCert
			}
			if cmd.Flags().Changed("trust-insecure") {
				idxOpt.TrustInsecure = trustInsecure
			}
			if cmd.Flags().Changed("error-retry-base-interval") {
				idxOpt.ErrorRetryBaseInterval = errorRetryInterval
			}

			indexStore, err := indexStoreFromURL(indexURL, idxOpt)
			if err != nil {
				return err
			}
			defer indexStore.Close()

			minChunk, avgChunk, maxChunk, err := parseChunkSizeParam(chunkSize)
			if err != nil {
				return err
			}

			agent := &Agent{
				writeStore:      chunkStore,
				readStore:       readStore,
				indexWriteStore: indexStore,
				n:               concurrency,
				minChunk:        minChunk,
				avgChunk:        avgChunk,
				maxChunk:        maxChunk,
				tmpDir:          os.TempDir(),
			}

			return agent.Run(ctx)
		},
	}

	flags := cmd.Flags()
	flags.StringVarP(&storeURL, "store", "s", "",
		"chunk store location; supports s3+https://, sftp://, https://, gs://, or local path (may be set via config defaults)")
	flags.StringVar(&indexURL, "index-store", "",
		"index store location (default: sibling 'index' directory of --store); same schemes as --store")
	flags.StringVarP(&cache, "cache", "c", "",
		"local chunk store used as download cache; chunks missing from the cache are fetched from --store and saved locally")
	flags.BoolVar(&cacheRepair, "cache-repair", true,
		"replace corrupt chunks in the cache by re-downloading them from --store")
	flags.IntVarP(&concurrency, "concurrency", "n", 10, "number of concurrent goroutines")
	flags.StringVarP(&chunkSize, "chunk-size", "m", "16:64:256", "min:avg:max chunk size in KB")
	flags.IntVarP(&errorRetry, "error-retry", "e", desync.DefaultErrorRetry, "number of times to retry on network error")
	flags.StringVar(&clientCert, "client-cert", "", "path to client certificate for TLS authentication")
	flags.StringVar(&clientKey, "client-key", "", "path to client key for TLS authentication")
	flags.StringVar(&caCert, "ca-cert", "", "CA certificate file to trust instead of OS trust store")
	flags.BoolVarP(&trustInsecure, "trust-insecure", "t", false, "trust invalid certificates")
	flags.DurationVarP(&errorRetryInterval, "error-retry-base-interval", "b",
		desync.DefaultErrorRetryBaseInterval, "initial retry delay, increases linearly with each attempt")
	flags.StringVar(&cfgFile, "config", "", "desync config file (default: $HOME/.config/desync/config.json)")
	flags.StringVar(&cfgFromGit, "config-from-git", "",
		"read desync config from a git object (e.g. origin/_desync:config.json)")
	flags.StringVar(&digestAlgorithm, "digest", "", "digest algorithm, sha512-256 or sha256 (default sha512-256)")

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func parseChunkSizeParam(s string) (min, avg, max uint64, err error) {
	sizes := strings.Split(s, ":")
	if len(sizes) != 3 {
		return 0, 0, 0, fmt.Errorf("invalid chunk size %q, expected min:avg:max", s)
	}
	parseInt := func(str, label string) (uint64, error) {
		var n int
		_, scanErr := fmt.Sscan(str, &n)
		if scanErr != nil {
			return 0, errors.Wrap(scanErr, label+" chunk size")
		}
		return uint64(n) * 1024, nil
	}
	if min, err = parseInt(sizes[0], "min"); err != nil {
		return
	}
	if avg, err = parseInt(sizes[1], "avg"); err != nil {
		return
	}
	if max, err = parseInt(sizes[2], "max"); err != nil {
		return
	}
	return
}
