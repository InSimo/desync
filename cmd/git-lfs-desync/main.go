package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/folbricht/desync"
	"github.com/minio/minio-go/v6/pkg/credentials"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// S3Creds holds credentials or references to an S3 credentials file.
type S3Creds struct {
	AccessKey          string `json:"access-key,omitempty"`
	SecretKey          string `json:"secret-key,omitempty"`
	AwsCredentialsFile string `json:"aws-credentials-file,omitempty"`
	AwsProfile         string `json:"aws-profile,omitempty"`
	AwsRegion          string `json:"aws-region,omitempty"`
}

// Config holds the global tool configuration.
type Config struct {
	S3Credentials map[string]S3Creds             `json:"s3-credentials"`
	StoreOptions  map[string]desync.StoreOptions `json:"store-options"`
}

// GetS3CredentialsFor attempts to find creds and region for an S3 location.
// Environment variables take precedence over config file values.
func (c Config) GetS3CredentialsFor(u *url.URL) (*credentials.Credentials, string) {
	accessKey := os.Getenv("S3_ACCESS_KEY")
	region := os.Getenv("S3_REGION")
	secretKey := os.Getenv("S3_SECRET_KEY")
	sessionToken := os.Getenv("S3_SESSION_TOKEN")
	if accessKey == "" && secretKey == "" {
		accessKey = os.Getenv("AWS_ACCESS_KEY_ID")
		secretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
		sessionToken = os.Getenv("AWS_SESSION_TOKEN")
	}
	if accessKey != "" || secretKey != "" {
		return NewStaticCredentials(accessKey, secretKey, sessionToken), region
	}

	key := &url.URL{
		Scheme: strings.TrimPrefix(u.Scheme, "s3+"),
		Host:   u.Host,
	}
	credsConfig := c.S3Credentials[key.String()]
	creds := NewStaticCredentials("", "", "")
	region = credsConfig.AwsRegion

	if credsConfig.AccessKey != "" {
		creds = NewStaticCredentials(credsConfig.AccessKey, credsConfig.SecretKey, "")
	} else if credsConfig.AwsCredentialsFile != "" {
		creds = NewRefreshableSharedCredentials(credsConfig.AwsCredentialsFile, credsConfig.AwsProfile, time.Now)
	}
	return creds, region
}

// GetStoreOptionsFor returns optional config options for a specific store.
// Returns an error if more than one config entry matches the location.
func (c Config) GetStoreOptionsFor(location string) (options desync.StoreOptions, err error) {
	options = desync.NewStoreOptionsWithDefaults()
	found := false
	for k, v := range c.StoreOptions {
		if locationMatch(k, location) {
			if found {
				return options, fmt.Errorf("multiple configuration entries match %q", location)
			}
			found = true
			options = v
		}
	}
	return options, nil
}

// locationMatch returns true if the pattern matches the location string.
func locationMatch(pattern, loc string) bool {
	l, err := url.Parse(loc)
	if err != nil {
		return false
	}
	if len(l.Scheme) > 1 {
		trimmedLoc := strings.TrimSuffix(loc, "/")
		trimmedPattern := strings.TrimSuffix(pattern, "/")
		m, _ := filepath.Match(trimmedPattern, trimmedLoc)
		return m
	}
	p1, err := filepath.Abs(pattern)
	if err != nil {
		return false
	}
	p2, err := filepath.Abs(loc)
	if err != nil {
		return false
	}
	m, _ := filepath.Match(p1, p2)
	return m
}

var cfg Config
var cfgFile string

func initConfig() error {
	if cfgFile == "" {
		switch runtime.GOOS {
		case "windows":
			cfgFile = filepath.Join(os.Getenv("HOMEDRIVE")+os.Getenv("HOMEPATH"), ".config", "desync", "config.json")
		default:
			cfgFile = filepath.Join(os.Getenv("HOME"), ".config", "desync", "config.json")
		}
		if _, err := os.Stat(cfgFile); os.IsNotExist(err) {
			return nil
		}
	}
	f, err := os.Open(cfgFile)
	if err != nil {
		return err
	}
	defer f.Close()
	if err = json.NewDecoder(f).Decode(&cfg); err != nil {
		return errors.Wrap(err, "reading "+cfgFile)
	}
	return nil
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
	if len(u.Scheme) <= 1 {
		return filepath.Join(filepath.Dir(storeURL), "index"), nil
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
    concurrent = false

  [lfs]
    standalonetransferagent = desync`,
		SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return initConfig()
		},
		RunE: func(cmd *cobra.Command, args []string) error {
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
			defer chunkStore.Close()

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
		"chunk store location (required); supports s3+https://, sftp://, https://, gs://, or local path")
	flags.StringVar(&indexURL, "index-store", "",
		"index store location (default: sibling 'index' directory of --store); same schemes as --store")
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
