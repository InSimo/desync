package cmdshared

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/folbricht/desync"
	"github.com/minio/minio-go/v6/pkg/credentials"
	"github.com/pkg/errors"
)


// S3Creds holds credentials or references to an S3 credentials file.
type S3Creds struct {
	AccessKey          string `json:"access-key,omitempty"`
	SecretKey          string `json:"secret-key,omitempty"`
	AwsCredentialsFile string `json:"aws-credentials-file,omitempty"`
	AwsProfile         string `json:"aws-profile,omitempty"`
	// Having an explicit aws region makes minio slightly faster because it avoids url parsing
	AwsRegion string `json:"aws-region,omitempty"`
}

// DefaultChunkSize is the built-in min:avg:max chunk size used when neither
// the CLI flag nor the config file specifies one.
const DefaultChunkSize = "16:64:256"

// Defaults holds config-file defaults for CLI flags that users often want to
// set once rather than on every invocation.
type Defaults struct {
	Digest      string   `json:"digest,omitempty"`
	Stores      []string `json:"stores,omitempty"`
	IndexStore  string   `json:"index-store,omitempty"`
	ChunkSize   string   `json:"chunk-size,omitempty"`
	Cache       string   `json:"cache,omitempty"`
	Concurrency int      `json:"concurrency,omitempty"`
	MaxInFlight int64    `json:"max-in-flight,omitempty"`
}

// Config is used to hold the global tool configuration. It's used to customize
// store features and provide credentials where needed.
type Config struct {
	S3Credentials map[string]S3Creds             `json:"s3-credentials"`
	StoreOptions  map[string]desync.StoreOptions `json:"store-options"`
	Defaults      Defaults                       `json:"defaults,omitempty"`
	// DesyncLFS, when explicitly set to false in desync-lfs.json, activates the
	// escape hatch: git-lfs-transfer delegates to the binary named by the
	// "desync-lfs.transfer.exec" git config key instead of handling the request.
	DesyncLFS *bool `json:"desync-lfs,omitempty"`
}

// GetS3CredentialsFor attempts to find creds and region for an S3 location in the
// config and the environment (which takes precedence). Returns a minio credentials
// struct and region string. If not found, the creds struct will return "" when invoked.
// Uses the scheme, host and port which need to match what's in the config file.
func (c Config) GetS3CredentialsFor(u *url.URL) (*credentials.Credentials, string) {
	// See if creds are defined in the ENV, if so, they take precedence
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

	// Look in the config to find a match for scheme+host
	key := &url.URL{
		Scheme: strings.TrimPrefix(u.Scheme, "s3+"),
		Host:   u.Host,
	}
	credsConfig := c.S3Credentials[key.String()]
	creds := NewStaticCredentials("", "", "")
	region = credsConfig.AwsRegion

	// if access access-key is present, it takes precedence
	if credsConfig.AccessKey != "" {
		creds = NewStaticCredentials(credsConfig.AccessKey, credsConfig.SecretKey, "")
	} else if credsConfig.AwsCredentialsFile != "" {
		creds = NewRefreshableSharedCredentials(credsConfig.AwsCredentialsFile, credsConfig.AwsProfile, time.Now)
	}
	return creds, region
}

// GetStoreOptionsFor returns optional config options for a specific store. Note that
// an error will be returned if the location string matches multiple entries in the
// config file.
func (c Config) GetStoreOptionsFor(location string) (options desync.StoreOptions, err error) {
	found := false
	options = desync.NewStoreOptionsWithDefaults()
	for k, v := range c.StoreOptions {
		if locationMatch(k, location) {
			if found {
				return options, fmt.Errorf("multiple configuration entries match the location %q", location)
			}
			found = true
			options = v
		}
	}
	return options, nil
}

// ResolveDigest returns cli if non-empty, otherwise c.Defaults.Digest.
// An empty return value is valid and causes SetDigestAlgorithm to select the
// built-in default (SHA512-256).
func (c Config) ResolveDigest(cli string) string {
	if cli != "" {
		return cli
	}
	return c.Defaults.Digest
}

// ResolveStores returns cli if non-nil/non-empty, otherwise c.Defaults.Stores.
func (c Config) ResolveStores(cli []string) []string {
	if len(cli) > 0 {
		return cli
	}
	return c.Defaults.Stores
}

// ResolveStore returns cli if non-empty, otherwise the first entry of
// c.Defaults.Stores. Returns "" when both are empty; callers report the error.
func (c Config) ResolveStore(cli string) string {
	if cli != "" {
		return cli
	}
	if len(c.Defaults.Stores) > 0 {
		return c.Defaults.Stores[0]
	}
	return ""
}

// ResolveIndexStore returns cli if non-empty, otherwise c.Defaults.IndexStore.
func (c Config) ResolveIndexStore(cli string) string {
	if cli != "" {
		return cli
	}
	return c.Defaults.IndexStore
}

// ResolveCache returns cli if non-empty, otherwise c.Defaults.Cache.
// An empty return value means no cache is configured.
func (c Config) ResolveCache(cli string) string {
	if cli != "" {
		return cli
	}
	return c.Defaults.Cache
}

// ResolveChunkSize returns cli if non-empty, otherwise c.Defaults.ChunkSize,
// otherwise DefaultChunkSize. The return value is always a valid non-empty
// min:avg:max string suitable for passing to ParseChunkSizeParam.
func (c Config) ResolveChunkSize(cli string) string {
	if cli != "" {
		return cli
	}
	if c.Defaults.ChunkSize != "" {
		return c.Defaults.ChunkSize
	}
	return DefaultChunkSize
}

// ResolveMaxInFlight returns cli if non-zero, otherwise c.Defaults.MaxInFlight,
// otherwise the DESYNC_MAX_INFLIGHT environment variable (parsed as bytes),
// otherwise 0 (disabled).
func (c Config) ResolveMaxInFlight(cli int64) int64 {
	if cli != 0 {
		return cli
	}
	if c.Defaults.MaxInFlight != 0 {
		return c.Defaults.MaxInFlight
	}
	if env := os.Getenv("DESYNC_MAX_INFLIGHT"); env != "" {
		if v, err := strconv.ParseInt(env, 10, 64); err == nil {
			return v
		}
	}
	return 0
}

// SetDigestAlgorithm sets the global desync.Digest to the algorithm named by
// algorithm. Valid values are "" or "sha512-256" (the default) and "sha256".
// Returns an error for any other value.
func SetDigestAlgorithm(algorithm string) error {
	switch algorithm {
	case "", "sha512-256":
		desync.Digest = desync.SHA512256{}
	case "sha256":
		desync.Digest = desync.SHA256{}
	default:
		return fmt.Errorf("invalid digest algorithm '%s'", algorithm)
	}
	return nil
}

// LoadConfigFromGitObject reads desync config JSON from a git object via
// `git cat-file --textconv <objectRef>`. When repoDir is non-empty the command
// is run with `-C repoDir`; an empty repoDir means use the process working
// directory (suitable for git-lfs-desync which runs inside the user's repo).
//
// Returns (cfg, false, nil) when git exits non-zero (object absent or not a
// git repo), so the caller can fall back gracefully.
// A real error (exec failure, JSON parse error) is returned as err.
func LoadConfigFromGitObject(repoDir, objectRef string) (Config, bool, error) {
	args := []string{"cat-file", "--textconv", objectRef}
	if repoDir != "" {
		args = append([]string{"-C", repoDir}, args...)
	}
	out, err := exec.Command("git", args...).Output()
	if err != nil {
		return Config{}, false, nil // object not found; caller falls back
	}
	cfg, err := LoadConfigFromReader(bytes.NewReader(out))
	if err != nil {
		return Config{}, true, fmt.Errorf("config object %q: %w", objectRef, err)
	}
	return cfg, true, nil
}

// LoadConfigFromReader JSON-decodes a Config from r.
func LoadConfigFromReader(r io.Reader) (Config, error) {
	var cfg Config
	if err := json.NewDecoder(r).Decode(&cfg); err != nil {
		return cfg, errors.Wrap(err, "decoding config")
	}
	return cfg, nil
}

// LoadConfig resolves the config file path (using the platform default if cfgFile
// is empty), silently returns an empty Config if the default path doesn't exist,
// and JSON-decodes the file otherwise. Returns the resolved path and any error.
func LoadConfig(cfgFile string) (Config, string, error) {
	var cfg Config
	defaultLocation := cfgFile == ""
	if defaultLocation {
		switch runtime.GOOS {
		case "windows":
			cfgFile = filepath.Join(os.Getenv("HOMEDRIVE")+os.Getenv("HOMEPATH"), ".config", "desync", "config.json")
		default:
			cfgFile = filepath.Join(os.Getenv("HOME"), ".config", "desync", "config.json")
		}
	}
	if _, err := os.Stat(cfgFile); os.IsNotExist(err) {
		if defaultLocation {
			return cfg, cfgFile, nil
		}
		return cfg, cfgFile, err
	}
	f, err := os.Open(cfgFile)
	if err != nil {
		return cfg, cfgFile, err
	}
	defer f.Close()
	if err = json.NewDecoder(f).Decode(&cfg); err != nil {
		return cfg, cfgFile, errors.Wrap(err, "reading "+cfgFile)
	}
	return cfg, cfgFile, nil
}
