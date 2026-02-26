package desyncconfig

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
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

// Config is used to hold the global tool configuration. It's used to customize
// store features and provide credentials where needed.
type Config struct {
	S3Credentials map[string]S3Creds             `json:"s3-credentials"`
	StoreOptions  map[string]desync.StoreOptions `json:"store-options"`
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

// locationMatch returns true if the two locations are equal. Locations can be URLs or local file paths.
// It can handle Unix as well as Windows paths. Example
// http://host/path/ is equal http://host/path (no trailing /) and /tmp/path is
// equal \tmp\path on Windows.
func locationMatch(pattern, loc string) bool {
	l, err := url.Parse(loc)
	if err != nil {
		return false
	}

	// See if we have a URL, Windows drive letters come out as single-letter
	// scheme, so we need more here.
	if len(l.Scheme) > 1 {
		// URL paths should only use / as separator, remove the trailing one, if any
		trimmedLoc := strings.TrimSuffix(loc, "/")
		trimmedPattern := strings.TrimSuffix(pattern, "/")
		m, _ := filepath.Match(trimmedPattern, trimmedLoc)
		return m
	}

	// We're dealing with a path.
	p1, err := filepath.Abs(pattern)
	if err != nil {
		return false
	}
	p2, err := filepath.Abs(loc)
	if err != nil {
		return false
	}
	m, err := filepath.Match(p1, p2)
	if err != nil {
		return false
	}
	return m
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
