package main

import (
	"os"
	"path/filepath"
	"time"

	"github.com/go-ini/ini"
	"github.com/minio/minio-go/v6/pkg/credentials"
	"github.com/pkg/errors"
)

// StaticCredentialsProvider implements credentials.Provider.
type StaticCredentialsProvider struct {
	creds credentials.Value
}

func (cp *StaticCredentialsProvider) IsExpired() bool { return false }

func (cp *StaticCredentialsProvider) Retrieve() (credentials.Value, error) {
	return cp.creds, nil
}

// NewStaticCredentials initializes a new set of S3 credentials.
func NewStaticCredentials(accessKey, secretKey, sessionToken string) *credentials.Credentials {
	p := &StaticCredentialsProvider{
		credentials.Value{
			AccessKeyID:     accessKey,
			SecretAccessKey: secretKey,
			SessionToken:    sessionToken,
		},
	}
	return credentials.New(p)
}

// RefreshableSharedCredentialsProvider reads credentials from an AWS credentials
// file and refreshes them periodically.
type RefreshableSharedCredentialsProvider struct {
	Filename string
	Profile  string
	exp      time.Time
	now      func() time.Time
}

// NewRefreshableSharedCredentials returns credentials backed by a shared credentials file.
func NewRefreshableSharedCredentials(filename, profile string, now func() time.Time) *credentials.Credentials {
	return credentials.New(&RefreshableSharedCredentialsProvider{
		Filename: filename,
		Profile:  profile,
		exp:      now().Add(time.Minute),
		now:      now,
	})
}

func (p *RefreshableSharedCredentialsProvider) IsExpired() bool {
	return p.now().After(p.exp)
}

func (p *RefreshableSharedCredentialsProvider) Retrieve() (credentials.Value, error) {
	filename, err := p.filename()
	if err != nil {
		return credentials.Value{}, err
	}
	creds, err := loadProfile(filename, p.profile())
	if err != nil {
		return credentials.Value{}, err
	}
	p.exp = p.now().Add(time.Minute)
	return creds, nil
}

func (p *RefreshableSharedCredentialsProvider) filename() (string, error) {
	if len(p.Filename) != 0 {
		return p.Filename, nil
	}
	if p.Filename = os.Getenv("AWS_SHARED_CREDENTIALS_FILE"); len(p.Filename) != 0 {
		return p.Filename, nil
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", errors.Wrap(err, "user home directory not found")
	}
	p.Filename = filepath.Join(homeDir, ".aws", "credentials")
	return p.Filename, nil
}

func (p *RefreshableSharedCredentialsProvider) profile() string {
	if p.Profile == "" {
		p.Profile = os.Getenv("AWS_PROFILE")
	}
	if p.Profile == "" {
		p.Profile = "default"
	}
	return p.Profile
}

func loadProfile(filename, profile string) (credentials.Value, error) {
	config, err := ini.Load(filename)
	if err != nil {
		return credentials.Value{}, errors.Wrap(err, "failed to load shared credentials file")
	}
	iniProfile, err := config.GetSection(profile)
	if err != nil {
		return credentials.Value{}, errors.Wrap(err, "failed to get profile")
	}
	id, err := iniProfile.GetKey("aws_access_key_id")
	if err != nil {
		return credentials.Value{}, errors.Wrapf(err, "shared credentials %s in %s did not contain aws_access_key_id", profile, filename)
	}
	secret, err := iniProfile.GetKey("aws_secret_access_key")
	if err != nil {
		return credentials.Value{}, errors.Wrapf(err, "shared credentials %s in %s did not contain aws_secret_access_key", profile, filename)
	}
	token := iniProfile.Key("aws_session_token")
	return credentials.Value{
		AccessKeyID:     id.String(),
		SecretAccessKey: secret.String(),
		SessionToken:    token.String(),
	}, nil
}
