package cmdshared

import (
	"strings"
	"testing"

	"github.com/folbricht/desync"
)

func TestResolveDigest(t *testing.T) {
	cfg := Config{Defaults: Defaults{Digest: "sha256"}}

	// CLI value wins
	if got := cfg.ResolveDigest("sha512-256"); got != "sha512-256" {
		t.Fatalf("expected sha512-256, got %s", got)
	}
	// Empty CLI falls back to config default
	if got := cfg.ResolveDigest(""); got != "sha256" {
		t.Fatalf("expected sha256, got %s", got)
	}
	// Both empty → empty string (built-in default applied by SetDigestAlgorithm)
	empty := Config{}
	if got := empty.ResolveDigest(""); got != "" {
		t.Fatalf("expected empty, got %s", got)
	}
}

func TestResolveStores(t *testing.T) {
	cfg := Config{Defaults: Defaults{Stores: []string{"/default/store"}}}

	// CLI value wins
	cli := []string{"/cli/store"}
	if got := cfg.ResolveStores(cli); len(got) != 1 || got[0] != "/cli/store" {
		t.Fatalf("unexpected result: %v", got)
	}
	// nil CLI falls back to config default
	if got := cfg.ResolveStores(nil); len(got) != 1 || got[0] != "/default/store" {
		t.Fatalf("unexpected result: %v", got)
	}
	// empty slice CLI falls back to config default
	if got := cfg.ResolveStores([]string{}); len(got) != 1 || got[0] != "/default/store" {
		t.Fatalf("unexpected result: %v", got)
	}
	// Both empty → nil
	empty := Config{}
	if got := empty.ResolveStores(nil); got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestResolveStore(t *testing.T) {
	cfg := Config{Defaults: Defaults{Stores: []string{"/first/store", "/second/store"}}}

	// CLI value wins
	if got := cfg.ResolveStore("/cli/store"); got != "/cli/store" {
		t.Fatalf("expected /cli/store, got %s", got)
	}
	// Empty CLI → first entry of Defaults.Stores
	if got := cfg.ResolveStore(""); got != "/first/store" {
		t.Fatalf("expected /first/store, got %s", got)
	}
	// Both empty → empty string
	empty := Config{}
	if got := empty.ResolveStore(""); got != "" {
		t.Fatalf("expected empty, got %s", got)
	}
}

func TestResolveIndexStore(t *testing.T) {
	cfg := Config{Defaults: Defaults{IndexStore: "/default/index"}}

	// CLI value wins
	if got := cfg.ResolveIndexStore("/cli/index"); got != "/cli/index" {
		t.Fatalf("expected /cli/index, got %s", got)
	}
	// Empty CLI falls back to config default
	if got := cfg.ResolveIndexStore(""); got != "/default/index" {
		t.Fatalf("expected /default/index, got %s", got)
	}
	// Both empty → empty string
	empty := Config{}
	if got := empty.ResolveIndexStore(""); got != "" {
		t.Fatalf("expected empty, got %s", got)
	}
}

func TestResolveChunkSize(t *testing.T) {
	cfg := Config{Defaults: Defaults{ChunkSize: "8:32:128"}}

	// CLI value wins
	if got := cfg.ResolveChunkSize("4:16:64"); got != "4:16:64" {
		t.Fatalf("expected 4:16:64, got %s", got)
	}
	// Empty CLI falls back to config default
	if got := cfg.ResolveChunkSize(""); got != "8:32:128" {
		t.Fatalf("expected 8:32:128, got %s", got)
	}
	// Both empty → built-in default
	empty := Config{}
	if got := empty.ResolveChunkSize(""); got != DefaultChunkSize {
		t.Fatalf("expected %s, got %s", DefaultChunkSize, got)
	}
}

func TestResolveCache(t *testing.T) {
	cfg := Config{Defaults: Defaults{Cache: "/default/cache"}}

	// CLI value wins
	if got := cfg.ResolveCache("/cli/cache"); got != "/cli/cache" {
		t.Fatalf("expected /cli/cache, got %s", got)
	}
	// Empty CLI falls back to config default
	if got := cfg.ResolveCache(""); got != "/default/cache" {
		t.Fatalf("expected /default/cache, got %s", got)
	}
	// Both empty → empty string (no cache)
	empty := Config{}
	if got := empty.ResolveCache(""); got != "" {
		t.Fatalf("expected empty, got %s", got)
	}
}

func TestParseByteSize(t *testing.T) {
	tests := []struct {
		in   string
		want int64
		err  bool
	}{
		{"", 0, false},
		{"0", 0, false},
		{"1234", 1234, false},
		{"1K", 1024, false},
		{"1KB", 1024, false},
		{"1k", 1024, false},
		{"10M", 10 * 1024 * 1024, false},
		{"10MB", 10 * 1024 * 1024, false},
		{"1G", 1024 * 1024 * 1024, false},
		{"1GB", 1024 * 1024 * 1024, false},
		{"2T", 2 * 1024 * 1024 * 1024 * 1024, false},
		{"2TB", 2 * 1024 * 1024 * 1024 * 1024, false},
		{"1.5G", int64(1.5 * 1024 * 1024 * 1024), false},
		{"100B", 100, false},
		{"abc", 0, true},
		{"10X", 0, true},
	}
	for _, tt := range tests {
		got, err := ParseByteSize(tt.in)
		if tt.err {
			if err == nil {
				t.Errorf("ParseByteSize(%q): expected error, got %d", tt.in, got)
			}
		} else {
			if err != nil {
				t.Errorf("ParseByteSize(%q): unexpected error: %v", tt.in, err)
			} else if got != tt.want {
				t.Errorf("ParseByteSize(%q) = %d, want %d", tt.in, got, tt.want)
			}
		}
	}
}

func TestResolveCacheMaxSize(t *testing.T) {
	cfg := Config{Defaults: Defaults{CacheMaxSize: "5G"}}

	// CLI value wins.
	if got := cfg.ResolveCacheMaxSize("10G"); got != "10G" {
		t.Fatalf("expected 10G, got %s", got)
	}
	// Empty CLI falls back to config default.
	if got := cfg.ResolveCacheMaxSize(""); got != "5G" {
		t.Fatalf("expected 5G, got %s", got)
	}
	// Both empty → empty string.
	empty := Config{}
	if got := empty.ResolveCacheMaxSize(""); got != "" {
		t.Fatalf("expected empty, got %s", got)
	}
}

func TestResolveCachePartitions(t *testing.T) {
	cfg := Config{Defaults: Defaults{CachePartitions: 32}}

	// CLI value wins.
	if got := cfg.ResolveCachePartitions(64); got != 64 {
		t.Fatalf("expected 64, got %d", got)
	}
	// Zero CLI falls back to config default.
	if got := cfg.ResolveCachePartitions(0); got != 32 {
		t.Fatalf("expected 32, got %d", got)
	}
	// Both zero → 0 (caller uses default).
	empty := Config{}
	if got := empty.ResolveCachePartitions(0); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
}

func TestMergeServerConfig(t *testing.T) {
	local := Config{
		Defaults: Defaults{
			Cache:       "/local/cache",
			Concurrency: 8,
		},
	}
	server := Config{
		S3Credentials: map[string]S3Creds{
			"https://s3.amazonaws.com": {AccessKey: "AKID", SecretKey: "secret"},
		},
		Defaults: Defaults{
			Stores:     []string{"s3+https://s3.amazonaws.com/bucket/chunks/"},
			IndexStore: "s3+https://s3.amazonaws.com/bucket/index/",
			ChunkSize:  "8:32:128",
			Digest:     "sha256",
			Cache:      "/server/cache", // should be ignored
		},
	}

	MergeServerConfig(&local, &server)

	if len(local.Defaults.Stores) != 1 || local.Defaults.Stores[0] != "s3+https://s3.amazonaws.com/bucket/chunks/" {
		t.Fatalf("stores not merged: %v", local.Defaults.Stores)
	}
	if local.S3Credentials == nil || local.S3Credentials["https://s3.amazonaws.com"].AccessKey != "AKID" {
		t.Fatal("s3 credentials not merged")
	}
	if local.Defaults.ChunkSize != "8:32:128" {
		t.Fatalf("chunk size not overridden: %s", local.Defaults.ChunkSize)
	}
	if local.Defaults.Digest != "sha256" {
		t.Fatalf("digest not overridden: %s", local.Defaults.Digest)
	}
	if local.Defaults.Cache != "/local/cache" {
		t.Fatalf("cache should be preserved: %s", local.Defaults.Cache)
	}
}

func TestMergeServerConfigLocalTakesPrecedence(t *testing.T) {
	local := Config{
		S3Credentials: map[string]S3Creds{
			"https://local.example.com": {AccessKey: "local"},
		},
		Defaults: Defaults{
			Stores: []string{"/local/store"},
		},
	}
	server := Config{
		S3Credentials: map[string]S3Creds{
			"https://server.example.com": {AccessKey: "server"},
		},
		Defaults: Defaults{
			Stores: []string{"s3+https://server/store"},
		},
	}

	MergeServerConfig(&local, &server)

	if local.Defaults.Stores[0] != "/local/store" {
		t.Fatalf("local store should win: %v", local.Defaults.Stores)
	}
	if _, ok := local.S3Credentials["https://server.example.com"]; ok {
		t.Fatal("server creds should not be merged when local has creds")
	}
}

func TestFilterServerConfig(t *testing.T) {
	full := Config{
		S3Credentials: map[string]S3Creds{
			"https://s3.amazonaws.com":  {AccessKey: "AKID", SecretKey: "secret"},
			"https://other.example.com": {AccessKey: "other"},
		},
		StoreOptions: map[string]desync.StoreOptions{
			"s3+https://s3.amazonaws.com/bucket/chunks/": {SafePruning: true},
			"s3+https://other.example.com/other/":        {SkipVerify: true},
		},
	}

	// Without credentials.
	filtered := FilterServerConfig(full,
		"s3+https://s3.amazonaws.com/bucket/chunks/",
		"s3+https://s3.amazonaws.com/bucket/index/",
		"16:64:256", "sha512-256", false)

	if filtered.S3Credentials != nil {
		t.Fatal("credentials should be excluded")
	}
	if filtered.Defaults.ChunkSize != "16:64:256" {
		t.Fatalf("chunk size: %s", filtered.Defaults.ChunkSize)
	}

	// With credentials — only matching entries.
	filtered = FilterServerConfig(full,
		"s3+https://s3.amazonaws.com/bucket/chunks/",
		"s3+https://s3.amazonaws.com/bucket/index/",
		"16:64:256", "", true)

	if _, ok := filtered.S3Credentials["https://s3.amazonaws.com"]; !ok {
		t.Fatal("matching creds should be included")
	}
	if _, ok := filtered.S3Credentials["https://other.example.com"]; ok {
		t.Fatal("non-matching creds should be excluded")
	}
	if _, ok := filtered.StoreOptions["s3+https://s3.amazonaws.com/bucket/chunks/"]; !ok {
		t.Fatal("matching store opts should be included")
	}
	if _, ok := filtered.StoreOptions["s3+https://other.example.com/other/"]; ok {
		t.Fatal("non-matching store opts should be excluded")
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	const json = `{
		"defaults": {
			"digest": "sha256",
			"stores": ["/store/a", "/store/b"],
			"index-store": "/index/a",
			"chunk-size": "8:32:128",
			"cache": "/cache/a"
		}
	}`
	cfg, err := LoadConfigFromReader(strings.NewReader(json))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Defaults.Digest != "sha256" {
		t.Fatalf("expected sha256, got %s", cfg.Defaults.Digest)
	}
	if len(cfg.Defaults.Stores) != 2 || cfg.Defaults.Stores[0] != "/store/a" {
		t.Fatalf("unexpected Stores: %v", cfg.Defaults.Stores)
	}
	if cfg.Defaults.IndexStore != "/index/a" {
		t.Fatalf("expected /index/a, got %s", cfg.Defaults.IndexStore)
	}
	if cfg.Defaults.ChunkSize != "8:32:128" {
		t.Fatalf("expected 8:32:128, got %s", cfg.Defaults.ChunkSize)
	}
	if cfg.Defaults.Cache != "/cache/a" {
		t.Fatalf("expected /cache/a, got %s", cfg.Defaults.Cache)
	}
}
