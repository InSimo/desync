package cmdshared

import (
	"strings"
	"testing"
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

func TestLoadConfigDefaults(t *testing.T) {
	const json = `{
		"defaults": {
			"digest": "sha256",
			"stores": ["/store/a", "/store/b"],
			"index-store": "/index/a",
			"chunk-size": "8:32:128"
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
}
