package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/folbricht/desync"
	"github.com/folbricht/desync/cmd/internal/cmdshared"
)

const configFileName = "desync-lfs.json"

// transferConfig holds the server-side LFS transfer configuration.
type transferConfig struct {
	Store              string                          `json:"store"`
	IndexStore         string                          `json:"index-store,omitempty"`
	Cache              string                          `json:"cache,omitempty"`
	ChunkSize          string                          `json:"chunk-size,omitempty"`
	Digest             string                          `json:"digest,omitempty"`
	SafePruning        bool                            `json:"safe-pruning,omitempty"`
	SafePropTime       string                          `json:"safe-propagation-time,omitempty"`
	S3Credentials      map[string]cmdshared.S3Creds    `json:"s3-credentials,omitempty"`
	StoreOptions       map[string]desync.StoreOptions  `json:"store-options,omitempty"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "git-lfs-transfer: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 3 {
		return fmt.Errorf("usage: git-lfs-transfer <path> <operation>")
	}

	repoPath := os.Args[1]
	operation := os.Args[2]

	if operation != "upload" && operation != "download" {
		return fmt.Errorf("unknown operation %q (expected upload or download)", operation)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()

	absPath, err := filepath.Abs(repoPath)
	if err != nil {
		return fmt.Errorf("resolving path %q: %w", repoPath, err)
	}

	tc, err := resolveConfig(absPath)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	// Build the cmdshared.Config from transferConfig for store factory.
	cfg := cmdshared.Config{
		S3Credentials: tc.S3Credentials,
		StoreOptions:  tc.StoreOptions,
	}

	if err := cmdshared.SetDigestAlgorithm(tc.Digest); err != nil {
		return err
	}

	chunkSizeStr := tc.ChunkSize
	if chunkSizeStr == "" {
		chunkSizeStr = cmdshared.DefaultChunkSize
	}
	minChunk, avgChunk, maxChunk, err := cmdshared.ParseChunkSizeParam(chunkSizeStr)
	if err != nil {
		return err
	}

	var cmdOpt cmdshared.CmdStoreOptions
	cmdOpt.N = 10

	// Open the write store (for uploads).
	writeStore, err := cmdshared.WritableStore(tc.Store, cfg, cmdOpt)
	if err != nil {
		return fmt.Errorf("opening store %q: %w", tc.Store, err)
	}
	defer writeStore.Close()

	// Build the read store (with optional cache).
	readStore, err := cmdshared.MultiStoreWithCache(cfg, cmdOpt, tc.Cache, tc.Store)
	if err != nil {
		return fmt.Errorf("opening read store: %w", err)
	}
	defer readStore.Close()

	// Open the index store.
	indexStore, err := cmdshared.WritableIndexStore(tc.IndexStore, cfg, cmdOpt)
	if err != nil {
		return fmt.Errorf("opening index store %q: %w", tc.IndexStore, err)
	}
	defer indexStore.Close()

	var safePropTime time.Duration
	if tc.SafePropTime != "" {
		safePropTime, err = time.ParseDuration(tc.SafePropTime)
		if err != nil {
			return fmt.Errorf("parsing safe-propagation-time %q: %w", tc.SafePropTime, err)
		}
	} else {
		safePropTime = desync.DefaultSafePropagationTime
	}

	srv := &Server{
		operation:    operation,
		writeStore:   writeStore,
		readStore:    readStore,
		indexStore:   indexStore,
		n:            cmdOpt.N,
		minChunk:     minChunk,
		avgChunk:     avgChunk,
		maxChunk:     maxChunk,
		safePruning:  tc.SafePruning,
		safePropTime: safePropTime,
		tmpDir:       os.TempDir(),
	}

	return srv.Run(ctx, os.Stdin, os.Stdout)
}

// resolveConfig finds and loads the transfer configuration.
//
// Lookup order:
//  1. Walk up from repoPath looking for desync-lfs.json.
//  2. Global fallback: /etc/desync/desync-lfs.json.
//  3. Convention: <repoPath>/desync-lfs/{chunks,index}.
//
// Relative store paths in the config are resolved against repoPath.
func resolveConfig(repoPath string) (*transferConfig, error) {
	// 1. Walk up from repoPath.
	dir := repoPath
	for {
		candidate := filepath.Join(dir, configFileName)
		if fileExists(candidate) {
			return loadConfig(candidate, repoPath)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	// 2. Global fallback.
	globalConfig := filepath.Join("/etc", "desync", configFileName)
	if fileExists(globalConfig) {
		return loadConfig(globalConfig, repoPath)
	}

	// 3. Convention-based: <repoPath>/desync-lfs/{chunks,index}.
	return &transferConfig{
		Store:      filepath.Join(repoPath, "desync-lfs", "chunks"),
		IndexStore: filepath.Join(repoPath, "desync-lfs", "index"),
	}, nil
}

func loadConfig(configPath, repoPath string) (*transferConfig, error) {
	f, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var tc transferConfig
	if err := json.NewDecoder(f).Decode(&tc); err != nil {
		return nil, fmt.Errorf("decoding %s: %w", configPath, err)
	}

	if tc.Store == "" {
		return nil, fmt.Errorf("config %s: 'store' is required", configPath)
	}

	// Expand %(path) templates before resolving paths.
	tc.Store = expandPathTemplate(tc.Store, repoPath)
	if tc.IndexStore != "" {
		tc.IndexStore = expandPathTemplate(tc.IndexStore, repoPath)
	}
	if tc.Cache != "" {
		tc.Cache = expandPathTemplate(tc.Cache, repoPath)
	}

	// Resolve relative local paths against repoPath.
	tc.Store = resolveStorePath(tc.Store, repoPath)
	if tc.IndexStore != "" {
		tc.IndexStore = resolveStorePath(tc.IndexStore, repoPath)
	} else {
		var err error
		tc.IndexStore, err = cmdshared.DeriveIndexURL(tc.Store)
		if err != nil {
			return nil, fmt.Errorf("deriving index store from %q: %w", tc.Store, err)
		}
	}
	if tc.Cache != "" {
		tc.Cache = resolveStorePath(tc.Cache, repoPath)
	}

	return &tc, nil
}

// resolveStorePath makes a relative local path absolute by joining it with
// basePath. URLs (scheme length > 1) and absolute paths are returned as-is.
func resolveStorePath(location, basePath string) string {
	u, err := url.Parse(location)
	if err == nil && len(u.Scheme) > 1 {
		return location // URL with scheme
	}
	if filepath.IsAbs(location) {
		return location
	}
	return filepath.Join(basePath, location)
}

// expandPathTemplate replaces %(path) with the repo path value.
func expandPathTemplate(s, repoPath string) string {
	return strings.ReplaceAll(s, "%(path)", repoPath)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
