package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/folbricht/desync/cmd/shared/cmdshared"
	"github.com/folbricht/desync/cmd/shared/pktline"
)

const configFileName = "desync-lfs.json"

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

	if containsParentRef(repoPath) {
		return fmt.Errorf("path %q contains parent directory reference", repoPath)
	}

	absPath, err := filepath.Abs(repoPath)
	if err != nil {
		return fmt.Errorf("resolving path %q: %w", repoPath, err)
	}

	if err := validateGitRepo(absPath); err != nil {
		return fmt.Errorf("not a git repository %q: %w", repoPath, err)
	}

	// Escape hatch: check git config trigger before loading JSON config so that
	// a per-repo opt-out works even when the config file is shared/global.
	gitDisabled := gitConfigValue(absPath, "desync-lfs") == "false"

	cfg, err := resolveConfig(absPath)
	if err != nil {
		if gitDisabled {
			// Config load failed but the git config already opted out — try to
			// delegate anyway rather than surfacing a config error.
			return tryDelegate(absPath)
		}
		return fmt.Errorf("loading config: %w", err)
	}

	// JSON config trigger: "desync-lfs": false in desync-lfs.json.
	cfgDisabled := cfg.DesyncLFS != nil && !*cfg.DesyncLFS

	if gitDisabled || cfgDisabled {
		return tryDelegate(absPath)
	}

	storeURL := cfg.ResolveStore("")
	if storeURL == "" {
		return fmt.Errorf("no store configured")
	}
	indexURL := cfg.ResolveIndexStore("")
	cacheURL := cfg.ResolveCache("")

	if err := cmdshared.SetDigestAlgorithm(cfg.ResolveDigest("")); err != nil {
		return err
	}

	minChunk, avgChunk, maxChunk, err := cmdshared.ParseChunkSizeParam(cfg.ResolveChunkSize(""))
	if err != nil {
		return err
	}

	var cmdOpt cmdshared.CmdStoreOptions
	cmdOpt.N = 10
	if cfg.Defaults.Concurrency > 0 {
		cmdOpt.N = cfg.Defaults.Concurrency
	}

	// Ensure local store directories exist (created on first use).
	if err := ensureLocalDir(storeURL); err != nil {
		return fmt.Errorf("creating store directory: %w", err)
	}
	if err := ensureLocalDir(indexURL); err != nil {
		return fmt.Errorf("creating index store directory: %w", err)
	}
	if cacheURL != "" {
		if err := ensureLocalDir(cacheURL); err != nil {
			return fmt.Errorf("creating cache directory: %w", err)
		}
	}

	// Read safe-pruning options from per-store config.
	storeOpts, err := cfg.GetStoreOptionsFor(storeURL)
	if err != nil {
		return fmt.Errorf("getting store options: %w", err)
	}

	// Open the write store (for uploads).
	writeStore, err := cmdshared.WritableStore(storeURL, cfg, cmdOpt)
	if err != nil {
		return fmt.Errorf("opening store %q: %w", storeURL, err)
	}
	defer writeStore.Close()

	// Build the read store (with optional cache).
	readStore, err := cmdshared.MultiStoreWithCache(cfg, cmdOpt, cacheURL, storeURL)
	if err != nil {
		return fmt.Errorf("opening read store: %w", err)
	}
	defer readStore.Close()

	// Open the index store.
	indexStore, err := cmdshared.WritableIndexStore(indexURL, cfg, cmdOpt)
	if err != nil {
		return fmt.Errorf("opening index store %q: %w", indexURL, err)
	}
	defer indexStore.Close()

	srv := &Server{
		operation:    operation,
		writeStore:   writeStore,
		readStore:    readStore,
		indexStore:   indexStore,
		n:            cmdOpt.N,
		minChunk:     minChunk,
		avgChunk:     avgChunk,
		maxChunk:     maxChunk,
		safePruning:  storeOpts.SafePruning,
		safePropTime: storeOpts.SafePropagationTime,
		logDir:       filepath.Join(absPath, "desync-lfs", "logs"),
	}

	runErr := srv.Run(ctx, os.Stdin, os.Stdout)
	if srv.logFile != nil {
		srv.logFile.Close()
	}
	if srv.hasLoggedErrors {
		fmt.Fprintf(os.Stderr, "Errors logged to %s\n", srv.logPath)
	}
	return runErr
}

// resolveConfig finds and loads the transfer configuration.
//
// Resolution order:
//  1. desync-lfs.config.object git config key — read config from a git object.
//     Falls through if the key is unset or the object is absent in the repo.
//  2. desync-lfs.config.path git config key — overrides the filename to search.
//     Default filename is "desync-lfs.json".
//  3. Walk up from repoPath looking for the config filename.
//  4. Global fallback: /etc/desync/<filename>.
//  5. Convention-based: <repoPath>/desync-lfs/{chunks,index}.
//
// The config file uses the same format as the main desync config.json.
// Relative store paths in the config are resolved against repoPath.
func resolveConfig(repoPath string) (cmdshared.Config, error) {
	// 1. desync-lfs.config.object — read config from a git object.
	if objectRef := gitConfigValue(repoPath, "desync-lfs.config.object"); objectRef != "" {
		cfg, found, err := cmdshared.LoadConfigFromGitObject(repoPath, objectRef)
		if err != nil {
			return cmdshared.Config{}, fmt.Errorf("git object config %q: %w", objectRef, err)
		}
		if found {
			return applyConfigDefaults(cfg, repoPath, objectRef)
		}
		// Object absent in repo — fall through to path-based strategy.
	}

	// 2. desync-lfs.config.path — override the config filename.
	configName := configFileName
	if p := gitConfigValue(repoPath, "desync-lfs.config.path"); p != "" {
		configName = p
	}

	// Absolute path: use directly.
	if filepath.IsAbs(configName) {
		if !fileExists(configName) {
			return cmdshared.Config{}, fmt.Errorf("config file not found: %s", configName)
		}
		return loadConfig(configName, repoPath)
	}

	// 3. Walk up from repoPath.
	dir := repoPath
	for {
		candidate := filepath.Join(dir, configName)
		if fileExists(candidate) {
			return loadConfig(candidate, repoPath)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	// 4. Global fallback.
	if globalConfig := filepath.Join("/etc", "desync", configName); fileExists(globalConfig) {
		return loadConfig(globalConfig, repoPath)
	}

	// 5. Convention-based: <repoPath>/desync-lfs/{chunks,index}.
	var cfg cmdshared.Config
	cfg.Defaults.Stores = []string{filepath.Join(repoPath, "desync-lfs", "chunks")}
	cfg.Defaults.IndexStore = filepath.Join(repoPath, "desync-lfs", "index")
	return cfg, nil
}

// containsParentRef reports whether path contains a ".." component that would
// traverse to a parent directory.  The check runs on the raw (unresolved) path
// so that traversal attempts are rejected before filepath.Abs silently resolves
// them.
func containsParentRef(path string) bool {
	for _, elem := range strings.Split(filepath.ToSlash(path), "/") {
		if elem == ".." {
			return true
		}
	}
	return false
}

// validateGitRepo returns an error if absPath is not a git repository.
func validateGitRepo(absPath string) error {
	cmd := exec.Command("git", "-C", absPath, "rev-parse", "--git-dir")
	if out, err := cmd.Output(); err != nil {
		return fmt.Errorf("git rev-parse --git-dir: %w", err)
	} else if strings.TrimSpace(string(out)) == "" {
		return fmt.Errorf("git rev-parse returned empty output")
	}
	return nil
}

// gitConfigValue runs `git -C repoPath config <key>` and returns the trimmed
// value, or "" if the key is unset, the directory is not a git repo, or git
// is not available.
func gitConfigValue(repoPath, key string) string {
	out, err := exec.Command("git", "-C", repoPath, "config", key).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func loadConfig(configPath, repoPath string) (cmdshared.Config, error) {
	f, err := os.Open(configPath)
	if err != nil {
		return cmdshared.Config{}, err
	}
	defer f.Close()

	cfg, err := cmdshared.LoadConfigFromReader(f)
	if err != nil {
		return cmdshared.Config{}, fmt.Errorf("%s: %w", configPath, err)
	}
	return applyConfigDefaults(cfg, repoPath, configPath)
}

// applyConfigDefaults validates and post-processes a freshly loaded Config:
// expands %(path) templates, resolves relative store paths, and derives the
// index store URL when absent.  source is used only in error messages.
func applyConfigDefaults(cfg cmdshared.Config, repoPath, source string) (cmdshared.Config, error) {
	// If desync-lfs is explicitly disabled, stores are not needed: the caller
	// will delegate to another binary (escape hatch).
	if cfg.DesyncLFS != nil && !*cfg.DesyncLFS {
		return cfg, nil
	}
	if len(cfg.Defaults.Stores) == 0 {
		return cfg, fmt.Errorf("config %s: 'defaults.stores' is required", source)
	}

	cfg.Defaults.Stores[0] = expandPathTemplate(cfg.Defaults.Stores[0], repoPath)
	if cfg.Defaults.IndexStore != "" {
		cfg.Defaults.IndexStore = expandPathTemplate(cfg.Defaults.IndexStore, repoPath)
	}
	if cfg.Defaults.Cache != "" {
		cfg.Defaults.Cache = expandPathTemplate(cfg.Defaults.Cache, repoPath)
	}

	cfg.Defaults.Stores[0] = resolveStorePath(cfg.Defaults.Stores[0], repoPath)
	if cfg.Defaults.IndexStore != "" {
		cfg.Defaults.IndexStore = resolveStorePath(cfg.Defaults.IndexStore, repoPath)
	} else {
		var err error
		cfg.Defaults.IndexStore, err = cmdshared.DeriveIndexURL(cfg.Defaults.Stores[0])
		if err != nil {
			return cfg, fmt.Errorf("deriving index store from %q: %w", cfg.Defaults.Stores[0], err)
		}
	}
	if cfg.Defaults.Cache != "" {
		cfg.Defaults.Cache = resolveStorePath(cfg.Defaults.Cache, repoPath)
	}

	return cfg, nil
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

// ensureLocalDir creates the directory if the location is a local path.
// URLs and existing directories are left untouched.
func ensureLocalDir(location string) error {
	u, err := url.Parse(location)
	if err == nil && len(u.Scheme) > 1 {
		return nil // URL with scheme — nothing to create
	}
	return os.MkdirAll(location, 0755)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// tryDelegate handles the escape hatch. It looks up the "desync-lfs.transfer.exec"
// git config key and replaces the current process with that binary (Unix) or
// runs it as a child and exits with its code (Windows). If no delegate is
// configured, or if exec fails, it sends a protocol-level 403 rejection to the
// client and returns an error.
func tryDelegate(repoPath string) error {
	execPath := gitConfigValue(repoPath, "desync-lfs.transfer.exec")
	if execPath != "" {
		if err := execDelegate(execPath, os.Args, os.Environ()); err != nil {
			// execDelegate only returns on failure (Unix exec error or Windows
			// process-start error).
			return rejectAndExit(fmt.Sprintf("delegate %q failed: %v", execPath, err))
		}
		// Unreachable on Unix (process replaced). On Windows execDelegate calls
		// os.Exit so we never get here either.
		return nil
	}
	return rejectAndExit("desync LFS is disabled for this repository and no delegate is configured (set desync-lfs.transfer.exec in git config)")
}

// rejectAndExit performs the minimum pkt-line handshake required to deliver a
// 403 error to the client, then returns an error so main exits non-zero.
// Write errors are swallowed — delivering the rejection is best-effort.
func rejectAndExit(msg string) error {
	w := pktline.NewWriter(os.Stdout)
	r := pktline.NewReader(os.Stdin)
	// Advertise version=1 — client expects this first.
	_ = w.WritePacketText("version=1")
	_ = w.WriteFlush()
	// Drain the client's "version 1" line and its trailing flush (best-effort).
	_, _ = r.ReadPacketText()
	_, _ = r.ReadPacket()
	// Send the protocol-level rejection.
	_ = w.WriteErrorStatus(403, msg)
	return fmt.Errorf("%s", msg)
}
