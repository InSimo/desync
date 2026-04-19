package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime/trace"
	"strings"
	"syscall"
	"time"

	desync "github.com/folbricht/desync"
	"github.com/folbricht/desync/cmd/shared/cmdshared"
	"github.com/spf13/cobra"
)

var cfg cmdshared.Config
var cfgFile string
var cfgFromGit string
var digestAlgorithm string

// expandGitObjectName replaces %(fieldname) placeholders in template.
// Supported fields: remote, operation.
// If remote is empty (e.g. when invoked by the smudge filter during git clone),
// it defaults to "origin" so that %(remote)/_desync:config.json works without
// hardcoding the remote name.
func expandGitObjectName(template, remote, operation string) string {
	if remote == "" {
		remote = "origin"
	}
	s := strings.ReplaceAll(template, "%(remote)", remote)
	s = strings.ReplaceAll(s, "%(operation)", operation)
	return s
}

func initConfig(gitObjectName string) error {
	if gitObjectName != "" {
		var (
			found bool
			err   error
		)
		cfg, found, err = cmdshared.LoadConfigFromGitObject("", gitObjectName)
		if err != nil {
			return fmt.Errorf("reading config from git object %q: %w", gitObjectName, err)
		}
		if !found {
			return fmt.Errorf("git object %q not found", gitObjectName)
		}
		return nil
	}
	var err error
	cfg, cfgFile, err = cmdshared.LoadConfig(cfgFile)
	return err
}

// deriveIndexURL is a package-local alias for cmdshared.DeriveIndexURL.
var deriveIndexURL = cmdshared.DeriveIndexURL

// buildClientParams groups the CLI flag values consumed by buildClient.
type buildClientParams struct {
	storeURL, indexURL, cache, chunkSize string
	storeOpt                             cmdshared.CmdStoreOptions
	maxStorageOps                        int32
	gate                                 *desync.Gate
}

// buildClient resolves configuration, opens stores, and returns a ready
// desync.Client. remote/operation/serverConfig come from the LFS init
// message in agent mode, or are supplied with defaults in import mode.
func buildClient(p buildClientParams, remote, operation string, serverConfig *cmdshared.Config) (*desync.Client, bool, time.Duration, error) {
	gitObjectName := expandGitObjectName(cfgFromGit, remote, operation)
	if err := initConfig(gitObjectName); err != nil {
		if serverConfig != nil {
			cfg = *serverConfig
		} else {
			return nil, false, 0, err
		}
	}

	// Merge server config into local config using the documented
	// field classification (server-mergeable vs local-only).
	cmdshared.MergeServerConfig(&cfg, serverConfig)

	if err := cmdshared.SetDigestAlgorithm(cfg.ResolveDigest(digestAlgorithm)); err != nil {
		return nil, false, 0, err
	}

	resolvedStore := cfg.ResolveStore(p.storeURL)
	resolvedIndex := cfg.ResolveIndexStore(p.indexURL)
	resolvedChunkSize := cfg.ResolveChunkSize(p.chunkSize)

	if resolvedStore == "" {
		return nil, false, 0, fmt.Errorf("--store is required")
	}

	if resolvedIndex == "" {
		var err error
		resolvedIndex, err = deriveIndexURL(resolvedStore)
		if err != nil {
			return nil, false, 0, err
		}
	}

	// Resolve cache directory (DESYNC_CACHE_DIR env var as fallback).
	resolvedCache := cfg.ResolveCache(p.cache)
	if resolvedCache == "" {
		resolvedCache = os.Getenv("DESYNC_CACHE_DIR")
	}
	if resolvedCache != "" {
		os.MkdirAll(resolvedCache, 0o755)
	}

	client, err := cmdshared.NewClientFromConfig(
		cfg, p.storeOpt, resolvedStore, resolvedIndex,
		resolvedCache, resolvedChunkSize, p.gate)
	if err != nil {
		return nil, false, 0, err
	}

	storageOps := int(p.maxStorageOps)
	if storageOps <= 0 {
		storageOps = 20
	}
	desync.InitWorkerPool(p.storeOpt.N, storageOps)

	safePruning := p.storeOpt.SafePruning
	safePropTime := p.storeOpt.SafePropagationTime

	// If the server enables safe-pruning, the client must
	// also use it to avoid corrupting the pruning protocol.
	if serverConfig != nil {
		if opts, err := serverConfig.GetStoreOptionsFor(resolvedStore); err == nil && opts.SafePruning {
			safePruning = true
			if opts.SafePropagationTime > safePropTime {
				safePropTime = opts.SafePropagationTime
			}
		}
	}

	return client, safePruning, safePropTime, nil
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
		storeURL      string
		indexURL      string
		cache         string
		chunkSize     string
		indexes       bool
		importDir     string
		maxInFlight          string
		maxStorageOps        int32
		noPipelined          bool
		maxConcurrentUploads int
		storeOpt      cmdshared.CmdStoreOptions
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
			if indexes && importDir != "" {
				return fmt.Errorf("--indexes and --import are mutually exclusive")
			}
			if indexes {
				return nil
			}
			if cfgFile != "" && cfgFromGit != "" {
				return fmt.Errorf("--config and --config-from-git are mutually exclusive")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if indexes {
				return runIndexes(args, os.Stdin, os.Stdout)
			}

			// Resolve the in-flight byte limit. max-in-flight is a local-only
			// field (never merged from server config or --config-from-git), so
			// we read the local config file directly here.
			localCfg, _, _ := cmdshared.LoadConfig(cfgFile)
			maxInFlightStr := localCfg.ResolveMaxInFlight(maxInFlight)
			if maxInFlightStr == "" {
				maxInFlightStr = "2G"
			}
			maxInFlightBytes, err := cmdshared.ParseByteSize(maxInFlightStr)
			if err != nil {
				return fmt.Errorf("invalid --max-in-flight %q: %w", maxInFlightStr, err)
			}

			// Open the cross-process gate for in-flight bytes and/or storage ops.
			gate, err := desync.OpenGate(maxInFlightBytes, maxStorageOps)
			if err != nil {
				return fmt.Errorf("opening in-flight byte gate: %w", err)
			}
			defer gate.Close()

			cleanupProfiling := cmdshared.InitProfiling()
			defer cleanupProfiling()

			// Optional execution tracing: write to DESYNC_TRACE_DIR/<pid>.trace.
			if traceDir := os.Getenv("DESYNC_TRACE_DIR"); traceDir != "" {
				os.MkdirAll(traceDir, 0o755)
				path := fmt.Sprintf("%s/%d.trace", traceDir, os.Getpid())
				if f, err := os.Create(path); err == nil {
					trace.Start(f)
					defer func() {
						trace.Stop()
						f.Close()
					}()
				}
			}

			p := buildClientParams{
				storeURL:      storeURL,
				indexURL:      indexURL,
				cache:         cache,
				chunkSize:     chunkSize,
				storeOpt:      storeOpt,
				maxStorageOps: maxStorageOps,
				gate:          gate,
			}

			if importDir != "" {
				client, _, _, err := buildClient(p, "", "upload", nil)
				if err != nil {
					return err
				}
				defer client.Close()
				return runImport(ctx, importDir, client, maxConcurrentUploads)
			}

			agent := &Agent{tmpDir: os.TempDir(), pipelinedEnabled: !noPipelined, maxConcurrentUploads: maxConcurrentUploads}
			defer agent.Close()

			agent.setup = func(remote, operation string, serverConfig *cmdshared.Config) error {
				client, safePruning, safePropTime, err := buildClient(p, remote, operation, serverConfig)
				if err != nil {
					return err
				}
				agent.client = client
				agent.safePruning = safePruning
				agent.safePropagationTime = safePropTime
				return nil
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
	flags.StringVarP(&chunkSize, "chunk-size", "m", "", "min:avg:max chunk size in KB (default 16:64:256)")
	flags.StringVar(&cfgFile, "config", "", "desync config file (default: $HOME/.config/desync/config.json)")
	flags.StringVar(&cfgFromGit, "config-from-git", "",
		"read desync config from a git object; %(remote) and %(operation) are replaced\n"+
			"with values from the LFS init message (e.g. %(remote)/_desync:config.json);\n"+
			"%(remote) defaults to \"origin\" when the remote is not available (e.g. smudge filter during git clone)")
	flags.StringVar(&digestAlgorithm, "digest", "", "digest algorithm, sha512-256 or sha256 (default sha512-256)")
	flags.StringVar(&maxInFlight, "max-in-flight", "",
		"maximum total bytes allowed in-flight across all concurrent agent processes;\n"+
			"accepts human-readable suffixes (e.g. 2G, 500M, 1.5GB; binary units);\n"+
			"limits memory usage when git-lfs spawns multiple agents (concurrent=true);\n"+
			"set to 0 to disable the limit (default 2G)")
	flags.Int32Var(&maxStorageOps, "max-storage-ops", 0,
		"maximum concurrent storage operations (GetChunk/StoreChunk/GetIndex/StoreIndex)\n"+
			"across all concurrent agent processes; limits S3/network backend load;\n"+
			"set to 0 to disable the limit (default: disabled)")
	flags.BoolVar(&indexes, "indexes", false,
		"translate LFS OIDs to desync index names and write to stdout (one per line);\n"+
			"reads OIDs from positional args, or from the first token of each stdin line when no args are given")
	flags.StringVar(&importDir, "import", "",
		"bulk-import LFS objects from a local directory; walks the directory\n"+
			"recursively, verifies SHA-256 of each file against its filename,\n"+
			"and uploads missing objects to the chunk and index stores")
	flags.BoolVar(&noPipelined, "no-pipelined", false,
		"disable pipelined mode (spawn one process per concurrent transfer instead of handling all in one)")
	flags.IntVar(&maxConcurrentUploads, "max-concurrent-uploads", 8,
		"maximum parallel upload/download operations in pipelined mode;\n"+
			"existence checks (HasIndex) run at full git-lfs concurrency")
	cmdshared.AddStoreOptions(&storeOpt, flags)

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func runIndexes(args []string, r io.Reader, w io.Writer) error {
	bw := bufio.NewWriter(w)
	defer bw.Flush()
	if len(args) > 0 {
		for _, oid := range args {
			if len(oid) < 4 {
				return fmt.Errorf("OID %q is too short (minimum 4 characters)", oid)
			}
			fmt.Fprintln(bw, oidIndexName(oid))
		}
		return nil
	}
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}
		oid := fields[0]
		if len(oid) < 4 {
			return fmt.Errorf("OID %q is too short (minimum 4 characters)", oid)
		}
		fmt.Fprintln(bw, oidIndexName(oid))
	}
	return scanner.Err()
}

