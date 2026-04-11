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

	desync "github.com/folbricht/desync"
	"github.com/folbricht/desync/cmd/shared/bytelimit"
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
		maxInFlight   int64
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

			// Open the cross-process gate for in-flight bytes and/or storage ops.
			gate, err := bytelimit.OpenGate(maxInFlight, maxStorageOps)
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

			agent := &Agent{tmpDir: os.TempDir(), gate: gate, pipelinedEnabled: !noPipelined, maxConcurrentUploads: maxConcurrentUploads}
			defer agent.Close()

			agent.setup = func(remote, operation string, serverConfig *cmdshared.Config) error {
				gitObjectName := expandGitObjectName(cfgFromGit, remote, operation)
				if err := initConfig(gitObjectName); err != nil {
					// If local config loading fails but we have server config,
					// use the server config as the base config.
					if serverConfig != nil {
						cfg = *serverConfig
					} else {
						return err
					}
				}

				// Merge server config: server-provided values fill in gaps
				// not covered by local config or command-line flags.
				if serverConfig != nil {
					if cfg.S3Credentials == nil {
						cfg.S3Credentials = serverConfig.S3Credentials
					}
					if cfg.StoreOptions == nil {
						cfg.StoreOptions = serverConfig.StoreOptions
					}
				}

				if err := cmdshared.SetDigestAlgorithm(cfg.ResolveDigest(digestAlgorithm)); err != nil {
					return err
				}

				// Server config provides store URLs as defaults when
				// not specified via flags or local config.
				if serverConfig != nil && storeURL == "" && cfg.ResolveStore("") == "" {
					storeURL = serverConfig.ResolveStore("")
				}
				if serverConfig != nil && indexURL == "" && cfg.ResolveIndexStore("") == "" {
					indexURL = serverConfig.ResolveIndexStore("")
				}
				// Chunk size from the server always takes priority — it must
				// match the server's chunking for data compatibility.
				if serverConfig != nil {
					if sc := serverConfig.ResolveChunkSize(""); sc != "" {
						chunkSize = sc
					}
				}

				resolvedStore := cfg.ResolveStore(storeURL)
				resolvedIndex := cfg.ResolveIndexStore(indexURL)
				resolvedChunkSize := cfg.ResolveChunkSize(chunkSize)

				if resolvedStore == "" {
					return fmt.Errorf("--store is required")
				}

				chunkStore, err := cmdshared.WritableStore(resolvedStore, cfg, storeOpt)
				if err != nil {
					return err
				}

				// Build the read store for downloads, optionally wrapping the remote
				// store with a local cache tier.
				readStore, err := cmdshared.MultiStoreWithCache(cfg, storeOpt, cache, resolvedStore)
				if err != nil {
					chunkStore.Close()
					return err
				}

				if resolvedIndex == "" {
					resolvedIndex, err = deriveIndexURL(resolvedStore)
					if err != nil {
						readStore.Close()
						chunkStore.Close()
						return err
					}
				}

				indexStore, err := cmdshared.WritableIndexStore(resolvedIndex, cfg, storeOpt)
				if err != nil {
					readStore.Close()
					chunkStore.Close()
					return err
				}

				minChunk, avgChunk, maxChunk, err := cmdshared.ParseChunkSizeParam(resolvedChunkSize)
				if err != nil {
					readStore.Close()
					chunkStore.Close()
					indexStore.Close()
					return err
				}
				desync.Init(maxChunk)
				storageOps := int(maxStorageOps)
				if storageOps <= 0 {
					storageOps = 20
				}
				desync.InitWorkerPool(storeOpt.N, storageOps)

				// Wrap stores with ops gating if a storage-ops limit is configured.
				var ws desync.WriteStore = chunkStore
				var rs desync.Store = readStore
				var is desync.IndexWriteStore = indexStore
				if gate.MaxOps() > 0 {
					ws = &bytelimit.GatedWriteStore{WriteStore: chunkStore, Gate: gate}
					rs = &bytelimit.GatedStore{Store: readStore, Gate: gate}
					is = &bytelimit.GatedIndexWriteStore{IndexWriteStore: indexStore, Gate: gate}
				}

				agent.client = desync.NewClient(rs, ws, is, desync.ClientOptions{
					MinChunk: minChunk,
					AvgChunk: avgChunk,
					MaxChunk: maxChunk,
					N:        storeOpt.N,
				})

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
	flags.Int64Var(&maxInFlight, "max-in-flight", 2*1024*1024*1024,
		"maximum total bytes allowed in-flight across all concurrent agent processes;\n"+
			"limits memory usage when git-lfs spawns multiple agents (concurrent=true);\n"+
			"set to 0 to disable the limit (default 2 GB)")
	flags.Int32Var(&maxStorageOps, "max-storage-ops", 0,
		"maximum concurrent storage operations (GetChunk/StoreChunk/GetIndex/StoreIndex)\n"+
			"across all concurrent agent processes; limits S3/network backend load;\n"+
			"set to 0 to disable the limit (default: disabled)")
	flags.BoolVar(&indexes, "indexes", false,
		"translate LFS OIDs to desync index names and write to stdout (one per line);\n"+
			"reads OIDs from positional args, or from the first token of each stdin line when no args are given")
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

