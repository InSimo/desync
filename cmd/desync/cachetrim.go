package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/folbricht/desync"
	"github.com/folbricht/desync/cmd/shared/cmdshared"
	"github.com/insimo/cacheevict"
	"github.com/spf13/cobra"
)

type cacheTrimOptions struct {
	cache    string
	maxSize  string
	maxFiles int64
	maxAge   time.Duration
}

func newCacheTrimCommand(ctx context.Context) *cobra.Command {
	var opt cacheTrimOptions

	cmd := &cobra.Command{
		Use:   "cache-trim",
		Short: "Remove oldest chunks to bring the cache within limits",
		Long: `Remove the oldest cached chunk files (by last access time) until the
cache is within the specified size, file count, or age limits. At least
one limit must be specified via flags or config file.

If no flags are given, falls back to cache-max-size and cache-max-files
from the config file or environment variables. If the mmap tracking file
exists, its counters are updated.`,
		Example: `  desync cache-trim -c /path/to/cache --max-size 10G
  desync cache-trim -c /path/to/cache --max-files 100000
  desync cache-trim -c /path/to/cache --max-age 720h
  desync cache-trim  # uses config defaults`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCacheTrim(ctx, opt)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringVarP(&opt.cache, "cache", "c", "", "cache store location")
	flags.StringVar(&opt.maxSize, "max-size", "", "maximum cache size (e.g. '10G', '500M')")
	flags.Int64Var(&opt.maxFiles, "max-files", 0, "maximum number of cached files")
	flags.DurationVar(&opt.maxAge, "max-age", 0, "maximum age of cached files (e.g. '720h' for 30 days)")
	return cmd
}

func runCacheTrim(ctx context.Context, opt cacheTrimOptions) error {
	opt.cache = cfg.ResolveCache(opt.cache)
	if opt.cache == "" {
		return errors.New("no cache location provided")
	}

	// Resolve size limit: flag > config > env.
	var maxSize int64
	maxSizeStr := opt.maxSize
	if maxSizeStr == "" {
		maxSizeStr = cfg.ResolveCacheMaxSize("")
	}
	if maxSizeStr != "" {
		var err error
		maxSize, err = cmdshared.ParseByteSize(maxSizeStr)
		if err != nil {
			return fmt.Errorf("invalid max-size: %w", err)
		}
	}

	// Resolve files limit: flag > config > env.
	maxFiles := opt.maxFiles
	if maxFiles == 0 {
		maxFiles = cfg.ResolveCacheMaxFiles(0)
	}

	maxAge := opt.maxAge

	if maxSize <= 0 && maxFiles <= 0 && maxAge <= 0 {
		return errors.New("at least one of --max-size, --max-files, or --max-age must be specified (or set via config)")
	}

	evictCfg := desync.DesyncCacheEvictConfig(opt.cache, false)

	// Get stats before trim for reporting.
	sizeBefore, filesBefore, err := cacheevict.Stats(evictCfg)
	if err != nil {
		return fmt.Errorf("scanning cache: %w", err)
	}

	if err := cacheevict.Trim(evictCfg, maxSize, maxFiles, maxAge); err != nil {
		return fmt.Errorf("trimming cache: %w", err)
	}

	// Get stats after trim.
	sizeAfter, filesAfter, err := cacheevict.Stats(evictCfg)
	if err != nil {
		return fmt.Errorf("scanning cache after trim: %w", err)
	}

	removedFiles := filesBefore - filesAfter
	removedSize := sizeBefore - sizeAfter

	if removedFiles == 0 {
		fmt.Fprintln(stdout, "Cache is already within limits, nothing to trim.")
	} else {
		fmt.Fprintf(stdout, "Removed %d files (%s)\n", removedFiles, formatBytes(removedSize))
		fmt.Fprintf(stdout, "Remaining: %d files (%s)\n", filesAfter, formatBytes(sizeAfter))
	}

	return nil
}
