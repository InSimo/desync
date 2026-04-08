package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/folbricht/desync"
	"github.com/insimo/cacheevict"
	"github.com/spf13/cobra"
)

type cacheStatsOptions struct {
	cache       string
	printFormat string
}

func newCacheStatsCommand(ctx context.Context) *cobra.Command {
	var opt cacheStatsOptions

	cmd := &cobra.Command{
		Use:   "cache-stats",
		Short: "Show cache statistics",
		Long: `Display cache size, file count, and (if available) hit/miss statistics.

Runs a directory scan to get actual values. If mmap-based tracking is active
(.cache-sizes file exists), also shows tracked values and hit/miss stats.
A warning is printed if tracked and scanned values diverge significantly.`,
		Example: `  desync cache-stats -c /path/to/cache
  desync cache-stats --format json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCacheStats(ctx, opt)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringVarP(&opt.cache, "cache", "c", "", "cache store location")
	flags.StringVar(&opt.printFormat, "format", "plain", "output format: plain or json")
	return cmd
}

type cacheStatsResult struct {
	ScanSize     int64   `json:"scan-size"`
	ScanFiles    int64   `json:"scan-files"`
	TrackedSize  int64   `json:"tracked-size,omitempty"`
	TrackedFiles int64   `json:"tracked-files,omitempty"`
	Hits         int64   `json:"hits,omitempty"`
	Misses       int64   `json:"misses,omitempty"`
	HitRatio     float64 `json:"hit-ratio,omitempty"`
	HasTracking  bool    `json:"has-tracking"`
	Diverged     bool    `json:"diverged,omitempty"`
}

func runCacheStats(ctx context.Context, opt cacheStatsOptions) error {
	opt.cache = cfg.ResolveCache(opt.cache)
	if opt.cache == "" {
		return errors.New("no cache location provided")
	}

	evictCfg := desync.DesyncCacheEvictConfig(opt.cache, false)

	scanSize, scanFiles, err := cacheevict.Stats(evictCfg)
	if err != nil {
		return fmt.Errorf("scanning cache: %w", err)
	}

	result := cacheStatsResult{
		ScanSize:  scanSize,
		ScanFiles: scanFiles,
	}

	// Check for mmap tracking file.
	h, err := cacheevict.OpenIfExists(evictCfg)
	if err == nil && h != nil {
		defer h.Close()
		result.HasTracking = true
		result.TrackedSize = h.TotalSize()
		result.TrackedFiles = h.TotalFiles()
		result.Hits = h.Hits()
		result.Misses = h.Misses()
		result.HitRatio = h.HitRatio()

		// Check for significant divergence (>5% difference).
		if scanFiles > 0 {
			sizeDiff := abs64(result.TrackedSize - scanSize)
			filesDiff := abs64(result.TrackedFiles - scanFiles)
			if float64(sizeDiff)/float64(scanSize) > 0.05 || float64(filesDiff)/float64(scanFiles) > 0.05 {
				result.Diverged = true
			}
		} else if result.TrackedFiles > 0 || result.TrackedSize > 0 {
			result.Diverged = true
		}
	}

	if opt.printFormat == "json" {
		return printJSON(stdout, result)
	}

	fmt.Fprintf(stdout, "Cache:       %s\n", opt.cache)
	fmt.Fprintf(stdout, "Total size:  %s (%d bytes)\n", formatBytes(scanSize), scanSize)
	fmt.Fprintf(stdout, "Total files: %d\n", scanFiles)

	if result.HasTracking {
		fmt.Fprintf(stdout, "\nTracking (mmap):\n")
		fmt.Fprintf(stdout, "  Tracked size:  %s (%d bytes)\n", formatBytes(result.TrackedSize), result.TrackedSize)
		fmt.Fprintf(stdout, "  Tracked files: %d\n", result.TrackedFiles)
		fmt.Fprintf(stdout, "  Hits:          %d\n", result.Hits)
		fmt.Fprintf(stdout, "  Misses:        %d\n", result.Misses)
		if result.Hits+result.Misses > 0 {
			fmt.Fprintf(stdout, "  Hit ratio:     %.1f%%\n", result.HitRatio*100)
		}

		if result.Diverged {
			fmt.Fprintf(stderr, "\nWarning: tracked counters diverge from actual cache content.\n")
			fmt.Fprintf(stderr, "Run 'desync cache-trim' to reconcile, unless concurrent operations\n")
			fmt.Fprintf(stderr, "are currently running against this cache.\n")
		}
	}

	return nil
}

func abs64(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

