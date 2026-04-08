package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/folbricht/desync"
	"github.com/insimo/cacheevict"
	"github.com/spf13/cobra"
)

type cacheClearOptions struct {
	cache string
	yes   bool
}

func newCacheClearCommand(ctx context.Context) *cobra.Command {
	var opt cacheClearOptions

	cmd := &cobra.Command{
		Use:   "cache-clear",
		Short: "Remove all chunks from the cache",
		Long: `Remove all cached chunk files from the cache directory. If mmap-based
tracking is active, counters are updated. Empty subdirectories are removed.`,
		Example: `  desync cache-clear -c /path/to/cache --yes`,
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCacheClear(ctx, opt)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringVarP(&opt.cache, "cache", "c", "", "cache store location")
	flags.BoolVarP(&opt.yes, "yes", "y", false, "do not ask for confirmation")
	return cmd
}

func runCacheClear(ctx context.Context, opt cacheClearOptions) error {
	opt.cache = cfg.ResolveCache(opt.cache)
	if opt.cache == "" {
		return errors.New("no cache location provided")
	}

	evictCfg := desync.DesyncCacheEvictConfig(opt.cache, false)

	// Get current stats before clearing.
	totalSize, totalFiles, err := cacheevict.Stats(evictCfg)
	if err != nil {
		return fmt.Errorf("scanning cache: %w", err)
	}

	if totalFiles == 0 {
		fmt.Fprintln(stdout, "Cache is empty, nothing to clear.")
		return nil
	}

	if !opt.yes {
		fmt.Fprintf(stdout, "About to remove %d files (%s) from %s\n", totalFiles, formatBytes(totalSize), opt.cache)
		fmt.Fprint(stdout, "Continue? [y/N] ")
		var answer string
		fmt.Fscanln(os.Stdin, &answer)
		if answer != "y" && answer != "Y" {
			return nil
		}
	}

	if err := cacheevict.Clear(evictCfg); err != nil {
		return fmt.Errorf("clearing cache: %w", err)
	}

	fmt.Fprintf(stdout, "Removed %d files (%s)\n", totalFiles, formatBytes(totalSize))
	return nil
}
