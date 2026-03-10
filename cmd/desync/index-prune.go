package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type indexPruneOptions struct {
	cmdStoreOptions
	indexStore       string
	yes              bool
	safeIndexPruning bool
}

func newIndexPruneCommand(ctx context.Context) *cobra.Command {
	var opt indexPruneOptions

	cmd := &cobra.Command{
		Use:   "index-prune --index-store <location> [<name>..]",
		Short: "Remove unreferenced indexes from an index store",
		Long: `Delete all indexes from an index store that are not listed in the provided
names. Pass index names as positional arguments or use '-' to read names from
STDIN (one per line).`,
		Example: `  desync index-prune --index-store /path/to/indexes --yes blob1.caibx blob2.caibx
  cat keep.txt | desync index-prune --index-store /path/to/indexes - --yes`,
		Args: cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runIndexPrune(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringVar(&opt.indexStore, "index-store", "", "target index store")
	flags.BoolVarP(&opt.yes, "yes", "y", false, "do not ask for confirmation")
	flags.BoolVar(&opt.safeIndexPruning, "safe-index-pruning", false, "enable safe concurrent index pruning protocol (see doc/safe-pruning-index.md)")
	addStoreOptions(&opt.cmdStoreOptions, flags)
	return cmd
}

func runIndexPrune(ctx context.Context, opt indexPruneOptions, args []string) error {
	if err := opt.cmdStoreOptions.Validate(); err != nil {
		return err
	}
	if opt.indexStore == "" {
		return errors.New("no index store provided")
	}

	// Build the keep set from positional args; '-' means read from stdin
	keep := make(map[string]struct{})
	for _, name := range args {
		if name == "-" {
			scanner := bufio.NewScanner(os.Stdin)
			for scanner.Scan() {
				if line := scanner.Text(); line != "" {
					keep[line] = struct{}{}
				}
			}
			if err := scanner.Err(); err != nil {
				return err
			}
		} else {
			keep[name] = struct{}{}
		}
	}

	// Open the index store
	is, err := openIndexStoreFromRoot(opt.indexStore, opt.cmdStoreOptions)
	if err != nil {
		return err
	}
	defer is.Close()

	// Make sure it supports pruning
	ps, ok := is.(desync.IndexPruneStore)
	if !ok {
		return fmt.Errorf("index store '%s' does not support pruning", opt.indexStore)
	}

	// Ask for confirmation unless -y was given
	if !opt.yes {
		fmt.Printf("Warning: The provided names reference %d indexes. Are you sure\nyou want to delete all other indexes from '%s'?\n", len(keep), ps)
	ask:
		for {
			var a string
			fmt.Printf("[y/N]: ")
			if _, err := fmt.Fscanln(os.Stdin, &a); err != nil {
				return err
			}
			switch a {
			case "y", "Y":
				break ask
			case "n", "N", "":
				return nil
			}
		}
	}

	if opt.safeIndexPruning {
		ss, ok := ps.(desync.SafePruneIndexStore)
		if !ok {
			return fmt.Errorf("index store '%s' does not support safe index pruning", ps)
		}
		return ss.SafePruneIndexes(ctx, keep)
	}
	return ps.PruneIndexes(ctx, keep)
}
