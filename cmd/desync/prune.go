package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type pruneOptions struct {
	cmdStoreOptions
	store      string
	indexStore string
	yes        bool
}

func newPruneCommand(ctx context.Context) *cobra.Command {
	var opt pruneOptions

	cmd := &cobra.Command{
		Use:   "prune [--index-store <location>] [<index>..]",
		Short: "Remove unreferenced chunks from a store",
		Long: `Read chunk IDs in from index files and delete any chunks from a store
that are not referenced in the provided index files. Use '-' to read a single index
from STDIN. Indexes can be provided as positional arguments, via --index-store, or both.`,
		Example: `  desync prune -s /path/to/local --yes file.caibx
  desync prune -s /path/to/local --index-store /path/to/indexes --yes`,
		Args: cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPrune(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringVarP(&opt.store, "store", "s", "", "target store")
	flags.StringVar(&opt.indexStore, "index-store", "", "index store to read all indexes from")
	flags.BoolVarP(&opt.yes, "yes", "y", false, "do not ask for confirmation")
	addStoreOptions(&opt.cmdStoreOptions, flags)
	return cmd
}

func runPrune(ctx context.Context, opt pruneOptions, args []string) error {
	if err := opt.cmdStoreOptions.validate(); err != nil {
		return err
	}
	opt.store = cfg.ResolveStore(opt.store)
	if opt.store == "" {
		return errors.New("no store provided")
	}
	if opt.indexStore == "" && len(args) == 0 {
		return errors.New("no index files or --index-store provided")
	}

	// Open the target store
	sr, err := storeFromLocation(opt.store, opt.cmdStoreOptions)
	if err != nil {
		return err
	}
	defer sr.Close()

	// Make sure this store can be used for pruning
	s, ok := sr.(desync.PruneStore)
	if !ok {
		if q, ok := sr.(*desync.WriteDedupQueue); ok {
			if s, ok = q.S.(desync.PruneStore); !ok {
				return fmt.Errorf("store '%s' does not support pruning", q.S)
			}
		} else {
			return fmt.Errorf("store '%s' does not support pruning", opt.store)
		}
	}

	// Read the input files and merge all chunk IDs in a map to de-dup them
	ids := make(map[desync.ChunkID]struct{})
	for _, name := range args {
		c, err := readCaibxFile(name, opt.cmdStoreOptions)
		if err != nil {
			return err
		}
		for _, c := range c.Chunks {
			ids[c.ID] = struct{}{}
		}
	}

	// If an index store was provided, list all indexes in it and collect chunk IDs
	if opt.indexStore != "" {
		is, err := openIndexStoreFromRoot(opt.indexStore, opt.cmdStoreOptions)
		if err != nil {
			return err
		}
		defer is.Close()
		lis, ok := is.(desync.ListableIndexStore)
		if !ok {
			return fmt.Errorf("index store '%s' does not support listing indexes", opt.indexStore)
		}
		names, err := lis.ListIndexes(ctx)
		if err != nil {
			return err
		}
		for _, name := range names {
			idx, err := is.GetIndex(name)
			if err != nil {
				return err
			}
			for _, c := range idx.Chunks {
				ids[c.ID] = struct{}{}
			}
		}
	}

	// If the -y option wasn't provided, ask the user to confirm before doing anything
	if !opt.yes {
		fmt.Printf("Warning: The provided index files reference %d unique chunks. Are you sure\nyou want to delete all other chunks from '%s'?\n", len(ids), s)
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

	if opt.cmdStoreOptions.safePruning {
		ss, ok := s.(desync.SafePruneStore)
		if !ok {
			return fmt.Errorf("store '%s' does not support safe pruning", s)
		}
		return ss.SafePrune(ctx, ids)
	}
	return s.Prune(ctx, ids)
}
