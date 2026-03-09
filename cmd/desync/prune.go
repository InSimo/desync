package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type pruneOptions struct {
	cmdStoreOptions
	store         string
	indexStore    string
	yes           bool
	finalize      bool
	reportMissing bool
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
  desync prune -s /path/to/local --index-store /path/to/indexes --yes
  desync prune -s /path/to/local --report-missing --yes file.caibx`,
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
	flags.BoolVar(&opt.finalize, "finalize", false, "delete already-marked chunks without marking new ones; requires --safe-pruning or safe-pruning enabled via config")
	flags.BoolVar(&opt.reportMissing, "report-missing", false, "after pruning, report any chunks referenced by indexes but absent from the store")
	addStoreOptions(&opt.cmdStoreOptions, flags)
	return cmd
}

// indexEntry holds the per-index information needed for missing-chunk reporting.
type indexEntry struct {
	name string
	ids  map[desync.ChunkID]struct{}
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

	// Read the input files and merge all chunk IDs in a map to de-dup them.
	// Also collect per-index entries when --report-missing is requested.
	ids := make(map[desync.ChunkID]struct{})
	var indexEntries []indexEntry
	for _, name := range args {
		c, err := readCaibxFile(name, opt.cmdStoreOptions)
		if err != nil {
			return err
		}
		if opt.reportMissing {
			entry := indexEntry{name: name, ids: make(map[desync.ChunkID]struct{})}
			for _, c := range c.Chunks {
				ids[c.ID] = struct{}{}
				entry.ids[c.ID] = struct{}{}
			}
			indexEntries = append(indexEntries, entry)
		} else {
			for _, c := range c.Chunks {
				ids[c.ID] = struct{}{}
			}
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
			if opt.reportMissing {
				entry := indexEntry{name: name, ids: make(map[desync.ChunkID]struct{})}
				for _, c := range idx.Chunks {
					ids[c.ID] = struct{}{}
					entry.ids[c.ID] = struct{}{}
				}
				indexEntries = append(indexEntries, entry)
			} else {
				for _, c := range idx.Chunks {
					ids[c.ID] = struct{}{}
				}
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

	mergedOpt, err := cfg.GetStoreOptionsFor(opt.store)
	if err != nil {
		return err
	}
	mergedOpt = opt.cmdStoreOptions.MergedWith(mergedOpt)

	if opt.finalize && !mergedOpt.SafePruning {
		return errors.New("--finalize requires safe-pruning to be enabled (--safe-pruning or via config)")
	}

	var missing []desync.ChunkID
	if mergedOpt.SafePruning {
		ss, ok := s.(desync.SafePruneStore)
		if !ok {
			return fmt.Errorf("store '%s' does not support safe pruning", s)
		}
		missing, err = ss.SafePrune(ctx, ids, opt.finalize)
	} else {
		missing, err = s.Prune(ctx, ids)
	}
	if err != nil {
		return err
	}

	if opt.reportMissing && len(missing) > 0 {
		return reportMissingChunks(missing, indexEntries)
	}
	return nil
}

// reportMissingChunks prints a summary of chunks that are referenced by
// indexes but absent from the store, then returns a non-nil error so the
// command exits with a non-zero status.
func reportMissingChunks(missing []desync.ChunkID, entries []indexEntry) error {
	missingSet := make(map[desync.ChunkID]struct{}, len(missing))
	for _, id := range missing {
		missingSet[id] = struct{}{}
	}

	type indexReport struct {
		name    string
		total   int
		missing []string
	}
	var affected []indexReport
	for _, e := range entries {
		var m []string
		for id := range e.ids {
			if _, absent := missingSet[id]; absent {
				m = append(m, id.String())
			}
		}
		if len(m) > 0 {
			sort.Strings(m)
			affected = append(affected, indexReport{
				name:    e.name,
				total:   len(e.ids),
				missing: m,
			})
		}
	}
	sort.Slice(affected, func(i, j int) bool { return affected[i].name < affected[j].name })

	fmt.Printf("Missing chunks: %d total, across %d index(es)\n\n", len(missing), len(affected))
	for _, r := range affected {
		fmt.Printf("%s: %d/%d chunks missing\n", r.name, len(r.missing), r.total)
		fmt.Printf("  %s\n", strings.Join(r.missing, "\n  "))
	}
	return fmt.Errorf("%d missing chunk(s) detected", len(missing))
}
