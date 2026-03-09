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
	dryRun        bool
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
  desync prune -s /path/to/local --report-missing --yes file.caibx
  desync prune -s /path/to/local --dry-run file.caibx`,
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
	flags.BoolVar(&opt.dryRun, "dry-run", false, "report what would be deleted without making any changes; cannot be combined with --yes")
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
	if opt.dryRun && opt.yes {
		return errors.New("--dry-run and --yes are mutually exclusive")
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
	// ids maps each unique ChunkID to its uncompressed size (from the index).
	// Also collect per-index entries when --report-missing is requested,
	// and track the number of indexes and their total (pre-dedup) size.
	// seenIndexes prevents double-counting when the same name appears in both
	// positional args and the index store.
	ids := make(map[desync.ChunkID]int64)
	seenIndexes := make(map[string]struct{})
	var numIndexes int
	var totalIndexSize int64
	var indexEntries []indexEntry
	for _, name := range args {
		if _, dup := seenIndexes[name]; dup {
			continue
		}
		seenIndexes[name] = struct{}{}
		c, err := readCaibxFile(name, opt.cmdStoreOptions)
		if err != nil {
			return err
		}
		numIndexes++
		if opt.reportMissing {
			entry := indexEntry{name: name, ids: make(map[desync.ChunkID]struct{})}
			for _, c := range c.Chunks {
				ids[c.ID] = int64(c.Size)
				totalIndexSize += int64(c.Size)
				entry.ids[c.ID] = struct{}{}
			}
			indexEntries = append(indexEntries, entry)
		} else {
			for _, c := range c.Chunks {
				ids[c.ID] = int64(c.Size)
				totalIndexSize += int64(c.Size)
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
			if _, dup := seenIndexes[name]; dup {
				continue
			}
			seenIndexes[name] = struct{}{}
			idx, err := is.GetIndex(name)
			if err != nil {
				return err
			}
			numIndexes++
			if opt.reportMissing {
				entry := indexEntry{name: name, ids: make(map[desync.ChunkID]struct{})}
				for _, c := range idx.Chunks {
					ids[c.ID] = int64(c.Size)
					totalIndexSize += int64(c.Size)
					entry.ids[c.ID] = struct{}{}
				}
				indexEntries = append(indexEntries, entry)
			} else {
				for _, c := range idx.Chunks {
					ids[c.ID] = int64(c.Size)
					totalIndexSize += int64(c.Size)
				}
			}
		}
	}

	// If the -y option wasn't provided (and this isn't a dry run), ask the user
	// to confirm before doing anything
	if !opt.yes && !opt.dryRun {
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
	var stats desync.PruneStats
	if mergedOpt.SafePruning {
		ss, ok := s.(desync.SafePruneStore)
		if !ok {
			return fmt.Errorf("store '%s' does not support safe pruning", s)
		}
		missing, stats, err = ss.SafePrune(ctx, ids, opt.finalize, opt.dryRun)
	} else {
		missing, stats, err = s.Prune(ctx, ids, opt.dryRun)
	}
	if err != nil {
		return err
	}

	printPruneStats(s, stats, mergedOpt.SafePruning, opt.dryRun, numIndexes, totalIndexSize)

	if opt.reportMissing && len(missing) > 0 {
		return reportMissingChunks(missing, indexEntries)
	}
	return nil
}

// formatBytes formats a byte count as a human-readable string (e.g. "1.2 MiB").
func formatBytes(n int64) string {
	const (
		KiB = 1024
		MiB = 1024 * KiB
		GiB = 1024 * MiB
		TiB = 1024 * GiB
	)
	switch {
	case n >= TiB:
		return fmt.Sprintf("%.1f TiB", float64(n)/TiB)
	case n >= GiB:
		return fmt.Sprintf("%.1f GiB", float64(n)/GiB)
	case n >= MiB:
		return fmt.Sprintf("%.1f MiB", float64(n)/MiB)
	case n >= KiB:
		return fmt.Sprintf("%.1f KiB", float64(n)/KiB)
	default:
		return fmt.Sprintf("%d B", n)
	}
}

// fmtChunkLine formats a count + optional stored/deduplicated sizes into a
// human-readable fragment like "3 chunks (1.2 MiB stored, 2.3 MiB deduplicated)".
func fmtChunkLine(css desync.ChunkSizeStats) string {
	s := fmt.Sprintf("%d chunks", css.Count)
	if css.StoredSize > 0 || css.DeduplicatedSize > 0 {
		s += " ("
		if css.StoredSize > 0 {
			s += formatBytes(css.StoredSize) + " stored"
		}
		if css.DeduplicatedSize > 0 {
			if css.StoredSize > 0 {
				s += ", "
			}
			s += formatBytes(css.DeduplicatedSize) + " deduplicated"
		}
		s += ")"
	}
	return s
}

// printPruneStats prints a summary of what was done (or would be done in dry-run mode).
func printPruneStats(s desync.PruneStore, stats desync.PruneStats, safePruning bool, dryRun bool, numIndexes int, totalIndexSize int64) {
	verb := "deleted"
	if dryRun {
		verb = "would be deleted"
	}
	if !safePruning {
		fmt.Printf("%s %s from '%s'\n", fmtChunkLine(stats.Deletable), verb, s)
		if stats.Kept.Count > 0 {
			fmt.Printf("%s kept in '%s'\n", fmtChunkLine(stats.Kept), s)
		}
	} else {
		fmt.Printf("Pruning '%s':\n", s)
		fmt.Printf("  %s %s\n", fmtChunkLine(stats.Deletable), verb)
		if stats.Prunable.Count > 0 {
			markedVerb := "marked for deletion"
			if dryRun {
				markedVerb = "would be marked for deletion"
			}
			fmt.Printf("  %s %s\n", fmtChunkLine(stats.Prunable), markedVerb)
		}
		if stats.Protected.Count > 0 {
			fmt.Printf("  %s protected by active writers\n", fmtChunkLine(stats.Protected))
		}
		if stats.Kept.Count > 0 {
			fmt.Printf("  %s kept\n", fmtChunkLine(stats.Kept))
		}
	}
	fmt.Printf("Total: %d index(es), %s\n", numIndexes, formatBytes(totalIndexSize))
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
