package desync

import "context"

// commonSafePruneIndexes implements the two-run safe index pruning algorithm
// used by each backend's SafePruneIndexes method.
//
// It reads the prunable set persisted by the previous run, lists the current
// indexes, and deletes only those that are absent from keep AND were already
// candidates in the previous run. Newly seen candidates (absent from keep but
// not yet in the prunable set) are written back as the new prunable set for
// the next run — they are never deleted in the same operation that first
// observes them.
//
// Correctness: an index I can be deleted only if it appeared in the previous
// run's ListIndexes snapshot. It therefore cannot be an index that was written
// in the race window between the current keep-set assembly and the current
// ListIndexes call. See doc/safe-pruning-index.md for a full discussion.
func commonSafePruneIndexes(ctx context.Context, keep map[string]struct{}, s SafePruneIndexStore) error {
	prev, err := s.ReadPrunableIndexSet(ctx)
	if err != nil {
		return err
	}

	names, err := s.ListIndexes(ctx)
	if err != nil {
		return err
	}

	var toDelete []string
	var newPrunable []string
	for _, name := range names {
		if _, ok := keep[name]; ok {
			continue
		}
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		if _, wasPrunable := prev[name]; wasPrunable {
			toDelete = append(toDelete, name)
		} else {
			newPrunable = append(newPrunable, name)
		}
	}

	if len(toDelete) > 0 {
		if err := s.DeleteIndexes(ctx, toDelete); err != nil {
			return err
		}
	}
	return s.WritePrunableIndexSet(ctx, newPrunable)
}

// commonPruneIndexes implements the shared index pruning logic used by each
// backend's PruneIndexes method. It lists all indexes in the store, collects
// the names that are absent from keep, and passes the full delete list to
// s.DeleteIndexes so that backends can issue a single batch request where
// supported.
func commonPruneIndexes(ctx context.Context, keep map[string]struct{}, s IndexPruneStore) error {
	names, err := s.ListIndexes(ctx)
	if err != nil {
		return err
	}
	var toDelete []string
	for _, name := range names {
		if _, ok := keep[name]; ok {
			continue
		}
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		toDelete = append(toDelete, name)
	}
	if len(toDelete) == 0 {
		return nil
	}
	return s.DeleteIndexes(ctx, toDelete)
}
