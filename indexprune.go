package desync

import "context"

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
