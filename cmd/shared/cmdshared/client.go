package cmdshared

import (
	"fmt"

	"github.com/folbricht/desync"
)

// NewClientFromConfig creates a Client by opening stores from the given
// config and options, and initializing global desync state (Init,
// InitWorkerPool). The caller should call Client.Close() when done.
//
// storeURL and indexURL are resolved store locations (not raw CLI flags).
// If indexURL is empty, it is derived from storeURL. cacheURL may be empty
// to disable caching. chunkSize is a resolved "min:avg:max" string (use
// cfg.ResolveChunkSize to get it).
func NewClientFromConfig(cfg Config, cmdOpt CmdStoreOptions, storeURL, indexURL, cacheURL, chunkSize string) (*desync.Client, error) {
	if storeURL == "" {
		return nil, fmt.Errorf("store URL is required")
	}

	chunkStore, err := WritableStore(storeURL, cfg, cmdOpt)
	if err != nil {
		return nil, fmt.Errorf("opening chunk store %q: %w", storeURL, err)
	}

	readStore, err := MultiStoreWithCache(cfg, cmdOpt, cacheURL, storeURL)
	if err != nil {
		chunkStore.Close()
		return nil, fmt.Errorf("opening read store: %w", err)
	}

	if indexURL == "" {
		indexURL, err = DeriveIndexURL(storeURL)
		if err != nil {
			readStore.Close()
			chunkStore.Close()
			return nil, fmt.Errorf("deriving index URL: %w", err)
		}
	}

	indexStore, err := WritableIndexStore(indexURL, cfg, cmdOpt)
	if err != nil {
		readStore.Close()
		chunkStore.Close()
		return nil, fmt.Errorf("opening index store %q: %w", indexURL, err)
	}

	minChunk, avgChunk, maxChunk, err := ParseChunkSizeParam(chunkSize)
	if err != nil {
		indexStore.Close()
		readStore.Close()
		chunkStore.Close()
		return nil, fmt.Errorf("parsing chunk size %q: %w", chunkSize, err)
	}

	desync.Init(maxChunk)

	return desync.NewClient(readStore, chunkStore, indexStore, desync.ClientOptions{
		MinChunk: minChunk,
		AvgChunk: avgChunk,
		MaxChunk: maxChunk,
		N:        cmdOpt.N,
	}), nil
}
