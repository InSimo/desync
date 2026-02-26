package desync

import (
	"os"

	"io"
)

// ConsoleIndexStore is used for writing/reading indexes from STDOUT/STDIN
type ConsoleIndexStore struct{}

// NewConsoleIndexStore creates an instance of an indexStore that reads/writes to and
// from console
func NewConsoleIndexStore() (ConsoleIndexStore, error) {
	return ConsoleIndexStore{}, nil
}

// GetIndexReader returns a reader from STDIN
func (s ConsoleIndexStore) GetIndexReader(string) (io.ReadCloser, error) {
	return io.NopCloser(os.Stdin), nil
}

// GetIndex reads an index from STDIN and returns it.
func (s ConsoleIndexStore) GetIndex(string) (i Index, e error) {
	return IndexFromReader(os.Stdin)
}

// StoreIndex writes the provided index to STDOUT. The name is ignored.
func (s ConsoleIndexStore) StoreIndex(name string, idx Index) error {
	_, err := idx.WriteTo(os.Stdout)
	return err
}

// HasIndex always returns false for the console store since stdin cannot be probed.
func (s ConsoleIndexStore) HasIndex(string) (bool, error) {
	return false, nil
}

func (s ConsoleIndexStore) String() string {
	return "-"
}

// Close the index store.
func (s ConsoleIndexStore) Close() error { return nil }
