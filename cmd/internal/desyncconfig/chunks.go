package desyncconfig

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseChunkSizeParam parses a "min:avg:max" chunk size string (values in KB)
// and returns the three sizes in bytes.
func ParseChunkSizeParam(s string) (min, avg, max uint64, err error) {
	sizes := strings.Split(s, ":")
	if len(sizes) != 3 {
		return 0, 0, 0, fmt.Errorf("invalid chunk size %q, expected min:avg:max", s)
	}
	parse := func(str, label string) (uint64, error) {
		n, e := strconv.Atoi(str)
		if e != nil {
			return 0, fmt.Errorf("%s chunk size: %w", label, e)
		}
		return uint64(n) * 1024, nil
	}
	if min, err = parse(sizes[0], "min"); err != nil {
		return
	}
	if avg, err = parse(sizes[1], "avg"); err != nil {
		return
	}
	if max, err = parse(sizes[2], "max"); err != nil {
		return
	}
	return
}
