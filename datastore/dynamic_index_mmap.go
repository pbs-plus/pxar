//go:build unix

package datastore

import (
	"fmt"
	"math"
	"os"

	"golang.org/x/sys/unix"
)

// OpenDynamicIndex maps a .didx file read-only so a large index costs evictable page cache instead of heap, matching PBS. Call Close to unmap.
func OpenDynamicIndex(path string) (*DynamicIndexReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := st.Size()
	if size < IndexHeaderSize {
		return nil, fmt.Errorf("dynamic index %s: need at least %d bytes, got %d", path, IndexHeaderSize, size)
	}
	if size > math.MaxInt {
		return nil, fmt.Errorf("dynamic index %s: size %d exceeds addressable range", path, size)
	}

	data, err := unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ, unix.MAP_PRIVATE)
	if err != nil {
		return nil, fmt.Errorf("mmap %s: %w", path, err)
	}

	reader, err := ParseDynamicIndex(data)
	if err != nil {
		_ = unix.Munmap(data)
		return nil, err
	}
	reader.unmap = func() error { return unix.Munmap(data) }
	return reader, nil
}
