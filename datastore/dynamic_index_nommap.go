//go:build !unix

package datastore

import "os"

// OpenDynamicIndex reads a .didx file into memory; only unix builds get the mmap path, so large indexes stay on the heap here.
func OpenDynamicIndex(path string) (*DynamicIndexReader, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseDynamicIndex(data)
}
