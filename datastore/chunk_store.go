package datastore

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ChunkStore manages chunk storage on the filesystem.
// Chunks are stored under base/.chunks/XX/XXYY... where XX are the first
// two hex characters of the SHA-256 digest.
type ChunkStore struct {
	base string
}

// NewChunkStore creates a ChunkStore rooted at base, creating the .chunks
// directory if needed.
func NewChunkStore(base string) (*ChunkStore, error) {
	cs := &ChunkStore{base: base}
	if err := os.MkdirAll(cs.chunkDir(), 0o755); err != nil {
		return nil, fmt.Errorf("create chunk dir: %w", err)
	}
	return cs, nil
}

// chunkDir returns the path to the .chunks directory.
func (cs *ChunkStore) chunkDir() string {
	return filepath.Join(cs.base, ".chunks")
}

// ChunkPath returns the filesystem path for a chunk identified by digest.
func (cs *ChunkStore) ChunkPath(digest [32]byte) string {
	var buf [64]byte
	hex.Encode(buf[:], digest[:])
	return filepath.Join(cs.chunkDir(), string(buf[:2]), string(buf[:]))
}

// InsertChunk stores a chunk. Returns (exists, size, error).
// If the chunk already exists, returns (true, existingSize, nil).
func (cs *ChunkStore) InsertChunk(digest [32]byte, data []byte) (bool, int, error) {
	path := cs.ChunkPath(digest)

	// Check if already exists
	if info, err := os.Stat(path); err == nil {
		return true, int(info.Size()), nil
	}

	// Create parent directory
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return false, 0, fmt.Errorf("create chunk dir %s: %w", dir, err)
	}

	// Atomic write: write to temp file then rename
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return false, 0, fmt.Errorf("write chunk: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return false, 0, fmt.Errorf("rename chunk: %w", err)
	}

	return false, len(data), nil
}

// LoadChunk reads a chunk from disk.
func (cs *ChunkStore) LoadChunk(digest [32]byte) ([]byte, error) {
	path := cs.ChunkPath(digest)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			var buf [64]byte
			hex.Encode(buf[:], digest[:])
			return nil, fmt.Errorf("chunk not found: %s", string(buf[:16]))
		}
		return nil, fmt.Errorf("read chunk: %w", err)
	}
	return data, nil
}

// TouchChunk updates the access time of a chunk file.
func (cs *ChunkStore) TouchChunk(digest [32]byte) error {
	path := cs.ChunkPath(digest)
	now := time.Now()
	return os.Chtimes(path, now, now)
}
