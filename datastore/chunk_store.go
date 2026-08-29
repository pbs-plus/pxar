package datastore

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ChunkStore manages chunk storage on the filesystem.
// Chunks are stored under base/.chunks/XXXX/XXXXYY... where XXXX are the first
// four hex characters of the SHA-256 digest (PBS-compatible layout).
type ChunkStore struct {
	base string
	uid  int
	gid  int
	sync bool
	mu   sync.Mutex
}

// NewChunkStore creates a ChunkStore rooted at base, creating the .chunks
// directory if needed.
func NewChunkStore(base string) (*ChunkStore, error) {
	return NewOwnedChunkStore(base, -1, -1, false)
}

// NewOwnedChunkStore creates a chunk store whose newly inserted chunks use the requested ownership.
func NewOwnedChunkStore(base string, uid, gid int, syncWrites bool) (*ChunkStore, error) {
	cs := &ChunkStore{base: base, uid: uid, gid: gid, sync: syncWrites}
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
	return filepath.Join(cs.chunkDir(), string(buf[:4]), string(buf[:]))
}

// InsertChunk stores a chunk. Returns (exists, size, error).
// If the chunk already exists, returns (true, existingSize, nil).
func (cs *ChunkStore) InsertChunk(digest [32]byte, data []byte) (bool, int, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	path := cs.ChunkPath(digest)

	if info, err := os.Stat(path); err == nil {
		if !info.Mode().IsRegular() {
			return false, 0, fmt.Errorf("chunk path %s is not a regular file", path)
		}
		if info.Size() > 0 && info.Size() <= int64(len(data)) {
			now := time.Now()
			if err := os.Chtimes(path, now, now); err != nil {
				return false, 0, fmt.Errorf("touch chunk: %w", err)
			}
			return true, int(info.Size()), nil
		}
	} else if !os.IsNotExist(err) {
		return false, 0, fmt.Errorf("stat chunk: %w", err)
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return false, 0, fmt.Errorf("create chunk dir %s: %w", dir, err)
	}
	if cs.uid >= 0 || cs.gid >= 0 {
		if err := os.Chown(dir, cs.uid, cs.gid); err != nil {
			return false, 0, fmt.Errorf("chown chunk dir %s: %w", dir, err)
		}
	}

	tmp, err := os.CreateTemp(dir, ".chunk-*")
	if err != nil {
		return false, 0, fmt.Errorf("create temporary chunk: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(0o644); err != nil {
		_ = tmp.Close()
		return false, 0, fmt.Errorf("chmod temporary chunk: %w", err)
	}
	if cs.uid >= 0 || cs.gid >= 0 {
		if err := tmp.Chown(cs.uid, cs.gid); err != nil {
			_ = tmp.Close()
			return false, 0, fmt.Errorf("chown temporary chunk: %w", err)
		}
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return false, 0, fmt.Errorf("write chunk: %w", err)
	}
	if cs.sync {
		if err := tmp.Sync(); err != nil {
			_ = tmp.Close()
			return false, 0, fmt.Errorf("sync chunk: %w", err)
		}
	}
	if err := tmp.Close(); err != nil {
		return false, 0, fmt.Errorf("close chunk: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return false, 0, fmt.Errorf("rename chunk: %w", err)
	}
	if cs.sync {
		if err := syncDir(filepath.Dir(path)); err != nil {
			return false, 0, fmt.Errorf("sync chunk dir: %w", err)
		}
	}

	return false, len(data), nil
}

// syncDir flushes directory metadata so a preceding rename survives a crash,
// mirroring pbs-datastore's dir-fsync on chunk insert.
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// StatChunk returns the stored chunk size, failing when missing or empty.
func (cs *ChunkStore) StatChunk(digest [32]byte) (int64, error) {
	info, err := os.Stat(cs.ChunkPath(digest))
	if err != nil {
		return 0, err
	}
	if !info.Mode().IsRegular() || info.Size() == 0 {
		return 0, fmt.Errorf("chunk %s is not a usable file", cs.ChunkPath(digest))
	}
	return info.Size(), nil
}

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
	cs.mu.Lock()
	defer cs.mu.Unlock()
	path := cs.ChunkPath(digest)
	now := time.Now()
	return os.Chtimes(path, now, now)
}
