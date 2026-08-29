package transfer

import (
	"fmt"
	"io"
	"sync"

	"github.com/pbs-plus/pxar/datastore"
)

// ReadSeeker implements io.ReadSeeker over a chunked archive stream.
// Instead of reconstructing the entire stream into memory, it lazily loads
// and decodes chunks on demand using the dynamic index and a chunk source.
// This is critical for same-datastore transfers where only a subset of files
// are needed — it avoids downloading the entire payload stream from PBS.
type ReadSeeker struct {
	source   datastore.ChunkSource
	idx      *datastore.DynamicIndexReader
	cache    map[int][]byte
	offset   int64
	size     int64
	maxCache int
	// offsetMu serializes Seek and Read so that concurrent goroutines
	// do not corrupt each other's seek positions.
	offsetMu sync.Mutex
	// mu protects the chunk cache from concurrent access.
	mu sync.RWMutex
	// disableCache prevents chunk caching when true. Decoded chunks are
	// returned and immediately discarded — appropriate for payload streams
	// during streaming restores to bound memory usage.
	disableCache bool
}

// NewReadSeeker creates a lazy read-seeker over chunked data.
// maxCache controls how many decoded chunks are kept in memory (0 = unlimited).
func NewReadSeeker(idx *datastore.DynamicIndexReader, source datastore.ChunkSource, maxCache int) *ReadSeeker {
	return &ReadSeeker{
		idx:      idx,
		source:   source,
		size:     int64(idx.IndexBytes()),
		cache:    make(map[int][]byte),
		maxCache: maxCache,
	}
}

func (r *ReadSeeker) Read(p []byte) (int, error) {
	r.offsetMu.Lock()
	defer r.offsetMu.Unlock()

	if r.offset >= r.size {
		return 0, io.EOF
	}

	totalRead := 0
	for totalRead < len(p) && r.offset < r.size {
		n, err := r.readAtInternal(p[totalRead:], r.offset)
		if err != nil && err != io.EOF {
			return totalRead, err
		}
		totalRead += n
		r.offset += int64(n)
		if err == io.EOF {
			break
		}
	}

	if totalRead == 0 && len(p) > 0 {
		return 0, io.EOF
	}
	return totalRead, nil
}

// ReadAt reads len(p) bytes starting at the given offset without mutating
// the seeker's internal position. It is safe for concurrent use.
func (r *ReadSeeker) ReadAt(p []byte, offset int64) (int, error) {
	if offset >= r.size {
		return 0, io.EOF
	}

	totalRead := 0
	for totalRead < len(p) {
		n, err := r.readAtInternal(p[totalRead:], offset+int64(totalRead))
		if err != nil && err != io.EOF {
			return totalRead, err
		}
		totalRead += n
		if err == io.EOF || n == 0 {
			break
		}
	}

	if totalRead == 0 && len(p) > 0 {
		return 0, io.EOF
	}
	return totalRead, nil
}

// readAtInternal copies into p from the chunk containing the given absolute
// offset. It returns the number of bytes copied (0 at stream end).
func (r *ReadSeeker) readAtInternal(p []byte, offset int64) (int, error) {
	if offset >= r.size {
		return 0, io.EOF
	}

	chunkIdx, ok := r.idx.ChunkFromOffset(uint64(offset))
	if !ok {
		return 0, io.EOF
	}

	chunkData, err := r.loadChunk(chunkIdx)
	if err != nil {
		return 0, fmt.Errorf("load chunk %d: %w", chunkIdx, err)
	}

	info, ok := r.idx.ChunkInfo(chunkIdx)
	if !ok {
		return 0, fmt.Errorf("chunk info %d not found", chunkIdx)
	}

	chunkStart := info.Start
	offsetInChunk := offset - int64(chunkStart)
	remaining := len(chunkData) - int(offsetInChunk)

	toCopy := min(remaining, len(p))

	copy(p, chunkData[offsetInChunk:offsetInChunk+int64(toCopy)])
	return toCopy, nil
}

func (r *ReadSeeker) Seek(offset int64, whence int) (int64, error) {
	r.offsetMu.Lock()
	defer r.offsetMu.Unlock()

	switch whence {
	case io.SeekStart:
		r.offset = offset
	case io.SeekCurrent:
		r.offset += offset
	case io.SeekEnd:
		r.offset = r.size + offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}
	if r.offset < 0 {
		r.offset = 0
		return 0, fmt.Errorf("negative position")
	}
	return r.offset, nil
}

// SetCacheSize adjusts the maximum number of decoded chunks kept in memory.
// Setting to 0 disables caching entirely — each chunk is decoded on demand
// and immediately discarded. This is appropriate for payload streams where
// content is streamed sequentially and caching would accumulate unbounded
// memory. Existing cached entries are evicted if the new size is lower.
func (r *ReadSeeker) SetCacheSize(n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n <= 0 {
		r.disableCache = true
		r.cache = nil
		r.maxCache = 0
		return
	}
	r.disableCache = false
	r.maxCache = n
	if r.cache == nil {
		r.cache = make(map[int][]byte)
	} else if len(r.cache) > n {
		count := 0
		for k := range r.cache {
			delete(r.cache, k)
			count++
			if count >= len(r.cache)-n {
				break
			}
		}
	}
}

// loadChunk loads and decodes a chunk, using cache if available.
// It is safe for concurrent use.
func (r *ReadSeeker) loadChunk(chunkIdx int) ([]byte, error) {
	if !r.disableCache {
		// Fast path: check cache under read lock.
		r.mu.RLock()
		data, ok := r.cache[chunkIdx]
		r.mu.RUnlock()
		if ok {
			return data, nil
		}
	}

	// Slow path: fetch and decode.
	digest := r.idx.Entry(chunkIdx).Digest
	raw, err := r.source.GetChunk(digest)
	if err != nil {
		return nil, err
	}

	decoded, err := datastore.DecodeBlob(nil, raw)
	if err != nil {
		return nil, fmt.Errorf("decode chunk: %w", err)
	}

	if r.disableCache {
		return decoded, nil
	}

	r.mu.Lock()
	// Double-check: another goroutine may have loaded it.
	if data, ok := r.cache[chunkIdx]; ok {
		r.mu.Unlock()
		return data, nil
	}

	// Evict oldest entries if cache is full.
	if r.maxCache > 0 && len(r.cache) >= r.maxCache {
		count := 0
		for k := range r.cache {
			delete(r.cache, k)
			count++
			if count >= r.maxCache/2 {
				break
			}
		}
	}

	r.cache[chunkIdx] = decoded
	r.mu.Unlock()
	return decoded, nil
}

// Close clears the chunk cache.
func (r *ReadSeeker) Close() error {
	r.cache = nil
	return nil
}
