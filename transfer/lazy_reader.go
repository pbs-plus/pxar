package transfer

import (
	"fmt"
	"io"

	"github.com/pbs-plus/pxar/datastore"
)

// ChunkedReadSeeker implements io.ReadSeeker over a chunked archive stream.
// Instead of reconstructing the entire stream into memory, it lazily loads
// and decodes chunks on demand using the dynamic index and a chunk source.
// This is critical for same-datastore transfers where only a subset of files
// are needed — it avoids downloading the entire payload stream from PBS.
type ChunkedReadSeeker struct {
	idx    *datastore.DynamicIndexReader
	source datastore.ChunkSource
	offset int64
	size   int64
	// LRU cache of decoded chunks: chunk index → decoded data
	cache map[int][]byte
	// Maximum number of cached chunks (0 = unlimited)
	maxCache int
}

// NewChunkedReadSeeker creates a lazy read-seeker over chunked data.
// maxCache controls how many decoded chunks are kept in memory (0 = unlimited).
func NewChunkedReadSeeker(idx *datastore.DynamicIndexReader, source datastore.ChunkSource, maxCache int) *ChunkedReadSeeker {
	return &ChunkedReadSeeker{
		idx:      idx,
		source:   source,
		size:     int64(idx.IndexBytes()),
		cache:    make(map[int][]byte),
		maxCache: maxCache,
	}
}

func (r *ChunkedReadSeeker) Read(p []byte) (int, error) {
	if r.offset >= r.size {
		return 0, io.EOF
	}

	totalRead := 0
	for totalRead < len(p) && r.offset < r.size {
		// Find the chunk containing the current offset
		chunkIdx, ok := r.idx.ChunkFromOffset(uint64(r.offset))
		if !ok {
			return totalRead, io.EOF
		}

		chunkData, err := r.loadChunk(chunkIdx)
		if err != nil {
			return totalRead, fmt.Errorf("load chunk %d: %w", chunkIdx, err)
		}

		info, ok := r.idx.ChunkInfo(chunkIdx)
		if !ok {
			return totalRead, fmt.Errorf("chunk info %d not found", chunkIdx)
		}

		// Calculate offset within this chunk
		chunkStart := info.Start
		offsetInChunk := r.offset - int64(chunkStart)
		remaining := len(chunkData) - int(offsetInChunk)

		toCopy := remaining
		available := len(p) - totalRead
		if toCopy > available {
			toCopy = available
		}

		copy(p[totalRead:], chunkData[offsetInChunk:offsetInChunk+int64(toCopy)])
		totalRead += toCopy
		r.offset += int64(toCopy)
	}

	return totalRead, nil
}

func (r *ChunkedReadSeeker) Seek(offset int64, whence int) (int64, error) {
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

// loadChunk loads and decodes a chunk, using cache if available.
func (r *ChunkedReadSeeker) loadChunk(chunkIdx int) ([]byte, error) {
	if data, ok := r.cache[chunkIdx]; ok {
		return data, nil
	}

	digest := r.idx.Entry(chunkIdx).Digest
	raw, err := r.source.GetChunk(digest)
	if err != nil {
		return nil, err
	}

	decoded, err := datastore.DecodeBlob(raw)
	if err != nil {
		return nil, fmt.Errorf("decode chunk: %w", err)
	}

	// Evict oldest entries if cache is full
	if r.maxCache > 0 && len(r.cache) >= r.maxCache {
		// Simple eviction: clear half the cache
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
	return decoded, nil
}

// Close clears the chunk cache.
func (r *ChunkedReadSeeker) Close() error {
	r.cache = nil
	return nil
}