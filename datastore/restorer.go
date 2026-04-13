package datastore

import (
	"fmt"
	"io"
)

// ChunkSource provides access to chunks by their digest.
type ChunkSource interface {
	// GetChunk retrieves a chunk by its SHA-256 digest.
	// Returns the raw chunk data (not decoded/blob-wrapped).
	GetChunk(digest [32]byte) ([]byte, error)
}

// Restorer reconstructs files from dynamic indexes using a chunk source.
type Restorer struct {
	source ChunkSource
}

// NewRestorer creates a new restorer with the given chunk source.
func NewRestorer(source ChunkSource) *Restorer {
	return &Restorer{source: source}
}

// RestoreFile reconstructs a complete file from a dynamic index.
// Writes the reconstructed file content to w.
func (r *Restorer) RestoreFile(idx *DynamicIndexReader, w io.Writer) error {
	if idx.Count() == 0 {
		return nil // Empty file
	}

	for i := 0; i < idx.Count(); i++ {
		entry := idx.Entry(i)
		chunkData, err := r.source.GetChunk(entry.Digest)
		if err != nil {
			return fmt.Errorf("chunk %d/%d (digest %x): %w", i+1, idx.Count(), entry.Digest[:8], err)
		}

		// Decode blob wrapper (handles both compressed and uncompressed)
		decoded, err := DecodeBlob(chunkData)
		if err != nil {
			return fmt.Errorf("decode chunk %d: %w", i, err)
		}

		if _, err := w.Write(decoded); err != nil {
			return fmt.Errorf("write chunk %d: %w", i, err)
		}
	}

	return nil
}

// RestoreRange reconstructs a specific byte range from a dynamic index.
// Useful for partial reads without downloading the entire file.
func (r *Restorer) RestoreRange(idx *DynamicIndexReader, offset, length uint64, w io.Writer) error {
	if length == 0 {
		return nil
	}

	// Find the first chunk containing the offset
	chunkIdx, ok := idx.ChunkFromOffset(offset)
	if !ok {
		return fmt.Errorf("offset %d beyond file size", offset)
	}

	endOffset := offset + length
	if endOffset > idx.IndexBytes() {
		endOffset = idx.IndexBytes()
	}

	bytesWritten := uint64(0)
	for i := chunkIdx; i < idx.Count() && bytesWritten < length; i++ {
		info, ok := idx.ChunkInfo(i)
		if !ok {
			return fmt.Errorf("chunk %d not found", i)
		}

		chunkData, err := r.source.GetChunk(info.Digest)
		if err != nil {
			return fmt.Errorf("chunk %d (digest %x): %w", i, info.Digest[:8], err)
		}

		// Decode blob wrapper (handles both compressed and uncompressed)
		decoded, err := DecodeBlob(chunkData)
		if err != nil {
			return fmt.Errorf("decode chunk %d: %w", i, err)
		}

		// Calculate slice within this chunk
		chunkStart := info.Start
		chunkEnd := info.End

		// Find overlap with requested range
		readStart := uint64(0)
		if offset > chunkStart {
			readStart = offset - chunkStart
		}

		readEnd := uint64(len(decoded))
		if endOffset < chunkEnd {
			readEnd = endOffset - chunkStart
		}

		if readStart < readEnd && readEnd <= uint64(len(decoded)) {
			n, err := w.Write(decoded[readStart:readEnd])
			if err != nil {
				return fmt.Errorf("write: %w", err)
			}
			bytesWritten += uint64(n)
		}
	}

	return nil
}

// FileSize returns the total size of the file represented by the index.
func (r *Restorer) FileSize(idx *DynamicIndexReader) uint64 {
	return idx.IndexBytes()
}

// ChunkStoreSource adapts a ChunkStore to the ChunkSource interface.
type ChunkStoreSource struct {
	store *ChunkStore
}

// NewChunkStoreSource creates a chunk source from a local chunk store.
func NewChunkStoreSource(store *ChunkStore) *ChunkStoreSource {
	return &ChunkStoreSource{store: store}
}

// GetChunk retrieves a chunk from the local store.
func (s *ChunkStoreSource) GetChunk(digest [32]byte) ([]byte, error) {
	return s.store.LoadChunk(digest)
}
