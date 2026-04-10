package datastore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

// FixedIndexReader reads a fixed-size chunk index.
type FixedIndexReader struct {
	header    FixedIndexHeader
	digests   [][32]byte
	size      uint64
	chunkSize uint64
}

// ReadFixedIndex parses a fixed index from raw bytes.
func ReadFixedIndex(data []byte) (*FixedIndexReader, error) {
	if len(data) < IndexHeaderSize {
		return nil, fmt.Errorf("fixed index: need at least %d bytes, got %d", IndexHeaderSize, len(data))
	}

	header, err := UnmarshalFixedIndexHeader(data[:IndexHeaderSize])
	if err != nil {
		return nil, err
	}
	if header.Magic != MagicFixedChunkIndex {
		return nil, fmt.Errorf("wrong magic for fixed index: %x", header.Magic)
	}

	remaining := data[IndexHeaderSize:]
	if len(remaining)%FixedDigestSize != 0 {
		return nil, fmt.Errorf("fixed index: digest data size %d not multiple of %d", len(remaining), FixedDigestSize)
	}

	count := len(remaining) / FixedDigestSize
	digests := make([][32]byte, count)
	for i := 0; i < count; i++ {
		copy(digests[i][:], remaining[i*FixedDigestSize:(i+1)*FixedDigestSize])
	}

	return &FixedIndexReader{
		header:    header,
		digests:   digests,
		size:      header.Size,
		chunkSize: header.ChunkSize,
	}, nil
}

// Count returns the number of chunks.
func (r *FixedIndexReader) Count() int { return len(r.digests) }

// IndexBytes returns the total virtual size.
func (r *FixedIndexReader) IndexBytes() uint64 { return r.size }

// CTime returns the creation timestamp.
func (r *FixedIndexReader) CTime() int64 { return r.header.Ctime }

// ChunkInfo returns chunk info at position pos.
func (r *FixedIndexReader) ChunkInfo(pos int) (ChunkInfo, bool) {
	if pos < 0 || pos >= len(r.digests) {
		return ChunkInfo{}, false
	}
	start := uint64(pos) * r.chunkSize
	end := start + r.chunkSize
	if end > r.size {
		end = r.size
	}
	return ChunkInfo{
		Start:  start,
		End:    end,
		Digest: r.digests[pos],
	}, true
}

// ChunkFromOffset returns the chunk index for the given byte offset.
func (r *FixedIndexReader) ChunkFromOffset(offset uint64) (int, bool) {
	if offset >= r.size {
		return 0, false
	}
	return int(offset / r.chunkSize), true
}

// IndexDigest returns the digest at position pos.
func (r *FixedIndexReader) IndexDigest(pos int) ([32]byte, bool) {
	if pos < 0 || pos >= len(r.digests) {
		return [32]byte{}, false
	}
	return r.digests[pos], true
}

// ComputeCsum computes the SHA-256 checksum over all digests.
func (r *FixedIndexReader) ComputeCsum() ([32]byte, uint64) {
	h := sha256.New()
	for _, d := range r.digests {
		h.Write(d[:])
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum, uint64(len(r.digests) * FixedDigestSize)
}

// FixedIndexWriter builds a fixed-size chunk index.
type FixedIndexWriter struct {
	header    FixedIndexHeader
	digests   [][32]byte
	size      uint64
	chunkSize uint64
	count     int
}

// NewFixedIndexWriter creates a writer. ChunkSize must be a power of 2.
func NewFixedIndexWriter(ctime int64, size, chunkSize uint64) (*FixedIndexWriter, error) {
	if chunkSize == 0 || (chunkSize&(chunkSize-1)) != 0 {
		return nil, fmt.Errorf("chunk size must be a power of 2, got %d", chunkSize)
	}

	count := int((size + chunkSize - 1) / chunkSize)
	if size == 0 {
		count = 0
	}

	return &FixedIndexWriter{
		header: FixedIndexHeader{
			Magic:     MagicFixedChunkIndex,
			Ctime:     ctime,
			Size:      size,
			ChunkSize: chunkSize,
		},
		digests:   make([][32]byte, count),
		size:      size,
		chunkSize: chunkSize,
		count:     count,
	}, nil
}

// Set sets the digest for chunk at index i.
func (w *FixedIndexWriter) Set(i int, digest [32]byte) {
	if i >= 0 && i < len(w.digests) {
		w.digests[i] = digest
	}
}

// Finish writes the complete index and returns raw bytes.
func (w *FixedIndexWriter) Finish() ([]byte, error) {
	// Compute index checksum
	csum, _ := w.computeCsum()
	w.header.IndexCsum = csum

	// Generate UUID
	var uuidInput [16]byte
	binary.LittleEndian.PutUint64(uuidInput[0:8], uint64(w.header.Ctime))
	binary.LittleEndian.PutUint64(uuidInput[8:16], w.size)
	uuidHash := sha256.Sum256(uuidInput[:])
	copy(w.header.UUID[:], uuidHash[:16])

	var buf bytes.Buffer
	buf.Grow(IndexHeaderSize + len(w.digests)*FixedDigestSize)

	// Write header
	var hdr [IndexHeaderSize]byte
	w.header.MarshalTo(hdr[:])
	buf.Write(hdr[:])

	// Write digests
	for _, d := range w.digests {
		buf.Write(d[:])
	}

	return buf.Bytes(), nil
}

func (w *FixedIndexWriter) computeCsum() ([32]byte, uint64) {
	h := sha256.New()
	for _, d := range w.digests {
		h.Write(d[:])
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum, uint64(len(w.digests) * FixedDigestSize)
}
