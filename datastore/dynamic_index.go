package datastore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

// DynamicEntry is a single entry in a dynamic index (40 bytes).
type DynamicEntry struct {
	EndOffset uint64
	Digest    [32]byte
}

// DynamicIndexReader reads a dynamic chunk index.
type DynamicIndexReader struct {
	header  DynamicIndexHeader
	entries []DynamicEntry
}

// ReadDynamicIndex parses a dynamic index from raw bytes.
func ReadDynamicIndex(data []byte) (*DynamicIndexReader, error) {
	if len(data) < IndexHeaderSize {
		return nil, fmt.Errorf("dynamic index: need at least %d bytes, got %d", IndexHeaderSize, len(data))
	}

	header, err := UnmarshalDynamicIndexHeader(data[:IndexHeaderSize])
	if err != nil {
		return nil, err
	}
	if header.Magic != MagicDynamicChunkIndex {
		return nil, fmt.Errorf("wrong magic for dynamic index: %x", header.Magic)
	}

	remaining := data[IndexHeaderSize:]
	if len(remaining)%DynamicEntrySize != 0 {
		return nil, fmt.Errorf("dynamic index: entry data size %d not multiple of %d", len(remaining), DynamicEntrySize)
	}

	count := len(remaining) / DynamicEntrySize
	entries := make([]DynamicEntry, count)
	for i := range count {
		off := i * DynamicEntrySize
		entries[i].EndOffset = binary.LittleEndian.Uint64(remaining[off : off+8])
		copy(entries[i].Digest[:], remaining[off+8:off+40])
	}

	return &DynamicIndexReader{header: header, entries: entries}, nil
}

// Count returns the number of entries.
func (r *DynamicIndexReader) Count() int { return len(r.entries) }

// IndexBytes returns the total virtual size (end offset of last entry).
func (r *DynamicIndexReader) IndexBytes() uint64 {
	if len(r.entries) == 0 {
		return 0
	}
	return r.entries[len(r.entries)-1].EndOffset
}

// CTime returns the creation timestamp.
func (r *DynamicIndexReader) CTime() int64 { return r.header.Ctime }

// Entry returns the entry at position i.
func (r *DynamicIndexReader) Entry(i int) DynamicEntry {
	return r.entries[i]
}

// ChunkInfo returns the chunk info at position i.
func (r *DynamicIndexReader) ChunkInfo(pos int) (ChunkInfo, bool) {
	if pos < 0 || pos >= len(r.entries) {
		return ChunkInfo{}, false
	}
	start := uint64(0)
	if pos > 0 {
		start = r.entries[pos-1].EndOffset
	}
	return ChunkInfo{
		Start:  start,
		End:    r.entries[pos].EndOffset,
		Digest: r.entries[pos].Digest,
	}, true
}

// ChunkFromOffset returns the chunk index containing the given byte offset.
// Uses binary search for O(log n) lookup.
func (r *DynamicIndexReader) ChunkFromOffset(offset uint64) (int, bool) {
	if len(r.entries) == 0 || offset >= r.entries[len(r.entries)-1].EndOffset {
		return 0, false
	}

	lo, hi := 0, len(r.entries)-1
	for lo < hi {
		mid := (lo + hi) / 2
		if r.entries[mid].EndOffset <= offset {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, true
}

// IndexDigest returns the digest at position pos.
func (r *DynamicIndexReader) IndexDigest(pos int) ([32]byte, bool) {
	if pos < 0 || pos >= len(r.entries) {
		return [32]byte{}, false
	}
	return r.entries[pos].Digest, true
}

// ComputeCsum computes the SHA-256 checksum over all entry data.
func (r *DynamicIndexReader) ComputeCsum() ([32]byte, uint64) {
	h := sha256.New()
	for _, e := range r.entries {
		var buf [DynamicEntrySize]byte
		binary.LittleEndian.PutUint64(buf[0:8], e.EndOffset)
		copy(buf[8:40], e.Digest[:])
		h.Write(buf[:])
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum, uint64(len(r.entries) * DynamicEntrySize)
}

// DynamicIndexWriter builds a dynamic chunk index.
type DynamicIndexWriter struct {
	header  DynamicIndexHeader
	entries []DynamicEntry
	buf     bytes.Buffer
}

// NewDynamicIndexWriter creates a new writer with the given creation time.
func NewDynamicIndexWriter(ctime int64) *DynamicIndexWriter {
	return &DynamicIndexWriter{
		header: DynamicIndexHeader{
			Magic: MagicDynamicChunkIndex,
			Ctime: ctime,
		},
	}
}

// Add appends an entry with the given end offset and digest.
func (w *DynamicIndexWriter) Add(endOffset uint64, digest [32]byte) {
	w.entries = append(w.entries, DynamicEntry{
		EndOffset: endOffset,
		Digest:    digest,
	})
}

// Finish writes the complete index and returns the raw bytes.
func (w *DynamicIndexWriter) Finish() ([]byte, error) {
	// Compute index checksum
	csum, _ := w.computeCsum()
	w.header.IndexCsum = csum

	// Generate UUID (simple: sha256 of ctime + entry count)
	var uuidInput [16]byte
	binary.LittleEndian.PutUint64(uuidInput[0:8], uint64(w.header.Ctime))
	binary.LittleEndian.PutUint64(uuidInput[8:16], uint64(len(w.entries)))
	uuidHash := sha256.Sum256(uuidInput[:])
	copy(w.header.UUID[:], uuidHash[:16])

	var buf bytes.Buffer
	buf.Grow(IndexHeaderSize + len(w.entries)*DynamicEntrySize)

	// Write header
	var hdr [IndexHeaderSize]byte
	w.header.MarshalTo(hdr[:])
	buf.Write(hdr[:])

	// Write entries
	var entryBuf [DynamicEntrySize]byte
	for _, e := range w.entries {
		binary.LittleEndian.PutUint64(entryBuf[0:8], e.EndOffset)
		copy(entryBuf[8:40], e.Digest[:])
		buf.Write(entryBuf[:])
	}

	return buf.Bytes(), nil
}

func (w *DynamicIndexWriter) computeCsum() ([32]byte, uint64) {
	h := sha256.New()
	var buf [DynamicEntrySize]byte
	for _, e := range w.entries {
		binary.LittleEndian.PutUint64(buf[0:8], e.EndOffset)
		copy(buf[8:40], e.Digest[:])
		h.Write(buf[:])
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum, uint64(len(w.entries) * DynamicEntrySize)
}
