package datastore

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

// DynamicEntry is a single entry in a dynamic index (40 bytes).
type DynamicEntry struct {
	EndOffset uint64
	Digest    [32]byte
}

// DynamicIndexReader reads a dynamic chunk index, decoding entries on access from retained backing bytes that must not be modified while in use.
type DynamicIndexReader struct {
	entries []byte
	count   int
	header  DynamicIndexHeader
	unmap   func() error
}

// ParseDynamicIndex parses a dynamic index from raw bytes, which the returned reader retains rather than copies.
func ParseDynamicIndex(data []byte) (*DynamicIndexReader, error) {
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

	return &DynamicIndexReader{
		header:  header,
		entries: remaining,
		count:   len(remaining) / DynamicEntrySize,
	}, nil
}

// Close unmaps a reader opened by OpenDynamicIndex, which must not be used afterwards; it is a no-op for ParseDynamicIndex readers.
func (r *DynamicIndexReader) Close() error {
	if r.unmap == nil {
		return nil
	}
	unmap := r.unmap
	r.unmap = nil
	r.entries = nil
	r.count = 0
	return unmap()
}

func (r *DynamicIndexReader) endOffset(i int) uint64 {
	off := i * DynamicEntrySize
	return binary.LittleEndian.Uint64(r.entries[off : off+8])
}

func (r *DynamicIndexReader) digest(i int) [32]byte {
	off := i*DynamicEntrySize + 8
	var d [32]byte
	copy(d[:], r.entries[off:off+32])
	return d
}

// Count returns the number of entries.
func (r *DynamicIndexReader) Count() int { return r.count }

// IndexBytes returns the total virtual size (end offset of last entry).
func (r *DynamicIndexReader) IndexBytes() uint64 {
	if r.count == 0 {
		return 0
	}
	return r.endOffset(r.count - 1)
}

// CTime returns the creation timestamp.
func (r *DynamicIndexReader) CTime() int64 { return r.header.Ctime }

// Entry returns the entry at position i.
func (r *DynamicIndexReader) Entry(i int) DynamicEntry {
	if i < 0 || i >= r.count {
		panic("dynamic index: entry index out of range")
	}
	return DynamicEntry{EndOffset: r.endOffset(i), Digest: r.digest(i)}
}

// ChunkInfo returns the chunk info at position i.
func (r *DynamicIndexReader) ChunkInfo(pos int) (ChunkInfo, bool) {
	if pos < 0 || pos >= r.count {
		return ChunkInfo{}, false
	}
	start := uint64(0)
	if pos > 0 {
		start = r.endOffset(pos - 1)
	}
	return ChunkInfo{
		Start:  start,
		End:    r.endOffset(pos),
		Digest: r.digest(pos),
	}, true
}

// ChunkFromOffset returns the chunk index containing the given byte offset.
// Uses binary search for O(log n) lookup.
func (r *DynamicIndexReader) ChunkFromOffset(offset uint64) (int, bool) {
	if r.count == 0 || offset >= r.endOffset(r.count-1) {
		return 0, false
	}

	lo, hi := 0, r.count-1
	for lo < hi {
		mid := (lo + hi) / 2
		if r.endOffset(mid) <= offset {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, true
}

func (r *DynamicIndexReader) IndexDigest(pos int) ([32]byte, bool) {
	if pos < 0 || pos >= r.count {
		return [32]byte{}, false
	}
	return r.digest(pos), true
}

func (r *DynamicIndexReader) UUID() [16]byte { return r.header.UUID }

func (r *DynamicIndexReader) IndexCsum() [32]byte { return r.header.IndexCsum }

func (r *DynamicIndexReader) LastEndOffset() uint64 {
	if r.count == 0 {
		return 0
	}
	return r.endOffset(r.count - 1)
}

func (r *DynamicIndexReader) ComputeCsum() ([32]byte, uint64) {
	h := sha256.New()
	h.Write(r.entries)
	var sum [32]byte
	h.Sum(sum[:0])
	return sum, r.LastEndOffset()
}

// DynamicIndexWriter builds a dynamic chunk index.
type DynamicIndexWriter struct {
	entries []DynamicEntry
	header  DynamicIndexHeader
	csum    [32]byte
	cached  bool
}

// NewDynamicIndexWriter creates a new writer with the given creation time.
func NewDynamicIndexWriter(ctime int64) *DynamicIndexWriter {
	return &DynamicIndexWriter{
		header: DynamicIndexHeader{
			Magic: MagicDynamicChunkIndex,
			UUID:  generateUUID(),
			Ctime: ctime,
		},
		entries: make([]DynamicEntry, 0, 256),
	}
}

// Add appends an entry with the given end offset and digest.
func (w *DynamicIndexWriter) Add(endOffset uint64, digest [32]byte) {
	w.entries = append(w.entries, DynamicEntry{
		EndOffset: endOffset,
		Digest:    digest,
	})
	w.cached = false
}

// LastEndOffset returns the end offset of the last entry, which is the total
// virtual size of the indexed stream. Returns 0 if there are no entries.
func (w *DynamicIndexWriter) LastEndOffset() uint64 {
	if len(w.entries) == 0 {
		return 0
	}
	return w.entries[len(w.entries)-1].EndOffset
}

// Csum returns the SHA-256 checksum over all entry data (end_offset || digest pairs).
// This matches PBS's compute_csum() and is the checksum stored in the manifest.
// The result is cached and invalidated by Add().
func (w *DynamicIndexWriter) Csum() [32]byte {
	if !w.cached {
		w.csum, _ = w.computeCsum()
		w.cached = true
	}
	return w.csum
}

// Finish writes the complete index and returns the raw bytes.
func (w *DynamicIndexWriter) Finish() ([]byte, error) {
	w.header.IndexCsum = w.Csum()

	// Generate random UUID matching PBS's Uuid::generate() (v4 random).
	if _, err := rand.Read(w.header.UUID[:]); err != nil {
		return nil, fmt.Errorf("generate uuid: %w", err)
	}

	size := IndexHeaderSize + len(w.entries)*DynamicEntrySize
	buf := make([]byte, IndexHeaderSize, size)
	w.header.MarshalTo(buf[:IndexHeaderSize])

	var entryBuf [DynamicEntrySize]byte
	for _, e := range w.entries {
		binary.LittleEndian.PutUint64(entryBuf[0:8], e.EndOffset)
		copy(entryBuf[8:40], e.Digest[:])
		buf = append(buf, entryBuf[:]...)
	}

	return buf, nil
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
	h.Sum(sum[:0])
	return sum, uint64(len(w.entries) * DynamicEntrySize)
}
