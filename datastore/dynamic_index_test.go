package datastore

import (
	"bytes"
	"crypto/sha256"
	"testing"
)

func TestDynamicIndexWriteReadRoundTrip(t *testing.T) {
	entries := []DynamicEntry{
		{EndOffset: 1000, Digest: sha256.Sum256([]byte("chunk0"))},
		{EndOffset: 3500, Digest: sha256.Sum256([]byte("chunk1"))},
		{EndOffset: 10000, Digest: sha256.Sum256([]byte("chunk2"))},
	}

	w := NewDynamicIndexWriter(1700000000)
	for _, e := range entries {
		w.Add(e.EndOffset, e.Digest)
	}

	raw, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}

	// Must start with correct magic
	if !bytes.HasPrefix(raw, MagicDynamicChunkIndex[:]) {
		t.Error("missing dynamic index magic")
	}

	r, err := ReadDynamicIndex(raw)
	if err != nil {
		t.Fatal(err)
	}

	if r.Count() != len(entries) {
		t.Fatalf("count = %d, want %d", r.Count(), len(entries))
	}

	for i, e := range entries {
		got := r.Entry(i)
		if got.EndOffset != e.EndOffset {
			t.Errorf("entry %d: offset = %d, want %d", i, got.EndOffset, e.EndOffset)
		}
		if got.Digest != e.Digest {
			t.Errorf("entry %d: digest mismatch", i)
		}
	}
}

func TestDynamicIndexChunkFromOffset(t *testing.T) {
	entries := []DynamicEntry{
		{EndOffset: 1000},
		{EndOffset: 3500},
		{EndOffset: 10000},
	}

	w := NewDynamicIndexWriter(1700000000)
	for _, e := range entries {
		w.Add(e.EndOffset, e.Digest)
	}
	raw, _ := w.Finish()

	r, _ := ReadDynamicIndex(raw)

	tests := []struct {
		offset   uint64
		wantIdx  int
		wantOK   bool
	}{
		{0, 0, true},
		{999, 0, true},
		{1000, 1, true},
		{3499, 1, true},
		{3500, 2, true},
		{9999, 2, true},
		{10000, 0, false}, // beyond end
	}

	for _, tt := range tests {
		idx, ok := r.ChunkFromOffset(tt.offset)
		if ok != tt.wantOK {
			t.Errorf("ChunkFromOffset(%d): ok=%v, want %v", tt.offset, ok, tt.wantOK)
		}
		if ok && idx != tt.wantIdx {
			t.Errorf("ChunkFromOffset(%d): idx=%d, want %d", tt.offset, idx, tt.wantIdx)
		}
	}
}

func TestDynamicIndexChunkInfo(t *testing.T) {
	entries := []DynamicEntry{
		{EndOffset: 1000},
		{EndOffset: 3500},
		{EndOffset: 10000},
	}

	w := NewDynamicIndexWriter(1700000000)
	for _, e := range entries {
		w.Add(e.EndOffset, e.Digest)
	}
	raw, _ := w.Finish()
	r, _ := ReadDynamicIndex(raw)

	// First chunk: offset 0 to 1000
	info, ok := r.ChunkInfo(0)
	if !ok {
		t.Fatal("expected chunk info for index 0")
	}
	if info.Start != 0 || info.End != 1000 {
		t.Errorf("chunk 0: [%d, %d), want [0, 1000)", info.Start, info.End)
	}

	// Second chunk: offset 1000 to 3500
	info, ok = r.ChunkInfo(1)
	if !ok {
		t.Fatal("expected chunk info for index 1")
	}
	if info.Start != 1000 || info.End != 3500 {
		t.Errorf("chunk 1: [%d, %d), want [1000, 3500)", info.Start, info.End)
	}
}

func TestDynamicIndexChecksumVerification(t *testing.T) {
	w := NewDynamicIndexWriter(1700000000)
	w.Add(1000, [32]byte{})
	w.Add(2000, [32]byte{})

	raw, _ := w.Finish()
	r, err := ReadDynamicIndex(raw)
	if err != nil {
		t.Fatal(err)
	}

	csum, size := r.ComputeCsum()
	if size != 80 { // 2 entries × 40 bytes
		t.Errorf("csum size = %d, want 80", size)
	}
	_ = csum
}

func TestDynamicIndexEmpty(t *testing.T) {
	w := NewDynamicIndexWriter(1700000000)
	raw, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}

	r, err := ReadDynamicIndex(raw)
	if err != nil {
		t.Fatal(err)
	}
	if r.Count() != 0 {
		t.Errorf("count = %d, want 0", r.Count())
	}
}

func TestDynamicIndexInvalidData(t *testing.T) {
	// Too short
	_, err := ReadDynamicIndex([]byte{1, 2, 3})
	if err == nil {
		t.Error("expected error for too-short data")
	}

	// Wrong magic
	buf := make([]byte, IndexHeaderSize)
	copy(buf[0:8], MagicFixedChunkIndex[:]) // wrong magic
	_, err = ReadDynamicIndex(buf)
	if err == nil {
		t.Error("expected error for wrong magic")
	}
}

func TestDynamicIndexCTime(t *testing.T) {
	ctime := int64(1700000000)
	w := NewDynamicIndexWriter(ctime)
	raw, _ := w.Finish()
	r, _ := ReadDynamicIndex(raw)

	if r.CTime() != ctime {
		t.Errorf("ctime = %d, want %d", r.CTime(), ctime)
	}
}

func TestDynamicIndexIndexBytes(t *testing.T) {
	w := NewDynamicIndexWriter(1700000000)
	w.Add(1000, [32]byte{})
	w.Add(5000, [32]byte{})
	w.Add(10000, [32]byte{})
	raw, _ := w.Finish()
	r, _ := ReadDynamicIndex(raw)

	if r.IndexBytes() != 10000 {
		t.Errorf("IndexBytes = %d, want 10000", r.IndexBytes())
	}
}

func TestDynamicIndexSize(t *testing.T) {
	w := NewDynamicIndexWriter(1700000000)
	w.Add(1000, [32]byte{})
	w.Add(5000, [32]byte{})
	raw, _ := w.Finish()

	expectedSize := IndexHeaderSize + 2*DynamicEntrySize
	if len(raw) != expectedSize {
		t.Errorf("file size = %d, want %d", len(raw), expectedSize)
	}
}
