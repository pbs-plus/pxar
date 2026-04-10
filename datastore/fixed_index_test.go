package datastore

import (
	"bytes"
	"crypto/sha256"
	"testing"
)

func mustFixedWriter(t *testing.T, ctime int64, size, chunkSize uint64) *FixedIndexWriter {
	t.Helper()
	w, err := NewFixedIndexWriter(ctime, size, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestFixedIndexWriteReadRoundTrip(t *testing.T) {
	size := uint64(10 << 20)
	chunkSize := uint64(64 << 10)
	digest1 := sha256.Sum256([]byte("chunk0"))
	digest2 := sha256.Sum256([]byte("chunk1"))

	w := mustFixedWriter(t, 1700000000, size, chunkSize)
	w.Set(0, digest1)
	w.Set(1, digest2)

	raw, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.HasPrefix(raw, MagicFixedChunkIndex[:]) {
		t.Error("missing fixed index magic")
	}

	r, err := ReadFixedIndex(raw)
	if err != nil {
		t.Fatal(err)
	}

	if r.Count() != 160 {
		t.Errorf("count = %d, want 160", r.Count())
	}

	d0, ok := r.IndexDigest(0)
	if !ok || d0 != digest1 {
		t.Errorf("digest[0] mismatch")
	}

	d1, ok := r.IndexDigest(1)
	if !ok || d1 != digest2 {
		t.Errorf("digest[1] mismatch")
	}
}

func TestFixedIndexChunkFromOffset(t *testing.T) {
	size := uint64(10 << 20)
	chunkSize := uint64(64 << 10)

	w := mustFixedWriter(t, 1700000000, size, chunkSize)
	raw, _ := w.Finish()
	r, _ := ReadFixedIndex(raw)

	tests := []struct {
		offset  uint64
		wantIdx int
		wantOK  bool
	}{
		{0, 0, true},
		{1, 0, true},
		{65535, 0, true},
		{65536, 1, true},
		{131071, 1, true},
		{131072, 2, true},
		{size - 1, 159, true},
		{size, 0, false},
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

func TestFixedIndexChunkInfo(t *testing.T) {
	w := mustFixedWriter(t, 1700000000, 256, 64)
	for i := range 4 {
		w.Set(i, sha256.Sum256([]byte{byte(i)}))
	}
	raw, _ := w.Finish()
	r, _ := ReadFixedIndex(raw)

	info, ok := r.ChunkInfo(0)
	if !ok {
		t.Fatal("expected chunk info for index 0")
	}
	if info.Start != 0 || info.End != 64 {
		t.Errorf("chunk 0: [%d, %d), want [0, 64)", info.Start, info.End)
	}

	info, ok = r.ChunkInfo(3)
	if !ok {
		t.Fatal("expected chunk info for index 3")
	}
	if info.Start != 192 || info.End != 256 {
		t.Errorf("chunk 3: [%d, %d), want [192, 256)", info.Start, info.End)
	}
}

func TestFixedIndexPartialLastChunk(t *testing.T) {
	w := mustFixedWriter(t, 1700000000, 100, 64)
	raw, _ := w.Finish()
	r, _ := ReadFixedIndex(raw)

	if r.Count() != 2 {
		t.Errorf("count = %d, want 2", r.Count())
	}

	info, ok := r.ChunkInfo(1)
	if !ok {
		t.Fatal("expected chunk info for index 1")
	}
	if info.Start != 64 || info.End != 100 {
		t.Errorf("last chunk: [%d, %d), want [64, 100)", info.Start, info.End)
	}
}

func TestFixedIndexCTime(t *testing.T) {
	ctime := int64(1700000000)
	w := mustFixedWriter(t, ctime, 1024, 512)
	raw, _ := w.Finish()
	r, _ := ReadFixedIndex(raw)

	if r.CTime() != ctime {
		t.Errorf("ctime = %d, want %d", r.CTime(), ctime)
	}
}

func TestFixedIndexInvalidChunkSize(t *testing.T) {
	_, err := NewFixedIndexWriter(1700000000, 1024, 100)
	if err == nil {
		t.Error("expected error for non-power-of-2 chunk size")
	}
}

func TestFixedIndexInvalidData(t *testing.T) {
	_, err := ReadFixedIndex([]byte{1, 2, 3})
	if err == nil {
		t.Error("expected error for too-short data")
	}

	buf := make([]byte, IndexHeaderSize)
	copy(buf[0:8], MagicDynamicChunkIndex[:])
	_, err = ReadFixedIndex(buf)
	if err == nil {
		t.Error("expected error for wrong magic")
	}
}

func TestFixedIndexSize(t *testing.T) {
	w := mustFixedWriter(t, 1700000000, 1024, 512)
	raw, _ := w.Finish()

	expectedSize := IndexHeaderSize + 2*FixedDigestSize
	if len(raw) != expectedSize {
		t.Errorf("file size = %d, want %d", len(raw), expectedSize)
	}
}

func TestFixedIndexComputeCsum(t *testing.T) {
	w := mustFixedWriter(t, 1700000000, 1024, 512)
	d := sha256.Sum256([]byte("test"))
	w.Set(0, d)
	raw, _ := w.Finish()
	r, _ := ReadFixedIndex(raw)

	csum, sz := r.ComputeCsum()
	if sz != 64 {
		t.Errorf("csum size = %d, want 64", sz)
	}
	_ = csum
}
