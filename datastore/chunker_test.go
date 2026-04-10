package datastore

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"testing"

	"github.com/sonroyaalmerol/pxar/buzhash"
)

func newTestStoreChunker(t *testing.T, compress bool) (*StoreChunker, *ChunkStore) {
	t.Helper()
	cs, _ := newTestChunkStore(t)
	config, err := buzhash.NewConfig(4096)
	if err != nil {
		t.Fatal(err)
	}
	return NewStoreChunker(cs, config, compress), cs
}

func TestStoreChunkerBasic(t *testing.T) {
	sc, _ := newTestStoreChunker(t, false)

	data := make([]byte, 100<<10)
	rand.Read(data)

	results, _, err := sc.ChunkStream(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	if len(results) == 0 {
		t.Fatal("expected at least one chunk")
	}

	total := 0
	for _, r := range results {
		if r.Size == 0 {
			t.Error("chunk has zero size")
		}
		total += r.Size
	}

	if total != len(data) {
		t.Errorf("total chunk bytes = %d, want %d", total, len(data))
	}

	// Verify digests are non-zero
	for i, r := range results {
		if r.Digest == [32]byte{} {
			t.Errorf("chunk %d has zero digest", i)
		}
	}
}

func TestStoreChunkerIndexRoundTrip(t *testing.T) {
	sc, _ := newTestStoreChunker(t, false)

	data := make([]byte, 50<<10)
	rand.Read(data)

	results, idxWriter, err := sc.ChunkStream(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	raw, err := idxWriter.Finish()
	if err != nil {
		t.Fatal(err)
	}

	reader, err := ReadDynamicIndex(raw)
	if err != nil {
		t.Fatal(err)
	}

	if reader.Count() != len(results) {
		t.Fatalf("index count = %d, want %d", reader.Count(), len(results))
	}

	// Verify each chunk's digest matches
	for i, r := range results {
		info, ok := reader.ChunkInfo(i)
		if !ok {
			t.Fatalf("ChunkInfo(%d) not found", i)
		}
		if info.Digest != r.Digest {
			t.Errorf("chunk %d: index digest mismatch", i)
		}
	}
}

func TestStoreChunkerChunkStoreIntegration(t *testing.T) {
	sc, _ := newTestStoreChunker(t, false)

	data := make([]byte, 50<<10)
	rand.Read(data)

	results, _, err := sc.ChunkStream(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	// Reconstruct original data by loading each chunk and decoding blob
	var reconstructed bytes.Buffer
	for i, r := range results {
		stored, err := sc.store.LoadChunk(r.Digest)
		if err != nil {
			t.Fatalf("LoadChunk %d: %v", i, err)
		}

		decoded, err := DecodeBlob(stored)
		if err != nil {
			t.Fatalf("DecodeBlob %d: %v", i, err)
		}

		reconstructed.Write(decoded)
	}

	if !bytes.Equal(reconstructed.Bytes(), data) {
		t.Error("reconstructed data doesn't match original")
	}
}

func TestStoreChunkerDeduplication(t *testing.T) {
	sc, _ := newTestStoreChunker(t, false)

	data := make([]byte, 20<<10)
	rand.Read(data)

	// First pass
	results1, _, err := sc.ChunkStream(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	// Second pass with same data
	results2, _, err := sc.ChunkStream(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	// All chunks should already exist on second pass
	for i, r := range results2 {
		if !r.Exists {
			t.Errorf("chunk %d should already exist on second pass", i)
		}
	}

	// Same number of chunks and same digests
	if len(results1) != len(results2) {
		t.Fatalf("different chunk counts: %d vs %d", len(results1), len(results2))
	}
	for i := range results1 {
		if results1[i].Digest != results2[i].Digest {
			t.Errorf("chunk %d: digest mismatch between passes", i)
		}
	}
}

func TestStoreChunkerCompressed(t *testing.T) {
	sc, _ := newTestStoreChunker(t, true)

	// Use compressible data
	data := bytes.Repeat([]byte("abcdefghij"), 5000) // 50KB of repeating pattern

	results, _, err := sc.ChunkStream(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	if len(results) == 0 {
		t.Fatal("expected at least one chunk")
	}

	// Load and verify each chunk can be decoded
	var reconstructed bytes.Buffer
	for i, r := range results {
		stored, err := sc.store.LoadChunk(r.Digest)
		if err != nil {
			t.Fatalf("LoadChunk %d: %v", i, err)
		}

		// Verify it's a valid blob (should be compressed given repeating data)
		decoded, err := DecodeBlob(stored)
		if err != nil {
			t.Fatalf("DecodeBlob %d: %v", i, err)
		}

		reconstructed.Write(decoded)
	}

	if !bytes.Equal(reconstructed.Bytes(), data) {
		t.Error("reconstructed data doesn't match original")
	}
}

func TestStoreChunkerCallback(t *testing.T) {
	sc, _ := newTestStoreChunker(t, false)

	data := make([]byte, 50<<10)
	rand.Read(data)

	var offsets []uint64
	var sizes []int

	_, _, err := sc.ChunkStreamCallback(bytes.NewReader(data), func(r ChunkResult) error {
		offsets = append(offsets, r.Offset)
		sizes = append(sizes, r.Size)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(offsets) == 0 {
		t.Fatal("callback was never called")
	}

	// First offset should be 0
	if offsets[0] != 0 {
		t.Errorf("first offset = %d, want 0", offsets[0])
	}

	// Offsets should be monotonically increasing
	for i := 1; i < len(offsets); i++ {
		if offsets[i] <= offsets[i-1] {
			t.Errorf("offsets not monotonic: %d <= %d", offsets[i], offsets[i-1])
		}
	}

	// Last offset + last size should equal total data size
	last := len(offsets) - 1
	if offsets[last]+uint64(sizes[last]) != uint64(len(data)) {
		t.Errorf("total = %d, want %d", offsets[last]+uint64(sizes[last]), len(data))
	}
}

func TestStoreChunkerCallbackEarlyStop(t *testing.T) {
	sc, _ := newTestStoreChunker(t, false)

	data := make([]byte, 100<<10)
	rand.Read(data)

	stopErr := fmt.Errorf("stop")
	count := 0

	results, _, err := sc.ChunkStreamCallback(bytes.NewReader(data), func(r ChunkResult) error {
		count++
		if count == 3 {
			return stopErr
		}
		return nil
	})

	if err != stopErr {
		t.Errorf("error = %v, want %v", err, stopErr)
	}
	if len(results) != 3 {
		t.Errorf("got %d results, want 3", len(results))
	}
}

func TestStoreChunkerEmptyInput(t *testing.T) {
	sc, _ := newTestStoreChunker(t, false)

	results, _, err := sc.ChunkStream(bytes.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Errorf("got %d chunks, want 0 for empty input", len(results))
	}
}

func TestStoreChunkerSmallInput(t *testing.T) {
	sc, _ := newTestStoreChunker(t, false)

	data := []byte("hello")

	results, _, err := sc.ChunkStream(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("got %d chunks, want 1", len(results))
	}
	if results[0].Size != 5 {
		t.Errorf("chunk size = %d, want 5", results[0].Size)
	}

	// Verify digest matches direct SHA-256
	expected := sha256.Sum256(data)
	if results[0].Digest != expected {
		t.Error("digest mismatch")
	}
}

func TestStoreChunkerDeterminism(t *testing.T) {
	sc1, _ := newTestStoreChunker(t, false)
	sc2, _ := newTestStoreChunker(t, false)

	data := make([]byte, 50<<10)
	rand.Read(data)

	r1, _, err := sc1.ChunkStream(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	r2, _, err := sc2.ChunkStream(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	if len(r1) != len(r2) {
		t.Fatalf("different chunk counts: %d vs %d", len(r1), len(r2))
	}

	for i := range r1 {
		if r1[i].Digest != r2[i].Digest {
			t.Errorf("chunk %d: digest mismatch", i)
		}
		if r1[i].Size != r2[i].Size {
			t.Errorf("chunk %d: size %d vs %d", i, r1[i].Size, r2[i].Size)
		}
	}
}

func TestStoreChunkerLoadVerifyChunks(t *testing.T) {
	sc, _ := newTestStoreChunker(t, false)

	// Create known data so we can verify chunk content
	data := make([]byte, 30<<10)
	for i := range data {
		data[i] = byte(i % 256)
	}

	results, _, err := sc.ChunkStream(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	// Verify each chunk's raw data via SHA-256
	for i, r := range results {
		stored, err := sc.store.LoadChunk(r.Digest)
		if err != nil {
			t.Fatalf("LoadChunk %d: %v", i, err)
		}
		decoded, err := DecodeBlob(stored)
		if err != nil {
			t.Fatalf("DecodeBlob %d: %v", i, err)
		}

		digest := sha256.Sum256(decoded)
		if digest != r.Digest {
			t.Errorf("chunk %d: decoded digest doesn't match", i)
		}
	}
}

func BenchmarkStoreChunker(b *testing.B) {
	data := make([]byte, 1<<20)
	rand.Read(data)

	buf := make([]byte, len(data))
	copy(buf, data)

	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		dir := b.TempDir()
		cs, _ := NewChunkStore(dir)
		config, _ := buzhash.NewConfig(64 << 10)
		sc := NewStoreChunker(cs, config, true)
		r := bytes.NewReader(buf)
		_, _, _ = sc.ChunkStream(r)
	}
}

// Ensure io.Reader is available for the callback test.
var _ io.Reader = (*bytes.Reader)(nil)
