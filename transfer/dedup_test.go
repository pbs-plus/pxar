package transfer_test

import (
	"bytes"
	"crypto/sha256"
	"io"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
)

// createChunkedArchive creates a v1 chunked archive in a temp ChunkStore.
// Returns the store, index data, and cleans up via t.Cleanup.
func createChunkedArchive(t *testing.T, files map[string]string) (*datastore.ChunkStore, []byte) {
	t.Helper()

	dir := t.TempDir()
	store, err := datastore.NewChunkStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	config, _ := buzhash.NewConfig(64 << 10) // 64KB avg for small test data

	// Encode archive to buffer
	var buf bytes.Buffer
	rootMeta := pxar.DirMetadata(0o755).Build()
	enc := encoder.NewEncoder(&buf, nil, &rootMeta, nil)

	// Sort for determinism but order doesn't matter for correctness
	for name, content := range files {
		_, err := enc.AddFile(fileMeta(0o644, 0, 0), name, []byte(content))
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	// Chunk and store
	chunker := datastore.NewStoreChunker(store, config, false)
	results, idx, err := chunker.ChunkStream(bytes.NewReader(buf.Bytes()), nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = results

	idxData, err := idx.Finish()
	if err != nil {
		t.Fatal(err)
	}

	return store, idxData
}

// createSplitChunkedArchive creates a v2 split chunked archive in a temp ChunkStore.
// Returns the store, meta index data, payload index data.
func createSplitChunkedArchive(t *testing.T, files map[string]string) (*datastore.ChunkStore, []byte, []byte) {
	t.Helper()

	dir := t.TempDir()
	store, err := datastore.NewChunkStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	config, _ := buzhash.NewConfig(64 << 10)

	var metaBuf, payloadBuf bytes.Buffer
	rootMeta := pxar.DirMetadata(0o755).Build()
	enc := encoder.NewEncoder(&metaBuf, &payloadBuf, &rootMeta, nil)

	for name, content := range files {
		_, err := enc.AddFile(fileMeta(0o644, 0, 0), name, []byte(content))
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	// Chunk metadata stream
	metaChunker := datastore.NewStoreChunker(store, config, false)
	_, metaIdx, err := metaChunker.ChunkStream(bytes.NewReader(metaBuf.Bytes()), nil)
	if err != nil {
		t.Fatal(err)
	}
	metaIdxData, err := metaIdx.Finish()
	if err != nil {
		t.Fatal(err)
	}

	// Chunk payload stream
	payloadChunker := datastore.NewStoreChunker(store, config, false)
	_, payloadIdx, err := payloadChunker.ChunkStream(bytes.NewReader(payloadBuf.Bytes()), nil)
	if err != nil {
		t.Fatal(err)
	}
	payloadIdxData, err := payloadIdx.Finish()
	if err != nil {
		t.Fatal(err)
	}

	return store, metaIdxData, payloadIdxData
}

// --- ReadSeeker tests ---

func TestReadSeekerBasicRead(t *testing.T) {
	store, idxData := createChunkedArchive(t, map[string]string{
		"hello.txt": "hello world",
	})
	source := datastore.NewChunkStoreSource(store)

	idx, err := datastore.ParseDynamicIndex(idxData)
	if err != nil {
		t.Fatal(err)
	}

	reader := transfer.NewReadSeeker(idx, source, 0)
	defer reader.Close()

	// Read all data
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(reader); err != nil {
		t.Fatal(err)
	}

	if buf.Len() == 0 {
		t.Error("expected non-empty data from ReadSeeker")
	}
}

func TestReadSeekerSeekAndRead(t *testing.T) {
	store, idxData := createChunkedArchive(t, map[string]string{
		"file.txt": "some content here",
	})
	source := datastore.NewChunkStoreSource(store)

	idx, err := datastore.ParseDynamicIndex(idxData)
	if err != nil {
		t.Fatal(err)
	}

	reader := transfer.NewReadSeeker(idx, source, 0)
	defer reader.Close()

	// Seek to start
	pos, err := reader.Seek(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if pos != 0 {
		t.Errorf("seek position = %d, want 0", pos)
	}

	// Read a small chunk
	p := make([]byte, 10)
	n, err := reader.Read(p)
	if err != nil {
		t.Fatal(err)
	}
	if n != 10 {
		t.Errorf("read %d bytes, want 10", n)
	}
}

func TestReadSeekerCaching(t *testing.T) {
	store, idxData := createChunkedArchive(t, map[string]string{
		"file.txt": "cached content test",
	})
	source := datastore.NewChunkStoreSource(store)

	idx, err := datastore.ParseDynamicIndex(idxData)
	if err != nil {
		t.Fatal(err)
	}

	reader := transfer.NewReadSeeker(idx, source, 4)
	defer reader.Close()

	// Read entire stream twice; second pass should hit cache
	var first bytes.Buffer
	if _, err := first.ReadFrom(reader); err != nil {
		t.Fatal(err)
	}

	// Seek back and read again
	if _, err := reader.Seek(0, 0); err != nil {
		t.Fatal(err)
	}

	var second bytes.Buffer
	if _, err := second.ReadFrom(reader); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(first.Bytes(), second.Bytes()) {
		t.Error("second read produced different data than first")
	}
}

func TestReadSeekerMatchesEager(t *testing.T) {
	store, idxData := createChunkedArchive(t, map[string]string{
		"file1.txt": "content one",
		"file2.txt": "content two",
	})
	source := datastore.NewChunkStoreSource(store)

	// Eager reconstruction
	eagerReader, err := transfer.NewChunkedReaderEager(idxData, source)
	if err != nil {
		t.Fatal(err)
	}
	defer eagerReader.Close()

	// Lazy reconstruction
	lazyReader, err := transfer.NewChunkedReader(idxData, source)
	if err != nil {
		t.Fatal(err)
	}
	defer lazyReader.Close()

	// Both should find the same files
	entry1, err := eagerReader.Lookup("/file1.txt")
	if err != nil {
		t.Fatal(err)
	}
	entry2, err := lazyReader.Lookup("/file1.txt")
	if err != nil {
		t.Fatal(err)
	}

	r1, err := eagerReader.ReadFileContentReader(entry1)
	if err != nil {
		t.Fatal(err)
	}
	defer r1.Close()
	content1, err := io.ReadAll(r1)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := lazyReader.ReadFileContentReader(entry2)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	content2, err := io.ReadAll(r2)
	if err != nil {
		t.Fatal(err)
	}

	if string(content1) != string(content2) {
		t.Errorf("eager = %q, lazy = %q", content1, content2)
	}
	if string(content1) != "content one" {
		t.Errorf("content = %q, want %q", content1, "content one")
	}
}

// --- SplitReader lazy tests ---

func TestSplitReaderLazyMatchesEager(t *testing.T) {
	store, metaIdxData, payloadIdxData := createSplitChunkedArchive(t, map[string]string{
		"data.bin": "payload data",
	})
	source := datastore.NewChunkStoreSource(store)

	eagerReader, err := transfer.NewSplitReaderEager(metaIdxData, payloadIdxData, source)
	if err != nil {
		t.Fatal(err)
	}
	defer eagerReader.Close()

	lazyReader, err := transfer.NewSplitReader(metaIdxData, payloadIdxData, source)
	if err != nil {
		t.Fatal(err)
	}
	defer lazyReader.Close()

	eagerEntry, err := eagerReader.Lookup("/data.bin")
	if err != nil {
		t.Fatal(err)
	}
	lazyEntry, err := lazyReader.Lookup("/data.bin")
	if err != nil {
		t.Fatal(err)
	}

	r3, err := eagerReader.ReadFileContentReader(eagerEntry)
	if err != nil {
		t.Fatal(err)
	}
	defer r3.Close()
	eagerContent, err := io.ReadAll(r3)
	if err != nil {
		t.Fatal(err)
	}
	r4, err := lazyReader.ReadFileContentReader(lazyEntry)
	if err != nil {
		t.Fatal(err)
	}
	defer r4.Close()
	lazyContent, err := io.ReadAll(r4)
	if err != nil {
		t.Fatal(err)
	}

	if string(eagerContent) != string(lazyContent) {
		t.Errorf("eager = %q, lazy = %q", eagerContent, lazyContent)
	}
	if string(eagerContent) != "payload data" {
		t.Errorf("content = %q, want %q", eagerContent, "payload data")
	}
}

// --- DedupWriter tests ---

func TestDedupWriterRoundTrip(t *testing.T) {
	store, _, payloadIdxData := createSplitChunkedArchive(t, map[string]string{
		"file.txt": "original content",
	})
	source := datastore.NewChunkStoreSource(store)

	// Read source payload index
	payloadIdx, err := datastore.ParseDynamicIndex(payloadIdxData)
	if err != nil {
		t.Fatal(err)
	}

	config, _ := buzhash.NewConfig(64 << 10)

	// Write a new archive with the same data (dedup should kick in)
	writer := transfer.NewDedupWriter(store, source, config, false, payloadIdx)
	rootMeta := pxar.DirMetadata(0o755).Build()
	if err := writer.Begin(&rootMeta, transfer.Options{Format: format.FormatVersion2}); err != nil {
		t.Fatal(err)
	}

	entry := &pxar.Entry{
		Path:     "file.txt",
		Kind:     pxar.KindFile,
		Metadata: pxar.FileMetadata(0o644).Owner(0, 0).Build(),
		FileSize: uint64(len("original content")),
	}
	if err := writer.WriteEntry(entry, []byte("original content")); err != nil {
		t.Fatal(err)
	}

	if err := writer.Finish(); err != nil {
		t.Fatal(err)
	}

	// Verify the new archive is readable
	newMetaIdxData := writer.MetaIndexData()
	newPayloadIdxData := writer.PayloadIndexData()

	newReader, err := transfer.NewSplitReader(newMetaIdxData, newPayloadIdxData, source)
	if err != nil {
		t.Fatal(err)
	}
	defer newReader.Close()

	newEntry, err := newReader.Lookup("/file.txt")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}

	r5, err := newReader.ReadFileContentReader(newEntry)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	defer r5.Close()
	content, err := io.ReadAll(r5)
	if err != nil {
		t.Fatalf("read content: %v", err)
	}
	if string(content) != "original content" {
		t.Errorf("content = %q, want %q", content, "original content")
	}

	// Check dedup stats
	hits, total := writer.DedupStats()
	if total == 0 {
		t.Error("expected at least 1 payload chunk")
	}
	// Since content is the same, chunks should be dedup hits
	if hits > 0 {
		t.Logf("dedup: %d/%d chunks reused", hits, total)
	}
}

// --- MapFileToPayloadChunks tests ---

func TestMapFileToPayloadChunksBasic(t *testing.T) {
	// Build a payload index manually with known layout
	// Chunk 0: bytes 0-999, Chunk 1: bytes 1000-1999
	idx := datastore.NewDynamicIndexWriter(0)

	digest0 := sha256.Sum256([]byte("chunk0"))
	digest1 := sha256.Sum256([]byte("chunk1"))
	idx.Add(1000, digest0)
	idx.Add(2000, digest1)

	idxData, err := idx.Finish()
	if err != nil {
		t.Fatal(err)
	}

	reader, err := datastore.ParseDynamicIndex(idxData)
	if err != nil {
		t.Fatal(err)
	}

	// Map a file starting at payload offset 0, size 500
	// Content starts at offset 16 (PXARPayload header), so content range is 16-516.
	// Chunk 0 covers 0-1000, so overlap is 16-516 — not the full chunk (0-1000).
	ranges := transfer.MapFileToPayloadChunks(reader, 0, 500)
	if len(ranges) != 1 {
		t.Fatalf("expected 1 range, got %d", len(ranges))
	}
	if ranges[0].ChunkIndex != 0 {
		t.Errorf("chunk index = %d, want 0", ranges[0].ChunkIndex)
	}
	if ranges[0].IsFullChunk {
		t.Error("expected NOT full chunk since content starts after PXARPayload header at offset 16")
	}
	if ranges[0].ContentStart != 16 {
		t.Errorf("content start = %d, want 16", ranges[0].ContentStart)
	}
}

func TestMapFileToPayloadChunksSpanning(t *testing.T) {
	idx := datastore.NewDynamicIndexWriter(0)

	digest0 := sha256.Sum256([]byte("chunk0"))
	digest1 := sha256.Sum256([]byte("chunk1"))
	idx.Add(1000, digest0)
	idx.Add(2000, digest1)

	idxData, err := idx.Finish()
	if err != nil {
		t.Fatal(err)
	}

	reader, err := datastore.ParseDynamicIndex(idxData)
	if err != nil {
		t.Fatal(err)
	}

	// File starting at payload offset 0, size 1500 (spans both chunks)
	// Content starts at offset 16 (PXARPayload header)
	// Content range: 16 to 16+1500 = 1516
	// Chunk 0 covers 0-1000 (overlap: 16-1000, not full)
	// Chunk 1 covers 1000-2000 (overlap: 1000-1516, not full)
	ranges := transfer.MapFileToPayloadChunks(reader, 0, 1500)
	if len(ranges) != 2 {
		t.Fatalf("expected 2 ranges, got %d", len(ranges))
	}
	if ranges[0].ChunkIndex != 0 {
		t.Errorf("first chunk index = %d, want 0", ranges[0].ChunkIndex)
	}
	if ranges[1].ChunkIndex != 1 {
		t.Errorf("second chunk index = %d, want 1", ranges[1].ChunkIndex)
	}
}

func TestMapFileToPayloadChunksNilIndex(t *testing.T) {
	ranges := transfer.MapFileToPayloadChunks(nil, 0, 100)
	if len(ranges) != 0 {
		t.Errorf("expected nil ranges for nil index, got %d", len(ranges))
	}
}

func TestMapFileToPayloadChunksEmptyFile(t *testing.T) {
	idx := datastore.NewDynamicIndexWriter(0)
	digest0 := sha256.Sum256([]byte("chunk0"))
	idx.Add(1000, digest0)

	idxData, err := idx.Finish()
	if err != nil {
		t.Fatal(err)
	}

	reader, err := datastore.ParseDynamicIndex(idxData)
	if err != nil {
		t.Fatal(err)
	}

	ranges := transfer.MapFileToPayloadChunks(reader, 0, 0)
	if len(ranges) != 0 {
		t.Errorf("expected no ranges for empty file, got %d", len(ranges))
	}
}

// --- ComputeContentDigest tests ---

func TestComputeContentDigestCorrectness(t *testing.T) {
	// Use a real split archive so the payload index properly contains
	// the PXARPayload header and file content.
	store, _, payloadIdxData := createSplitChunkedArchive(t, map[string]string{
		"file.txt": "hello world",
	})

	// Read file content from the split archive using eager reader
	// to get the ground truth
	source := datastore.NewChunkStoreSource(store)
	metaIdxData, _ := createSplitChunkedArchiveMeta(t, store, map[string]string{
		"file.txt": "hello world",
	})

	reader, err := transfer.NewSplitReaderEager(metaIdxData, payloadIdxData, source)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	entry, err := reader.Lookup("/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	r6, err := reader.ReadFileContentReader(entry)
	if err != nil {
		t.Fatal(err)
	}
	defer r6.Close()
	content, err := io.ReadAll(r6)
	if err != nil {
		t.Fatal(err)
	}
	expected := sha256.Sum256(content)

	// Now use ComputeContentDigest with the payload index
	payloadIdx, err := datastore.ParseDynamicIndex(payloadIdxData)
	if err != nil {
		t.Fatal(err)
	}

	result, err := transfer.ComputeContentDigest(source, payloadIdx, entry.PayloadOffset, uint64(len(content)))
	if err != nil {
		t.Fatalf("ComputeContentDigest: %v", err)
	}
	if result != expected {
		t.Errorf("digest mismatch: got %x, want %x", result[:8], expected[:8])
	}
}

// createSplitChunkedArchiveMeta re-creates just the metadata index data.
func createSplitChunkedArchiveMeta(t *testing.T, store *datastore.ChunkStore, files map[string]string) ([]byte, []byte) {
	t.Helper()

	config, _ := buzhash.NewConfig(64 << 10)

	var metaBuf, payloadBuf bytes.Buffer
	rootMeta := pxar.DirMetadata(0o755).Build()
	enc := encoder.NewEncoder(&metaBuf, &payloadBuf, &rootMeta, nil)

	for name, content := range files {
		_, err := enc.AddFile(fileMeta(0o644, 0, 0), name, []byte(content))
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	metaChunker := datastore.NewStoreChunker(store, config, false)
	_, metaIdx, err := metaChunker.ChunkStream(bytes.NewReader(metaBuf.Bytes()), nil)
	if err != nil {
		t.Fatal(err)
	}
	metaIdxData, err := metaIdx.Finish()
	if err != nil {
		t.Fatal(err)
	}

	return metaIdxData, nil
}

// --- ReferenceSourcePayloadChunks tests ---

func TestReferenceSourcePayloadChunks(t *testing.T) {
	store, _, payloadIdxData := createSplitChunkedArchive(t, map[string]string{
		"file.txt": "test content",
	})

	payloadIdx, err := datastore.ParseDynamicIndex(payloadIdxData)
	if err != nil {
		t.Fatal(err)
	}

	config, _ := buzhash.NewConfig(64 << 10)
	source := datastore.NewChunkStoreSource(store)

	writer := transfer.NewDedupWriter(store, source, config, false, payloadIdx)
	// Before Begin, calling ReferenceSourcePayloadChunks should be safe
	writer.ReferenceSourcePayloadChunks()
}

// --- DynamicIndex round-trip test ---

func TestDynamicIndexRoundTrip(t *testing.T) {
	idx := datastore.NewDynamicIndexWriter(12345)

	d1 := sha256.Sum256([]byte("data1"))
	d2 := sha256.Sum256([]byte("data2"))
	d3 := sha256.Sum256([]byte("data3"))

	idx.Add(1000, d1)
	idx.Add(2500, d2)
	idx.Add(5000, d3)

	data, err := idx.Finish()
	if err != nil {
		t.Fatal(err)
	}

	reader, err := datastore.ParseDynamicIndex(data)
	if err != nil {
		t.Fatal(err)
	}

	if reader.Count() != 3 {
		t.Fatalf("count = %d, want 3", reader.Count())
	}
	if reader.CTime() != 12345 {
		t.Errorf("ctime = %d, want 12345", reader.CTime())
	}
	if reader.IndexBytes() != 5000 {
		t.Errorf("index bytes = %d, want 5000", reader.IndexBytes())
	}

	// Verify ChunkFromOffset
	chunk, ok := reader.ChunkFromOffset(0)
	if !ok || chunk != 0 {
		t.Errorf("offset 0: chunk = %d, ok = %v", chunk, ok)
	}
	chunk, ok = reader.ChunkFromOffset(1500)
	if !ok || chunk != 1 {
		t.Errorf("offset 1500: chunk = %d, ok = %v", chunk, ok)
	}
	chunk, ok = reader.ChunkFromOffset(3000)
	if !ok || chunk != 2 {
		t.Errorf("offset 3000: chunk = %d, ok = %v", chunk, ok)
	}
}

// TestLookupDynamicEntriesPinsChunkIdentity mirrors Rust's
// lookup_dynamic_entries_pins_chunk_identity test. Verifies that dedup-collided
// chunks (same digest, different positions) are NOT treated as the same chunk.
// This pins that MapFileToPayloadChunks returns position-based ranges, not
// digest-based — any range-relative substitute would alias dedup-collided entries
// and resurrect the backwards-PXAR_PAYLOAD_REF bug class.
func TestLookupDynamicEntriesPinsChunkIdentity(t *testing.T) {
	idx := datastore.NewDynamicIndexWriter(0)

	var digestA [32]byte
	for i := range digestA {
		digestA[i] = 0xAA
	}
	var digestB [32]byte
	for i := range digestB {
		digestB[i] = 0xBB
	}

	// chunk 0: ends at 1024, digest A
	idx.Add(1024, digestA)
	// chunk 1: ends at 2048, digest B
	idx.Add(2048, digestB)
	// chunk 2: ends at 3072, digest A again — dedup collision (same digest, distinct position)
	idx.Add(3072, digestA)

	data, err := idx.Finish()
	if err != nil {
		t.Fatal(err)
	}

	reader, err := datastore.ParseDynamicIndex(data)
	if err != nil {
		t.Fatal(err)
	}

	if reader.Count() != 3 {
		t.Fatalf("count = %d, want 3", reader.Count())
	}

	// Verify end offsets match Rust expectations
	c0, _ := reader.ChunkInfo(0)
	c1, _ := reader.ChunkInfo(1)
	c2, _ := reader.ChunkInfo(2)

	if c0.End != 1024 {
		t.Errorf("chunk 0 end = %d, want 1024", c0.End)
	}
	if c1.End != 2048 {
		t.Errorf("chunk 1 end = %d, want 2048", c1.End)
	}
	if c2.End != 3072 {
		t.Errorf("chunk 2 end = %d, want 3072", c2.End)
	}

	// Dedup collision: same digest but different positions
	if c0.Digest != c2.Digest {
		t.Error("chunk 0 and 2 should have same digest (dedup collision)")
	}

	// They are NOT the same chunk — position-based identity
	if c0 == c2 {
		t.Error("chunk 0 and 2 must not be pointer-equivalent")
	}
	if c0.Start == c2.Start && c0.End == c2.End {
		t.Error("chunk 0 and 2 must differ in position")
	}

	// MapFileToPayloadChunks over the full range returns 3 distinct ranges
	ranges := transfer.MapFileToPayloadChunks(reader, 0, 3072)
	if len(ranges) != 3 {
		t.Fatalf("MapFileToPayloadChunks returned %d ranges, want 3", len(ranges))
	}

	// Each range must map to a distinct chunk index
	for i, r := range ranges {
		ci, ok := reader.ChunkInfo(i)
		if !ok {
			t.Errorf("range %d: no chunk info", i)
			continue
		}
		if r.ChunkIndex != i {
			t.Errorf("range %d: ChunkIdx = %d, want %d", i, r.ChunkIndex, i)
		}
		// Verify end offset matches Rust: end_offset = chunk_end
		if r.ChunkEnd != ci.End {
			t.Errorf("range %d: ChunkEnd = %d, want %d", i, r.ChunkEnd, ci.End)
		}
	}

	// Specifically, range 0 and range 2 must NOT be the same
	if ranges[0].ChunkIndex == ranges[2].ChunkIndex {
		t.Error("ranges[0] and ranges[2] must map to different chunk indices")
	}
}

func TestRecordMaxAcceptsIncreasing(t *testing.T) {
	var last *uint64
	if !transfer.RecordMax(&last, 100) {
		t.Error("first call should accept 100")
	}
	if last == nil || *last != 100 {
		t.Errorf("last = %v, want 100", last)
	}
	if !transfer.RecordMax(&last, 200) {
		t.Error("second call should accept 200")
	}
	if last == nil || *last != 200 {
		t.Errorf("last = %v, want 200", last)
	}
}

func TestRecordMaxRejectsEqualAndBackwards(t *testing.T) {
	v := uint64(200)
	last := &v
	// duplicate offset
	if transfer.RecordMax(&last, 200) {
		t.Error("should reject equal offset")
	}
	if *last != 200 {
		t.Errorf("rejected offset must not update state: last = %d, want 200", *last)
	}
	// backwards offset
	if transfer.RecordMax(&last, 150) {
		t.Error("should reject backwards offset")
	}
	if *last != 200 {
		t.Errorf("rejected offset must not update state: last = %d, want 200", *last)
	}
}

func TestRecordMaxFirstCallAcceptsZero(t *testing.T) {
	var last *uint64
	if !transfer.RecordMax(&last, 0) {
		t.Error("first call should accept 0")
	}
	if last == nil || *last != 0 {
		t.Errorf("last = %v, want 0", last)
	}
}

func TestRecordMaxPersistsAcrossResets(t *testing.T) {
	var last *uint64
	if !transfer.RecordMax(&last, 1000) {
		t.Error("should accept 1000")
	}
	// a per-range implementation would clear here on cache flush
	if transfer.RecordMax(&last, 500) {
		t.Error("should reject 500 after 1000")
	}
	if last == nil || *last != 1000 {
		t.Errorf("last = %v, want 1000", last)
	}
	if !transfer.RecordMax(&last, 1500) {
		t.Error("should accept 1500")
	}
	if last == nil || *last != 1500 {
		t.Errorf("last = %v, want 1500", last)
	}
}
