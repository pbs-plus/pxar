package transfer_test

import (
	"bytes"
	"crypto/sha256"
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
	results, idx, err := chunker.ChunkStream(bytes.NewReader(buf.Bytes()))
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
	_, metaIdx, err := metaChunker.ChunkStream(bytes.NewReader(metaBuf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	metaIdxData, err := metaIdx.Finish()
	if err != nil {
		t.Fatal(err)
	}

	// Chunk payload stream
	payloadChunker := datastore.NewStoreChunker(store, config, false)
	_, payloadIdx, err := payloadChunker.ChunkStream(bytes.NewReader(payloadBuf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	payloadIdxData, err := payloadIdx.Finish()
	if err != nil {
		t.Fatal(err)
	}

	return store, metaIdxData, payloadIdxData
}

// --- ChunkedReadSeeker tests ---

func TestChunkedReadSeekerBasicRead(t *testing.T) {
	store, idxData := createChunkedArchive(t, map[string]string{
		"hello.txt": "hello world",
	})
	source := datastore.NewChunkStoreSource(store)

	idx, err := datastore.ReadDynamicIndex(idxData)
	if err != nil {
		t.Fatal(err)
	}

	reader := transfer.NewChunkedReadSeeker(idx, source, 0)
	defer reader.Close()

	// Read all data
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(reader); err != nil {
		t.Fatal(err)
	}

	if buf.Len() == 0 {
		t.Error("expected non-empty data from ChunkedReadSeeker")
	}
}

func TestChunkedReadSeekerSeekAndRead(t *testing.T) {
	store, idxData := createChunkedArchive(t, map[string]string{
		"file.txt": "some content here",
	})
	source := datastore.NewChunkStoreSource(store)

	idx, err := datastore.ReadDynamicIndex(idxData)
	if err != nil {
		t.Fatal(err)
	}

	reader := transfer.NewChunkedReadSeeker(idx, source, 0)
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

func TestChunkedReadSeekerCaching(t *testing.T) {
	store, idxData := createChunkedArchive(t, map[string]string{
		"file.txt": "cached content test",
	})
	source := datastore.NewChunkStoreSource(store)

	idx, err := datastore.ReadDynamicIndex(idxData)
	if err != nil {
		t.Fatal(err)
	}

	reader := transfer.NewChunkedReadSeeker(idx, source, 4)
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

func TestChunkedReadSeekerMatchesEager(t *testing.T) {
	store, idxData := createChunkedArchive(t, map[string]string{
		"file1.txt": "content one",
		"file2.txt": "content two",
	})
	source := datastore.NewChunkStoreSource(store)

	// Eager reconstruction
	eagerReader, err := transfer.NewChunkedArchiveReaderEager(idxData, source)
	if err != nil {
		t.Fatal(err)
	}
	defer eagerReader.Close()

	// Lazy reconstruction
	lazyReader, err := transfer.NewChunkedArchiveReader(idxData, source)
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

	content1, err := eagerReader.ReadFileContent(entry1)
	if err != nil {
		t.Fatal(err)
	}
	content2, err := lazyReader.ReadFileContent(entry2)
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

// --- SplitArchiveReader lazy tests ---

func TestSplitArchiveReaderLazyMatchesEager(t *testing.T) {
	store, metaIdxData, payloadIdxData := createSplitChunkedArchive(t, map[string]string{
		"data.bin": "payload data",
	})
	source := datastore.NewChunkStoreSource(store)

	eagerReader, err := transfer.NewSplitArchiveReaderEager(metaIdxData, payloadIdxData, source)
	if err != nil {
		t.Fatal(err)
	}
	defer eagerReader.Close()

	lazyReader, err := transfer.NewSplitArchiveReader(metaIdxData, payloadIdxData, source)
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

	eagerContent, err := eagerReader.ReadFileContent(eagerEntry)
	if err != nil {
		t.Fatal(err)
	}
	lazyContent, err := lazyReader.ReadFileContent(lazyEntry)
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

// --- DedupSplitArchiveWriter tests ---

func TestDedupSplitArchiveWriterRoundTrip(t *testing.T) {
	store, _, payloadIdxData := createSplitChunkedArchive(t, map[string]string{
		"file.txt": "original content",
	})
	source := datastore.NewChunkStoreSource(store)

	// Read source payload index
	payloadIdx, err := datastore.ReadDynamicIndex(payloadIdxData)
	if err != nil {
		t.Fatal(err)
	}

	config, _ := buzhash.NewConfig(64 << 10)

	// Write a new archive with the same data (dedup should kick in)
	writer := transfer.NewDedupSplitArchiveWriter(store, source, config, false, payloadIdx)
	rootMeta := pxar.DirMetadata(0o755).Build()
	if err := writer.Begin(&rootMeta, transfer.WriterOptions{Format: format.FormatVersion2}); err != nil {
		t.Fatal(err)
	}

	entry := &pxar.Entry{
		Path: "file.txt",
		Kind: pxar.KindFile,
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

	newReader, err := transfer.NewSplitArchiveReader(newMetaIdxData, newPayloadIdxData, source)
	if err != nil {
		t.Fatal(err)
	}
	defer newReader.Close()

	newEntry, err := newReader.Lookup("/file.txt")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}

	content, err := newReader.ReadFileContent(newEntry)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
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

	reader, err := datastore.ReadDynamicIndex(idxData)
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

	reader, err := datastore.ReadDynamicIndex(idxData)
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

	reader, err := datastore.ReadDynamicIndex(idxData)
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

	reader, err := transfer.NewSplitArchiveReaderEager(metaIdxData, payloadIdxData, source)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	entry, err := reader.Lookup("/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	content, err := reader.ReadFileContent(entry)
	if err != nil {
		t.Fatal(err)
	}
	expected := sha256.Sum256(content)

	// Now use ComputeContentDigest with the payload index
	payloadIdx, err := datastore.ReadDynamicIndex(payloadIdxData)
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
	_, metaIdx, err := metaChunker.ChunkStream(bytes.NewReader(metaBuf.Bytes()))
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

	payloadIdx, err := datastore.ReadDynamicIndex(payloadIdxData)
	if err != nil {
		t.Fatal(err)
	}

	config, _ := buzhash.NewConfig(64 << 10)
	source := datastore.NewChunkStoreSource(store)

	writer := transfer.NewDedupSplitArchiveWriter(store, source, config, false, payloadIdx)
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

	reader, err := datastore.ReadDynamicIndex(data)
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

