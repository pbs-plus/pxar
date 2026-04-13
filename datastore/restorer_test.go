package datastore

import (
	"bytes"
	"crypto/sha256"
	"testing"
	"time"
)

func TestRestorerRestoreFile(t *testing.T) {
	// Create a temp directory for chunk store
	tmpDir := t.TempDir()
	store, err := NewChunkStore(tmpDir)
	if err != nil {
		t.Fatalf("NewChunkStore: %v", err)
	}

	// Create chunks
	chunk1 := []byte("Hello, ")
	chunk2 := []byte("World!")

	digest1 := sha256Sum(chunk1)
	digest2 := sha256Sum(chunk2)

	// Store chunks as encoded blobs
	blob1, _ := EncodeBlob(chunk1)
	blob2, _ := EncodeBlob(chunk2)
	store.InsertChunk(digest1, blob1.Bytes())
	store.InsertChunk(digest2, blob2.Bytes())

	// Create index
	idx := NewDynamicIndexWriter(time.Now().Unix())
	idx.Add(uint64(len(chunk1)), digest1)
	idx.Add(uint64(len(chunk1)+len(chunk2)), digest2)
	idxData, _ := idx.Finish()

	reader, err := ReadDynamicIndex(idxData)
	if err != nil {
		t.Fatalf("ReadDynamicIndex: %v", err)
	}

	// Restore file
	source := NewChunkStoreSource(store)
	restorer := NewRestorer(source)

	var buf bytes.Buffer
	if err := restorer.RestoreFile(reader, &buf); err != nil {
		t.Fatalf("RestoreFile: %v", err)
	}

	want := "Hello, World!"
	if got := buf.String(); got != want {
		t.Errorf("restored = %q, want %q", got, want)
	}
}

func TestRestorerRestoreFileCompressedChunks(t *testing.T) {
	// Create a temp directory for chunk store
	tmpDir := t.TempDir()
	store, err := NewChunkStore(tmpDir)
	if err != nil {
		t.Fatalf("NewChunkStore: %v", err)
	}

	// Create compressible chunk (repetitive data compresses well)
	chunk1 := bytes.Repeat([]byte("A"), 1000)
	chunk2 := bytes.Repeat([]byte("B"), 1000)

	digest1 := sha256Sum(chunk1)
	digest2 := sha256Sum(chunk2)

	// Store chunks as compressed blobs
	blob1, _ := EncodeCompressedBlob(chunk1)
	blob2, _ := EncodeCompressedBlob(chunk2)
	store.InsertChunk(digest1, blob1.Bytes())
	store.InsertChunk(digest2, blob2.Bytes())

	// Create index
	idx := NewDynamicIndexWriter(time.Now().Unix())
	idx.Add(uint64(len(chunk1)), digest1)
	idx.Add(uint64(len(chunk1)+len(chunk2)), digest2)
	idxData, _ := idx.Finish()

	reader, err := ReadDynamicIndex(idxData)
	if err != nil {
		t.Fatalf("ReadDynamicIndex: %v", err)
	}

	// Restore file
	source := NewChunkStoreSource(store)
	restorer := NewRestorer(source)

	var buf bytes.Buffer
	if err := restorer.RestoreFile(reader, &buf); err != nil {
		t.Fatalf("RestoreFile: %v", err)
	}

	want := append(chunk1, chunk2...)
	if got := buf.Bytes(); !bytes.Equal(got, want) {
		t.Errorf("restored length = %d, want %d", len(got), len(want))
	}
}

func TestRestorerRestoreRange(t *testing.T) {
	// Create a temp directory for chunk store
	tmpDir := t.TempDir()
	store, err := NewChunkStore(tmpDir)
	if err != nil {
		t.Fatalf("NewChunkStore: %v", err)
	}

	// Create chunks
	chunk1 := []byte("0123456789") // 10 bytes
	chunk2 := []byte("abcdefghij") // 10 bytes
	chunk3 := []byte("ABCDEFGHIJ") // 10 bytes

	digest1 := sha256Sum(chunk1)
	digest2 := sha256Sum(chunk2)
	digest3 := sha256Sum(chunk3)

	// Store chunks
	blob1, _ := EncodeBlob(chunk1)
	blob2, _ := EncodeBlob(chunk2)
	blob3, _ := EncodeBlob(chunk3)
	store.InsertChunk(digest1, blob1.Bytes())
	store.InsertChunk(digest2, blob2.Bytes())
	store.InsertChunk(digest3, blob3.Bytes())

	// Create index
	idx := NewDynamicIndexWriter(time.Now().Unix())
	idx.Add(10, digest1)
	idx.Add(20, digest2)
	idx.Add(30, digest3)
	idxData, _ := idx.Finish()

	reader, err := ReadDynamicIndex(idxData)
	if err != nil {
		t.Fatalf("ReadDynamicIndex: %v", err)
	}

	source := NewChunkStoreSource(store)
	restorer := NewRestorer(source)

	// Test cases
	tests := []struct {
		name   string
		offset uint64
		length uint64
		want   string
	}{
		{"first chunk only", 0, 5, "01234"},
		{"cross chunk boundary", 8, 4, "89ab"},
		{"middle of second chunk", 12, 3, "cde"},
		{"cross two boundaries", 8, 8, "89abcdef"},
		{"last part of file", 25, 5, "FGHIJ"},
		{"entire file", 0, 30, "0123456789abcdefghijABCDEFGHIJ"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := restorer.RestoreRange(reader, tc.offset, tc.length, &buf); err != nil {
				t.Fatalf("RestoreRange: %v", err)
			}
			if got := buf.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRestorerEmptyFile(t *testing.T) {
	source := NewChunkStoreSource(nil)
	restorer := NewRestorer(source)

	// Create empty index
	idx := NewDynamicIndexWriter(time.Now().Unix())
	idxData, _ := idx.Finish()
	reader, _ := ReadDynamicIndex(idxData)

	var buf bytes.Buffer
	if err := restorer.RestoreFile(reader, &buf); err != nil {
		t.Fatalf("RestoreFile empty: %v", err)
	}
	if buf.Len() != 0 {
		t.Errorf("empty file should produce no output, got %d bytes", buf.Len())
	}
}

func TestRestorerMissingChunk(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewChunkStore(tmpDir)
	if err != nil {
		t.Fatalf("NewChunkStore: %v", err)
	}

	// Create one chunk but not the other
	chunk1 := []byte("only this chunk exists")
	digest1 := sha256Sum(chunk1)
	blob1, _ := EncodeBlob(chunk1)
	store.InsertChunk(digest1, blob1.Bytes())

	// Create index referencing both chunks
	idx := NewDynamicIndexWriter(time.Now().Unix())
	idx.Add(uint64(len(chunk1)), digest1)
	// Reference a non-existent chunk
	missingDigest := sha256Sum([]byte("missing"))
	idx.Add(uint64(len(chunk1)+100), missingDigest)

	idxData, _ := idx.Finish()
	reader, _ := ReadDynamicIndex(idxData)

	source := NewChunkStoreSource(store)
	restorer := NewRestorer(source)

	var buf bytes.Buffer
	if err := restorer.RestoreFile(reader, &buf); err == nil {
		t.Error("RestoreFile should fail when chunk is missing")
	}
}

func TestChunkStoreSource(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := NewChunkStore(tmpDir)

	data := []byte("test chunk data")
	digest := sha256Sum(data)
	blob, _ := EncodeBlob(data)
	store.InsertChunk(digest, blob.Bytes())

	source := NewChunkStoreSource(store)
	got, err := source.GetChunk(digest)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}

	// GetChunk returns raw blob, not decoded data
	if !bytes.Equal(got, blob.Bytes()) {
		t.Error("GetChunk returned wrong data")
	}
}

func TestRestorerFileSize(t *testing.T) {
	idx := NewDynamicIndexWriter(time.Now().Unix())
	digest := sha256Sum([]byte("x"))
	idx.Add(100, digest)
	idx.Add(250, digest)
	idx.Add(500, digest)

	idxData, _ := idx.Finish()
	reader, _ := ReadDynamicIndex(idxData)

	source := NewChunkStoreSource(nil)
	restorer := NewRestorer(source)

	if size := restorer.FileSize(reader); size != 500 {
		t.Errorf("FileSize = %d, want 500", size)
	}
}

// Helper function
func sha256Sum(data []byte) [32]byte {
	return sha256.Sum256(data)
}
