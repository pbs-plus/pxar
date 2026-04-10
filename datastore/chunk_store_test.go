package datastore

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
)

func newTestChunkStore(t *testing.T) (*ChunkStore, string) {
	t.Helper()
	dir := t.TempDir()
	cs, err := NewChunkStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	return cs, dir
}

func TestChunkStoreInsertLoad(t *testing.T) {
	cs, _ := newTestChunkStore(t)
	data := []byte("hello chunk")
	digest := sha256.Sum256(data)

	exists, size, err := cs.InsertChunk(digest, data)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Error("first insert should not report exists")
	}
	if size != len(data) {
		t.Errorf("size = %d, want %d", size, len(data))
	}

	loaded, err := cs.LoadChunk(digest)
	if err != nil {
		t.Fatal(err)
	}
	if string(loaded) != string(data) {
		t.Errorf("loaded = %q, want %q", loaded, data)
	}
}

func TestChunkStoreDeduplication(t *testing.T) {
	cs, _ := newTestChunkStore(t)
	data := []byte("duplicate chunk")
	digest := sha256.Sum256(data)

	_, _, _ = cs.InsertChunk(digest, data)
	exists, _, err := cs.InsertChunk(digest, data)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Error("second insert should report exists")
	}
}

func TestChunkStorePathGeneration(t *testing.T) {
	cs, _ := newTestChunkStore(t)
	digest := sha256.Sum256([]byte("test"))

	path := cs.ChunkPath(digest)
	hex := fmtDigest(digest)

	expected := filepath.Join(cs.chunkDir(), hex[:2], hex)
	if path != expected {
		t.Errorf("path = %s, want %s", path, expected)
	}
}

func TestChunkStoreLoadMissing(t *testing.T) {
	cs, _ := newTestChunkStore(t)
	digest := sha256.Sum256([]byte("nonexistent"))

	_, err := cs.LoadChunk(digest)
	if err == nil {
		t.Error("expected error for missing chunk")
	}
}

func TestChunkStoreTouchChunk(t *testing.T) {
	cs, _ := newTestChunkStore(t)
	data := []byte("touch me")
	digest := sha256.Sum256(data)

	_, _, _ = cs.InsertChunk(digest, data)

	err := cs.TouchChunk(digest)
	if err != nil {
		t.Fatal(err)
	}

	// Verify file still exists and was touched
	path := cs.ChunkPath(digest)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Error("chunk file is empty after touch")
	}
}

func TestChunkStoreMultipleChunks(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	for i := 0; i < 10; i++ {
		data := []byte{byte(i)}
		digest := sha256.Sum256(data)
		_, _, err := cs.InsertChunk(digest, data)
		if err != nil {
			t.Fatal(err)
		}
	}

	entries, err := os.ReadDir(cs.chunkDir())
	if err != nil {
		t.Fatal(err)
	}

	// Should have subdirectories for each unique first 2 hex chars
	subdirs := 0
	for _, e := range entries {
		if e.IsDir() {
			subdirs++
		}
	}
	if subdirs == 0 {
		t.Error("expected chunk subdirectories")
	}
}

func TestChunkStoreInitCreatesDir(t *testing.T) {
	dir := t.TempDir()
	chunkDir := filepath.Join(dir, ".chunks")

	// Verify .chunks doesn't exist yet
	if _, err := os.Stat(chunkDir); !os.IsNotExist(err) {
		t.Fatal(".chunks should not exist yet")
	}

	_, err := NewChunkStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(chunkDir); os.IsNotExist(err) {
		t.Error(".chunks directory was not created")
	}
}

func TestChunkStoreLargeChunk(t *testing.T) {
	cs, _ := newTestChunkStore(t)
	data := make([]byte, 4<<20) // 4MB
	for i := range data {
		data[i] = byte(i & 0xFF)
	}
	digest := sha256.Sum256(data)

	_, _, err := cs.InsertChunk(digest, data)
	if err != nil {
		t.Fatal(err)
	}

	loaded, err := cs.LoadChunk(digest)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != len(data) {
		t.Errorf("size = %d, want %d", len(loaded), len(data))
	}
}
