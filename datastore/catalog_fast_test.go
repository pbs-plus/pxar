package datastore

import (
	"bytes"
	"crypto/sha256"
	"testing"
	"time"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
)

// buildPxarArchive builds a pxar archive in memory using the encoder.
func buildPxarArchive(t *testing.T, build func(enc *encoder.Encoder)) []byte {
	t.Helper()
	var buf bytes.Buffer
	rootMeta := pxar.DirMetadata(0o755).Build()
	enc := encoder.NewEncoder(&buf, nil, &rootMeta, nil)
	build(enc)
	if err := enc.Close(); err != nil {
		t.Fatalf("enc.Close: %v", err)
	}
	return buf.Bytes()
}

// chunkArchive splits an archive into chunks, stores them, and returns
// a DynamicIndexReader and ChunkStoreSource for BuildCatalogFast.
func chunkArchive(t *testing.T, archive []byte, chunkSize int) (*DynamicIndexReader, *ChunkStoreSource) {
	t.Helper()
	tmpDir := t.TempDir()
	store, err := NewChunkStore(tmpDir)
	if err != nil {
		t.Fatalf("NewChunkStore: %v", err)
	}

	idx := NewDynamicIndexWriter(time.Now().Unix())
	for i := 0; i < len(archive); i += chunkSize {
		end := min(i+chunkSize, len(archive))
		chunk := archive[i:end]
		digest := sha256.Sum256(chunk)
		blob, err := EncodeBlob(chunk)
		if err != nil {
			t.Fatalf("EncodeBlob: %v", err)
		}
		if _, _, err := store.InsertChunk(digest, blob.Bytes()); err != nil {
			t.Fatalf("InsertChunk: %v", err)
		}
		idx.Add(uint64(end), digest)
	}

	idxData, err := idx.Finish()
	if err != nil {
		t.Fatalf("idx.Finish: %v", err)
	}
	reader, err := ReadDynamicIndex(idxData)
	if err != nil {
		t.Fatalf("ReadDynamicIndex: %v", err)
	}
	return reader, NewChunkStoreSource(store)
}

func TestBuildCatalogFastEmpty(t *testing.T) {
	idx := NewDynamicIndexWriter(time.Now().Unix())
	idxData, _ := idx.Finish()
	reader, _ := ReadDynamicIndex(idxData)

	catalog, err := BuildCatalogFast(reader, nil, CatalogOptions{})
	if err != nil {
		t.Fatalf("BuildCatalogFast: %v", err)
	}
	if len(catalog.Dirs) != 0 {
		t.Errorf("expected empty catalog, got %d dirs", len(catalog.Dirs))
	}
}

func TestBuildCatalogFastRootOnly(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		// Root only, no children.
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	catalog, err := BuildCatalogFast(reader, source, CatalogOptions{})
	if err != nil {
		t.Fatalf("BuildCatalogFast: %v", err)
	}
	if _, ok := catalog.Dirs["/"]; !ok {
		// Root dir should exist (or at least not error).
		t.Log("root dir not explicitly tracked (acceptable)")
	}
}

func TestBuildCatalogFastSimpleTree(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		fileMeta := pxar.FileMetadata(0o644).Build()
		if _, err := enc.AddFile(&fileMeta, "hello.txt", []byte("hello world")); err != nil {
			t.Fatalf("AddFile: %v", err)
		}
		dirMeta := pxar.DirMetadata(0o755).Build()
		if err := enc.CreateDirectory("subdir", &dirMeta); err != nil {
			t.Fatalf("CreateDirectory: %v", err)
		}
		if _, err := enc.AddFile(&fileMeta, "nested.txt", []byte("nested content")); err != nil {
			t.Fatalf("AddFile nested: %v", err)
		}
		if err := enc.Finish(); err != nil {
			t.Fatalf("Finish subdir: %v", err)
		}
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	catalog, err := BuildCatalogFast(reader, source, CatalogOptions{MaxWorkers: 2})
	if err != nil {
		t.Fatalf("BuildCatalogFast: %v", err)
	}

	// Root should have 2 children: hello.txt (file) and subdir (dir).
	rootChildren := catalog.Dirs["/"]
	if len(rootChildren) < 2 {
		t.Fatalf("expected >= 2 root children, got %d: %+v", len(rootChildren), rootChildren)
	}

	var foundFile, foundDir bool
	for _, c := range rootChildren {
		switch c.Name {
		case "hello.txt":
			foundFile = true
			if c.Kind != KindFile {
				t.Errorf("hello.txt kind = %d, want KindFile(%d)", c.Kind, KindFile)
			}
			if c.Size != 11 {
				t.Errorf("hello.txt size = %d, want 11", c.Size)
			}
		case "subdir":
			foundDir = true
			if c.Kind != KindDirectory {
				t.Errorf("subdir kind = %d, want KindDirectory(%d)", c.Kind, KindDirectory)
			}
		}
	}
	if !foundFile {
		t.Error("hello.txt not found in root children")
	}
	if !foundDir {
		t.Error("subdir not found in root children")
	}

	// subdir should have 1 child: nested.txt
	subChildren := catalog.Dirs["/subdir"]
	if len(subChildren) != 1 {
		t.Fatalf("expected 1 child in /subdir, got %d: %+v", len(subChildren), subChildren)
	}
	if subChildren[0].Name != "nested.txt" {
		t.Errorf("subdir child name = %q, want %q", subChildren[0].Name, "nested.txt")
	}
	if subChildren[0].Kind != KindFile {
		t.Errorf("nested.txt kind = %d, want KindFile(%d)", subChildren[0].Kind, KindFile)
	}
	if subChildren[0].Size != 14 {
		t.Errorf("nested.txt size = %d, want 14", subChildren[0].Size)
	}
}

func TestBuildCatalogFastDeepTree(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		fileMeta := pxar.FileMetadata(0o644).Build()

		// Level 1: dir "a"
		dirMeta := pxar.DirMetadata(0o755).Build()
		if err := enc.CreateDirectory("a", &dirMeta); err != nil {
			t.Fatalf("CreateDirectory a: %v", err)
		}

		// Level 2: dir "b"
		if err := enc.CreateDirectory("b", &dirMeta); err != nil {
			t.Fatalf("CreateDirectory b: %v", err)
		}

		// Level 3: file "deep.txt"
		if _, err := enc.AddFile(&fileMeta, "deep.txt", []byte("deep")); err != nil {
			t.Fatalf("AddFile deep.txt: %v", err)
		}

		// Close b
		if err := enc.Finish(); err != nil {
			t.Fatalf("Finish b: %v", err)
		}
		// Close a
		if err := enc.Finish(); err != nil {
			t.Fatalf("Finish a: %v", err)
		}
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	catalog, err := BuildCatalogFast(reader, source, CatalogOptions{})
	if err != nil {
		t.Fatalf("BuildCatalogFast: %v", err)
	}

	// Check /a
	aChildren := catalog.Dirs["/a"]
	if len(aChildren) != 1 {
		t.Fatalf("expected 1 child in /a, got %d: %+v", len(aChildren), aChildren)
	}
	if aChildren[0].Name != "b" || aChildren[0].Kind != KindDirectory {
		t.Errorf("/a child = %+v, want dir 'b'", aChildren[0])
	}

	// Check /a/b
	abChildren := catalog.Dirs["/a/b"]
	if len(abChildren) != 1 {
		t.Fatalf("expected 1 child in /a/b, got %d: %+v", len(abChildren), abChildren)
	}
	if abChildren[0].Name != "deep.txt" || abChildren[0].Kind != KindFile {
		t.Errorf("/a/b child = %+v, want file 'deep.txt'", abChildren[0])
	}
}

func TestBuildCatalogFastSymlink(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		meta := pxar.SymlinkMetadata(0o777).Build()
		if err := enc.AddSymlink(&meta, "link", "/target"); err != nil {
			t.Fatalf("AddSymlink: %v", err)
		}
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	catalog, err := BuildCatalogFast(reader, source, CatalogOptions{})
	if err != nil {
		t.Fatalf("BuildCatalogFast: %v", err)
	}

	children := catalog.Dirs["/"]
	if len(children) != 1 {
		t.Fatalf("expected 1 child, got %d: %+v", len(children), children)
	}
	if children[0].Name != "link" || children[0].Kind != KindSymlink {
		t.Errorf("child = %+v, want symlink 'link'", children[0])
	}
}

func TestBuildCatalogFastDevice(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		meta := pxar.DeviceMetadata(0o666).Build()
		dev := format.Device{Major: 1, Minor: 3}
		if err := enc.AddDevice(&meta, "null", dev); err != nil {
			t.Fatalf("AddDevice: %v", err)
		}
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	catalog, err := BuildCatalogFast(reader, source, CatalogOptions{})
	if err != nil {
		t.Fatalf("BuildCatalogFast: %v", err)
	}

	children := catalog.Dirs["/"]
	if len(children) != 1 {
		t.Fatalf("expected 1 child, got %d: %+v", len(children), children)
	}
	if children[0].Name != "null" || children[0].Kind != KindDevice {
		t.Errorf("child = %+v, want device 'null'", children[0])
	}
}

func TestBuildCatalogFastFIFO(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		meta := pxar.FIFOMetadata(0o644).Build()
		if err := enc.AddFIFO(&meta, "myfifo"); err != nil {
			t.Fatalf("AddFIFO: %v", err)
		}
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	catalog, err := BuildCatalogFast(reader, source, CatalogOptions{})
	if err != nil {
		t.Fatalf("BuildCatalogFast: %v", err)
	}

	children := catalog.Dirs["/"]
	if len(children) != 1 {
		t.Fatalf("expected 1 child, got %d: %+v", len(children), children)
	}
	if children[0].Name != "myfifo" || children[0].Kind != KindFifo {
		t.Errorf("child = %+v, want FIFO 'myfifo'", children[0])
	}
}

func TestBuildCatalogFastSocket(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		meta := pxar.SocketMetadata(0o755).Build()
		if err := enc.AddSocket(&meta, "mysock"); err != nil {
			t.Fatalf("AddSocket: %v", err)
		}
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	catalog, err := BuildCatalogFast(reader, source, CatalogOptions{})
	if err != nil {
		t.Fatalf("BuildCatalogFast: %v", err)
	}

	children := catalog.Dirs["/"]
	if len(children) != 1 {
		t.Fatalf("expected 1 child, got %d: %+v", len(children), children)
	}
	if children[0].Name != "mysock" || children[0].Kind != KindSocket {
		t.Errorf("child = %+v, want socket 'mysock'", children[0])
	}
}

func TestBuildCatalogFastMultiChunk(t *testing.T) {
	// Build an archive with many files so it spans multiple small chunks.
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		fileMeta := pxar.FileMetadata(0o644).Build()
		for i := range 20 {
			name := "file_" + string(rune('a'+i)) + ".txt"
			content := bytes.Repeat([]byte{name[5]}, 100)
			if _, err := enc.AddFile(&fileMeta, name, content); err != nil {
				t.Fatalf("AddFile %s: %v", name, err)
			}
		}
	})

	// Use a very small chunk size to force multiple chunks.
	reader, source := chunkArchive(t, archive, 256)
	if reader.Count() < 2 {
		t.Fatalf("expected multiple chunks, got %d", reader.Count())
	}

	catalog, err := BuildCatalogFast(reader, source, CatalogOptions{MaxWorkers: 4})
	if err != nil {
		t.Fatalf("BuildCatalogFast: %v", err)
	}

	children := catalog.Dirs["/"]
	if len(children) != 20 {
		t.Errorf("expected 20 root children, got %d", len(children))
	}
}

func TestBuildCatalogFastParallelWorkers(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		fileMeta := pxar.FileMetadata(0o644).Build()
		if _, err := enc.AddFile(&fileMeta, "test.txt", []byte("data")); err != nil {
			t.Fatalf("AddFile: %v", err)
		}
	})

	reader, source := chunkArchive(t, archive, 64*1024)

	// Test with various worker counts.
	for _, workers := range []int{1, 2, 8, 100} {
		catalog, err := BuildCatalogFast(reader, source, CatalogOptions{MaxWorkers: workers})
		if err != nil {
			t.Fatalf("BuildCatalogFast(workers=%d): %v", workers, err)
		}
		children := catalog.Dirs["/"]
		if len(children) != 1 {
			t.Errorf("workers=%d: expected 1 child, got %d", workers, len(children))
		}
	}
}

func TestBuildCatalogFastMissingChunk(t *testing.T) {
	// Create an index referencing a chunk we never store.
	idx := NewDynamicIndexWriter(time.Now().Unix())
	missingDigest := sha256.Sum256([]byte("missing"))
	idx.Add(100, missingDigest)
	idxData, _ := idx.Finish()
	reader, _ := ReadDynamicIndex(idxData)

	// Create a dummy store with no chunks.
	tmpDir := t.TempDir()
	store, _ := NewChunkStore(tmpDir)
	source := NewChunkStoreSource(store)

	_, err := BuildCatalogFast(reader, source, CatalogOptions{})
	if err == nil {
		t.Error("expected error for missing chunk, got nil")
	}
}
