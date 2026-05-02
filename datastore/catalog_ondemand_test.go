package datastore

import (
	"bytes"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
)

// --- DirIndex tests ---

func TestBuildDirIndexEmpty(t *testing.T) {
	idx := NewDynamicIndexWriter(0)
	idxData, _ := idx.Finish()
	reader, _ := ReadDynamicIndex(idxData)

	result, err := BuildDirIndex(reader, nil, CatalogOptions{})
	if err != nil {
		t.Fatalf("BuildDirIndex: %v", err)
	}
	if result.Index.NumDirs() != 0 {
		t.Errorf("expected 0 dirs, got %d", result.Index.NumDirs())
	}
}

func TestBuildDirIndexRootOnly(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {})

	reader, source := chunkArchive(t, archive, 64*1024)
	result, err := BuildDirIndex(reader, source, CatalogOptions{})
	if err != nil {
		t.Fatalf("BuildDirIndex: %v", err)
	}
	if !result.Index.HasDir("/") {
		t.Error("expected root directory in index")
	}
}

func TestBuildDirIndexSimpleTree(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		fileMeta := pxar.FileMetadata(0o644).Build()
		enc.AddFile(&fileMeta, "hello.txt", []byte("hello world"))

		dirMeta := pxar.DirMetadata(0o755).Build()
		enc.CreateDirectory("subdir", &dirMeta)
		enc.AddFile(&fileMeta, "nested.txt", []byte("nested content"))
		enc.Finish()
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	result, err := BuildDirIndex(reader, source, CatalogOptions{})
	if err != nil {
		t.Fatalf("BuildDirIndex: %v", err)
	}

	for _, path := range []string{"/", "/subdir"} {
		if !result.Index.HasDir(path) {
			t.Errorf("expected %q in index", path)
		}
	}
	if result.Index.NumDirs() != 2 {
		t.Errorf("expected 2 dirs, got %d", result.Index.NumDirs())
	}
}

func TestBuildDirIndexDeepTree(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		dirMeta := pxar.DirMetadata(0o755).Build()
		fileMeta := pxar.FileMetadata(0o644).Build()

		enc.CreateDirectory("a", &dirMeta)
		enc.CreateDirectory("b", &dirMeta)
		enc.AddFile(&fileMeta, "deep.txt", []byte("deep"))
		enc.Finish() // b
		enc.Finish() // a
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	result, err := BuildDirIndex(reader, source, CatalogOptions{})
	if err != nil {
		t.Fatalf("BuildDirIndex: %v", err)
	}

	for _, path := range []string{"/", "/a", "/a/b"} {
		if !result.Index.HasDir(path) {
			t.Errorf("expected %q in index", path)
		}
	}
}

func TestBuildDirIndexMultiChunk(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		fileMeta := pxar.FileMetadata(0o644).Build()
		dirMeta := pxar.DirMetadata(0o755).Build()

		// Many files to force multiple chunks.
		for i := range 20 {
			name := "file_" + string(rune('a'+i)) + ".txt"
			enc.AddFile(&fileMeta, name, bytes.Repeat([]byte{byte(i)}, 100))
		}
		enc.CreateDirectory("subdir", &dirMeta)
		for i := range 10 {
			name := "nested_" + string(rune('a'+i)) + ".txt"
			enc.AddFile(&fileMeta, name, bytes.Repeat([]byte{byte(i)}, 100))
		}
		enc.Finish()
	})

	reader, source := chunkArchive(t, archive, 256)
	if reader.Count() < 2 {
		t.Fatalf("expected multiple chunks, got %d", reader.Count())
	}

	result, err := BuildDirIndex(reader, source, CatalogOptions{MaxWorkers: 4})
	if err != nil {
		t.Fatalf("BuildDirIndex: %v", err)
	}

	if !result.Index.HasDir("/") {
		t.Error("expected / in index")
	}
	if !result.Index.HasDir("/subdir") {
		t.Error("expected /subdir in index")
	}
}

// --- OnDemandCatalog tests ---

func TestOnDemandListDirRoot(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		fileMeta := pxar.FileMetadata(0o644).Build()
		dirMeta := pxar.DirMetadata(0o755).Build()

		enc.AddFile(&fileMeta, "hello.txt", []byte("hello world"))
		enc.CreateDirectory("subdir", &dirMeta)
		enc.AddFile(&fileMeta, "nested.txt", []byte("nested"))
		enc.Finish()
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	dirIndex, err := BuildDirIndex(reader, source, CatalogOptions{})
	if err != nil {
		t.Fatalf("BuildDirIndex: %v", err)
	}

	cat := NewOnDemandCatalog(dirIndex.Index, reader, source)
	children, err := cat.ListDir("/")
	if err != nil {
		t.Fatalf("ListDir /: %v", err)
	}

	if len(children) != 2 {
		t.Fatalf("expected 2 root children, got %d: %+v", len(children), children)
	}

	var foundFile, foundDir bool
	for _, c := range children {
		switch c.Name {
		case "hello.txt":
			foundFile = true
			if c.Kind != KindFile {
				t.Errorf("hello.txt kind = %d, want KindFile", c.Kind)
			}
			if c.Size != 11 {
				t.Errorf("hello.txt size = %d, want 11", c.Size)
			}
		case "subdir":
			foundDir = true
			if c.Kind != KindDirectory {
				t.Errorf("subdir kind = %d, want KindDirectory", c.Kind)
			}
		}
	}
	if !foundFile {
		t.Error("hello.txt not found in root children")
	}
	if !foundDir {
		t.Error("subdir not found in root children")
	}
}

func TestOnDemandListDirSubdir(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		fileMeta := pxar.FileMetadata(0o644).Build()
		dirMeta := pxar.DirMetadata(0o755).Build()

		enc.AddFile(&fileMeta, "top.txt", []byte("top"))
		enc.CreateDirectory("subdir", &dirMeta)
		enc.AddFile(&fileMeta, "nested.txt", []byte("nested content"))
		enc.Finish()
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	dirIndex, _ := BuildDirIndex(reader, source, CatalogOptions{})
	cat := NewOnDemandCatalog(dirIndex.Index, reader, source)

	children, err := cat.ListDir("/subdir")
	if err != nil {
		t.Fatalf("ListDir /subdir: %v", err)
	}

	if len(children) != 1 {
		t.Fatalf("expected 1 child in /subdir, got %d: %+v", len(children), children)
	}
	if children[0].Name != "nested.txt" {
		t.Errorf("child name = %q, want %q", children[0].Name, "nested.txt")
	}
	if children[0].Kind != KindFile {
		t.Errorf("child kind = %d, want KindFile", children[0].Kind)
	}
	if children[0].Size != 14 {
		t.Errorf("child size = %d, want 14", children[0].Size)
	}
}

func TestOnDemandListDirDeepNested(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		dirMeta := pxar.DirMetadata(0o755).Build()
		fileMeta := pxar.FileMetadata(0o644).Build()

		enc.CreateDirectory("a", &dirMeta)
		enc.CreateDirectory("b", &dirMeta)
		enc.AddFile(&fileMeta, "deep.txt", []byte("deep"))
		enc.Finish() // b
		enc.AddFile(&fileMeta, "a_file.txt", []byte("afile"))
		enc.Finish() // a
		enc.AddFile(&fileMeta, "root.txt", []byte("root"))
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	dirIndex, _ := BuildDirIndex(reader, source, CatalogOptions{})
	cat := NewOnDemandCatalog(dirIndex.Index, reader, source)

	// Root should have: a (dir), root.txt (file)
	root, err := cat.ListDir("/")
	if err != nil {
		t.Fatalf("ListDir /: %v", err)
	}
	if len(root) != 2 {
		t.Fatalf("expected 2 root children, got %d", len(root))
	}

	// /a should have: b (dir), a_file.txt (file)
	a, err := cat.ListDir("/a")
	if err != nil {
		t.Fatalf("ListDir /a: %v", err)
	}
	if len(a) != 2 {
		t.Fatalf("expected 2 children in /a, got %d: %+v", len(a), a)
	}

	// /a/b should have: deep.txt (file)
	ab, err := cat.ListDir("/a/b")
	if err != nil {
		t.Fatalf("ListDir /a/b: %v", err)
	}
	if len(ab) != 1 || ab[0].Name != "deep.txt" {
		t.Errorf("expected deep.txt in /a/b, got %+v", ab)
	}
}

func TestOnDemandListDirSkipsSubtrees(t *testing.T) {
	// Root has a large nested subtree; ListDir("/") should skip it
	// and still find siblings after it.
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		fileMeta := pxar.FileMetadata(0o644).Build()
		dirMeta := pxar.DirMetadata(0o755).Build()

		enc.AddFile(&fileMeta, "before.txt", []byte("before"))

		// Large nested subtree.
		enc.CreateDirectory("bigdir", &dirMeta)
		for i := range 20 {
			name := "file_" + string(rune('a'+i)) + ".txt"
			enc.AddFile(&fileMeta, name, bytes.Repeat([]byte{byte(i)}, 50))
		}
		enc.CreateDirectory("inner", &dirMeta)
		enc.AddFile(&fileMeta, "inner_file.txt", []byte("inner"))
		enc.Finish() // inner
		enc.Finish() // bigdir

		enc.AddFile(&fileMeta, "after.txt", []byte("after"))
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	dirIndex, _ := BuildDirIndex(reader, source, CatalogOptions{})
	cat := NewOnDemandCatalog(dirIndex.Index, reader, source)

	root, err := cat.ListDir("/")
	if err != nil {
		t.Fatalf("ListDir /: %v", err)
	}

	// Root should have: before.txt, bigdir (dir), after.txt
	if len(root) != 3 {
		t.Fatalf("expected 3 root children, got %d: %+v", len(root), root)
	}

	names := make(map[string]bool)
	for _, c := range root {
		names[c.Name] = true
	}
	for _, expected := range []string{"before.txt", "bigdir", "after.txt"} {
		if !names[expected] {
			t.Errorf("expected %q in root children", expected)
		}
	}
}

func TestOnDemandListDirEntryTypes(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		fileMeta := pxar.FileMetadata(0o644).Build()
		symMeta := pxar.SymlinkMetadata(0o777).Build()
		devMeta := pxar.DeviceMetadata(0o666).Build()
		fifoMeta := pxar.FIFOMetadata(0o644).Build()
		sockMeta := pxar.SocketMetadata(0o755).Build()

		enc.AddFile(&fileMeta, "file.txt", []byte("data"))
		enc.AddSymlink(&symMeta, "link", "/target")
		enc.AddDevice(&devMeta, "null", format.Device{Major: 1, Minor: 3})
		enc.AddFIFO(&fifoMeta, "myfifo")
		enc.AddSocket(&sockMeta, "mysock")
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	dirIndex, _ := BuildDirIndex(reader, source, CatalogOptions{})
	cat := NewOnDemandCatalog(dirIndex.Index, reader, source)

	children, err := cat.ListDir("/")
	if err != nil {
		t.Fatalf("ListDir /: %v", err)
	}
	if len(children) != 5 {
		t.Fatalf("expected 5 children, got %d: %+v", len(children), children)
	}

	expected := map[string]EntryKind{
		"file.txt": KindFile,
		"link":     KindSymlink,
		"null":     KindDevice,
		"myfifo":   KindFifo,
		"mysock":   KindSocket,
	}
	for _, c := range children {
		want, ok := expected[c.Name]
		if !ok {
			t.Errorf("unexpected child %q", c.Name)
			continue
		}
		if c.Kind != want {
			t.Errorf("%q kind = %d, want %d", c.Name, c.Kind, want)
		}
	}
}

func TestOnDemandListDirNotFound(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {})
	reader, source := chunkArchive(t, archive, 64*1024)
	dirIndex, _ := BuildDirIndex(reader, source, CatalogOptions{})
	cat := NewOnDemandCatalog(dirIndex.Index, reader, source)

	_, err := cat.ListDir("/nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent directory")
	}
}

func TestOnDemandListDirEmptyDir(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		dirMeta := pxar.DirMetadata(0o755).Build()
		fileMeta := pxar.FileMetadata(0o644).Build()

		enc.AddFile(&fileMeta, "before.txt", []byte("before"))
		enc.CreateDirectory("empty", &dirMeta)
		enc.Finish() // empty — no children
		enc.AddFile(&fileMeta, "after.txt", []byte("after"))
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	dirIndex, _ := BuildDirIndex(reader, source, CatalogOptions{})
	cat := NewOnDemandCatalog(dirIndex.Index, reader, source)

	// Root should still list all 3 children.
	root, err := cat.ListDir("/")
	if err != nil {
		t.Fatalf("ListDir /: %v", err)
	}
	if len(root) != 3 {
		t.Fatalf("expected 3 root children, got %d: %+v", len(root), root)
	}

	// Empty dir should return empty children.
	empty, err := cat.ListDir("/empty")
	if err != nil {
		t.Fatalf("ListDir /empty: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("expected 0 children in empty dir, got %d", len(empty))
	}
}

func TestOnDemandListDirMultiChunk(t *testing.T) {
	// Force multiple small chunks and list directories that span boundaries.
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		fileMeta := pxar.FileMetadata(0o644).Build()
		dirMeta := pxar.DirMetadata(0o755).Build()

		for i := range 20 {
			name := "file_" + string(rune('a'+i)) + ".txt"
			enc.AddFile(&fileMeta, name, bytes.Repeat([]byte{byte(i)}, 100))
		}
		enc.CreateDirectory("subdir", &dirMeta)
		for i := range 10 {
			name := "nested_" + string(rune('a'+i)) + ".txt"
			enc.AddFile(&fileMeta, name, bytes.Repeat([]byte{byte(i)}, 100))
		}
		enc.Finish()
	})

	reader, source := chunkArchive(t, archive, 256)
	if reader.Count() < 2 {
		t.Fatalf("expected multiple chunks, got %d", reader.Count())
	}

	dirIndex, _ := BuildDirIndex(reader, source, CatalogOptions{MaxWorkers: 4})
	cat := NewOnDemandCatalog(dirIndex.Index, reader, source)

	root, err := cat.ListDir("/")
	if err != nil {
		t.Fatalf("ListDir /: %v", err)
	}
	if len(root) != 21 { // 20 files + 1 subdir
		t.Errorf("expected 21 root children, got %d", len(root))
	}

	sub, err := cat.ListDir("/subdir")
	if err != nil {
		t.Fatalf("ListDir /subdir: %v", err)
	}
	if len(sub) != 10 {
		t.Errorf("expected 10 children in /subdir, got %d", len(sub))
	}
}

func TestOnDemandCachesChunks(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		fileMeta := pxar.FileMetadata(0o644).Build()
		enc.AddFile(&fileMeta, "test.txt", []byte("data"))
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	dirIndex, _ := BuildDirIndex(reader, source, CatalogOptions{})
	cat := NewOnDemandCatalog(dirIndex.Index, reader, source)

	// First call — fetches and caches.
	children1, err := cat.ListDir("/")
	if err != nil {
		t.Fatalf("ListDir / (1st): %v", err)
	}

	// Second call — should use cache (same result).
	children2, err := cat.ListDir("/")
	if err != nil {
		t.Fatalf("ListDir / (2nd): %v", err)
	}

	if len(children1) != len(children2) {
		t.Errorf("cached result differs: %d vs %d children", len(children1), len(children2))
	}
}

func TestOnDemandDirPaths(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		dirMeta := pxar.DirMetadata(0o755).Build()
		fileMeta := pxar.FileMetadata(0o644).Build()

		enc.CreateDirectory("a", &dirMeta)
		enc.CreateDirectory("b", &dirMeta)
		enc.AddFile(&fileMeta, "deep.txt", []byte("deep"))
		enc.Finish() // b
		enc.Finish() // a
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	dirIndex, _ := BuildDirIndex(reader, source, CatalogOptions{})
	cat := NewOnDemandCatalog(dirIndex.Index, reader, source)

	paths := cat.DirPaths()
	if len(paths) != 3 { // /, /a, /a/b
		t.Errorf("expected 3 dir paths, got %d: %v", len(paths), paths)
	}
	if cat.NumDirs() != 3 {
		t.Errorf("expected NumDirs=3, got %d", cat.NumDirs())
	}
}

// --- BuildResult / RootChildren tests ---

func TestBuildDirIndexRootChildren(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		fileMeta := pxar.FileMetadata(0o644).Build()
		dirMeta := pxar.DirMetadata(0o755).Build()

		enc.AddFile(&fileMeta, "hello.txt", []byte("hello world"))
		enc.CreateDirectory("subdir", &dirMeta)
		enc.AddFile(&fileMeta, "nested.txt", []byte("nested"))
		enc.Finish()
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	result, err := BuildDirIndex(reader, source, CatalogOptions{})
	if err != nil {
		t.Fatalf("BuildDirIndex: %v", err)
	}

	if len(result.RootChildren) != 2 {
		t.Fatalf("expected 2 root children, got %d: %+v", len(result.RootChildren), result.RootChildren)
	}

	var foundFile, foundDir bool
	for _, c := range result.RootChildren {
		switch c.Name {
		case "hello.txt":
			foundFile = true
			if c.Kind != KindFile {
				t.Errorf("hello.txt kind = %d, want KindFile", c.Kind)
			}
			if c.Size != 11 {
				t.Errorf("hello.txt size = %d, want 11", c.Size)
			}
		case "subdir":
			foundDir = true
			if c.Kind != KindDirectory {
				t.Errorf("subdir kind = %d, want KindDirectory", c.Kind)
			}
		}
	}
	if !foundFile {
		t.Error("hello.txt not found in root children")
	}
	if !foundDir {
		t.Error("subdir not found in root children")
	}
}

func TestBuildDirIndexRootChildrenEmpty(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {})

	reader, source := chunkArchive(t, archive, 64*1024)
	result, err := BuildDirIndex(reader, source, CatalogOptions{})
	if err != nil {
		t.Fatalf("BuildDirIndex: %v", err)
	}

	if len(result.RootChildren) != 0 {
		t.Errorf("expected 0 root children for empty root, got %d", len(result.RootChildren))
	}
}

func TestBuildDirIndexEndOffsets(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		fileMeta := pxar.FileMetadata(0o644).Build()
		dirMeta := pxar.DirMetadata(0o755).Build()

		enc.AddFile(&fileMeta, "before.txt", []byte("before"))
		enc.CreateDirectory("subdir", &dirMeta)
		enc.AddFile(&fileMeta, "nested.txt", []byte("nested"))
		enc.Finish()
		enc.AddFile(&fileMeta, "after.txt", []byte("after"))
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	result, err := BuildDirIndex(reader, source, CatalogOptions{})
	if err != nil {
		t.Fatalf("BuildDirIndex: %v", err)
	}

	// Root should have an end offset.
	rootLoc := result.Index.entries["/"]
	if rootLoc.endChunkIdx == 0 && rootLoc.endOffset == 0 {
		t.Error("root directory has no end offset recorded")
	}

	// Subdir should have an end offset.
	subLoc := result.Index.entries["/subdir"]
	if subLoc.endChunkIdx == 0 && subLoc.endOffset == 0 {
		t.Error("/subdir has no end offset recorded")
	}

	// Subdir end should be before root end (subdir is nested).
	if subLoc.endChunkIdx < subLoc.chunkIdx {
		t.Error("/subdir end offset is before its start")
	}
}
