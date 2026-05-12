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
	reader, _ := ParseDynamicIndex(idxData)

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
		_, _ = enc.AddFile(&fileMeta, "hello.txt", []byte("hello world"))

		dirMeta := pxar.DirMetadata(0o755).Build()
		_ = enc.CreateDirectory("subdir", &dirMeta)
		_, _ = enc.AddFile(&fileMeta, "nested.txt", []byte("nested content"))
		_ = enc.Finish()
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

		_ = enc.CreateDirectory("a", &dirMeta)
		_ = enc.CreateDirectory("b", &dirMeta)
		_, _ = enc.AddFile(&fileMeta, "deep.txt", []byte("deep"))
		_ = enc.Finish() // b
		_ = enc.Finish() // a
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
			_, _ = enc.AddFile(&fileMeta, name, bytes.Repeat([]byte{byte(i)}, 100))
		}
		_ = enc.CreateDirectory("subdir", &dirMeta)
		for i := range 10 {
			name := "nested_" + string(rune('a'+i)) + ".txt"
			_, _ = enc.AddFile(&fileMeta, name, bytes.Repeat([]byte{byte(i)}, 100))
		}
		_ = enc.Finish()
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

		_, _ = enc.AddFile(&fileMeta, "hello.txt", []byte("hello world"))
		_ = enc.CreateDirectory("subdir", &dirMeta)
		_, _ = enc.AddFile(&fileMeta, "nested.txt", []byte("nested"))
		_ = enc.Finish()
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	dirIndex, err := BuildDirIndex(reader, source, CatalogOptions{})
	if err != nil {
		t.Fatalf("BuildDirIndex: %v", err)
	}

	cat := NewOnDemandCatalog(dirIndex.Index, reader, source)
	var children []CatalogChild
	if listErr := cat.ListDir("/", func(ch CatalogChild) error {
		children = append(children, ch)
		return nil
	}); listErr != nil {
		t.Fatalf("ListDir /: %v", listErr)
	}

	if len(children) != 2 {
		t.Fatalf("expected 2 root children, got %d: %+v", len(children), children)
	}

	var foundFile, foundDir bool
	for _, c := range children {
		switch c.Name {
		case "hello.txt":
			foundFile = true
			if c.Kind != pxar.KindFile {
				t.Errorf("hello.txt kind = %d, want pxar.KindFile", c.Kind)
			}
			if c.Size != 11 {
				t.Errorf("hello.txt size = %d, want 11", c.Size)
			}
		case "subdir":
			foundDir = true
			if c.Kind != pxar.KindDirectory {
				t.Errorf("subdir kind = %d, want pxar.KindDirectory", c.Kind)
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

		_, _ = enc.AddFile(&fileMeta, "top.txt", []byte("top"))
		_ = enc.CreateDirectory("subdir", &dirMeta)
		_, _ = enc.AddFile(&fileMeta, "nested.txt", []byte("nested content"))
		_ = enc.Finish()
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	dirIndex, _ := BuildDirIndex(reader, source, CatalogOptions{})
	cat := NewOnDemandCatalog(dirIndex.Index, reader, source)

	var children []CatalogChild
	if listErr := cat.ListDir("/subdir", func(ch CatalogChild) error {
		children = append(children, ch)
		return nil
	}); listErr != nil {
		t.Fatalf("ListDir /subdir: %v", listErr)
	}

	if len(children) != 1 {
		t.Fatalf("expected 1 child in /subdir, got %d: %+v", len(children), children)
	}
	if children[0].Name != "nested.txt" {
		t.Errorf("child name = %q, want %q", children[0].Name, "nested.txt")
	}
	if children[0].Kind != pxar.KindFile {
		t.Errorf("child kind = %d, want pxar.KindFile", children[0].Kind)
	}
	if children[0].Size != 14 {
		t.Errorf("child size = %d, want 14", children[0].Size)
	}
}

func TestOnDemandListDirDeepNested(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		dirMeta := pxar.DirMetadata(0o755).Build()
		fileMeta := pxar.FileMetadata(0o644).Build()

		_ = enc.CreateDirectory("a", &dirMeta)
		_ = enc.CreateDirectory("b", &dirMeta)
		_, _ = enc.AddFile(&fileMeta, "deep.txt", []byte("deep"))
		_ = enc.Finish() // b
		_, _ = enc.AddFile(&fileMeta, "a_file.txt", []byte("afile"))
		_ = enc.Finish() // a
		_, _ = enc.AddFile(&fileMeta, "root.txt", []byte("root"))
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	dirIndex, _ := BuildDirIndex(reader, source, CatalogOptions{})
	cat := NewOnDemandCatalog(dirIndex.Index, reader, source)

	// Root should have: a (dir), root.txt (file)
	var root []CatalogChild
	if listErr := cat.ListDir("/", func(ch CatalogChild) error {
		root = append(root, ch)
		return nil
	}); listErr != nil {
		t.Fatalf("ListDir /: %v", listErr)
	}
	if len(root) != 2 {
		t.Fatalf("expected 2 root children, got %d", len(root))
	}

	// /a should have: b (dir), a_file.txt (file)
	var a []CatalogChild
	if listErr := cat.ListDir("/a", func(ch CatalogChild) error {
		a = append(a, ch)
		return nil
	}); listErr != nil {
		t.Fatalf("ListDir /a: %v", listErr)
	}
	if len(a) != 2 {
		t.Fatalf("expected 2 children in /a, got %d: %+v", len(a), a)
	}

	// /a/b should have: deep.txt (file)
	var ab []CatalogChild
	if listErr := cat.ListDir("/a/b", func(ch CatalogChild) error {
		ab = append(ab, ch)
		return nil
	}); listErr != nil {
		t.Fatalf("ListDir /a/b: %v", listErr)
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

		_, _ = enc.AddFile(&fileMeta, "before.txt", []byte("before"))

		// Large nested subtree.
		_ = enc.CreateDirectory("bigdir", &dirMeta)
		for i := range 20 {
			name := "file_" + string(rune('a'+i)) + ".txt"
			_, _ = enc.AddFile(&fileMeta, name, bytes.Repeat([]byte{byte(i)}, 50))
		}
		_ = enc.CreateDirectory("inner", &dirMeta)
		_, _ = enc.AddFile(&fileMeta, "inner_file.txt", []byte("inner"))
		_ = enc.Finish() // inner
		_ = enc.Finish() // bigdir

		_, _ = enc.AddFile(&fileMeta, "after.txt", []byte("after"))
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	dirIndex, _ := BuildDirIndex(reader, source, CatalogOptions{})
	cat := NewOnDemandCatalog(dirIndex.Index, reader, source)

	var root []CatalogChild
	if listErr := cat.ListDir("/", func(ch CatalogChild) error {
		root = append(root, ch)
		return nil
	}); listErr != nil {
		t.Fatalf("ListDir /: %v", listErr)
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

		_, _ = enc.AddFile(&fileMeta, "file.txt", []byte("data"))
		_ = enc.AddSymlink(&symMeta, "link", "/target")
		_ = enc.AddDevice(&devMeta, "null", format.Device{Major: 1, Minor: 3})
		_ = enc.AddFIFO(&fifoMeta, "myfifo")
		_ = enc.AddSocket(&sockMeta, "mysock")
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	dirIndex, _ := BuildDirIndex(reader, source, CatalogOptions{})
	cat := NewOnDemandCatalog(dirIndex.Index, reader, source)

	var children []CatalogChild
	if listErr := cat.ListDir("/", func(ch CatalogChild) error {
		children = append(children, ch)
		return nil
	}); listErr != nil {
		t.Fatalf("ListDir /: %v", listErr)
	}
	if len(children) != 5 {
		t.Fatalf("expected 5 children, got %d: %+v", len(children), children)
	}

	expected := map[string]pxar.EntryKind{
		"file.txt": pxar.KindFile,
		"link":     pxar.KindSymlink,
		"null":     pxar.KindDevice,
		"myfifo":   pxar.KindFIFO,
		"mysock":   pxar.KindSocket,
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

	if listErr := cat.ListDir("/nonexistent", func(ch CatalogChild) error { return nil }); listErr == nil {
		t.Error("expected error for nonexistent directory")
	}
}

func TestOnDemandListDirEmptyDir(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		dirMeta := pxar.DirMetadata(0o755).Build()
		fileMeta := pxar.FileMetadata(0o644).Build()

		_, _ = enc.AddFile(&fileMeta, "before.txt", []byte("before"))
		_ = enc.CreateDirectory("empty", &dirMeta)
		_ = enc.Finish() // empty — no children
		_, _ = enc.AddFile(&fileMeta, "after.txt", []byte("after"))
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	dirIndex, _ := BuildDirIndex(reader, source, CatalogOptions{})
	cat := NewOnDemandCatalog(dirIndex.Index, reader, source)

	// Root should still list all 3 children.
	var root []CatalogChild
	if listErr := cat.ListDir("/", func(ch CatalogChild) error {
		root = append(root, ch)
		return nil
	}); listErr != nil {
		t.Fatalf("ListDir /: %v", listErr)
	}
	if len(root) != 3 {
		t.Fatalf("expected 3 root children, got %d: %+v", len(root), root)
	}

	// Empty dir should return empty children.
	var empty []CatalogChild
	if listErr := cat.ListDir("/empty", func(ch CatalogChild) error {
		empty = append(empty, ch)
		return nil
	}); listErr != nil {
		t.Fatalf("ListDir /empty: %v", listErr)
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
			_, _ = enc.AddFile(&fileMeta, name, bytes.Repeat([]byte{byte(i)}, 100))
		}
		_ = enc.CreateDirectory("subdir", &dirMeta)
		for i := range 10 {
			name := "nested_" + string(rune('a'+i)) + ".txt"
			_, _ = enc.AddFile(&fileMeta, name, bytes.Repeat([]byte{byte(i)}, 100))
		}
		_ = enc.Finish()
	})

	reader, source := chunkArchive(t, archive, 256)
	if reader.Count() < 2 {
		t.Fatalf("expected multiple chunks, got %d", reader.Count())
	}

	dirIndex, _ := BuildDirIndex(reader, source, CatalogOptions{MaxWorkers: 4})
	cat := NewOnDemandCatalog(dirIndex.Index, reader, source)

	var root []CatalogChild
	if listErr := cat.ListDir("/", func(ch CatalogChild) error {
		root = append(root, ch)
		return nil
	}); listErr != nil {
		t.Fatalf("ListDir /: %v", listErr)
	}
	if len(root) != 21 { // 20 files + 1 subdir
		t.Errorf("expected 21 root children, got %d", len(root))
	}

	var sub []CatalogChild
	if listErr := cat.ListDir("/subdir", func(ch CatalogChild) error {
		sub = append(sub, ch)
		return nil
	}); listErr != nil {
		t.Fatalf("ListDir /subdir: %v", listErr)
	}
	if len(sub) != 10 {
		t.Errorf("expected 10 children in /subdir, got %d", len(sub))
	}
}

func TestOnDemandCachesChunks(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		fileMeta := pxar.FileMetadata(0o644).Build()
		_, _ = enc.AddFile(&fileMeta, "test.txt", []byte("data"))
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	dirIndex, _ := BuildDirIndex(reader, source, CatalogOptions{})
	cat := NewOnDemandCatalog(dirIndex.Index, reader, source)

	// First call — fetches and caches.
	var children1 []CatalogChild
	if listErr := cat.ListDir("/", func(ch CatalogChild) error {
		children1 = append(children1, ch)
		return nil
	}); listErr != nil {
		t.Fatalf("ListDir / (1st): %v", listErr)
	}

	// Second call — should use cache (same result).
	var children2 []CatalogChild
	if listErr := cat.ListDir("/", func(ch CatalogChild) error {
		children2 = append(children2, ch)
		return nil
	}); listErr != nil {
		t.Fatalf("ListDir / (2nd): %v", listErr)
	}

	if len(children1) != len(children2) {
		t.Errorf("cached result differs: %d vs %d children", len(children1), len(children2))
	}
}

func TestOnDemandDirPaths(t *testing.T) {
	archive := buildPxarArchive(t, func(enc *encoder.Encoder) {
		dirMeta := pxar.DirMetadata(0o755).Build()
		fileMeta := pxar.FileMetadata(0o644).Build()

		_ = enc.CreateDirectory("a", &dirMeta)
		_ = enc.CreateDirectory("b", &dirMeta)
		_, _ = enc.AddFile(&fileMeta, "deep.txt", []byte("deep"))
		_ = enc.Finish() // b
		_ = enc.Finish() // a
	})

	reader, source := chunkArchive(t, archive, 64*1024)
	dirIndex, _ := BuildDirIndex(reader, source, CatalogOptions{})
	cat := NewOnDemandCatalog(dirIndex.Index, reader, source)

	var paths []string
	err := cat.DirPaths(func(p string) error {
		paths = append(paths, p)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
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

		_, _ = enc.AddFile(&fileMeta, "hello.txt", []byte("hello world"))
		_ = enc.CreateDirectory("subdir", &dirMeta)
		_, _ = enc.AddFile(&fileMeta, "nested.txt", []byte("nested"))
		_ = enc.Finish()
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
			if c.Kind != pxar.KindFile {
				t.Errorf("hello.txt kind = %d, want pxar.KindFile", c.Kind)
			}
			if c.Size != 11 {
				t.Errorf("hello.txt size = %d, want 11", c.Size)
			}
		case "subdir":
			foundDir = true
			if c.Kind != pxar.KindDirectory {
				t.Errorf("subdir kind = %d, want pxar.KindDirectory", c.Kind)
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

		_, _ = enc.AddFile(&fileMeta, "before.txt", []byte("before"))
		_ = enc.CreateDirectory("subdir", &dirMeta)
		_, _ = enc.AddFile(&fileMeta, "nested.txt", []byte("nested"))
		_ = enc.Finish()
		_, _ = enc.AddFile(&fileMeta, "after.txt", []byte("after"))
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
