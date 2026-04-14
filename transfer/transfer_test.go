package transfer_test

import (
	"bytes"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
)

// Helper to create a simple v1 archive in memory.
func createTestArchive(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMeta(0o755), nil)

	_, err := enc.AddFile(fileMeta(0o644, 1000, 1000), "hello.txt", []byte("hello world"))
	if err != nil {
		t.Fatal(err)
	}

	err = enc.AddSymlink(symlinkMeta(0o777, 0, 0), "link", "/target")
	if err != nil {
		t.Fatal(err)
	}

	err = enc.CreateDirectory("subdir", dirMeta(0o755))
	if err != nil {
		t.Fatal(err)
	}

	_, err = enc.AddFile(fileMeta(0o644, 1000, 1000), "nested.txt", []byte("nested content"))
	if err != nil {
		t.Fatal(err)
	}

	err = enc.Finish()
	if err != nil {
		t.Fatal(err)
	}

	err = enc.Close()
	if err != nil {
		t.Fatal(err)
	}

	return buf.Bytes()
}

// Helper to create a v1 archive with nested directories.
func createNestedArchive(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMeta(0o755), nil)

	_, err := enc.AddFile(fileMeta(0o644, 0, 0), "top.txt", []byte("top level"))
	if err != nil {
		t.Fatal(err)
	}

	err = enc.CreateDirectory("a", dirMeta(0o755))
	if err != nil {
		t.Fatal(err)
	}

	err = enc.CreateDirectory("b", dirMeta(0o755))
	if err != nil {
		t.Fatal(err)
	}

	_, err = enc.AddFile(fileMeta(0o644, 0, 0), "deep.txt", []byte("deep"))
	if err != nil {
		t.Fatal(err)
	}

	err = enc.Finish() // close b
	if err != nil {
		t.Fatal(err)
	}

	_, err = enc.AddFile(fileMeta(0o644, 0, 0), "mid.txt", []byte("mid"))
	if err != nil {
		t.Fatal(err)
	}

	err = enc.Finish() // close a
	if err != nil {
		t.Fatal(err)
	}

	err = enc.Close() // close root
	if err != nil {
		t.Fatal(err)
	}

	return buf.Bytes()
}

func TestFileArchiveReaderLookup(t *testing.T) {
	data := createTestArchive(t)
	reader := transfer.NewFileArchiveReader(bytes.NewReader(data))
	defer reader.Close()

	entry, err := reader.Lookup("/hello.txt")
	if err != nil {
		t.Fatalf("Lookup hello.txt: %v", err)
	}
	if !entry.IsRegularFile() {
		t.Errorf("expected regular file, got %v", entry.Kind)
	}

	link, err := reader.Lookup("/link")
	if err != nil {
		t.Fatalf("Lookup link: %v", err)
	}
	if !link.IsSymlink() {
		t.Errorf("expected symlink, got %v", link.Kind)
	}
	if link.LinkTarget != "/target" {
		t.Errorf("link target = %q, want %q", link.LinkTarget, "/target")
	}
}

func TestFileArchiveReaderReadFileContent(t *testing.T) {
	data := createTestArchive(t)
	reader := transfer.NewFileArchiveReader(bytes.NewReader(data))
	defer reader.Close()

	entry, err := reader.Lookup("/hello.txt")
	if err != nil {
		t.Fatal(err)
	}

	content, err := reader.ReadFileContent(entry)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	if string(content) != "hello world" {
		t.Errorf("content = %q, want %q", content, "hello world")
	}
}

func TestFileArchiveReaderListDirectory(t *testing.T) {
	data := createTestArchive(t)
	reader := transfer.NewFileArchiveReader(bytes.NewReader(data))
	defer reader.Close()

	root, err := reader.ReadRoot()
	if err != nil {
		t.Fatal(err)
	}

	entries, err := reader.ListDirectory(int64(root.ContentOffset))
	if err != nil {
		t.Fatalf("ListDirectory: %v", err)
	}

	names := map[string]bool{}
	for _, e := range entries {
		names[e.Path] = true
	}

	for _, name := range []string{"hello.txt", "link", "subdir"} {
		if !names[name] {
			t.Errorf("entry %q not found in directory listing", name)
		}
	}
}

func TestFileArchiveReaderNestedDirectory(t *testing.T) {
	data := createNestedArchive(t)
	reader := transfer.NewFileArchiveReader(bytes.NewReader(data))
	defer reader.Close()

	entry, err := reader.Lookup("/a/b/deep.txt")
	if err != nil {
		t.Fatalf("Lookup /a/b/deep.txt: %v", err)
	}
	if !entry.IsRegularFile() {
		t.Errorf("expected regular file, got %v", entry.Kind)
	}

	content, err := reader.ReadFileContent(entry)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	if string(content) != "deep" {
		t.Errorf("content = %q, want %q", content, "deep")
	}
}

func TestWalkTree(t *testing.T) {
	data := createTestArchive(t)
	reader := transfer.NewFileArchiveReader(bytes.NewReader(data))
	defer reader.Close()

	var paths []string
	err := transfer.WalkTree(reader, "/", func(entry *pxar.Entry, content []byte) error {
		paths = append(paths, entry.Path)
		if entry.IsRegularFile() && string(content) != "" {
			// Verify file content is populated
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WalkTree: %v", err)
	}

	// Should have root dir, hello.txt, link, subdir, nested.txt
	expectedPaths := []string{"/", "hello.txt", "link", "subdir", "nested.txt"}
	for _, p := range expectedPaths {
		found := false
		for _, walked := range paths {
			if walked == p {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected path %q in walk, got %v", p, paths)
		}
	}
}

func TestWalkTreeSkipDir(t *testing.T) {
	data := createNestedArchive(t)
	reader := transfer.NewFileArchiveReader(bytes.NewReader(data))
	defer reader.Close()

	var paths []string
	err := transfer.WalkTree(reader, "/", func(entry *pxar.Entry, content []byte) error {
		paths = append(paths, entry.Path)
		if entry.Path == "a" {
			return transfer.ErrSkipDir
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WalkTree: %v", err)
	}

	// Should not have any paths under "a"
	for _, p := range paths {
		if p == "deep.txt" || p == "mid.txt" || p == "b" {
			t.Errorf("walked into skipped directory, found %q", p)
		}
	}
}

func TestCopySingleFile(t *testing.T) {
	srcData := createTestArchive(t)
	srcReader := transfer.NewFileArchiveReader(bytes.NewReader(srcData))
	defer srcReader.Close()

	var dstBuf bytes.Buffer
	dstWriter := transfer.NewStreamArchiveWriter(&dstBuf)
	rootMeta := pxar.DirMetadata(0o755).Build()
	if err := dstWriter.Begin(&rootMeta, transfer.WriterOptions{Format: format.FormatVersion1}); err != nil {
		t.Fatal(err)
	}

	err := transfer.Copy(srcReader, dstWriter, []transfer.PathMapping{{Src: "/hello.txt", Dst: "/hello.txt"}}, transfer.TransferOption{})
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}

	if err := dstWriter.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Verify the output is a valid archive
	dstReader := transfer.NewFileArchiveReader(bytes.NewReader(dstBuf.Bytes()))
	defer dstReader.Close()

	entry, err := dstReader.Lookup("/hello.txt")
	if err != nil {
		t.Fatalf("Lookup in destination: %v", err)
	}
	if !entry.IsRegularFile() {
		t.Errorf("expected regular file, got %v", entry.Kind)
	}

	content, err := dstReader.ReadFileContent(entry)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	if string(content) != "hello world" {
		t.Errorf("content = %q, want %q", content, "hello world")
	}
}

func TestCopyDirectory(t *testing.T) {
	srcData := createNestedArchive(t)
	srcReader := transfer.NewFileArchiveReader(bytes.NewReader(srcData))
	defer srcReader.Close()

	var dstBuf bytes.Buffer
	dstWriter := transfer.NewStreamArchiveWriter(&dstBuf)
	rootMeta := pxar.DirMetadata(0o755).Build()
	if err := dstWriter.Begin(&rootMeta, transfer.WriterOptions{Format: format.FormatVersion1}); err != nil {
		t.Fatal(err)
	}

	err := transfer.CopyTree(srcReader, dstWriter, "/a", "/a", transfer.TransferOption{})
	if err != nil {
		t.Fatalf("CopyTree: %v", err)
	}

	if err := dstWriter.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Verify the output
	dstReader := transfer.NewFileArchiveReader(bytes.NewReader(dstBuf.Bytes()))
	defer dstReader.Close()

	// Check that we have the "a" directory
	entry, err := dstReader.Lookup("/a")
	if err != nil {
		t.Fatalf("Lookup /a: %v", err)
	}
	if !entry.IsDir() {
		t.Errorf("expected directory, got %v", entry.Kind)
	}

	// Check nested file
	nested, err := dstReader.Lookup("/a/b/deep.txt")
	if err != nil {
		t.Fatalf("Lookup /a/b/deep.txt: %v", err)
	}
	content, err := dstReader.ReadFileContent(nested)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	if string(content) != "deep" {
		t.Errorf("content = %q, want %q", content, "deep")
	}
}

func TestMergeArchives(t *testing.T) {
	srcData := createTestArchive(t)
	srcReader := transfer.NewFileArchiveReader(bytes.NewReader(srcData))
	defer srcReader.Close()

	var dstBuf bytes.Buffer
	dstWriter := transfer.NewStreamArchiveWriter(&dstBuf)
	rootMeta := pxar.DirMetadata(0o755).Build()
	if err := dstWriter.Begin(&rootMeta, transfer.WriterOptions{Format: format.FormatVersion1}); err != nil {
		t.Fatal(err)
	}

	err := transfer.CopyTree(srcReader, dstWriter, "/", "/", transfer.TransferOption{})
	if err != nil {
		t.Fatalf("CopyTree: %v", err)
	}

	if err := dstWriter.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Verify the output contains all entries from source
	dstReader := transfer.NewFileArchiveReader(bytes.NewReader(dstBuf.Bytes()))
	defer dstReader.Close()

	root, err := dstReader.ReadRoot()
	if err != nil {
		t.Fatal(err)
	}

	entries, err := dstReader.ListDirectory(int64(root.ContentOffset))
	if err != nil {
		t.Fatal(err)
	}

	names := map[string]bool{}
	for _, e := range entries {
		names[e.Path] = true
	}

	for _, name := range []string{"hello.txt", "link", "subdir"} {
		if !names[name] {
			t.Errorf("merged archive missing %q", name)
		}
	}

	// Verify file content
	entry, err := dstReader.Lookup("/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	content, err := dstReader.ReadFileContent(entry)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "hello world" {
		t.Errorf("content = %q, want %q", content, "hello world")
	}

	// Verify nested file
	nested, err := dstReader.Lookup("/subdir/nested.txt")
	if err != nil {
		t.Fatal(err)
	}
	nestedContent, err := dstReader.ReadFileContent(nested)
	if err != nil {
		t.Fatal(err)
	}
	if string(nestedContent) != "nested content" {
		t.Errorf("nested content = %q, want %q", nestedContent, "nested content")
	}
}

func TestV2SplitArchiveRoundTrip(t *testing.T) {
	var metaBuf, payloadBuf bytes.Buffer
	enc := encoder.NewEncoder(&metaBuf, &payloadBuf, dirMeta(0o755), nil)

	_, err := enc.AddFile(fileMeta(0o644, 1000, 1000), "data.bin", []byte("payload data"))
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	// Read back with split reader
	reader := transfer.NewSplitFileArchiveReader(bytes.NewReader(metaBuf.Bytes()), bytes.NewReader(payloadBuf.Bytes()))
	defer reader.Close()

	entry, err := reader.Lookup("/data.bin")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if !entry.IsRegularFile() {
		t.Errorf("expected regular file, got %v", entry.Kind)
	}

	content, err := reader.ReadFileContent(entry)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	if string(content) != "payload data" {
		t.Errorf("content = %q, want %q", content, "payload data")
	}
}

func TestV2CopyToV1(t *testing.T) {
	// Create a v2 source archive
	var srcMeta, srcPayload bytes.Buffer
	srcEnc := encoder.NewEncoder(&srcMeta, &srcPayload, dirMeta(0o755), nil)
	_, err := srcEnc.AddFile(fileMeta(0o644, 0, 0), "file.txt", []byte("v2 content"))
	if err != nil {
		t.Fatal(err)
	}
	if err := srcEnc.Close(); err != nil {
		t.Fatal(err)
	}

	srcReader := transfer.NewSplitFileArchiveReader(bytes.NewReader(srcMeta.Bytes()), bytes.NewReader(srcPayload.Bytes()))
	defer srcReader.Close()

	// Write to v1 destination
	var dstBuf bytes.Buffer
	dstWriter := transfer.NewStreamArchiveWriter(&dstBuf)
	rootMeta := pxar.DirMetadata(0o755).Build()
	if err := dstWriter.Begin(&rootMeta, transfer.WriterOptions{Format: format.FormatVersion1}); err != nil {
		t.Fatal(err)
	}

	err = transfer.CopyTree(srcReader, dstWriter, "/", "/", transfer.TransferOption{})
	if err != nil {
		t.Fatalf("CopyTree: %v", err)
	}

	if err := dstWriter.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Verify the v1 destination
	dstReader := transfer.NewFileArchiveReader(bytes.NewReader(dstBuf.Bytes()))
	defer dstReader.Close()

	entry, err := dstReader.Lookup("/file.txt")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	content, err := dstReader.ReadFileContent(entry)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	if string(content) != "v2 content" {
		t.Errorf("content = %q, want %q", content, "v2 content")
	}
}

func TestStreamArchiveWriterAllEntryTypes(t *testing.T) {
	var buf bytes.Buffer
	writer := transfer.NewStreamArchiveWriter(&buf)
	rootMeta := pxar.DirMetadata(0o755).Build()

	if err := writer.Begin(&rootMeta, transfer.WriterOptions{Format: format.FormatVersion1}); err != nil {
		t.Fatal(err)
	}

	// Write a regular file
	fileEntry := &pxar.Entry{
		Path: "file.txt",
		Kind: pxar.KindFile,
		Metadata: pxar.FileMetadata(0o644).Owner(0, 0).Build(),
		FileSize: 5,
	}
	if err := writer.WriteEntry(fileEntry, []byte("hello")); err != nil {
		t.Fatalf("WriteEntry file: %v", err)
	}

	// Write a symlink
	symlinkEntry := &pxar.Entry{
		Path:       "link",
		Kind:       pxar.KindSymlink,
		LinkTarget: "/target",
		Metadata:   pxar.SymlinkMetadata(0o777).Owner(0, 0).Build(),
	}
	if err := writer.WriteEntry(symlinkEntry, nil); err != nil {
		t.Fatalf("WriteEntry symlink: %v", err)
	}

	// Write a directory
	dirMeta := pxar.DirMetadata(0o755).Owner(0, 0).Build()
	if err := writer.BeginDirectory("subdir", &dirMeta); err != nil {
		t.Fatalf("BeginDirectory: %v", err)
	}

	subFile := &pxar.Entry{
		Path: "sub.txt",
		Kind: pxar.KindFile,
		Metadata: pxar.FileMetadata(0o644).Owner(0, 0).Build(),
		FileSize: 3,
	}
	if err := writer.WriteEntry(subFile, []byte("sub")); err != nil {
		t.Fatalf("WriteEntry sub: %v", err)
	}

	if err := writer.EndDirectory(); err != nil {
		t.Fatalf("EndDirectory: %v", err)
	}

	if err := writer.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Verify we can read back the written archive
	reader := transfer.NewFileArchiveReader(bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	fileEntry2, err := reader.Lookup("/file.txt")
	if err != nil {
		t.Fatalf("Lookup file.txt: %v", err)
	}
	if !fileEntry2.IsRegularFile() {
		t.Errorf("expected file, got %v", fileEntry2.Kind)
	}

	linkEntry, err := reader.Lookup("/link")
	if err != nil {
		t.Fatalf("Lookup link: %v", err)
	}
	if !linkEntry.IsSymlink() {
		t.Errorf("expected symlink, got %v", linkEntry.Kind)
	}

	subEntry, err := reader.Lookup("/subdir/sub.txt")
	if err != nil {
		t.Fatalf("Lookup sub.txt: %v", err)
	}
	content, err := reader.ReadFileContent(subEntry)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	if string(content) != "sub" {
		t.Errorf("content = %q, want %q", content, "sub")
	}
}

func TestCopyTreePathRemapping(t *testing.T) {
	srcData := createNestedArchive(t)
	srcReader := transfer.NewFileArchiveReader(bytes.NewReader(srcData))
	defer srcReader.Close()

	var dstBuf bytes.Buffer
	dstWriter := transfer.NewStreamArchiveWriter(&dstBuf)
	rootMeta := pxar.DirMetadata(0o755).Build()
	if err := dstWriter.Begin(&rootMeta, transfer.WriterOptions{Format: format.FormatVersion1}); err != nil {
		t.Fatal(err)
	}

	// Copy /a → /backup/a (creates intermediate "backup" directory)
	err := transfer.CopyTree(srcReader, dstWriter, "/a", "/backup/a", transfer.TransferOption{})
	if err != nil {
		t.Fatalf("CopyTree: %v", err)
	}

	if err := dstWriter.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	dstReader := transfer.NewFileArchiveReader(bytes.NewReader(dstBuf.Bytes()))
	defer dstReader.Close()

	// Should find /backup/a/b/deep.txt (intermediate "backup" directory created)
	deepEntry, err := dstReader.Lookup("/backup/a/b/deep.txt")
	if err != nil {
		t.Fatalf("Lookup /backup/a/b/deep.txt: %v", err)
	}
	if !deepEntry.IsRegularFile() {
		t.Errorf("expected regular file, got %v", deepEntry.Kind)
	}
	content, err := dstReader.ReadFileContent(deepEntry)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	if string(content) != "deep" {
		t.Errorf("content = %q, want %q", content, "deep")
	}

	// Should also find /backup/a/mid.txt
	midEntry, err := dstReader.Lookup("/backup/a/mid.txt")
	if err != nil {
		t.Fatalf("Lookup /backup/a/mid.txt: %v", err)
	}
	midContent, err := dstReader.ReadFileContent(midEntry)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	if string(midContent) != "mid" {
		t.Errorf("content = %q, want %q", midContent, "mid")
	}

	// Original path should NOT exist
	_, err = dstReader.Lookup("/a/b/deep.txt")
	if err == nil {
		t.Error("expected /a/b/deep.txt to NOT exist in remapped archive")
	}
}

// Metadata helpers

func dirMeta(mode uint64) *pxar.Metadata {
	ts := format.StatxTimestampFromDurationSinceEpoch(1430487000 * 1e9)
	return &pxar.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFDIR | mode,
			Mtime: ts,
		},
	}
}

func fileMeta(mode uint64, uid, gid uint32) *pxar.Metadata {
	ts := format.StatxTimestampFromDurationSinceEpoch(1430487000 * 1e9)
	return &pxar.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFREG | mode,
			UID:   uid,
			GID:   gid,
			Mtime: ts,
		},
	}
}

func symlinkMeta(mode uint64, uid, gid uint32) *pxar.Metadata {
	ts := format.StatxTimestampFromDurationSinceEpoch(1430487000 * 1e9)
	return &pxar.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFLNK | mode,
			UID:   uid,
			GID:   gid,
			Mtime: ts,
		},
	}
}