package transfer_test

import (
	"bytes"
	"io"
	"strings"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/accessor"
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

func TestFileReaderLookup(t *testing.T) {
	data := createTestArchive(t)
	reader := transfer.NewFileReader(bytes.NewReader(data))
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

func TestFileReaderReadFileContent(t *testing.T) {
	data := createTestArchive(t)
	reader := transfer.NewFileReader(bytes.NewReader(data))
	defer reader.Close()

	entry, err := reader.Lookup("/hello.txt")
	if err != nil {
		t.Fatal(err)
	}

	r, err := reader.ReadFileContentReader(entry)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	defer r.Close()
	content, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read content: %v", err)
	}
	if string(content) != "hello world" {
		t.Errorf("content = %q, want %q", content, "hello world")
	}
}

func TestFileReaderListDirectory(t *testing.T) {
	data := createTestArchive(t)
	reader := transfer.NewFileReader(bytes.NewReader(data))
	defer reader.Close()

	root, err := reader.ReadRoot()
	if err != nil {
		t.Fatal(err)
	}

	names := map[string]bool{}
	if err := reader.ListDirectory(int64(root.ContentOffset), accessor.ListOption{}, func(entry *pxar.Entry) error {
		names[entry.Path] = true
		return nil
	}); err != nil {
		t.Fatalf("ListDirectory: %v", err)
	}

	for _, name := range []string{"hello.txt", "link", "subdir"} {
		if !names[name] {
			t.Errorf("entry %q not found in directory listing", name)
		}
	}
}

func TestFileReaderNestedDirectory(t *testing.T) {
	data := createNestedArchive(t)
	reader := transfer.NewFileReader(bytes.NewReader(data))
	defer reader.Close()

	entry, err := reader.Lookup("/a/b/deep.txt")
	if err != nil {
		t.Fatalf("Lookup /a/b/deep.txt: %v", err)
	}
	if !entry.IsRegularFile() {
		t.Errorf("expected regular file, got %v", entry.Kind)
	}

	r2, err := reader.ReadFileContentReader(entry)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	defer r2.Close()
	content, err := io.ReadAll(r2)
	if err != nil {
		t.Fatalf("read content: %v", err)
	}
	if string(content) != "deep" {
		t.Errorf("content = %q, want %q", content, "deep")
	}
}

func TestCopySingleFile(t *testing.T) {
	srcData := createTestArchive(t)
	srcReader := transfer.NewFileReader(bytes.NewReader(srcData))
	defer srcReader.Close()

	var dstBuf bytes.Buffer
	dstWriter := transfer.NewStreamWriter(&dstBuf)
	rootMeta := pxar.DirMetadata(0o755).Build()
	if err := dstWriter.Begin(&rootMeta, transfer.Options{Format: format.FormatVersion1}); err != nil {
		t.Fatal(err)
	}

	err := transfer.Copy(srcReader, dstWriter, []transfer.PathMapping{{Src: "/hello.txt", Dst: "/hello.txt"}}, transfer.CopyOption{})
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}

	if err := dstWriter.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Verify the output is a valid archive
	dstReader := transfer.NewFileReader(bytes.NewReader(dstBuf.Bytes()))
	defer dstReader.Close()

	entry, err := dstReader.Lookup("/hello.txt")
	if err != nil {
		t.Fatalf("Lookup in destination: %v", err)
	}
	if !entry.IsRegularFile() {
		t.Errorf("expected regular file, got %v", entry.Kind)
	}

	r3, err := dstReader.ReadFileContentReader(entry)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	defer r3.Close()
	content, err := io.ReadAll(r3)
	if err != nil {
		t.Fatalf("read content: %v", err)
	}
	if string(content) != "hello world" {
		t.Errorf("content = %q, want %q", content, "hello world")
	}
}

func TestCopyDirectory(t *testing.T) {
	srcData := createNestedArchive(t)
	srcReader := transfer.NewFileReader(bytes.NewReader(srcData))
	defer srcReader.Close()

	var dstBuf bytes.Buffer
	dstWriter := transfer.NewStreamWriter(&dstBuf)
	rootMeta := pxar.DirMetadata(0o755).Build()
	if err := dstWriter.Begin(&rootMeta, transfer.Options{Format: format.FormatVersion1}); err != nil {
		t.Fatal(err)
	}

	err := transfer.CopyTree(srcReader, dstWriter, "/a", "/a", transfer.CopyOption{})
	if err != nil {
		t.Fatalf("CopyTree: %v", err)
	}

	if err := dstWriter.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Verify the output
	dstReader := transfer.NewFileReader(bytes.NewReader(dstBuf.Bytes()))
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
	r4, err := dstReader.ReadFileContentReader(nested)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	defer r4.Close()
	content, err := io.ReadAll(r4)
	if err != nil {
		t.Fatalf("read content: %v", err)
	}
	if string(content) != "deep" {
		t.Errorf("content = %q, want %q", content, "deep")
	}
}

func TestMergeArchives(t *testing.T) {
	srcData := createTestArchive(t)
	srcReader := transfer.NewFileReader(bytes.NewReader(srcData))
	defer srcReader.Close()

	var dstBuf bytes.Buffer
	dstWriter := transfer.NewStreamWriter(&dstBuf)
	rootMeta := pxar.DirMetadata(0o755).Build()
	if err := dstWriter.Begin(&rootMeta, transfer.Options{Format: format.FormatVersion1}); err != nil {
		t.Fatal(err)
	}

	err := transfer.CopyTree(srcReader, dstWriter, "/", "/", transfer.CopyOption{})
	if err != nil {
		t.Fatalf("CopyTree: %v", err)
	}

	if err := dstWriter.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Verify the output contains all entries from source
	dstReader := transfer.NewFileReader(bytes.NewReader(dstBuf.Bytes()))
	defer dstReader.Close()

	root, err := dstReader.ReadRoot()
	if err != nil {
		t.Fatal(err)
	}

	names := map[string]bool{}
	if err := dstReader.ListDirectory(int64(root.ContentOffset), accessor.ListOption{}, func(entry *pxar.Entry) error {
		names[entry.Path] = true
		return nil
	}); err != nil {
		t.Fatal(err)
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
	r5, err := dstReader.ReadFileContentReader(entry)
	if err != nil {
		t.Fatal(err)
	}
	defer r5.Close()
	content, err := io.ReadAll(r5)
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
	r6, err := dstReader.ReadFileContentReader(nested)
	if err != nil {
		t.Fatal(err)
	}
	defer r6.Close()
	nestedContent, err := io.ReadAll(r6)
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
	reader := transfer.NewSplitFileReader(bytes.NewReader(metaBuf.Bytes()), bytes.NewReader(payloadBuf.Bytes()))
	defer reader.Close()

	entry, err := reader.Lookup("/data.bin")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if !entry.IsRegularFile() {
		t.Errorf("expected regular file, got %v", entry.Kind)
	}

	r7, err := reader.ReadFileContentReader(entry)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	defer r7.Close()
	content, err := io.ReadAll(r7)
	if err != nil {
		t.Fatalf("read content: %v", err)
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

	srcReader := transfer.NewSplitFileReader(bytes.NewReader(srcMeta.Bytes()), bytes.NewReader(srcPayload.Bytes()))
	defer srcReader.Close()

	// Write to v1 destination
	var dstBuf bytes.Buffer
	dstWriter := transfer.NewStreamWriter(&dstBuf)
	rootMeta := pxar.DirMetadata(0o755).Build()
	if err := dstWriter.Begin(&rootMeta, transfer.Options{Format: format.FormatVersion1}); err != nil {
		t.Fatal(err)
	}

	err = transfer.CopyTree(srcReader, dstWriter, "/", "/", transfer.CopyOption{})
	if err != nil {
		t.Fatalf("CopyTree: %v", err)
	}

	if err := dstWriter.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Verify the v1 destination
	dstReader := transfer.NewFileReader(bytes.NewReader(dstBuf.Bytes()))
	defer dstReader.Close()

	entry, err := dstReader.Lookup("/file.txt")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	r8, err := dstReader.ReadFileContentReader(entry)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	defer r8.Close()
	content, err := io.ReadAll(r8)
	if err != nil {
		t.Fatalf("read content: %v", err)
	}
	if string(content) != "v2 content" {
		t.Errorf("content = %q, want %q", content, "v2 content")
	}
}

func TestStreamWriterAllEntryTypes(t *testing.T) {
	var buf bytes.Buffer
	writer := transfer.NewStreamWriter(&buf)
	rootMeta := pxar.DirMetadata(0o755).Build()

	if err := writer.Begin(&rootMeta, transfer.Options{Format: format.FormatVersion1}); err != nil {
		t.Fatal(err)
	}

	// Write a regular file
	fileEntry := &pxar.Entry{
		Path:     "file.txt",
		Kind:     pxar.KindFile,
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
		Path:     "sub.txt",
		Kind:     pxar.KindFile,
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
	reader := transfer.NewFileReader(bytes.NewReader(buf.Bytes()))
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
	r9, err := reader.ReadFileContentReader(subEntry)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	defer r9.Close()
	content, err := io.ReadAll(r9)
	if err != nil {
		t.Fatalf("read content: %v", err)
	}
	if string(content) != "sub" {
		t.Errorf("content = %q, want %q", content, "sub")
	}
}

func TestCopyTreePathRemapping(t *testing.T) {
	srcData := createNestedArchive(t)
	srcReader := transfer.NewFileReader(bytes.NewReader(srcData))
	defer srcReader.Close()

	var dstBuf bytes.Buffer
	dstWriter := transfer.NewStreamWriter(&dstBuf)
	rootMeta := pxar.DirMetadata(0o755).Build()
	if err := dstWriter.Begin(&rootMeta, transfer.Options{Format: format.FormatVersion1}); err != nil {
		t.Fatal(err)
	}

	// Copy /a → /backup/a (creates intermediate "backup" directory)
	err := transfer.CopyTree(srcReader, dstWriter, "/a", "/backup/a", transfer.CopyOption{})
	if err != nil {
		t.Fatalf("CopyTree: %v", err)
	}

	if err := dstWriter.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	dstReader := transfer.NewFileReader(bytes.NewReader(dstBuf.Bytes()))
	defer dstReader.Close()

	// Should find /backup/a/b/deep.txt (intermediate "backup" directory created)
	deepEntry, err := dstReader.Lookup("/backup/a/b/deep.txt")
	if err != nil {
		t.Fatalf("Lookup /backup/a/b/deep.txt: %v", err)
	}
	if !deepEntry.IsRegularFile() {
		t.Errorf("expected regular file, got %v", deepEntry.Kind)
	}
	r10, err := dstReader.ReadFileContentReader(deepEntry)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	defer r10.Close()
	content, err := io.ReadAll(r10)
	if err != nil {
		t.Fatalf("read content: %v", err)
	}
	if string(content) != "deep" {
		t.Errorf("content = %q, want %q", content, "deep")
	}

	// Should also find /backup/a/mid.txt
	midEntry, err := dstReader.Lookup("/backup/a/mid.txt")
	if err != nil {
		t.Fatalf("Lookup /backup/a/mid.txt: %v", err)
	}
	r11, err := dstReader.ReadFileContentReader(midEntry)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	defer r11.Close()
	midContent, err := io.ReadAll(r11)
	if err != nil {
		t.Fatalf("read content: %v", err)
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
	ts := format.NewStatxTimestampFromDuration(1430487000 * 1e9)
	return &pxar.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFDIR | mode,
			Mtime: ts,
		},
	}
}

func fileMeta(mode uint64, uid, gid uint32) *pxar.Metadata {
	ts := format.NewStatxTimestampFromDuration(1430487000 * 1e9)
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
	ts := format.NewStatxTimestampFromDuration(1430487000 * 1e9)
	return &pxar.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFLNK | mode,
			UID:   uid,
			GID:   gid,
			Mtime: ts,
		},
	}
}

func TestTreeWalker(t *testing.T) {
	data := createTestArchive(t)
	reader := transfer.NewFileReader(bytes.NewReader(data))
	defer reader.Close()

	walker := transfer.NewTreeWalker(reader, 0)
	if err := walker.Init("/"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	var paths []string
	for walker.Next() {
		entry := walker.Entry()
		paths = append(paths, entry.Path)
	}
	if err := walker.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}

	expectedPaths := []string{"/", "/hello.txt", "/link", "/subdir", "/subdir/nested.txt"}
	if len(paths) != len(expectedPaths) {
		t.Errorf("expected %d entries, got %d: %v", len(expectedPaths), len(paths), paths)
	}
}

func TestTreeWalkerWithFilter(t *testing.T) {
	data := createTestArchive(t)
	reader := transfer.NewFileReader(bytes.NewReader(data))
	defer reader.Close()

	walker := transfer.NewTreeWalker(reader, transfer.WalkFiles)
	if err := walker.Init("/"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	for walker.Next() {
		entry := walker.Entry()
		if entry.Kind != pxar.KindFile {
			t.Errorf("expected only files, got kind %d for %q", entry.Kind, entry.Path)
		}
	}
	if err := walker.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
}

func TestEntryPathBytes(t *testing.T) {
	entry := pxar.Entry{Path: "/some/path.txt"}

	pb := entry.PathBytes()
	if string(pb) != "/some/path.txt" {
		t.Errorf("PathBytes = %q, want %q", pb, "/some/path.txt")
	}

	fnb := entry.FileNameBytes()
	if string(fnb) != "path.txt" {
		t.Errorf("FileNameBytes = %q, want %q", fnb, "path.txt")
	}

	// Verify zero-copy: the byte slices should reference the same memory
	if len(pb) > 0 {
		// Just verify they don't panic and return correct content
		var buf strings.Builder
		buf.Write(pb)
		if buf.String() != "/some/path.txt" {
			t.Errorf("Write(PathBytes) = %q, want %q", buf.String(), "/some/path.txt")
		}
	}
}
