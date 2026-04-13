package accessor

import (
	"bytes"
	"testing"
	"time"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
)

func TestAccessorReadRoot(t *testing.T) {
	archive := encodeSimpleArchive(t, "test.txt", []byte("hello"))
	acc := NewAccessor(bytes.NewReader(archive))

	root, err := acc.ReadRoot()
	if err != nil {
		t.Fatalf("ReadRoot: %v", err)
	}
	if !root.IsDir() {
		t.Errorf("root kind = %v, want directory", root.Kind)
	}
	if root.Path != "/" {
		t.Errorf("root path = %q, want %q", root.Path, "/")
	}
}

func TestAccessorLookupFile(t *testing.T) {
	archive := encodeSimpleArchive(t, "test.txt", []byte("hello world"))
	acc := NewAccessor(bytes.NewReader(archive))

	entry, err := acc.Lookup("/test.txt")
	if err != nil {
		t.Fatalf("Lookup /test.txt: %v", err)
	}
	if !entry.IsRegularFile() {
		t.Errorf("kind = %v, want file", entry.Kind)
	}
	if entry.FileSize != 11 {
		t.Errorf("size = %d, want 11", entry.FileSize)
	}
	if entry.FileName() != "test.txt" {
		t.Errorf("name = %q, want %q", entry.FileName(), "test.txt")
	}
}

func TestAccessorLookupRoot(t *testing.T) {
	archive := encodeSimpleArchive(t, "test.txt", []byte("hello"))
	acc := NewAccessor(bytes.NewReader(archive))

	entry, err := acc.Lookup("/")
	if err != nil {
		t.Fatalf("Lookup /: %v", err)
	}
	if !entry.IsDir() {
		t.Errorf("kind = %v, want directory", entry.Kind)
	}
}

func TestAccessorLookupNotFound(t *testing.T) {
	archive := encodeSimpleArchive(t, "test.txt", []byte("hello"))
	acc := NewAccessor(bytes.NewReader(archive))

	_, err := acc.Lookup("/nonexistent.txt")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestAccessorListDirectory(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMetadata(0o755), nil)
	enc.AddFile(fileMetadata(0o644, 1000, 1000), "file1.txt", []byte("one"))
	enc.AddFile(fileMetadata(0o644, 1000, 1000), "file2.txt", []byte("two"))
	enc.Close()

	acc := NewAccessor(bytes.NewReader(buf.Bytes()))

	rootOffset, err := acc.getRootContentOffset()
	if err != nil {
		t.Fatalf("getRootContentOffset: %v", err)
	}

	entries, err := acc.ListDirectory(rootOffset)
	if err != nil {
		t.Fatalf("ListDirectory: %v", err)
	}

	names := make(map[string]bool)
	for _, e := range entries {
		names[e.FileName()] = true
	}
	if !names["file1.txt"] {
		t.Error("file1.txt not found in directory listing")
	}
	if !names["file2.txt"] {
		t.Error("file2.txt not found in directory listing")
	}
}

func TestAccessorReadFileContent(t *testing.T) {
	content := []byte("This is the file content!")
	archive := encodeSimpleArchive(t, "data.bin", content)
	acc := NewAccessor(bytes.NewReader(archive))

	entry, err := acc.Lookup("/data.bin")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}

	data, err := acc.ReadFileContent(entry)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	if string(data) != string(content) {
		t.Errorf("content = %q, want %q", data, content)
	}
}

func TestAccessorLookupNestedDirectory(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMetadata(0o755), nil)
	enc.CreateDirectory("subdir", dirMetadata(0o755))
	enc.AddFile(fileMetadata(0o644, 1000, 1000), "nested.txt", []byte("nested content"))
	enc.Finish()
	enc.Close()

	acc := NewAccessor(bytes.NewReader(buf.Bytes()))

	// Lookup directory
	dirEntry, err := acc.Lookup("/subdir")
	if err != nil {
		t.Fatalf("Lookup /subdir: %v", err)
	}
	if !dirEntry.IsDir() {
		t.Errorf("subdir kind = %v, want directory", dirEntry.Kind)
	}

	// Lookup nested file
	fileEntry, err := acc.Lookup("/subdir/nested.txt")
	if err != nil {
		t.Fatalf("Lookup /subdir/nested.txt: %v", err)
	}
	if !fileEntry.IsRegularFile() {
		t.Errorf("nested.txt kind = %v, want file", fileEntry.Kind)
	}
	if fileEntry.FileSize != 14 {
		t.Errorf("nested.txt size = %d, want 14", fileEntry.FileSize)
	}
}

func TestAccessorListNestedDirectory(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMetadata(0o755), nil)
	enc.CreateDirectory("subdir", dirMetadata(0o755))
	enc.AddFile(fileMetadata(0o644, 1000, 1000), "a.txt", []byte("aaa"))
	enc.AddFile(fileMetadata(0o644, 1000, 1000), "b.txt", []byte("bbb"))
	enc.Finish()
	enc.Close()

	acc := NewAccessor(bytes.NewReader(buf.Bytes()))

	dirEntry, err := acc.Lookup("/subdir")
	if err != nil {
		t.Fatalf("Lookup /subdir: %v", err)
	}

	_, err = acc.getRootContentOffset()
	if err != nil {
		t.Fatalf("getRootContentOffset: %v", err)
	}

	subDirOffset, err := acc.findDirContentOffset(int64(dirEntry.FileOffset))
	if err != nil {
		t.Fatalf("findDirContentOffset: %v", err)
	}

	entries, err := acc.ListDirectory(subDirOffset)
	if err != nil {
		t.Fatalf("ListDirectory subdir: %v", err)
	}

	names := make(map[string]bool)
	for _, e := range entries {
		names[e.FileName()] = true
	}
	if !names["a.txt"] {
		t.Error("a.txt not found in subdir listing")
	}
	if !names["b.txt"] {
		t.Error("b.txt not found in subdir listing")
	}
}

func TestAccessorReadNestedFileContent(t *testing.T) {
	content := []byte("deeply nested file data")
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMetadata(0o755), nil)
	enc.CreateDirectory("level1", dirMetadata(0o755))
	enc.CreateDirectory("level2", dirMetadata(0o755))
	enc.AddFile(fileMetadata(0o644, 1000, 1000), "deep.txt", content)
	enc.Finish()
	enc.Finish()
	enc.Close()

	acc := NewAccessor(bytes.NewReader(buf.Bytes()))

	entry, err := acc.Lookup("/level1/level2/deep.txt")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}

	data, err := acc.ReadFileContent(entry)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	if string(data) != string(content) {
		t.Errorf("content = %q, want %q", data, content)
	}
}

func TestAccessorSymlink(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMetadata(0o755), nil)
	enc.AddSymlink(symlinkMetadata(0o777, 0, 0), "link", "/usr/bin/python3")
	enc.Close()

	acc := NewAccessor(bytes.NewReader(buf.Bytes()))

	entry, err := acc.Lookup("/link")
	if err != nil {
		t.Fatalf("Lookup /link: %v", err)
	}
	if !entry.IsSymlink() {
		t.Errorf("kind = %v, want symlink", entry.Kind)
	}
	if entry.LinkTarget != "/usr/bin/python3" {
		t.Errorf("target = %q, want %q", entry.LinkTarget, "/usr/bin/python3")
	}
}

func TestAccessorDevice(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMetadata(0o755), nil)
	enc.AddDevice(deviceMetadata(format.ModeIFCHR|0o644, 0, 0), "null", format.Device{Major: 1, Minor: 3})
	enc.Close()

	acc := NewAccessor(bytes.NewReader(buf.Bytes()))

	entry, err := acc.Lookup("/null")
	if err != nil {
		t.Fatalf("Lookup /null: %v", err)
	}
	if !entry.IsDevice() {
		t.Errorf("kind = %v, want device", entry.Kind)
	}
	if entry.DeviceInfo.Major != 1 || entry.DeviceInfo.Minor != 3 {
		t.Errorf("device = %+v, want Major=1, Minor=3", entry.DeviceInfo)
	}
}

func TestAccessorFIFO(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMetadata(0o755), nil)
	enc.AddFIFO(fifoMetadata(0o644, 1000, 1000), "pipe")
	enc.Close()

	acc := NewAccessor(bytes.NewReader(buf.Bytes()))

	entry, err := acc.Lookup("/pipe")
	if err != nil {
		t.Fatalf("Lookup /pipe: %v", err)
	}
	if !entry.IsFifo() {
		t.Errorf("kind = %v, want fifo", entry.Kind)
	}
}

func TestAccessorSocket(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMetadata(0o755), nil)
	enc.AddSocket(socketMetadata(0o644, 1000, 1000), "sock")
	enc.Close()

	acc := NewAccessor(bytes.NewReader(buf.Bytes()))

	entry, err := acc.Lookup("/sock")
	if err != nil {
		t.Fatalf("Lookup /sock: %v", err)
	}
	if !entry.IsSocket() {
		t.Errorf("kind = %v, want socket", entry.Kind)
	}
}

func TestAccessorHardlink(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMetadata(0o755), nil)
	offset, _ := enc.AddFile(fileMetadata(0o644, 1000, 1000), "original.txt", []byte("content"))
	enc.AddHardlink("link.txt", "original.txt", offset)
	enc.Close()

	acc := NewAccessor(bytes.NewReader(buf.Bytes()))

	entry, err := acc.Lookup("/link.txt")
	if err != nil {
		t.Fatalf("Lookup /link.txt: %v", err)
	}
	if !entry.IsHardlink() {
		t.Errorf("kind = %v, want hardlink", entry.Kind)
	}
	if entry.LinkTarget != "original.txt" {
		t.Errorf("target = %q, want %q", entry.LinkTarget, "original.txt")
	}
}

func TestAccessorMetadata(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMetadata(0o755), nil)
	enc.AddFile(fileMetadata(0o644, 1000, 1000), "test.txt", []byte("hello"))
	enc.Close()

	acc := NewAccessor(bytes.NewReader(buf.Bytes()))

	entry, err := acc.Lookup("/test.txt")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if entry.Metadata.Stat.UID != 1000 {
		t.Errorf("UID = %d, want 1000", entry.Metadata.Stat.UID)
	}
	if entry.Metadata.Stat.GID != 1000 {
		t.Errorf("GID = %d, want 1000", entry.Metadata.Stat.GID)
	}
	if entry.Metadata.Stat.FileMode() != 0o644 {
		t.Errorf("FileMode = %o, want 0o644", entry.Metadata.Stat.FileMode())
	}
}

func TestAccessorRootIsNotRegularFile(t *testing.T) {
	archive := encodeSimpleArchive(t, "test.txt", []byte("hello"))
	acc := NewAccessor(bytes.NewReader(archive))

	root, err := acc.ReadRoot()
	if err != nil {
		t.Fatalf("ReadRoot: %v", err)
	}

	_, err = acc.ReadFileContent(root)
	if err == nil {
		t.Error("expected error when reading directory content")
	}
}

func TestAccessorMultipleLookups(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMetadata(0o755), nil)
	enc.AddFile(fileMetadata(0o644, 1000, 1000), "a.txt", []byte("aaa"))
	enc.AddFile(fileMetadata(0o644, 1000, 1000), "b.txt", []byte("bbb"))
	enc.AddFile(fileMetadata(0o644, 1000, 1000), "c.txt", []byte("ccc"))
	enc.Close()

	acc := NewAccessor(bytes.NewReader(buf.Bytes()))

	for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
		entry, err := acc.Lookup("/" + name)
		if err != nil {
			t.Fatalf("Lookup /%s: %v", name, err)
		}
		if !entry.IsRegularFile() {
			t.Errorf("%s kind = %v, want file", name, entry.Kind)
		}
	}
}

func TestAccessorV2Archive(t *testing.T) {
	content := []byte("Hello from split archive v2!")
	var archiveBuf bytes.Buffer
	var payloadBuf bytes.Buffer
	enc := encoder.NewEncoder(&archiveBuf, &payloadBuf, dirMetadata(0o755), nil)
	enc.AddFile(fileMetadata(0o644, 1000, 1000), "file.txt", content)
	enc.Close()

	// For split archives, provide both archive and payload readers
	acc := NewAccessor(bytes.NewReader(archiveBuf.Bytes()), bytes.NewReader(payloadBuf.Bytes()))

	root, err := acc.ReadRoot()
	if err != nil {
		t.Fatalf("ReadRoot: %v", err)
	}
	if !root.IsDir() {
		t.Errorf("root kind = %v, want directory", root.Kind)
	}

	entry, err := acc.Lookup("/file.txt")
	if err != nil {
		t.Fatalf("Lookup /file.txt: %v", err)
	}
	if !entry.IsRegularFile() {
		t.Errorf("kind = %v, want file", entry.Kind)
	}
	if entry.FileSize != uint64(len(content)) {
		t.Errorf("file size = %d, want %d", entry.FileSize, len(content))
	}
	if entry.PayloadOffset == 0 {
		t.Error("PayloadOffset should be set for split archive files")
	}

	// Test reading file content from payload stream
	data, err := acc.ReadFileContent(entry)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	if string(data) != string(content) {
		t.Errorf("content = %q, want %q", data, content)
	}
}

func TestAccessorV2ArchiveWithoutPayloadReader(t *testing.T) {
	content := []byte("Hello from split archive v2!")
	var archiveBuf bytes.Buffer
	var payloadBuf bytes.Buffer
	enc := encoder.NewEncoder(&archiveBuf, &payloadBuf, dirMetadata(0o755), nil)
	enc.AddFile(fileMetadata(0o644, 1000, 1000), "file.txt", content)
	enc.Close()

	// Try to read without providing payload reader - should fail
	acc := NewAccessor(bytes.NewReader(archiveBuf.Bytes()))

	entry, err := acc.Lookup("/file.txt")
	if err != nil {
		t.Fatalf("Lookup /file.txt: %v", err)
	}

	// Reading content should fail because payload reader is not provided
	_, err = acc.ReadFileContent(entry)
	if err == nil {
		t.Error("ReadFileContent should fail without payload reader for split archives")
	}
}

// Helpers

func encodeSimpleArchive(t *testing.T, name string, content []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMetadata(0o755), nil)
	_, err := enc.AddFile(fileMetadata(0o644, 1000, 1000), name, content)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	enc.Close()
	return buf.Bytes()
}

func dirMetadata(mode uint64) *pxar.Metadata {
	ts := format.StatxTimestampFromDurationSinceEpoch(1430487000 * time.Second)
	return &pxar.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFDIR | mode,
			Mtime: ts,
		},
	}
}

func fileMetadata(mode uint64, uid, gid uint32) *pxar.Metadata {
	ts := format.StatxTimestampFromDurationSinceEpoch(1430487000 * time.Second)
	return &pxar.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFREG | mode,
			UID:   uid,
			GID:   gid,
			Mtime: ts,
		},
	}
}

func symlinkMetadata(mode uint64, uid, gid uint32) *pxar.Metadata {
	ts := format.StatxTimestampFromDurationSinceEpoch(1430487000 * time.Second)
	return &pxar.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFLNK | mode,
			UID:   uid,
			GID:   gid,
			Mtime: ts,
		},
	}
}

func deviceMetadata(mode uint64, uid, gid uint32) *pxar.Metadata {
	ts := format.StatxTimestampFromDurationSinceEpoch(1430487000 * time.Second)
	return &pxar.Metadata{
		Stat: format.Stat{
			Mode:  mode,
			UID:   uid,
			GID:   gid,
			Mtime: ts,
		},
	}
}

func fifoMetadata(mode uint64, uid, gid uint32) *pxar.Metadata {
	ts := format.StatxTimestampFromDurationSinceEpoch(1430487000 * time.Second)
	return &pxar.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFIFO | mode,
			UID:   uid,
			GID:   gid,
			Mtime: ts,
		},
	}
}

func socketMetadata(mode uint64, uid, gid uint32) *pxar.Metadata {
	ts := format.StatxTimestampFromDurationSinceEpoch(1430487000 * time.Second)
	return &pxar.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFSOCK | mode,
			UID:   uid,
			GID:   gid,
			Mtime: ts,
		},
	}
}
