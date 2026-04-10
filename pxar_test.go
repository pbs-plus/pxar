package pxar_test

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/sonroyaalmerol/pxar"
	"github.com/sonroyaalmerol/pxar/accessor"
	"github.com/sonroyaalmerol/pxar/decoder"
	"github.com/sonroyaalmerol/pxar/encoder"
	"github.com/sonroyaalmerol/pxar/format"
)

func TestMetadataBuilder(t *testing.T) {
	m := pxar.FileMetadata(0o644).
		UID(1000).
		GID(1000).
		Build()

	if !m.IsRegularFile() {
		t.Error("expected regular file")
	}
	if m.Stat.UID != 1000 {
		t.Errorf("UID = %d, want 1000", m.Stat.UID)
	}
	if m.Stat.GID != 1000 {
		t.Errorf("GID = %d, want 1000", m.Stat.GID)
	}
	if m.FileMode() != 0o644 {
		t.Errorf("FileMode = %o, want 0o644", m.FileMode())
	}
}

func TestMetadataBuilderDir(t *testing.T) {
	m := pxar.DirMetadata(0o755).Build()
	if !m.IsDir() {
		t.Error("expected directory")
	}
}

func TestACLEmpty(t *testing.T) {
	a := pxar.ACL{}
	if !a.IsEmpty() {
		t.Error("empty ACL should report empty")
	}

	a.Users = append(a.Users, format.ACLUser{UID: 1000})
	if a.IsEmpty() {
		t.Error("ACL with users should not be empty")
	}
}

func TestEntryKindChecks(t *testing.T) {
	tests := []struct {
		kind     pxar.EntryKind
		isDir    bool
		isFile   bool
		isSymlink bool
	}{
		{pxar.KindDirectory, true, false, false},
		{pxar.KindFile, false, true, false},
		{pxar.KindSymlink, false, false, true},
		{pxar.KindHardlink, false, false, false},
		{pxar.KindDevice, false, false, false},
		{pxar.KindFifo, false, false, false},
		{pxar.KindSocket, false, false, false},
	}
	for _, tt := range tests {
		e := &pxar.Entry{Kind: tt.kind}
		if e.IsDir() != tt.isDir {
			t.Errorf("Kind %v: IsDir() = %v, want %v", tt.kind, e.IsDir(), tt.isDir)
		}
		if e.IsRegularFile() != tt.isFile {
			t.Errorf("Kind %v: IsRegularFile() = %v, want %v", tt.kind, e.IsRegularFile(), tt.isFile)
		}
		if e.IsSymlink() != tt.isSymlink {
			t.Errorf("Kind %v: IsSymlink() = %v, want %v", tt.kind, e.IsSymlink(), tt.isSymlink)
		}
	}
}

func TestEntryFileName(t *testing.T) {
	e := &pxar.Entry{Path: "usr/bin/test"}
	if e.FileName() != "test" {
		t.Errorf("FileName() = %q, want %q", e.FileName(), "test")
	}
}

func TestCheckPathComponent(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{"test.txt", true},
		{"foo", true},
		{".", false},
		{"..", false},
		{"foo/bar", false},
		{"", false},
	}
	for _, tt := range tests {
		got := pxar.CheckPathComponent(tt.path)
		if got != tt.want {
			t.Errorf("CheckPathComponent(%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}

// Integration tests

func TestRoundTripV1Simple(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMeta(0o755), nil)

	_, err := enc.AddFile(fileMeta(0o644, 1000, 1000), "hello.txt", []byte("hello world"))
	if err != nil {
		t.Fatal(err)
	}
	enc.Close()

	dec := decoder.NewDecoder(bytes.NewReader(buf.Bytes()), nil)
	entries := collectAll(t, dec)

	if len(entries) < 2 {
		t.Fatalf("expected at least 2 entries, got %d", len(entries))
	}
	if !entries[0].IsDir() {
		t.Errorf("first entry should be directory, got %v", entries[0].Kind)
	}

	found := false
	for _, e := range entries {
		if e.FileName() == "hello.txt" {
			found = true
			if !e.IsRegularFile() {
				t.Errorf("hello.txt kind = %v", e.Kind)
			}
			if e.FileSize != 11 {
				t.Errorf("hello.txt size = %d, want 11", e.FileSize)
			}
		}
	}
	if !found {
		t.Error("hello.txt not found")
	}
}

func TestRoundTripV1AllEntryTypes(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMeta(0o755), nil)

	enc.AddFile(fileMeta(0o644, 1000, 1000), "file.txt", []byte("content"))
	enc.AddSymlink(symlinkMeta(0o777, 0, 0), "link", "/target")
	enc.AddDevice(deviceMeta(format.ModeIFCHR|0o644, 0, 0), "dev", format.Device{Major: 1, Minor: 3})
	enc.AddFIFO(fifoMeta(0o644, 1000, 1000), "pipe")
	enc.AddSocket(socketMeta(0o644, 1000, 1000), "sock")
	enc.Close()

	dec := decoder.NewDecoder(bytes.NewReader(buf.Bytes()), nil)
	kinds := map[pxar.EntryKind]int{}
	for {
		e, err := dec.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		kinds[e.Kind]++
	}

	if kinds[pxar.KindDirectory] < 1 {
		t.Errorf("expected at least 1 directory, got %d", kinds[pxar.KindDirectory])
	}
	if kinds[pxar.KindFile] != 1 {
		t.Errorf("expected 1 file, got %d", kinds[pxar.KindFile])
	}
	if kinds[pxar.KindSymlink] != 1 {
		t.Errorf("expected 1 symlink, got %d", kinds[pxar.KindSymlink])
	}
	if kinds[pxar.KindDevice] != 1 {
		t.Errorf("expected 1 device, got %d", kinds[pxar.KindDevice])
	}
	if kinds[pxar.KindFifo] != 1 {
		t.Errorf("expected 1 fifo, got %d", kinds[pxar.KindFifo])
	}
	if kinds[pxar.KindSocket] != 1 {
		t.Errorf("expected 1 socket, got %d", kinds[pxar.KindSocket])
	}
}

func TestRoundTripV1NestedDirectories(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMeta(0o755), nil)

	enc.CreateDirectory("a", dirMeta(0o755))
	enc.CreateDirectory("b", dirMeta(0o755))
	enc.AddFile(fileMeta(0o644, 1000, 1000), "deep.txt", []byte("deep"))
	enc.Finish()
	enc.AddFile(fileMeta(0o644, 1000, 1000), "mid.txt", []byte("mid"))
	enc.Finish()
	enc.AddFile(fileMeta(0o644, 1000, 1000), "top.txt", []byte("top"))
	enc.Close()

	dec := decoder.NewDecoder(bytes.NewReader(buf.Bytes()), nil)
	paths := map[string]bool{}
	for {
		e, err := dec.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		paths[e.Path] = true
	}

	for _, p := range []string{"/", "/a", "/a/b", "/a/b/deep.txt", "/a/mid.txt", "/top.txt"} {
		if !paths[p] {
			t.Errorf("path %q not found; got %v", p, paths)
		}
	}
}

func TestRoundTripV1FileContents(t *testing.T) {
	content := []byte("This is a test file with some binary data: \x00\x01\x02\xff")
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMeta(0o755), nil)
	enc.AddFile(fileMeta(0o644, 1000, 1000), "binary.dat", content)
	enc.Close()

	dec := decoder.NewDecoder(bytes.NewReader(buf.Bytes()), nil)
	for {
		e, err := dec.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if e.IsRegularFile() && e.FileName() == "binary.dat" {
			got, err := io.ReadAll(dec.Contents())
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != string(content) {
				t.Errorf("content mismatch: got %d bytes, want %d bytes", len(got), len(content))
			}
		}
	}
}

func TestRoundTripV1StreamingWrite(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMeta(0o755), nil)

	fw, err := enc.CreateFile(fileMeta(0o644, 0, 0), "stream.txt", 12)
	if err != nil {
		t.Fatal(err)
	}
	fw.Write([]byte("hello "))
	fw.WriteAll([]byte("world!"))
	fw.Close()
	enc.Close()

	dec := decoder.NewDecoder(bytes.NewReader(buf.Bytes()), nil)
	for {
		e, err := dec.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if e.IsRegularFile() {
			got, _ := io.ReadAll(dec.Contents())
			if string(got) != "hello world!" {
				t.Errorf("streaming content = %q, want %q", got, "hello world!")
			}
		}
	}
}

func TestRoundTripV1XAttrs(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMeta(0o755), nil)

	meta := fileMeta(0o644, 1000, 1000)
	meta.XAttrs = []format.XAttr{
		format.NewXAttr([]byte("user.test"), []byte("value1")),
		format.NewXAttr([]byte("user.other"), []byte("value2")),
	}
	enc.AddFile(meta, "xattr.txt", []byte("data"))
	enc.Close()

	dec := decoder.NewDecoder(bytes.NewReader(buf.Bytes()), nil)
	for {
		e, err := dec.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if e.FileName() == "xattr.txt" {
			if len(e.Metadata.XAttrs) != 2 {
				t.Fatalf("expected 2 xattrs, got %d", len(e.Metadata.XAttrs))
			}
			if string(e.Metadata.XAttrs[0].Name()) != "user.test" {
				t.Errorf("xattr[0] name = %q", e.Metadata.XAttrs[0].Name())
			}
			if string(e.Metadata.XAttrs[1].Value()) != "value2" {
				t.Errorf("xattr[1] value = %q", e.Metadata.XAttrs[1].Value())
			}
		}
	}
}

func TestRoundTripV1Hardlink(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMeta(0o755), nil)

	offset, _ := enc.AddFile(fileMeta(0o644, 1000, 1000), "original.txt", []byte("data"))
	enc.AddHardlink("link.txt", "original.txt", offset)
	enc.Close()

	dec := decoder.NewDecoder(bytes.NewReader(buf.Bytes()), nil)

	var fileContent []byte
	var linkTarget string
	for {
		e, err := dec.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if e.IsRegularFile() && e.FileName() == "original.txt" {
			fileContent, _ = io.ReadAll(dec.Contents())
		}
		if e.IsHardlink() {
			linkTarget = e.LinkTarget
		}
	}
	if string(fileContent) != "data" {
		t.Errorf("original content = %q, want %q", fileContent, "data")
	}
	if linkTarget != "original.txt" {
		t.Errorf("link target = %q, want %q", linkTarget, "original.txt")
	}
}

func TestRoundTripV2SplitArchive(t *testing.T) {
	var archiveBuf bytes.Buffer
	var payloadBuf bytes.Buffer
	enc := encoder.NewEncoder(&archiveBuf, &payloadBuf, dirMeta(0o755), nil)

	enc.AddFile(fileMeta(0o644, 1000, 1000), "file.txt", []byte("payload data"))
	enc.Close()

	data := archiveBuf.Bytes()
	htype := binary.LittleEndian.Uint64(data[0:8])
	if htype != format.PXARFormatVersion {
		t.Errorf("archive first header = %x, want FORMAT_VERSION", htype)
	}

	payloadData := payloadBuf.Bytes()
	htype = binary.LittleEndian.Uint64(payloadData[0:8])
	if htype != format.PXARPayloadStartMarker {
		t.Errorf("payload start = %x, want PAYLOAD_START_MARKER", htype)
	}

	dec := decoder.NewDecoder(bytes.NewReader(archiveBuf.Bytes()), bytes.NewReader(payloadBuf.Bytes()))
	entries := collectAll(t, dec)

	if len(entries) < 3 {
		t.Fatalf("expected at least 3 entries (version, root, file), got %d", len(entries))
	}
	if entries[0].Kind != pxar.KindVersion {
		t.Errorf("first entry kind = %v, want version", entries[0].Kind)
	}
}

func TestRoundTripV2WithPrelude(t *testing.T) {
	var archiveBuf bytes.Buffer
	var payloadBuf bytes.Buffer
	prelude := []byte("test prelude data")
	enc := encoder.NewEncoder(&archiveBuf, &payloadBuf, dirMeta(0o755), prelude)

	enc.AddFile(fileMeta(0o644, 1000, 1000), "file.txt", []byte("content"))
	enc.Close()

	dec := decoder.NewDecoder(bytes.NewReader(archiveBuf.Bytes()), bytes.NewReader(payloadBuf.Bytes()))
	entries := collectAll(t, dec)

	if len(entries) < 4 {
		t.Fatalf("expected at least 4 entries, got %d", len(entries))
	}
	if entries[0].Kind != pxar.KindVersion {
		t.Errorf("first entry = %v, want version", entries[0].Kind)
	}
	if entries[1].Kind != pxar.KindPrelude {
		t.Errorf("second entry = %v, want prelude", entries[1].Kind)
	}
}

func TestAccessorRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMeta(0o755), nil)

	enc.AddFile(fileMeta(0o644, 1000, 1000), "file1.txt", []byte("content1"))
	enc.AddFile(fileMeta(0o644, 1000, 1000), "file2.txt", []byte("content2"))
	enc.AddSymlink(symlinkMeta(0o777, 0, 0), "link", "/target")
	enc.Close()

	acc := accessor.NewAccessor(bytes.NewReader(buf.Bytes()))

	f1, err := acc.Lookup("/file1.txt")
	if err != nil {
		t.Fatalf("Lookup file1.txt: %v", err)
	}
	if !f1.IsRegularFile() {
		t.Errorf("file1 kind = %v", f1.Kind)
	}

	f2, err := acc.Lookup("/file2.txt")
	if err != nil {
		t.Fatalf("Lookup file2.txt: %v", err)
	}
	if f2.FileSize != 8 {
		t.Errorf("file2 size = %d, want 8", f2.FileSize)
	}

	link, err := acc.Lookup("/link")
	if err != nil {
		t.Fatalf("Lookup link: %v", err)
	}
	if !link.IsSymlink() {
		t.Errorf("link kind = %v", link.Kind)
	}
	if link.LinkTarget != "/target" {
		t.Errorf("link target = %q", link.LinkTarget)
	}

	content, err := acc.ReadFileContent(f1)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	if string(content) != "content1" {
		t.Errorf("content = %q, want %q", content, "content1")
	}
}

func TestAccessorRoundTripNested(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMeta(0o755), nil)

	enc.CreateDirectory("subdir", dirMeta(0o755))
	enc.AddFile(fileMeta(0o644, 1000, 1000), "nested.txt", []byte("nested"))
	enc.Finish()
	enc.Close()

	acc := accessor.NewAccessor(bytes.NewReader(buf.Bytes()))

	entry, err := acc.Lookup("/subdir/nested.txt")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if !entry.IsRegularFile() {
		t.Errorf("kind = %v", entry.Kind)
	}

	content, err := acc.ReadFileContent(entry)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	if string(content) != "nested" {
		t.Errorf("content = %q", content)
	}
}

func TestEncoderDecoderAccessorRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMeta(0o755), nil)

	enc.AddFile(fileMeta(0o644, 0, 0), "readme.txt", []byte("readme content"))
	enc.CreateDirectory("src", dirMeta(0o755))
	enc.AddFile(fileMeta(0o644, 0, 0), "main.go", []byte("package main"))
	enc.Finish()
	enc.AddSocket(socketMeta(0o644, 0, 0), "sock")
	enc.Close()

	archiveData := buf.Bytes()

	// Test decoder
	dec := decoder.NewDecoder(bytes.NewReader(archiveData), nil)
	decEntries := collectAll(t, dec)

	// Test accessor
	acc := accessor.NewAccessor(bytes.NewReader(archiveData))

	for _, e := range decEntries {
		if e.Path == "/" {
			continue
		}
		accEntry, err := acc.Lookup(e.Path)
		if err != nil {
			t.Errorf("Accessor Lookup %q failed: %v", e.Path, err)
			continue
		}
		if accEntry.Kind != e.Kind {
			t.Errorf("Accessor %q kind = %v, decoder kind = %v", e.Path, accEntry.Kind, e.Kind)
		}
	}

	readme, err := acc.Lookup("/readme.txt")
	if err != nil {
		t.Fatal(err)
	}
	data, err := acc.ReadFileContent(readme)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "readme content" {
		t.Errorf("readme content = %q", data)
	}

	mainFile, err := acc.Lookup("/src/main.go")
	if err != nil {
		t.Fatal(err)
	}
	data, err = acc.ReadFileContent(mainFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "package main" {
		t.Errorf("main.go content = %q", data)
	}

	sock, err := acc.Lookup("/sock")
	if err != nil {
		t.Fatal(err)
	}
	if !sock.IsSocket() {
		t.Errorf("sock kind = %v", sock.Kind)
	}
}

// Helpers

func dirMeta(mode uint64) *pxar.Metadata {
	ts := format.StatxTimestampFromDurationSinceEpoch(1430487000 * time.Second)
	return &pxar.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFDIR | mode,
			Mtime: ts,
		},
	}
}

func fileMeta(mode uint64, uid, gid uint32) *pxar.Metadata {
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

func symlinkMeta(mode uint64, uid, gid uint32) *pxar.Metadata {
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

func deviceMeta(mode uint64, uid, gid uint32) *pxar.Metadata {
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

func fifoMeta(mode uint64, uid, gid uint32) *pxar.Metadata {
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

func socketMeta(mode uint64, uid, gid uint32) *pxar.Metadata {
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

func collectAll(t *testing.T, dec *decoder.Decoder) []pxar.Entry {
	t.Helper()
	var entries []pxar.Entry
	for {
		e, err := dec.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, *e)
	}
	return entries
}
