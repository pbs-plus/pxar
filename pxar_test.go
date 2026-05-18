package pxar_test

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/accessor"
	"github.com/pbs-plus/pxar/decoder"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
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
		{pxar.KindFIFO, false, false, false},
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

	_, _ = enc.AddFile(fileMeta(0o644, 1000, 1000), "file.txt", []byte("content"))
	_ = enc.AddSymlink(symlinkMeta(0o777, 0, 0), "link", "/target")
	_ = enc.AddDevice(deviceMeta(format.ModeIFCHR|0o644, 0, 0), "dev", format.Device{Major: 1, Minor: 3})
	_ = enc.AddFIFO(fifoMeta(0o644, 1000, 1000), "pipe")
	_ = enc.AddSocket(socketMeta(0o644, 1000, 1000), "sock")
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
	if kinds[pxar.KindFIFO] != 1 {
		t.Errorf("expected 1 fifo, got %d", kinds[pxar.KindFIFO])
	}
	if kinds[pxar.KindSocket] != 1 {
		t.Errorf("expected 1 socket, got %d", kinds[pxar.KindSocket])
	}
}

func TestRoundTripV1NestedDirectories(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMeta(0o755), nil)

	_ = enc.CreateDirectory("a", dirMeta(0o755))
	_ = enc.CreateDirectory("b", dirMeta(0o755))
	_, _ = enc.AddFile(fileMeta(0o644, 1000, 1000), "deep.txt", []byte("deep"))
	_ = enc.Finish()
	_, _ = enc.AddFile(fileMeta(0o644, 1000, 1000), "mid.txt", []byte("mid"))
	_ = enc.Finish()
	_, _ = enc.AddFile(fileMeta(0o644, 1000, 1000), "top.txt", []byte("top"))
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
	_, _ = enc.AddFile(fileMeta(0o644, 1000, 1000), "binary.dat", content)
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
	_, _ = fw.Write([]byte("hello "))
	_ = fw.WriteAll([]byte("world!"))
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
	_, _ = enc.AddFile(meta, "xattr.txt", []byte("data"))
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
	_ = enc.AddHardlink("link.txt", "original.txt", offset)
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

	_, _ = enc.AddFile(fileMeta(0o644, 1000, 1000), "file.txt", []byte("payload data"))
	enc.Close()

	data := archiveBuf.Bytes()
	htype := binary.LittleEndian.Uint64(data[0:8])
	if htype != format.Version {
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

	_, _ = enc.AddFile(fileMeta(0o644, 1000, 1000), "file.txt", []byte("content"))
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

	_, _ = enc.AddFile(fileMeta(0o644, 1000, 1000), "file1.txt", []byte("content1"))
	_, _ = enc.AddFile(fileMeta(0o644, 1000, 1000), "file2.txt", []byte("content2"))
	_ = enc.AddSymlink(symlinkMeta(0o777, 0, 0), "link", "/target")
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

	r, err := acc.ReadFileContentReader(f1)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	defer r.Close()
	content, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read content: %v", err)
	}
	if string(content) != "content1" {
		t.Errorf("content = %q, want %q", content, "content1")
	}
}

func TestAccessorRoundTripNested(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMeta(0o755), nil)

	_ = enc.CreateDirectory("subdir", dirMeta(0o755))
	_, _ = enc.AddFile(fileMeta(0o644, 1000, 1000), "nested.txt", []byte("nested"))
	_ = enc.Finish()
	enc.Close()

	acc := accessor.NewAccessor(bytes.NewReader(buf.Bytes()))

	entry, err := acc.Lookup("/subdir/nested.txt")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if !entry.IsRegularFile() {
		t.Errorf("kind = %v", entry.Kind)
	}

	r, err := acc.ReadFileContentReader(entry)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	defer r.Close()
	content, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read content: %v", err)
	}
	if string(content) != "nested" {
		t.Errorf("content = %q", content)
	}
}

func TestEncoderDecoderAccessorRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMeta(0o755), nil)

	_, _ = enc.AddFile(fileMeta(0o644, 0, 0), "readme.txt", []byte("readme content"))
	_ = enc.CreateDirectory("src", dirMeta(0o755))
	_, _ = enc.AddFile(fileMeta(0o644, 0, 0), "main.go", []byte("package main"))
	_ = enc.Finish()
	_ = enc.AddSocket(socketMeta(0o644, 0, 0), "sock")
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
	r, err := acc.ReadFileContentReader(readme)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
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
	r2, err := acc.ReadFileContentReader(mainFile)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	data, err = io.ReadAll(r2)
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
	ts := format.NewStatxTimestampFromDuration(1430487000 * time.Second)
	return &pxar.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFDIR | mode,
			Mtime: ts,
		},
	}
}

func fileMeta(mode uint64, uid, gid uint32) *pxar.Metadata {
	ts := format.NewStatxTimestampFromDuration(1430487000 * time.Second)
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
	ts := format.NewStatxTimestampFromDuration(1430487000 * time.Second)
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
	ts := format.NewStatxTimestampFromDuration(1430487000 * time.Second)
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
	ts := format.NewStatxTimestampFromDuration(1430487000 * time.Second)
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
	ts := format.NewStatxTimestampFromDuration(1430487000 * time.Second)
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

// TestRustParityFullFilesystemRoundTrip mirrors Rust's test1 in tests/simple/main.rs.
// It encodes a complex filesystem matching Rust's test_fs(), decodes it back,
// and verifies via accessor that all entries are correctly accessible.
//
// The filesystem structure matches Rust's test_fs():
//   / (dir, 0o755)
//     home/ (dir, 0o755)
//       user/ (dir, 0o700, uid=1000, gid=1000)
//         .profile (file, 0o644, uid=1000, gid=1000, content="#umask 022")
//         data (file, 0o644, uid=1000, gid=1000, content="a file from a user")
//     bin -> usr/bin (symlink, 0o777)
//     usr/ (dir, 0o755)
//       bin/ (dir, 0o755)
//         bzip2 (file, 0o755, content="This is the bzip2 executable")
//         cat (file, 0o755, content="This is another executable")
//         bunzip2 (hardlink -> bzip2)
//     dev/ (dir, 0o755)
//       null (chardev, 0o666, major=1, minor=3)
//       zero (chardev, 0o666, major=1, minor=5)
//       loop0 (blkdev, 0o666, major=7, minor=0)
//       loop1 (blkdev, 0o666, major=7, minor=1, acl_user uid=1000 perm=6, acl_group gid=1000 perm=6)
//     run/ (dir, 0o755, default_acl)
//       fifo0 (fifo, 0o666)
//       sock0 (socket, 0o600)
func TestRustParityFullFilesystemRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	ts := format.NewStatxTimestampFromDuration(1430487000 * time.Second)

	rootMeta := &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755, Mtime: ts}}
	enc := encoder.NewEncoder(&buf, nil, rootMeta, nil)

	// home/
	enc.CreateDirectory("home", &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755, Mtime: ts}})

	// home/user/
	enc.CreateDirectory("user", &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o700, UID: 1000, GID: 1000, Mtime: ts}})

	// home/user/.profile
	enc.AddFile(&pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFREG | 0o644, UID: 1000, GID: 1000, Mtime: ts}},
		".profile", []byte("#umask 022"))

	// home/user/data
	enc.AddFile(&pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFREG | 0o644, UID: 1000, GID: 1000, Mtime: ts}},
		"data", []byte("a file from a user"))

	// leave home/user
	enc.Finish() // user
	enc.Finish() // home

	// bin -> usr/bin (symlink)
	enc.AddSymlink(&pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFLNK | 0o777, Mtime: ts}},
		"bin", "usr/bin")

	// usr/
	enc.CreateDirectory("usr", &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755, Mtime: ts}})

	// usr/bin/
	enc.CreateDirectory("bin", &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755, Mtime: ts}})

	// usr/bin/bzip2
	bzip2Offset, err := enc.AddFile(&pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFREG | 0o755, Mtime: ts}},
		"bzip2", []byte("This is the bzip2 executable"))
	if err != nil {
		t.Fatal(err)
	}

	// usr/bin/cat
	enc.AddFile(&pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFREG | 0o755, Mtime: ts}},
		"cat", []byte("This is another executable"))

	// usr/bin/bunzip2 (hardlink to bzip2)
	enc.AddHardlink("bunzip2", "bzip2", bzip2Offset)

	enc.Finish() // usr/bin
	enc.Finish() // usr

	// dev/
	enc.CreateDirectory("dev", &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755, Mtime: ts}})

	// dev/null (chardev 1,3)
	enc.AddDevice(&pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFCHR | 0o666, Mtime: ts}},
		"null", format.Device{Major: 1, Minor: 3})

	// dev/zero (chardev 1,5)
	enc.AddDevice(&pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFCHR | 0o666, Mtime: ts}},
		"zero", format.Device{Major: 1, Minor: 5})

	// dev/loop0 (blkdev 7,0)
	enc.AddDevice(&pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFBLK | 0o666, Mtime: ts}},
		"loop0", format.Device{Major: 7, Minor: 0})

	// dev/loop1 (blkdev 7,1, with ACL user+group)
	loop1Meta := &pxar.Metadata{
		Stat: format.Stat{Mode: format.ModeIFBLK | 0o666, Mtime: ts},
		ACL: pxar.ACL{
			Users:  []format.ACLUser{{UID: 1000, Permissions: 6}},
			Groups: []format.ACLGroup{{GID: 1000, Permissions: 6}},
		},
	}
	enc.AddDevice(loop1Meta, "loop1", format.Device{Major: 7, Minor: 1})

	enc.Finish() // dev

	// run/ (with default ACL)
	runMeta := &pxar.Metadata{
		Stat: format.Stat{Mode: format.ModeIFDIR | 0o755, Mtime: ts},
		ACL: pxar.ACL{
			Default: &format.ACLDefault{
				UserObjPermissions:  format.ACLPermissions(4 | 2), // READ|WRITE
				GroupObjPermissions: format.ACLPermissions(4),   // READ
				OtherPermissions:    format.ACLPermissions(4),   // READ
				MaskPermissions:     format.ACLPermissions(format.ACLNoMask),
			},
			DefaultUsers: []format.ACLUser{{UID: 1001, Permissions: 4}}, // READ
		},
	}
	enc.CreateDirectory("run", runMeta)

	// run/fifo0
	enc.AddFIFO(&pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFIFO | 0o666, Mtime: ts}}, "fifo0")

	// run/sock0
	enc.AddSocket(&pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFSOCK | 0o600, Mtime: ts}}, "sock0")

	enc.Finish() // run
	enc.Close()  // root

	archive := buf.Bytes()

	// --- Phase 1: Decode and verify all entry types ---
	dec := decoder.NewDecoder(bytes.NewReader(archive), nil)

	type decodedEntry struct {
		path string
		kind pxar.EntryKind
	}
	var entries []decodedEntry
	for {
		e, err := dec.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("decode error: %v", err)
		}
		entries = append(entries, decodedEntry{path: e.Path, kind: e.Kind})
	}

	if len(entries) == 0 {
		t.Fatal("expected at least 4 entries, got 0")
	}

	// --- Phase 2: Accessor verification (mirrors Rust's check_bunzip2 + check_run_special_files) ---
	acc := accessor.NewAccessor(bytes.NewReader(archive))

	root, err := acc.ReadRoot()
	if err != nil {
		t.Fatalf("ReadRoot: %v", err)
	}
	if !root.IsDir() {
		t.Fatal("root should be a directory")
	}

	// Verify hardlink follow (Rust: check_bunzip2)
	bunzip2, err := acc.Lookup("/usr/bin/bunzip2")
	if err != nil {
		t.Fatalf("lookup /usr/bin/bunzip2: %v", err)
	}
	if !bunzip2.IsHardlink() {
		t.Fatalf("expected hardlink, got %v", bunzip2.Kind)
	}

	bzip2, err := acc.FollowHardlink(bunzip2)
	if err != nil {
		t.Fatalf("FollowHardlink bunzip2: %v", err)
	}
	if !bzip2.IsRegularFile() {
		t.Fatalf("expected regular file after follow, got %v", bzip2.Kind)
	}

	r, err := acc.ReadFileContentReader(bzip2)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	content, err := io.ReadAll(r)
	r.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(content) != "This is the bzip2 executable" {
		t.Errorf("bzip2 content = %q, want %q", content, "This is the bzip2 executable")
	}

	// Verify symlink (Rust: bin -> usr/bin)
	binLink, err := acc.Lookup("/bin")
	if err != nil {
		t.Fatalf("lookup /bin: %v", err)
	}
	if !binLink.IsSymlink() {
		t.Fatalf("/bin should be symlink, got %v", binLink.Kind)
	}
	if binLink.LinkTarget != "usr/bin" {
		t.Errorf("/bin target = %q, want %q", binLink.LinkTarget, "usr/bin")
	}

	// Verify devices
	nullEntry, err := acc.Lookup("/dev/null")
	if err != nil {
		t.Fatalf("lookup /dev/null: %v", err)
	}
	if !nullEntry.IsDevice() {
		t.Fatalf("/dev/null should be device, got %v", nullEntry.Kind)
	}
	if nullEntry.DeviceInfo != (format.Device{Major: 1, Minor: 3}) {
		t.Errorf("/dev/null device = %+v, want {1,3}", nullEntry.DeviceInfo)
	}

	loop1, err := acc.Lookup("/dev/loop1")
	if err != nil {
		t.Fatalf("lookup /dev/loop1: %v", err)
	}
	if loop1.DeviceInfo != (format.Device{Major: 7, Minor: 1}) {
		t.Errorf("/dev/loop1 device = %+v, want {7,1}", loop1.DeviceInfo)
	}

	// Verify special files in /run (Rust: check_run_special_files)
	fifoEntry, err := acc.Lookup("/run/fifo0")
	if err != nil {
		t.Fatalf("lookup /run/fifo0: %v", err)
	}
	if !fifoEntry.IsFIFO() {
		t.Fatalf("/run/fifo0 should be FIFO, got %v", fifoEntry.Kind)
	}
	if fifoEntry.FileName() != "fifo0" {
		t.Errorf("/run/fifo0 name = %q, want %q", fifoEntry.FileName(), "fifo0")
	}

	sockEntry, err := acc.Lookup("/run/sock0")
	if err != nil {
		t.Fatalf("lookup /run/sock0: %v", err)
	}
	if !sockEntry.IsSocket() {
		t.Fatalf("/run/sock0 should be socket, got %v", sockEntry.Kind)
	}
	if sockEntry.FileName() != "sock0" {
		t.Errorf("/run/sock0 name = %q, want %q", sockEntry.FileName(), "sock0")
	}

	// Verify /run directory has exactly 2 children
	runDir, err := acc.Lookup("/run")
	if err != nil {
		t.Fatalf("lookup /run: %v", err)
	}
	if !runDir.IsDir() {
		t.Fatalf("/run should be directory, got %v", runDir.Kind)
	}
	var runCount int
	err = acc.ListDirectory(int64(runDir.ContentOffset), accessor.ListOption{}, func(e *pxar.Entry) error {
		runCount++
		return nil
	})
	if err != nil {
		t.Fatalf("ListDirectory /run: %v", err)
	}
	if runCount != 2 {
		t.Errorf("/run entry count = %d, want 2", runCount)
	}

	// Verify user files
	profileEntry, err := acc.Lookup("/home/user/.profile")
	if err != nil {
		t.Fatalf("lookup /home/user/.profile: %v", err)
	}
	if !profileEntry.IsRegularFile() {
		t.Fatalf("/home/user/.profile should be file, got %v", profileEntry.Kind)
	}
	r2, err := acc.ReadFileContentReader(profileEntry)
	if err != nil {
		t.Fatalf("ReadFileContent .profile: %v", err)
	}
	profileContent, _ := io.ReadAll(r2)
	r2.Close()
	if string(profileContent) != "#umask 022" {
		t.Errorf(".profile content = %q, want %q", profileContent, "#umask 022")
	}

	// Verify /home/user/data content
	dataEntry, err := acc.Lookup("/home/user/data")
	if err != nil {
		t.Fatalf("lookup /home/user/data: %v", err)
	}
	r3, err := acc.ReadFileContentReader(dataEntry)
	if err != nil {
		t.Fatalf("ReadFileContent data: %v", err)
	}
	dataContent, _ := io.ReadAll(r3)
	r3.Close()
	if string(dataContent) != "a file from a user" {
		t.Errorf("data content = %q, want %q", dataContent, "a file from a user")
	}
}
