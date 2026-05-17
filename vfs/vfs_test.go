package vfs_test

import (
	"bytes"
	"io"
	"os"
	"testing"
	"time"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
	"github.com/pbs-plus/pxar/vfs"
)

func buildTestArchive(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer

	ts := format.NewStatxTimestamp(1000000, 0)
	rootMeta := &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755, Mtime: ts}}

	enc := encoder.NewEncoder(&buf, nil, rootMeta, nil)

	_, err := enc.AddFile(
		&pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFREG | 0o644, UID: 1000, GID: 1000, Mtime: ts}},
		"file.txt",
		[]byte("hello world"),
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := enc.CreateDirectory("dir", &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755, Mtime: ts}}); err != nil {
		t.Fatal(err)
	}
	_, err = enc.AddFile(
		&pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFREG | 0o644, Mtime: ts}},
		"nested.txt",
		[]byte("nested content"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.Finish(); err != nil {
		t.Fatal(err)
	}

	if err := enc.AddSymlink(
		&pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFLNK | 0o777, Mtime: ts}},
		"link",
		"/target",
	); err != nil {
		t.Fatal(err)
	}

	if err := enc.AddDevice(
		&pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFCHR | 0o666, Mtime: ts}},
		"dev",
		format.Device{Major: 1, Minor: 3},
	); err != nil {
		t.Fatal(err)
	}

	if err := enc.AddFIFO(
		&pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFIFO | 0o666, Mtime: ts}},
		"fifo",
	); err != nil {
		t.Fatal(err)
	}

	if err := enc.AddSocket(
		&pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFSOCK | 0o600, Mtime: ts}},
		"sock",
	); err != nil {
		t.Fatal(err)
	}

	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	return &buf
}

func newTestFS(t *testing.T) vfs.FileSystem {
	t.Helper()
	buf := buildTestArchive(t)
	ar := transfer.NewFileArchiveReader(bytes.NewReader(buf.Bytes()))
	return vfs.NewLocalFS(ar)
}

func TestLocalFSStat(t *testing.T) {
	fs := newTestFS(t)
	defer fs.Close()

	fi, err := fs.Stat("/")
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsDir() {
		t.Error("root should be a directory")
	}

	fi, err = fs.Stat("file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if fi.IsDir() {
		t.Error("file.txt should not be a directory")
	}
	if fi.Size() != 11 {
		t.Errorf("file.txt size = %d, want 11", fi.Size())
	}
	if fi.UID() != 1000 || fi.GID() != 1000 {
		t.Errorf("file.txt uid/gid = %d/%d, want 1000/1000", fi.UID(), fi.GID())
	}

	fi, err = fs.Stat("link")
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsSymlink() {
		t.Error("link should be a symlink")
	}

	fi, err = fs.Stat("dev")
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsDevice() {
		t.Error("dev should be a device")
	}

	fi, err = fs.Stat("fifo")
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsFifo() {
		t.Error("fifo should be a FIFO")
	}

	fi, err = fs.Stat("sock")
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsSocket() {
		t.Error("sock should be a socket")
	}
}

func TestLocalFSReadDir(t *testing.T) {
	fs := newTestFS(t)
	defer fs.Close()

	entries, err := fs.ReadDir("/")
	if err != nil {
		t.Fatal(err)
	}

	names := make(map[string]bool)
	for _, e := range entries {
		names[e.Name] = true
	}

	for _, want := range []string{"file.txt", "dir", "link", "dev", "fifo", "sock"} {
		if !names[want] {
			t.Errorf("missing entry %q in readdir", want)
		}
	}
	if len(entries) != 6 {
		t.Errorf("got %d entries, want 6", len(entries))
	}
}

func TestLocalFSOpen(t *testing.T) {
	fs := newTestFS(t)
	defer fs.Close()

	fh, err := fs.Open("file.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fh.Close() }()

	data, err := io.ReadAll(fh)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello world" {
		t.Errorf("file.txt content = %q, want %q", data, "hello world")
	}
}

func TestLocalFSReadFile(t *testing.T) {
	fs := newTestFS(t)
	defer fs.Close()

	data, err := fs.ReadFile("file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello world" {
		t.Errorf("file.txt content = %q, want %q", data, "hello world")
	}
}

func TestLocalFSReadlink(t *testing.T) {
	fs := newTestFS(t)
	defer fs.Close()

	target, err := fs.Readlink("link")
	if err != nil {
		t.Fatal(err)
	}
	if target != "/target" {
		t.Errorf("link target = %q, want %q", target, "/target")
	}
}

func TestLocalFSNestedDir(t *testing.T) {
	fs := newTestFS(t)
	defer fs.Close()

	data, err := fs.ReadFile("dir/nested.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "nested content" {
		t.Errorf("nested.txt = %q, want %q", data, "nested content")
	}

	fi, err := fs.Stat("dir")
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsDir() {
		t.Error("dir should be a directory")
	}

	entries, err := fs.ReadDir("dir")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name != "nested.txt" {
		t.Errorf("dir entries = %v, want [nested.txt]", entries)
	}
}

func TestLocalFSStatCaching(t *testing.T) {
	fs := newTestFS(t)
	defer fs.Close()

	// Stat same path twice — second should hit cache
	fi1, err := fs.Stat("file.txt")
	if err != nil {
		t.Fatal(err)
	}
	fi2, err := fs.Stat("file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if fi1.UID() != fi2.UID() {
		t.Error("cached stat should return same data")
	}
}

func TestLocalFSErrorPaths(t *testing.T) {
	fs := newTestFS(t)
	defer fs.Close()

	_, err := fs.Stat("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent path")
	}

	_, err = fs.Open("dir")
	if err == nil {
		t.Error("expected error opening directory")
	}

	_, err = fs.Readlink("file.txt")
	if err == nil {
		t.Error("expected error readlink on non-symlink")
	}
}

func TestEntryToFileInfo(t *testing.T) {
	ts := format.NewStatxTimestamp(1000000, 500)
	e := &pxar.Entry{
		Path: "test.txt",
		Kind: pxar.KindFile,
		Metadata: pxar.Metadata{
			Stat: format.Stat{
				Mode:  format.ModeIFREG | 0o644,
				UID:   1000,
				GID:   1000,
				Mtime: ts,
			},
		},
		FileSize: 42,
	}

	fi := pxar.EntryToFileInfo(e)
	if fi.Name() != "test.txt" {
		t.Errorf("Name = %q, want %q", fi.Name(), "test.txt")
	}
	if fi.Size() != 42 {
		t.Errorf("Size = %d, want 42", fi.Size())
	}
	if fi.UID() != 1000 || fi.GID() != 1000 {
		t.Errorf("UID/GID = %d/%d, want 1000/1000", fi.UID(), fi.GID())
	}
	if fi.ModTime().Unix() != 1000000 {
		t.Errorf("ModTime unix = %d, want 1000000", fi.ModTime().Unix())
	}
	if fi.IsDir() {
		t.Error("regular file should not report IsDir()")
	}
}

func TestEntryToXAttrs(t *testing.T) {
	// Entry with no xattrs
	e := &pxar.Entry{Path: "empty", Kind: pxar.KindFile}
	if xa := pxar.EntryToXAttrs(e); xa != nil {
		t.Errorf("expected nil for entry with no xattrs, got %v", xa)
	}

	// Entry with xattrs
	e.Metadata.XAttrs = []format.XAttr{
		format.NewXAttr([]byte("user.test"), []byte("value")),
	}
	xa := pxar.EntryToXAttrs(e)
	if xa == nil {
		t.Fatal("expected non-nil xattrs")
	}
	if string(xa["user.test"]) != "value" {
		t.Errorf("xattr user.test = %q, want %q", xa["user.test"], "value")
	}

	// Entry with fcaps
	e2 := &pxar.Entry{
		Path: "caps",
		Kind: pxar.KindFile,
		Metadata: pxar.Metadata{
			FCaps: []byte{0x01, 0x02},
		},
	}
	xa2 := pxar.EntryToXAttrs(e2)
	if xa2 == nil {
		t.Fatal("expected non-nil xattrs for fcaps")
	}
	if string(xa2["security.capability"]) != "\x01\x02" {
		t.Errorf("fcaps = %v, want [1 2]", xa2["security.capability"])
	}
}

func TestWriteTree(t *testing.T) {
	var buf bytes.Buffer
	w := transfer.NewStreamArchiveWriter(&buf)

	ts := format.NewStatxTimestamp(1000000, 0)
	rootMeta := &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755, Mtime: ts}}
	fileMeta := &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFREG | 0o644, Mtime: ts}}
	dirMeta := &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755, Mtime: ts}}
	linkMeta := &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFLNK | 0o777, Mtime: ts}}

	err := vfs.WriteTree(w, rootMeta, transfer.WriterOptions{}, func(dir string) ([]vfs.ChildEntry, error) {
		switch dir {
		case "/":
			return []vfs.ChildEntry{
				{Name: "hello.txt", Kind: pxar.KindFile, Meta: fileMeta, Content: []byte("hello")},
				{Name: "sub", Kind: pxar.KindDirectory, Meta: dirMeta, Children: func(dir string) ([]vfs.ChildEntry, error) {
					return []vfs.ChildEntry{
						{Name: "world.txt", Kind: pxar.KindFile, Meta: fileMeta, Content: []byte("world")},
					}, nil
				}},
				{Name: "link", Kind: pxar.KindSymlink, Meta: linkMeta, LinkTarget: "/target"},
			}, nil
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Read back and verify
	ar := transfer.NewFileArchiveReader(bytes.NewReader(buf.Bytes()))
	fs := vfs.NewLocalFS(ar)
	defer fs.Close()

	data, err := fs.ReadFile("hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Errorf("hello.txt = %q, want %q", data, "hello")
	}

	data, err = fs.ReadFile("sub/world.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "world" {
		t.Errorf("sub/world.txt = %q, want %q", data, "world")
	}

	target, err := fs.Readlink("link")
	if err != nil {
		t.Fatal(err)
	}
	if target != "/target" {
		t.Errorf("link = %q, want %q", target, "/target")
	}
}

func TestStreamTreeWriter(t *testing.T) {
	var buf bytes.Buffer
	ts := format.NewStatxTimestamp(1000000, 0)
	rootMeta := &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755, Mtime: ts}}
	fileMeta := &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFREG | 0o644, Mtime: ts}}

	w := vfs.NewStreamTreeWriter(&buf, nil, rootMeta, nil)

	err := w.WriteTree(func(dir string) ([]vfs.ChildEntry, error) {
		if dir == "/" {
			return []vfs.ChildEntry{
				{Name: "test.txt", Kind: pxar.KindFile, Meta: fileMeta, Content: []byte("stream test")},
			}, nil
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Read back
	ar := transfer.NewFileArchiveReader(bytes.NewReader(buf.Bytes()))
	fs := vfs.NewLocalFS(ar)
	defer fs.Close()

	data, err := fs.ReadFile("test.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "stream test" {
		t.Errorf("test.txt = %q, want %q", data, "stream test")
	}
}

func TestWalkTree(t *testing.T) {
	// Build source archive
	srcBuf := buildTestArchive(t)
	srcAR := transfer.NewFileArchiveReader(bytes.NewReader(srcBuf.Bytes()))
	srcFS := vfs.NewLocalFS(srcAR)
	defer srcFS.Close()

	// Walk source into destination
	var dstBuf bytes.Buffer
	dstW := transfer.NewStreamArchiveWriter(&dstBuf)

	ts := format.NewStatxTimestamp(1000000, 0)
	rootMeta := &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755, Mtime: ts}}

	copied := 0
	err := vfs.WalkTree(srcFS, dstW, rootMeta, transfer.WriterOptions{}, func(srcPath string, info *pxar.FileInfo, entry *vfs.ChildEntry) error {
		copied++
		// Pass through — walk from source and write to dest
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify the copy
	dstAR := transfer.NewFileArchiveReader(bytes.NewReader(dstBuf.Bytes()))
	dstFS := vfs.NewLocalFS(dstAR)
	defer dstFS.Close()

	data, err := dstFS.ReadFile("file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello world" {
		t.Errorf("copied file.txt = %q, want %q", data, "hello world")
	}

	if copied == 0 {
		t.Error("expected at least one entry to be visited")
	}
}

func TestNewFileInfo(t *testing.T) {
	fi := pxar.NewFileInfo("test", 42, os.FileMode(0o644)|os.ModeDir, time.Unix(1000000, 0), 1000, 1000)
	if fi.Name() != "test" {
		t.Errorf("Name = %q", fi.Name())
	}
	if fi.Size() != 42 {
		t.Errorf("Size = %d", fi.Size())
	}
	if !fi.IsDir() {
		t.Error("should be dir")
	}
	if fi.UID() != 1000 {
		t.Errorf("UID = %d", fi.UID())
	}
}
