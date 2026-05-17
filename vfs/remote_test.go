package vfs_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
	"github.com/pbs-plus/pxar/vfs"
)

// --- Direct transport (in-process, no serialization) ---

// directTransport implements vfs.RPCTransport by calling RemoteServer
// handler methods directly. Used for testing without any wire format.
type directTransport struct {
	server *vfs.RemoteServer
}

func (t *directTransport) Call(_ context.Context, method string, req any, resp any) error {
	switch method {
	case vfs.MethodStat:
		r := req.(*vfs.StatRequest)
		out, err := t.server.HandleStat(r)
		if err != nil {
			return err
		}
		if resp != nil {
			*(resp.(*vfs.StatResponse)) = *out
		}
		return nil

	case vfs.MethodReadDir:
		r := req.(*vfs.ReadDirRequest)
		out, err := t.server.HandleReadDir(r)
		if err != nil {
			return err
		}
		if resp != nil {
			*(resp.(*vfs.ReadDirResponse)) = *out
		}
		return nil

	case vfs.MethodReadFile:
		r := req.(*vfs.ReadFileRequest)
		out, err := t.server.HandleReadFile(r)
		if err != nil {
			return err
		}
		if resp != nil {
			*(resp.(*[]byte)) = out
		}
		return nil

	case vfs.MethodReadlink:
		r := req.(*vfs.ReadlinkRequest)
		out, err := t.server.HandleReadlink(r)
		if err != nil {
			return err
		}
		if resp != nil {
			*(resp.(*vfs.ReadlinkResponse)) = *out
		}
		return nil

	case vfs.MethodListXAttrs:
		r := req.(*vfs.ListXAttrsRequest)
		out, err := t.server.HandleListXAttrs(r)
		if err != nil {
			return err
		}
		if resp != nil {
			*(resp.(*map[string][]byte)) = out
		}
		return nil

	case vfs.MethodError:
		r := req.(*vfs.ErrorRequest)
		return t.server.HandleError(r)

	case vfs.MethodDone:
		return t.server.HandleDone()

	default:
		return fmt.Errorf("unknown method: %s", method)
	}
}

func (t *directTransport) CallBinary(_ context.Context, method string, req any, dst []byte) (int, error) {
	switch method {
	case vfs.MethodRead:
		r := req.(*vfs.ReadRequest)
		out, err := t.server.HandleRead(r)
		if err != nil {
			return 0, err
		}
		return copy(dst, out), nil

	case vfs.MethodReadFile:
		r := req.(*vfs.ReadFileRequest)
		out, err := t.server.HandleReadFile(r)
		if err != nil {
			return 0, err
		}
		return copy(dst, out), nil

	default:
		return 0, fmt.Errorf("unknown binary method: %s", method)
	}
}

func (t *directTransport) Close() error { return nil }

// --- Helpers ---

func newRemoteTestFS(t *testing.T) vfs.RemoteFS {
	t.Helper()
	buf := buildTestArchive(t)
	ar := transfer.NewFileArchiveReader(bytes.NewReader(buf.Bytes()))
	localFS := vfs.NewLocalFS(ar)

	server := vfs.NewRemoteServer(localFS)
	transport := &directTransport{server: server}
	return vfs.NewRemoteFS(transport)
}

// --- Parity Tests (mirror every LocalFS test) ---

func TestRemoteStat(t *testing.T) {
	fs := newRemoteTestFS(t)
	defer func() { _ = fs.Close() }()

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

func TestRemoteReadDir(t *testing.T) {
	fs := newRemoteTestFS(t)
	defer func() { _ = fs.Close() }()

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

func TestRemoteOpen(t *testing.T) {
	fs := newRemoteTestFS(t)
	defer func() { _ = fs.Close() }()

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

func TestRemoteReadFile(t *testing.T) {
	fs := newRemoteTestFS(t)
	defer func() { _ = fs.Close() }()

	data, err := fs.ReadFile("file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello world" {
		t.Errorf("file.txt content = %q, want %q", data, "hello world")
	}
}

func TestRemoteReadlink(t *testing.T) {
	fs := newRemoteTestFS(t)
	defer func() { _ = fs.Close() }()

	target, err := fs.Readlink("link")
	if err != nil {
		t.Fatal(err)
	}
	if target != "/target" {
		t.Errorf("link target = %q, want %q", target, "/target")
	}
}

func TestRemoteNestedDir(t *testing.T) {
	fs := newRemoteTestFS(t)
	defer func() { _ = fs.Close() }()

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

func TestRemoteStatCaching(t *testing.T) {
	fs := newRemoteTestFS(t)
	defer func() { _ = fs.Close() }()

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

func TestRemoteErrorPaths(t *testing.T) {
	fs := newRemoteTestFS(t)
	defer func() { _ = fs.Close() }()

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

func TestRemoteSeekAndReadAt(t *testing.T) {
	fs := newRemoteTestFS(t)
	defer func() { _ = fs.Close() }()

	fh, err := fs.Open("file.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fh.Close() }()

	off, err := fh.Seek(6, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
	if off != 6 {
		t.Errorf("seek offset = %d, want 6", off)
	}

	buf := make([]byte, 5)
	n, err := fh.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Errorf("read after seek: n = %d, want 5", n)
	}
	if string(buf) != "world" {
		t.Errorf("read after seek = %q, want %q", string(buf), "world")
	}

	buf2 := make([]byte, 5)
	n, err = fh.ReadAt(buf2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Errorf("ReadAt: n = %d, want 5", n)
	}
	if string(buf2) != "hello" {
		t.Errorf("ReadAt = %q, want %q", string(buf2), "hello")
	}
}

func TestRemoteSendError(t *testing.T) {
	fs := newRemoteTestFS(t)
	defer func() { _ = fs.Close() }()

	_ = fs.SendError(fmt.Errorf("test error"))
}

func TestRemoteDone(t *testing.T) {
	fs := newRemoteTestFS(t)
	defer func() { _ = fs.Close() }()

	_ = fs.Done()
}

func TestRemoteListXAttrs(t *testing.T) {
	var buf bytes.Buffer
	ts := format.NewStatxTimestamp(1000000, 0)
	rootMeta := &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755, Mtime: ts}}

	enc := encoder.NewEncoder(&buf, nil, rootMeta, nil)
	_, err := enc.AddFile(
		&pxar.Metadata{
			Stat: format.Stat{Mode: format.ModeIFREG | 0o644, Mtime: ts},
			XAttrs: []format.XAttr{
				format.NewXAttr([]byte("user.test"), []byte("value")),
				format.NewXAttr([]byte("user.other"), []byte("data")),
			},
		},
		"xattr_file.txt",
		[]byte("content"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	ar := transfer.NewFileArchiveReader(bytes.NewReader(buf.Bytes()))
	localFS := vfs.NewLocalFS(ar)

	server := vfs.NewRemoteServer(localFS)
	fs := vfs.NewRemoteFS(&directTransport{server: server})
	defer func() { _ = fs.Close() }()

	xattrs, err := fs.ListXAttrs("xattr_file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if len(xattrs) != 2 {
		t.Fatalf("expected 2 xattrs, got %d", len(xattrs))
	}
	if string(xattrs["user.test"]) != "value" {
		t.Errorf("user.test = %q, want %q", xattrs["user.test"], "value")
	}
	if string(xattrs["user.other"]) != "data" {
		t.Errorf("user.other = %q, want %q", xattrs["user.other"], "data")
	}
}

func TestRemoteLargeFile(t *testing.T) {
	content := bytes.Repeat([]byte("ABCDEFGH"), 32*1024) // 256KB

	var buf bytes.Buffer
	ts := format.NewStatxTimestamp(1000000, 0)
	rootMeta := &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755, Mtime: ts}}

	enc := encoder.NewEncoder(&buf, nil, rootMeta, nil)
	_, err := enc.AddFile(
		&pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFREG | 0o644, Mtime: ts}},
		"large.bin",
		content,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	ar := transfer.NewFileArchiveReader(bytes.NewReader(buf.Bytes()))
	localFS := vfs.NewLocalFS(ar)
	server := vfs.NewRemoteServer(localFS)
	fs := vfs.NewRemoteFS(&directTransport{server: server})
	defer func() { _ = fs.Close() }()

	data, err := fs.ReadFile("large.bin")
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != len(content) {
		t.Fatalf("large.bin size = %d, want %d", len(data), len(content))
	}
	if !bytes.Equal(data, content) {
		t.Error("large.bin content mismatch")
	}
}

func TestRemoteStreamingRead(t *testing.T) {
	content := []byte("0123456789ABCDEF")

	var buf bytes.Buffer
	ts := format.NewStatxTimestamp(1000000, 0)
	rootMeta := &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755, Mtime: ts}}

	enc := encoder.NewEncoder(&buf, nil, rootMeta, nil)
	_, err := enc.AddFile(
		&pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFREG | 0o644, Mtime: ts}},
		"stream.bin",
		content,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	ar := transfer.NewFileArchiveReader(bytes.NewReader(buf.Bytes()))
	localFS := vfs.NewLocalFS(ar)
	server := vfs.NewRemoteServer(localFS)
	fs := vfs.NewRemoteFS(&directTransport{server: server})
	defer func() { _ = fs.Close() }()

	fh, err := fs.Open("stream.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fh.Close() }()

	chunk1 := make([]byte, 8)
	n1, err := fh.Read(chunk1)
	if err != nil {
		t.Fatal(err)
	}
	if n1 != 8 || string(chunk1) != "01234567" {
		t.Errorf("chunk1 = %q (n=%d), want %q", string(chunk1), n1, "01234567")
	}

	chunk2 := make([]byte, 8)
	n2, err := fh.Read(chunk2)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n2 != 8 || string(chunk2) != "89ABCDEF" {
		t.Errorf("chunk2 = %q (n=%d), want %q", string(chunk2), n2, "89ABCDEF")
	}
}

// TestRemoteEmptyFile verifies empty file handling.
func TestRemoteEmptyFile(t *testing.T) {
	var buf bytes.Buffer
	ts := format.NewStatxTimestamp(1000000, 0)
	rootMeta := &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755, Mtime: ts}}

	enc := encoder.NewEncoder(&buf, nil, rootMeta, nil)
	_, err := enc.AddFile(
		&pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFREG | 0o644, Mtime: ts}},
		"empty.txt",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	ar := transfer.NewFileArchiveReader(bytes.NewReader(buf.Bytes()))
	localFS := vfs.NewLocalFS(ar)
	server := vfs.NewRemoteServer(localFS)
	fs := vfs.NewRemoteFS(&directTransport{server: server})
	defer func() { _ = fs.Close() }()

	data, err := fs.ReadFile("empty.txt")
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 0 {
		t.Errorf("empty.txt size = %d, want 0", len(data))
	}

	fi, err := fs.Stat("empty.txt")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != 0 {
		t.Errorf("empty.txt stat size = %d, want 0", fi.Size())
	}
}
