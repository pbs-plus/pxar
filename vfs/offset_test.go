package vfs_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
	"github.com/pbs-plus/pxar/vfs"
)

// --- offset test helpers ---

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

func fileMetaXattr(mode uint64, uid, gid uint32, xattrs ...[2]string) *pxar.Metadata {
	ts := format.NewStatxTimestampFromDuration(1430487000 * time.Second)
	m := &pxar.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFREG | mode,
			UID:   uid,
			GID:   gid,
			Mtime: ts,
		},
	}
	for _, xa := range xattrs {
		m.XAttrs = append(m.XAttrs, format.NewXAttr([]byte(xa[0]), []byte(xa[1])))
	}
	return m
}

// buildTestArchive creates a test archive:
//
//	/ (dir)
//	├── hello.txt (file, "hello world")
//	├── link -> hello.txt (symlink)
//	├── subdir (dir)
//	│   └── nested.txt (file, "nested content")
//	└── xattr.txt (file, "data", xattr: user.foo=bar)
func buildTestArchive(tb testing.TB) *transfer.FileReader {
	tb.Helper()
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMeta(0o755), nil)

	_, _ = enc.AddFile(fileMeta(0o644, 1000, 1000), "hello.txt", []byte("hello world"))
	_ = enc.AddSymlink(symlinkMeta(0o777, 0, 0), "link", "hello.txt")
	_ = enc.CreateDirectory("subdir", dirMeta(0o755))
	_, _ = enc.AddFile(fileMeta(0o644, 1000, 1000), "nested.txt", []byte("nested content"))
	_ = enc.Finish()
	_, _ = enc.AddFile(fileMetaXattr(0o644, 1000, 1000, [2]string{"user.foo", "bar"}), "xattr.txt", []byte("data"))
	_ = enc.Close()

	reader := bytes.NewReader(buf.Bytes())
	return transfer.NewFileReader(reader)
}

func TestFS_Root(t *testing.T) {
	ar := buildTestArchive(t)
	defer ar.Close()

	fs := vfs.NewLocalFS(ar)
	defer fs.Close()

	root, err := fs.Root()
	if err != nil {
		t.Fatal(err)
	}
	if !root.IsDir() {
		t.Error("root should be a directory")
	}
	if root.Name() != "/" {
		t.Errorf("root name should be /, got %q", root.Name())
	}
}

func TestFS_Lookup(t *testing.T) {
	ar := buildTestArchive(t)
	defer ar.Close()

	fs := vfs.NewLocalFS(ar)
	defer fs.Close()

	fi, err := fs.Lookup("hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Name() != "hello.txt" {
		t.Errorf("expected name hello.txt, got %q", fi.Name())
	}
	if !fi.IsFile() {
		t.Error("expected regular file")
	}
	if fi.Size() != 11 {
		t.Errorf("expected size 11, got %d", fi.Size())
	}
}

func TestFS_ReadDir(t *testing.T) {
	ar := buildTestArchive(t)
	defer ar.Close()

	fs := vfs.NewLocalFS(ar)
	defer fs.Close()

	root, err := fs.Root()
	if err != nil {
		t.Fatal(err)
	}

	entries, err := fs.ReadDir(root.ContentOffset)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) < 3 {
		t.Errorf("expected at least 3 entries, got %d", len(entries))
	}

	names := make(map[string]bool)
	for _, e := range entries {
		names[e.Name()] = true
	}
	for _, n := range []string{"hello.txt", "link", "subdir", "xattr.txt"} {
		if !names[n] {
			t.Errorf("missing entry %q", n)
		}
	}
}

func TestFS_GetAttr(t *testing.T) {
	ar := buildTestArchive(t)
	defer ar.Close()

	fs := vfs.NewLocalFS(ar)
	defer fs.Close()

	// First, lookup to populate the cache
	fi, err := fs.Lookup("hello.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Now GetAttr by entry offset
	fi2, err := fs.GetAttr(fi.EntryRangeStart)
	if err != nil {
		t.Fatal(err)
	}
	if fi2.Name() != "hello.txt" {
		t.Errorf("expected hello.txt, got %q", fi2.Name())
	}
}

func TestFS_Read(t *testing.T) {
	ar := buildTestArchive(t)
	defer ar.Close()

	fs := vfs.NewLocalFS(ar)
	defer fs.Close()

	// Lookup to populate cache
	_, err := fs.Lookup("hello.txt")
	if err != nil {
		t.Fatal(err)
	}

	// ReadDir to populate cache for all entries
	root, _ := fs.Root()
	entries, err := fs.ReadDir(root.ContentOffset)
	if err != nil {
		t.Fatal(err)
	}

	// Find hello.txt content range
	var helloEntry pxar.FileInfo
	for _, e := range entries {
		if e.Name() == "hello.txt" {
			helloEntry = e
			break
		}
	}
	if helloEntry.ContentRange == nil {
		t.Fatal("hello.txt has no content range")
	}

	data, err := fs.Read(helloEntry.ContentRange[0], helloEntry.ContentRange[1], 0, 11)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello world" {
		t.Errorf("expected 'hello world', got %q", string(data))
	}

	// Partial read with offset
	data2, err := fs.Read(helloEntry.ContentRange[0], helloEntry.ContentRange[1], 6, 5)
	if err != nil {
		t.Fatal(err)
	}
	if string(data2) != "world" {
		t.Errorf("expected 'world', got %q", string(data2))
	}
}

func TestFS_ReadLink(t *testing.T) {
	ar := buildTestArchive(t)
	defer ar.Close()

	fs := vfs.NewLocalFS(ar)
	defer fs.Close()

	fi, err := fs.Lookup("link")
	if err != nil {
		t.Fatal(err)
	}

	target, err := fs.ReadLink(fi.EntryRangeStart)
	if err != nil {
		t.Fatal(err)
	}
	if string(target) != "hello.txt" {
		t.Errorf("expected target 'hello.txt', got %q", string(target))
	}
}

func TestFS_ListXAttrs(t *testing.T) {
	ar := buildTestArchive(t)
	defer ar.Close()

	fs := vfs.NewLocalFS(ar)
	defer fs.Close()

	fi, err := fs.Lookup("xattr.txt")
	if err != nil {
		t.Fatal(err)
	}

	xattrs, err := fs.ListXAttrs(fi.EntryRangeStart)
	if err != nil {
		t.Fatal(err)
	}
	if len(xattrs) == 0 {
		t.Error("expected xattrs")
	}
	if string(xattrs["user.foo"]) != "bar" {
		t.Errorf("expected user.foo=bar, got %q", string(xattrs["user.foo"]))
	}

	// Regular file without xattrs should return nil
	fi2, _ := fs.Lookup("hello.txt")
	xattrs2, err := fs.ListXAttrs(fi2.EntryRangeStart)
	if err != nil {
		t.Fatal(err)
	}
	if len(xattrs2) != 0 {
		t.Errorf("expected no xattrs for hello.txt, got %v", xattrs2)
	}
}

func TestFS_Stats(t *testing.T) {
	ar := buildTestArchive(t)
	defer ar.Close()

	fs := vfs.NewLocalFS(ar)
	defer fs.Close()

	_, _ = fs.Root()
	_, _ = fs.Lookup("hello.txt")
	_, _ = fs.Lookup("subdir")

	stats := fs.Stats()
	if stats.FilesAccessed < 1 {
		t.Errorf("expected at least 1 file, got %d", stats.FilesAccessed)
	}
	if stats.FoldersAccessed < 1 {
		t.Errorf("expected at least 1 folder, got %d", stats.FoldersAccessed)
	}
}

func TestFS_ReadContentReader(t *testing.T) {
	ar := buildTestArchive(t)
	defer ar.Close()

	fs := vfs.NewLocalFS(ar)
	defer fs.Close()

	root, _ := fs.Root()
	entries, err := fs.ReadDir(root.ContentOffset)
	if err != nil {
		t.Fatal(err)
	}

	var helloEntry pxar.FileInfo
	for _, e := range entries {
		if e.Name() == "hello.txt" {
			helloEntry = e
			break
		}
	}
	if helloEntry.ContentRange == nil {
		t.Fatal("hello.txt has no content range")
	}

	rc, err := fs.ReadContentReader(helloEntry.ContentRange[0], helloEntry.ContentRange[1])
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello world" {
		t.Errorf("expected 'hello world', got %q", string(data))
	}
}

// --- Remote round-trip test ---

func TestFS_RemoteRoundTrip(t *testing.T) {
	ar := buildTestArchive(t)
	defer ar.Close()

	serverFS := vfs.NewLocalFS(ar)
	defer serverFS.Close()

	tp := &testTransport{srv: vfs.NewRemoteServer(serverFS)}
	client := vfs.NewRemoteFS(tp)
	defer client.Close()

	// Root
	root, err := client.Root()
	if err != nil {
		t.Fatal(err)
	}
	if !root.IsDir() {
		t.Error("root should be dir")
	}

	// Lookup
	fi, err := client.Lookup("hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Name() != "hello.txt" {
		t.Errorf("expected hello.txt, got %q", fi.Name())
	}

	// GetAttr
	fi2, err := client.GetAttr(fi.EntryRangeStart)
	if err != nil {
		t.Fatal(err)
	}
	if fi2.Name() != "hello.txt" {
		t.Errorf("expected hello.txt, got %q", fi2.Name())
	}

	// ReadDir
	entries, err := client.ReadDir(root.ContentOffset)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) < 3 {
		t.Errorf("expected at least 3 entries, got %d", len(entries))
	}

	// ReadLink
	linkFi, err := client.Lookup("link")
	if err != nil {
		t.Fatal(err)
	}
	target, err := client.ReadLink(linkFi.EntryRangeStart)
	if err != nil {
		t.Fatal(err)
	}
	if string(target) != "hello.txt" {
		t.Errorf("expected target 'hello.txt', got %q", string(target))
	}

	// ListXAttrs
	xaFi, err := client.Lookup("xattr.txt")
	if err != nil {
		t.Fatal(err)
	}
	xattrs, err := client.ListXAttrs(xaFi.EntryRangeStart)
	if err != nil {
		t.Fatal(err)
	}
	if string(xattrs["user.foo"]) != "bar" {
		t.Errorf("expected user.foo=bar, got %q", string(xattrs["user.foo"]))
	}

	// Read
	var helloEntry pxar.FileInfo
	for _, e := range entries {
		if e.Name() == "hello.txt" {
			helloEntry = e
			break
		}
	}
	if helloEntry.ContentRange == nil {
		t.Fatal("hello.txt has no content range")
	}
	data, err := client.Read(helloEntry.ContentRange[0], helloEntry.ContentRange[1], 0, 11)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello world" {
		t.Errorf("expected 'hello world', got %q", string(data))
	}
}

// --- testTransport implements vfs.RPCTransport using direct calls ---

type testTransport struct {
	srv *vfs.RemoteServer
}

func (t *testTransport) Call(_ context.Context, method string, req, resp any) error {
	switch method {
	case vfs.MethodRoot:
		fi, err := t.srv.HandleRoot()
		if err != nil {
			return err
		}
		if d, ok := resp.(*pxar.FileInfo); ok {
			*d = *fi
		}
		return nil
	case vfs.MethodLookup:
		reqMap := req.(map[string]string)
		fi, err := t.srv.HandleLookup(reqMap["path"])
		if err != nil {
			return err
		}
		if d, ok := resp.(*pxar.FileInfo); ok {
			*d = *fi
		}
		return nil
	case vfs.MethodReadDir:
		reqMap := req.(map[string]uint64)
		entries, err := t.srv.HandleReadDir(reqMap["offset"])
		if err != nil {
			return err
		}
		if d, ok := resp.(*[]pxar.FileInfo); ok {
			*d = entries
		}
		return nil
	case vfs.MethodGetAttr:
		reqMap := req.(map[string]uint64)
		fi, err := t.srv.HandleGetAttr(reqMap["entry_start"])
		if err != nil {
			return err
		}
		if d, ok := resp.(*pxar.FileInfo); ok {
			*d = *fi
		}
		return nil
	case vfs.MethodReadLink:
		reqMap := req.(map[string]uint64)
		target, err := t.srv.HandleReadLink(reqMap["entry_start"])
		if err != nil {
			return err
		}
		if d, ok := resp.(*[]byte); ok {
			*d = target
		}
		return nil
	case vfs.MethodListXAttrs:
		reqMap := req.(map[string]uint64)
		xattrs, err := t.srv.HandleListXAttrs(reqMap["entry_start"])
		if err != nil {
			return err
		}
		if d, ok := resp.(*map[string][]byte); ok {
			*d = xattrs
		}
		return nil
	case vfs.MethodDone:
		return t.srv.HandleDone()
	default:
		return fmt.Errorf("unknown method: %s", method)
	}
}

func (t *testTransport) CallBinary(_ context.Context, method string, req any, dst []byte) (int, error) {
	if method == vfs.MethodRead {
		reqMap := req.(map[string]uint64)
		data, err := t.srv.HandleRead(reqMap["content_start"], reqMap["content_end"], reqMap["offset"], uint(reqMap["size"]))
		if err != nil {
			return 0, err
		}
		return copy(dst, data), nil
	}
	return 0, fmt.Errorf("unknown binary method: %s", method)
}

func (t *testTransport) CallStream(_ context.Context, method string, req any) (io.ReadCloser, error) {
	if method == vfs.MethodReadStream {
		reqMap := req.(map[string]uint64)
		return t.srv.HandleReadStream(reqMap["content_start"], reqMap["content_end"])
	}
	return nil, fmt.Errorf("unknown stream method: %s", method)
}

func (t *testTransport) Close() error { return nil }

func TestLocalFS_CacheEviction(t *testing.T) {
	ar := buildTestArchive(t)
	defer ar.Close()

	fs := vfs.NewLocalFS(ar)
	fs.SetMaxCache(2) // very small limit
	defer fs.Close()

	// Access root to populate cache
	root, err := fs.Root()
	if err != nil {
		t.Fatal(err)
	}

	// ReadDir populates cache with entries
	_, err = fs.ReadDir(root.ContentOffset)
	if err != nil {
		t.Fatal(err)
	}

	// Access more entries to trigger eviction
	_, err = fs.Lookup("hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	_, err = fs.Lookup("xattr.txt")
	if err != nil {
		t.Fatal(err)
	}

	// The caches should not exceed maxCache (2 entries each)
	// We can't directly inspect the internal maps, but we verify
	// that lookups still work after eviction
	fi, err := fs.Lookup("hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Name() != "hello.txt" {
		t.Errorf("expected hello.txt, got %q", fi.Name())
	}
}

func TestLocalFS_SetMaxCacheZero(t *testing.T) {
	ar := buildTestArchive(t)
	defer ar.Close()

	fs := vfs.NewLocalFS(ar)
	fs.SetMaxCache(0) // unlimited
	defer fs.Close()

	root, err := fs.Root()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := fs.ReadDir(root.ContentOffset)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) < 3 {
		t.Errorf("expected at least 3 entries, got %d", len(entries))
	}
}

func TestLocalFS_CloseClearsCaches(t *testing.T) {
	ar := buildTestArchive(t)
	fs := vfs.NewLocalFS(ar)

	_, err := fs.Root()
	if err != nil {
		t.Fatal(err)
	}

	// Close should clear caches and not panic
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLocalFS_CacheMissFallback_ListXAttrs(t *testing.T) {
	ar := buildTestArchive(t)
	defer ar.Close()

	fs := vfs.NewLocalFS(ar).SetMaxCache(1) // aggressive eviction
	defer fs.Close()

	// ReadDir populates the cache, then evicts as new entries come in.
	root, err := fs.Root()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := fs.ReadDir(root.ContentOffset)
	if err != nil {
		t.Fatal(err)
	}

	// Find xattr.txt entry offset
	var xattrOffset uint64
	for _, e := range entries {
		if e.Name() == "xattr.txt" {
			xattrOffset = e.EntryRangeStart
			break
		}
	}
	if xattrOffset == 0 {
		t.Fatal("xattr.txt not found")
	}

	// Force heavy cache pressure so xattr.txt entry is evicted
	for i := 0; i < 100; i++ {
		_, _ = fs.Lookup("hello.txt")
		_, _ = fs.Lookup("xattr.txt")
	}

	// ListXAttrs should still work via re-read fallback
	xattrs, err := fs.ListXAttrs(xattrOffset)
	if err != nil {
		t.Fatalf("ListXAttrs cache-miss fallback failed: %v", err)
	}
	if len(xattrs) == 0 {
		t.Error("expected xattrs")
	}
	if string(xattrs["user.foo"]) != "bar" {
		t.Errorf("expected user.foo=bar, got %q", string(xattrs["user.foo"]))
	}
}

func TestLocalFS_CacheMissFallback_GetAttr(t *testing.T) {
	ar := buildTestArchive(t)
	defer ar.Close()

	fs := vfs.NewLocalFS(ar).SetMaxCache(1)
	defer fs.Close()

	root, err := fs.Root()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := fs.ReadDir(root.ContentOffset)
	if err != nil {
		t.Fatal(err)
	}

	var helloOffset uint64
	for _, e := range entries {
		if e.Name() == "hello.txt" {
			helloOffset = e.EntryRangeStart
			break
		}
	}

	// Force cache pressure
	for i := 0; i < 100; i++ {
		_, _ = fs.Lookup("xattr.txt")
	}

	// GetAttr should still work via re-read fallback
	fi, err := fs.GetAttr(helloOffset)
	if err != nil {
		t.Fatalf("GetAttr cache-miss fallback failed: %v", err)
	}
	if fi.Name() != "hello.txt" {
		t.Errorf("expected hello.txt, got %q", fi.Name())
	}
}
