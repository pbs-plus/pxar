package fusefs

import (
	"bytes"
	"testing"
	"time"

	"github.com/sonroyaalmerol/pxar/encoder"
	"github.com/sonroyaalmerol/pxar/format"
	pxar "github.com/sonroyaalmerol/pxar"
	"syscall"
)

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

// createTestArchive builds a pxar archive with a known structure:
// root/
//   hello.txt      (11 bytes)
//   subdir/
//     nested.txt   (14 bytes)
//   link           -> hello.txt
func createTestArchive(t *testing.T) (*bytes.Reader, int64) {
	t.Helper()

	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMeta(0o755), nil)

	enc.AddFile(fileMeta(0o644, 1000, 1000), "hello.txt", []byte("hello world"))

	enc.CreateDirectory("subdir", dirMeta(0o755))
	enc.AddFile(fileMeta(0o644, 1000, 1000), "nested.txt", []byte("nested content"))
	enc.Finish() // finish subdir

	enc.AddSymlink(symlinkMeta(0o777, 0, 0), "link", "hello.txt")

	enc.Close() // closes root

	data := buf.Bytes()
	return bytes.NewReader(data), int64(len(data))
}

func TestSessionGetattrRoot(t *testing.T) {
	r, size := createTestArchive(t)

	sess, err := NewSession(r, size)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	attr, err := sess.Getattr(RootInode)
	if err != nil {
		t.Fatal(err)
	}
	if attr.Ino != RootInode {
		t.Errorf("root inode = %d, want %d", attr.Ino, RootInode)
	}
	if attr.Mode&syscall.S_IFDIR == 0 {
		t.Error("root should be a directory")
	}
}

func TestSessionLookup(t *testing.T) {
	r, size := createTestArchive(t)

	sess, err := NewSession(r, size)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	inode, attr, err := sess.Lookup(RootInode, "hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if inode == 0 {
		t.Error("inode should not be 0")
	}
	if attr.Mode&syscall.S_IFREG == 0 {
		t.Errorf("hello.txt should be a regular file, got mode %o", attr.Mode)
	}
	if attr.Size != 11 {
		t.Errorf("size = %d, want 11", attr.Size)
	}
}

func TestSessionLookupNotFound(t *testing.T) {
	r, size := createTestArchive(t)

	sess, err := NewSession(r, size)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	_, _, err = sess.Lookup(RootInode, "nonexistent")
	if err == nil {
		t.Error("expected ENOENT for missing file")
	}
}

func TestSessionRead(t *testing.T) {
	r, size := createTestArchive(t)

	sess, err := NewSession(r, size)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	inode, _, err := sess.Lookup(RootInode, "hello.txt")
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 100)
	n, err := sess.Read(inode, buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "hello world" {
		t.Errorf("content = %q, want %q", string(buf[:n]), "hello world")
	}
}

func TestSessionReadAtOffset(t *testing.T) {
	r, size := createTestArchive(t)

	sess, err := NewSession(r, size)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	inode, _, err := sess.Lookup(RootInode, "hello.txt")
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 5)
	n, err := sess.Read(inode, buf, 6)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "world" {
		t.Errorf("content = %q, want %q", string(buf[:n]), "world")
	}
}

func TestSessionReadlink(t *testing.T) {
	r, size := createTestArchive(t)

	sess, err := NewSession(r, size)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	inode, attr, err := sess.Lookup(RootInode, "link")
	if err != nil {
		t.Fatal(err)
	}
	if attr.Mode&syscall.S_IFLNK == 0 {
		t.Error("link should be a symlink")
	}

	target, err := sess.Readlink(inode)
	if err != nil {
		t.Fatal(err)
	}
	if target != "hello.txt" {
		t.Errorf("target = %q, want %q", target, "hello.txt")
	}
}

func TestSessionReaddirRoot(t *testing.T) {
	r, size := createTestArchive(t)

	sess, err := NewSession(r, size)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	entries, err := sess.Readdir(RootInode, 0)
	if err != nil {
		t.Fatal(err)
	}

	names := make(map[string]bool)
	for _, e := range entries {
		names[e.Name] = true
	}

	if !names["hello.txt"] {
		t.Error("missing hello.txt in directory listing")
	}
	if !names["subdir"] {
		t.Error("missing subdir in directory listing")
	}
	if !names["link"] {
		t.Error("missing link in directory listing")
	}
}

func TestSessionReaddirNested(t *testing.T) {
	r, size := createTestArchive(t)

	sess, err := NewSession(r, size)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	inode, _, err := sess.Lookup(RootInode, "subdir")
	if err != nil {
		t.Fatal(err)
	}

	entries, err := sess.Readdir(inode, 0)
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, e := range entries {
		if e.Name == "nested.txt" {
			found = true
			break
		}
	}
	if !found {
		t.Error("missing nested.txt in subdir listing")
	}
}

func TestSessionReadNestedFile(t *testing.T) {
	r, size := createTestArchive(t)

	sess, err := NewSession(r, size)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	inode, _, err := sess.Lookup(RootInode, "subdir")
	if err != nil {
		t.Fatal(err)
	}

	nestedInode, _, err := sess.Lookup(inode, "nested.txt")
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 100)
	n, err := sess.Read(nestedInode, buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "nested content" {
		t.Errorf("content = %q, want %q", string(buf[:n]), "nested content")
	}
}

func TestSessionGetattrFile(t *testing.T) {
	r, size := createTestArchive(t)

	sess, err := NewSession(r, size)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	inode, _, err := sess.Lookup(RootInode, "hello.txt")
	if err != nil {
		t.Fatal(err)
	}

	attr, err := sess.Getattr(inode)
	if err != nil {
		t.Fatal(err)
	}
	if attr.Size != 11 {
		t.Errorf("size = %d, want 11", attr.Size)
	}
	if attr.Mode&syscall.S_IFREG == 0 {
		t.Error("should be regular file")
	}
	if attr.Nlink != 1 {
		t.Errorf("nlink = %d, want 1", attr.Nlink)
	}
}

func TestSessionGetattrDir(t *testing.T) {
	r, size := createTestArchive(t)

	sess, err := NewSession(r, size)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	inode, _, err := sess.Lookup(RootInode, "subdir")
	if err != nil {
		t.Fatal(err)
	}

	attr, err := sess.Getattr(inode)
	if err != nil {
		t.Fatal(err)
	}
	if attr.Mode&syscall.S_IFDIR == 0 {
		t.Error("subdir should be a directory")
	}
	if attr.Nlink != 2 {
		t.Errorf("nlink = %d, want 2 for directory", attr.Nlink)
	}
}

func TestSessionListXAttr(t *testing.T) {
	r, size := createTestArchive(t)

	sess, err := NewSession(r, size)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	names, err := sess.ListXAttr(RootInode)
	if err != nil {
		t.Logf("ListXAttr on root: %v (may be expected)", err)
	}
	// Root has no xattrs, should return nil or empty
	_ = names
}

func TestSessionForget(t *testing.T) {
	r, size := createTestArchive(t)

	sess, err := NewSession(r, size)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	inode, _, _ := sess.Lookup(RootInode, "hello.txt")
	sess.Forget(inode, 1)
}

func TestSessionStatfs(t *testing.T) {
	r, size := createTestArchive(t)

	sess, err := NewSession(r, size)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	stat, err := sess.Statfs()
	if err != nil {
		t.Fatal(err)
	}
	if stat.Blocks == 0 {
		t.Error("statfs blocks should not be 0")
	}
}
