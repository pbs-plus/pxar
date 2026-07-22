package interop

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/accessor"
	"github.com/pbs-plus/pxar/format"
)

func mustReadContent(t *testing.T, a *accessor.Accessor, entry *pxar.Entry) []byte {
	t.Helper()
	rc, err := a.ReadFileContentReader(entry)
	if err != nil {
		t.Fatalf("read content %q: %v", entry.Path, err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("readall %q: %v", entry.Path, err)
	}
	return data
}

func TestAccessorReadsRustArchive(t *testing.T) {
	dir := interopDir(t)
	f, err := os.Open(filepath.Join(dir, "rust_v1.pxar"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	a := accessor.NewAccessor(f)

	root, err := a.ReadRoot()
	if err != nil {
		t.Fatalf("ReadRoot: %v", err)
	}
	if !root.IsDir() {
		t.Fatalf("root is not a directory")
	}
	if root.Metadata.Stat.Mode != format.ModeIFDIR|0o755 {
		t.Errorf("root mode: got %o, want 40755", root.Metadata.Stat.Mode)
	}
	if root.Metadata.Stat.UID != 1000 || root.Metadata.Stat.GID != 1000 {
		t.Errorf("root owner: %d:%d, want 1000:1000", root.Metadata.Stat.UID, root.Metadata.Stat.GID)
	}

	aTxt, err := a.Lookup("/a.txt")
	if err != nil {
		t.Fatalf("lookup /a.txt: %v", err)
	}
	if string(mustReadContent(t, a, aTxt)) != "hello pxar" {
		t.Errorf("/a.txt content mismatch")
	}
	aTxtFull, err := a.ReadEntryAt(int64(aTxt.FileOffset))
	if err != nil {
		t.Fatalf("ReadEntryAt /a.txt: %v", err)
	}
	if len(aTxtFull.Metadata.XAttrs) != 2 {
		t.Errorf("/a.txt xattrs: got %d, want 2", len(aTxtFull.Metadata.XAttrs))
	}
	if aTxtFull.Metadata.FCaps == nil {
		t.Errorf("/a.txt missing fcaps")
	}

	dir1, err := a.Lookup("/dir1")
	if err != nil {
		t.Fatalf("lookup /dir1: %v", err)
	}
	dir1Full, err := a.ReadEntryAt(int64(dir1.FileOffset))
	if err != nil {
		t.Fatalf("ReadEntryAt /dir1: %v", err)
	}
	if dir1Full.Metadata.QuotaProjectID == nil || *dir1Full.Metadata.QuotaProjectID != 4711 {
		t.Errorf("/dir1 quota: %+v", dir1Full.Metadata.QuotaProjectID)
	}
	if len(dir1Full.Metadata.ACL.Users) != 2 {
		t.Errorf("/dir1 acl users: got %d, want 2", len(dir1Full.Metadata.ACL.Users))
	}
	if dir1Full.Metadata.ACL.Default == nil {
		t.Errorf("/dir1 missing default acl")
	}

	nested, err := a.Lookup("/dir1/nested.txt")
	if err != nil {
		t.Fatalf("lookup /dir1/nested.txt: %v", err)
	}
	if string(mustReadContent(t, a, nested)) != "nested content" {
		t.Errorf("/dir1/nested.txt content mismatch")
	}

	big, err := a.Lookup("/big.bin")
	if err != nil {
		t.Fatalf("lookup /big.bin: %v", err)
	}
	if got := mustReadContent(t, a, big); fnv1a(got) != fnv1a(bigContent()) {
		t.Errorf("/big.bin content hash mismatch")
	}

	uni, err := a.Lookup("/many/übêr-ño")
	if err != nil {
		t.Fatalf("lookup unicode: %v", err)
	}
	if string(mustReadContent(t, a, uni)) != "unic" {
		t.Errorf("/übêr-ño content mismatch")
	}

	link, err := a.Lookup("/link")
	if err != nil {
		t.Fatalf("lookup /link: %v", err)
	}
	if !link.IsSymlink() || link.LinkTarget != "a.txt" {
		t.Errorf("/link: kind=%v target=%q", link.Kind, link.LinkTarget)
	}

	cdev, err := a.Lookup("/cdev")
	if err != nil {
		t.Fatalf("lookup /cdev: %v", err)
	}
	if !cdev.IsDevice() || cdev.DeviceInfo.Major != 1 || cdev.DeviceInfo.Minor != 3 {
		t.Errorf("/cdev: %+v", cdev.DeviceInfo)
	}

	hl, err := a.Lookup("/hl")
	if err != nil {
		t.Fatalf("lookup /hl: %v", err)
	}
	if !hl.IsHardlink() {
		t.Fatalf("/hl is not a hardlink")
	}
	target, err := a.FollowHardlink(hl)
	if err != nil {
		t.Fatalf("FollowHardlink /hl: %v", err)
	}
	if string(mustReadContent(t, a, target)) != "hello pxar" {
		t.Errorf("/hl target content mismatch")
	}

	var count int
	if err := a.ListDirectory(0, accessor.ListOption{}, func(e *pxar.Entry) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("ListDirectory root: %v", err)
	}
	if count != 11 {
		t.Errorf("root list count: got %d, want 11", count)
	}
}

func TestAccessorReadsRustSplitArchive(t *testing.T) {
	dir := interopDir(t)
	metaF, err := os.Open(filepath.Join(dir, "rust_v2.mpxar"))
	if err != nil {
		t.Fatal(err)
	}
	defer metaF.Close()
	payloadF, err := os.Open(filepath.Join(dir, "rust_v2.ppxar"))
	if err != nil {
		t.Fatal(err)
	}
	defer payloadF.Close()

	a := accessor.NewAccessor(metaF, payloadF)

	big, err := a.Lookup("/big.bin")
	if err != nil {
		t.Fatalf("lookup /big.bin: %v", err)
	}
	if got := mustReadContent(t, a, big); fnv1a(got) != fnv1a(bigContent()) {
		t.Errorf("/big.bin split content hash mismatch")
	}
	if got := mustReadContent(t, a, mustLookup(t, a, "/a.txt")); !bytes.Equal(got, []byte("hello pxar")) {
		t.Errorf("/a.txt split content: %q", got)
	}
}

func mustLookup(t *testing.T, a *accessor.Accessor, path string) *pxar.Entry {
	t.Helper()
	e, err := a.Lookup(path)
	if err != nil {
		t.Fatalf("lookup %s: %v", path, err)
	}
	return e
}

var _ = fmt.Sprintf
