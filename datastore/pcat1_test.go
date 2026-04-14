package datastore

import (
	"bytes"
	"testing"
)

func TestCatalogWriterEmptyRoot(t *testing.T) {
	var buf bytes.Buffer
	cw := NewCatalogWriter(&buf)

	cw.StartDirectory("")

	if err := cw.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	if buf.Len() < 8+8 {
		t.Fatalf("catalog too short: %d bytes", buf.Len())
	}

	tree, err := ReadCatalogTree(buf.Bytes())
	if err != nil {
		t.Fatalf("ReadCatalogTree: %v", err)
	}
	if tree.Name != "" {
		t.Errorf("root name: got %q, want empty", tree.Name)
	}
	if len(tree.Children) != 0 {
		t.Errorf("root children: got %d, want 0", len(tree.Children))
	}
}

func TestCatalogWriterFilesOnly(t *testing.T) {
	var buf bytes.Buffer
	cw := NewCatalogWriter(&buf)

	cw.StartDirectory("root")
	cw.AddFile("file1.txt", 100, 1700000000)
	cw.AddFile("file2.txt", 200, 1700000001)

	if err := cw.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	tree, err := ReadCatalogTree(buf.Bytes())
	if err != nil {
		t.Fatalf("ReadCatalogTree: %v", err)
	}
	if tree.Name != "root" {
		t.Errorf("root name: got %q, want %q", tree.Name, "root")
	}
	if len(tree.Children) != 2 {
		t.Fatalf("root children: got %d, want 2", len(tree.Children))
	}
	if tree.Children[0].Name != "file1.txt" {
		t.Errorf("child[0] name: got %q", tree.Children[0].Name)
	}
	if tree.Children[0].Size != 100 {
		t.Errorf("child[0] size: got %d, want 100", tree.Children[0].Size)
	}
	if tree.Children[0].Mtime != 1700000000 {
		t.Errorf("child[0] mtime: got %d, want 1700000000", tree.Children[0].Mtime)
	}
	if tree.Children[1].Name != "file2.txt" {
		t.Errorf("child[1] name: got %q", tree.Children[1].Name)
	}
	if tree.Children[1].Size != 200 {
		t.Errorf("child[1] size: got %d, want 200", tree.Children[1].Size)
	}
}

func TestCatalogWriterNestedDirs(t *testing.T) {
	var buf bytes.Buffer
	cw := NewCatalogWriter(&buf)

	cw.StartDirectory("root")
	cw.AddFile("top.txt", 50, 1000)
	cw.StartDirectory("subdir")
	cw.AddFile("nested.txt", 75, 2000)
	cw.EndDirectory()
	cw.AddSymlink("link")

	if err := cw.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	tree, err := ReadCatalogTree(buf.Bytes())
	if err != nil {
		t.Fatalf("ReadCatalogTree: %v", err)
	}
	if tree.Name != "root" {
		t.Errorf("root name: got %q", tree.Name)
	}
	if len(tree.Children) != 3 {
		t.Fatalf("root children: got %d, want 3", len(tree.Children))
	}

	if tree.Children[0].Name != "top.txt" {
		t.Errorf("child[0]: got %q", tree.Children[0].Name)
	}
	if tree.Children[0].EntryType != CatalogEntryTypeFile {
		t.Errorf("child[0] type: got %d, want file", tree.Children[0].EntryType)
	}

	if tree.Children[1].Name != "subdir" {
		t.Errorf("child[1]: got %q", tree.Children[1].Name)
	}
	if tree.Children[1].EntryType != CatalogEntryTypeDir {
		t.Errorf("child[1] type: got %d, want dir", tree.Children[1].EntryType)
	}
	if len(tree.Children[1].Children) != 1 {
		t.Fatalf("subdir children: got %d, want 1", len(tree.Children[1].Children))
	}
	if tree.Children[1].Children[0].Name != "nested.txt" {
		t.Errorf("nested file: got %q", tree.Children[1].Children[0].Name)
	}

	if tree.Children[2].Name != "link" {
		t.Errorf("child[2]: got %q", tree.Children[2].Name)
	}
	if tree.Children[2].EntryType != CatalogEntryTypeSymlink {
		t.Errorf("child[2] type: got %d, want symlink", tree.Children[2].EntryType)
	}
}

func TestCatalogWriterDeepNesting(t *testing.T) {
	var buf bytes.Buffer
	cw := NewCatalogWriter(&buf)

	cw.StartDirectory("a")
	cw.StartDirectory("b")
	cw.StartDirectory("c")
	cw.AddFile("deep.txt", 42, 999)
	cw.EndDirectory()
	cw.EndDirectory()

	if err := cw.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	tree, err := ReadCatalogTree(buf.Bytes())
	if err != nil {
		t.Fatalf("ReadCatalogTree: %v", err)
	}
	if tree.Name != "a" {
		t.Errorf("root: got %q", tree.Name)
	}
	if len(tree.Children) != 1 || tree.Children[0].Name != "b" {
		t.Errorf("a/b not found")
	}
	if len(tree.Children[0].Children) != 1 || tree.Children[0].Children[0].Name != "c" {
		t.Errorf("a/b/c not found")
	}
	deepest := tree.Children[0].Children[0].Children[0]
	if deepest.Name != "deep.txt" || deepest.Size != 42 || deepest.Mtime != 999 {
		t.Errorf("deep.txt: got name=%q size=%d mtime=%d", deepest.Name, deepest.Size, deepest.Mtime)
	}
}

func TestCatalogWriterDeviceTypes(t *testing.T) {
	var buf bytes.Buffer
	cw := NewCatalogWriter(&buf)

	cw.StartDirectory("root")
	cw.AddBlockDevice("sda")
	cw.AddCharDevice("null")
	cw.AddFifo("pipe")
	cw.AddSocket("sock")
	cw.AddHardlink("hardlink")

	if err := cw.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	tree, err := ReadCatalogTree(buf.Bytes())
	if err != nil {
		t.Fatalf("ReadCatalogTree: %v", err)
	}
	if len(tree.Children) != 5 {
		t.Fatalf("children: got %d, want 5", len(tree.Children))
	}
	expectedTypes := []CatalogEntryType{
		CatalogEntryTypeBlockDev,
		CatalogEntryTypeCharDev,
		CatalogEntryTypeFifo,
		CatalogEntryTypeSocket,
		CatalogEntryTypeHardlink,
	}
	for i, exp := range expectedTypes {
		if tree.Children[i].EntryType != exp {
			t.Errorf("child[%d] type: got %d, want %d", i, tree.Children[i].EntryType, exp)
		}
	}
}

func TestCatalogEncodeU64(t *testing.T) {
	var buf bytes.Buffer
	cw := NewCatalogWriter(&buf)

	cw.StartDirectory("")
	cw.AddFile("bigfile", 1<<32, 0)
	cw.Finish()

	tree, err := ReadCatalogTree(buf.Bytes())
	if err != nil {
		t.Fatalf("ReadCatalogTree: %v", err)
	}
	if tree.Children[0].Size != 1<<32 {
		t.Errorf("size: got %d, want %d", tree.Children[0].Size, uint64(1<<32))
	}
}

func TestCatalogEncodeNegativeMtime(t *testing.T) {
	var buf bytes.Buffer
	cw := NewCatalogWriter(&buf)

	cw.StartDirectory("")
	cw.AddFile("neg", 0, -1)
	cw.AddFile("pos", 0, 1)
	cw.AddFile("zero", 0, 0)
	cw.AddFile("large_neg", 0, -100000)
	cw.Finish()

	tree, err := ReadCatalogTree(buf.Bytes())
	if err != nil {
		t.Fatalf("ReadCatalogTree: %v", err)
	}
	tests := []struct {
		idx   int
		mtime int64
	}{
		{0, -1},
		{1, 1},
		{2, 0},
		{3, -100000},
	}
	for _, tt := range tests {
		if tree.Children[tt.idx].Mtime != tt.mtime {
			t.Errorf("child[%d] mtime: got %d, want %d", tt.idx, tree.Children[tt.idx].Mtime, tt.mtime)
		}
	}
}
