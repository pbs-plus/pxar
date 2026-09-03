package decoder

import (
	"bytes"
	"testing"

	"github.com/pbs-plus/pxar/accessor"
	"github.com/pbs-plus/pxar/encoder"
)

// TestDecoderReportsEntryOffsets pins decoder offsets to accessor.ReadEntryAt semantics.
func TestDecoderReportsEntryOffsets(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMetadata(0o755), nil)
	if _, err := enc.AddFile(fileMetadata(0o644, 1000, 1000), "a.txt", []byte("aaa")); err != nil {
		t.Fatalf("add a.txt: %v", err)
	}
	if err := enc.CreateDirectory("sub", dirMetadata(0o755)); err != nil {
		t.Fatalf("create sub: %v", err)
	}
	if _, err := enc.AddFile(fileMetadata(0o644, 1000, 1000), "b.txt", []byte("bbbbb")); err != nil {
		t.Fatalf("add b.txt: %v", err)
	}
	if err := enc.Finish(); err != nil {
		t.Fatalf("finish sub: %v", err)
	}
	if _, err := enc.AddFile(fileMetadata(0o644, 1000, 1000), "c.txt", []byte("cc")); err != nil {
		t.Fatalf("add c.txt: %v", err)
	}
	enc.Close()

	archive := buf.Bytes()
	entries := collectEntries(t, NewDecoder(bytes.NewReader(archive), nil))
	acc := accessor.NewAccessor(bytes.NewReader(archive), nil)

	checked := 0
	for _, e := range entries {
		if e.Path == "/" {
			continue
		}
		if e.FileOffset == 0 {
			t.Errorf("%s: FileOffset not reported", e.Path)
			continue
		}
		got, err := acc.ReadEntryAt(int64(e.FileOffset))
		if err != nil {
			t.Errorf("%s: ReadEntryAt(%d): %v", e.Path, e.FileOffset, err)
			continue
		}
		if got.FileName() != e.FileName() {
			t.Errorf("offset %d: got name %q, want %q", e.FileOffset, got.FileName(), e.FileName())
		}
		if got.FileSize != e.FileSize {
			t.Errorf("%s: got size %d, want %d", e.Path, got.FileSize, e.FileSize)
		}
		checked++
	}
	if checked != 4 {
		t.Fatalf("checked %d entries, want 4 (a.txt, sub, b.txt, c.txt)", checked)
	}
}
