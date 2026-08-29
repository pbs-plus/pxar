package datastore

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
)

func TestDynamicIndexStreamWriterRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stream.didx")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewDynamicIndexStreamWriter(file, 1_700_000_000)
	if err != nil {
		t.Fatal(err)
	}
	entries := []DynamicEntry{
		{EndOffset: 1_000, Digest: sha256.Sum256([]byte("one"))},
		{EndOffset: 3_500, Digest: sha256.Sum256([]byte("two"))},
		{EndOffset: 10_000, Digest: sha256.Sum256([]byte("three"))},
	}
	for _, entry := range entries {
		if err := writer.Add(entry.EndOffset, entry.Digest); err != nil {
			t.Fatal(err)
		}
	}
	csum, size, err := writer.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if size != entries[len(entries)-1].EndOffset {
		t.Fatalf("size = %d, want %d", size, entries[len(entries)-1].EndOffset)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	index, err := OpenDynamicIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = index.Close() }()
	if index.Count() != len(entries) || index.IndexCsum() != csum {
		t.Fatalf("index count/checksum mismatch: count=%d checksum=%x", index.Count(), index.IndexCsum())
	}
	for i, want := range entries {
		if got := index.Entry(i); got != want {
			t.Fatalf("entry %d = %+v, want %+v", i, got, want)
		}
	}
}

func TestDynamicIndexStreamWriterRejectsInvalidOrder(t *testing.T) {
	file, err := os.Create(filepath.Join(t.TempDir(), "invalid.didx"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	writer, err := NewDynamicIndexStreamWriter(file, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Add(10, [32]byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Add(10, [32]byte{2}); err == nil {
		t.Fatal("accepted duplicate end offset")
	}
	if _, _, err := writer.Finish(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := writer.Finish(); err == nil {
		t.Fatal("accepted a second finish")
	}
}
