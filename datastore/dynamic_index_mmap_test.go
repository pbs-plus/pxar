package datastore

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOpenDynamicIndexMatchesParse(t *testing.T) {
	w := NewDynamicIndexWriter(1700000000)
	for i := range 5 {
		var d [32]byte
		d[0] = byte(i + 1)
		w.Add(uint64((i+1)*1024), d)
	}
	raw, err := w.Finish()
	if err != nil {
		t.Fatalf("finish: %v", err)
	}

	path := filepath.Join(t.TempDir(), "test.didx")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	want, err := ParseDynamicIndex(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	got, err := OpenDynamicIndex(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if got.Count() != want.Count() {
		t.Fatalf("count = %d, want %d", got.Count(), want.Count())
	}
	for i := range want.Count() {
		if got.Entry(i) != want.Entry(i) {
			t.Fatalf("entry %d = %+v, want %+v", i, got.Entry(i), want.Entry(i))
		}
	}
	if got.IndexBytes() != want.IndexBytes() || got.CTime() != want.CTime() {
		t.Fatalf("header mismatch: %d/%d vs %d/%d",
			got.IndexBytes(), got.CTime(), want.IndexBytes(), want.CTime())
	}
	gotCsum, gotEnd := got.ComputeCsum()
	wantCsum, wantEnd := want.ComputeCsum()
	if gotCsum != wantCsum || gotEnd != wantEnd {
		t.Fatalf("csum mismatch")
	}
	if pos, ok := got.ChunkFromOffset(2000); !ok || pos != 1 {
		t.Fatalf("ChunkFromOffset(2000) = %d, %v", pos, ok)
	}

	if err := got.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := got.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if err := want.Close(); err != nil {
		t.Fatalf("close parsed reader: %v", err)
	}
}

func TestOpenDynamicIndexRejectsShortFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "short.didx")
	if err := os.WriteFile(path, []byte("nope"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := OpenDynamicIndex(path); err == nil {
		t.Fatal("expected error for short file")
	}
}
