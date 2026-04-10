package backupproxy

import (
	"context"
	"fmt"
	"testing"

	"github.com/pbs-plus/pxar/format"
)

// mockFS is a test FileSystemAccessor using function fields.
type mockFS struct {
	statFn     func(path string) (format.Stat, error)
	readDirFn  func(path string) ([]DirEntry, error)
	readFileFn func(path string, offset, length int64) ([]byte, error)
	readLinkFn func(path string) (string, error)
}

func (m *mockFS) Stat(path string) (format.Stat, error) {
	return m.statFn(path)
}

func (m *mockFS) ReadDir(path string) ([]DirEntry, error) {
	return m.readDirFn(path)
}

func (m *mockFS) ReadFile(path string, offset, length int64) ([]byte, error) {
	return m.readFileFn(path, offset, length)
}

func (m *mockFS) ReadLink(path string) (string, error) {
	return m.readLinkFn(path)
}

func TestLocalClientStat(t *testing.T) {
	want := format.Stat{Mode: format.ModeIFREG | 0o644, UID: 1000, GID: 1000}
	fs := &mockFS{
		statFn: func(path string) (format.Stat, error) {
			if path != "/test.txt" {
				t.Errorf("stat path = %q, want %q", path, "/test.txt")
			}
			return want, nil
		},
	}

	c := NewLocalClient(fs)
	got, err := c.Stat(context.Background(), "/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("stat = %+v, want %+v", got, want)
	}
}

func TestLocalClientReadDir(t *testing.T) {
	entries := []DirEntry{
		{Name: "a.txt", Stat: format.Stat{Mode: format.ModeIFREG | 0o644}},
		{Name: "subdir", Stat: format.Stat{Mode: format.ModeIFDIR | 0o755}},
	}
	fs := &mockFS{
		readDirFn: func(path string) ([]DirEntry, error) {
			return entries, nil
		},
	}

	c := NewLocalClient(fs)
	got, err := c.ReadDir(context.Background(), "/")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2", len(got))
	}
	if got[0].Name != "a.txt" {
		t.Errorf("entry[0].Name = %q, want %q", got[0].Name, "a.txt")
	}
}

func TestLocalClientReadFile(t *testing.T) {
	content := []byte("hello world")
	fs := &mockFS{
		readFileFn: func(path string, offset, length int64) ([]byte, error) {
			if offset != 6 || length != 5 {
				t.Errorf("read(%q, %d, %d), want (%q, 6, 5)", path, offset, length, path)
			}
			return content[6:11], nil
		},
	}

	c := NewLocalClient(fs)
	got, err := c.ReadFile(context.Background(), "/test.txt", 6, 5)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "world" {
		t.Errorf("data = %q, want %q", string(got), "world")
	}
}

func TestLocalClientReadLink(t *testing.T) {
	fs := &mockFS{
		readLinkFn: func(path string) (string, error) {
			return "/usr/bin/python3", nil
		},
	}

	c := NewLocalClient(fs)
	got, err := c.ReadLink(context.Background(), "/link")
	if err != nil {
		t.Fatal(err)
	}
	if got != "/usr/bin/python3" {
		t.Errorf("target = %q, want %q", got, "/usr/bin/python3")
	}
}

func TestLocalClientStatError(t *testing.T) {
	fs := &mockFS{
		statFn: func(path string) (format.Stat, error) {
			return format.Stat{}, fmt.Errorf("not found")
		},
	}

	c := NewLocalClient(fs)
	_, err := c.Stat(context.Background(), "/missing")
	if err == nil {
		t.Error("expected error")
	}
}

func TestLocalClientReadFileError(t *testing.T) {
	fs := &mockFS{
		readFileFn: func(path string, offset, length int64) ([]byte, error) {
			return nil, fmt.Errorf("permission denied")
		},
	}

	c := NewLocalClient(fs)
	_, err := c.ReadFile(context.Background(), "/secret", 0, 100)
	if err == nil {
		t.Error("expected error")
	}
}
