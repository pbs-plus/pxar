package backupproxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"testing"

	"github.com/pbs-plus/pxar/accessor"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
)

// mockClient implements ClientProvider using function fields.
type mockClient struct {
	statFn     func(ctx context.Context, path string) (format.Stat, error)
	readDirFn  func(ctx context.Context, path string) ([]DirEntry, error)
	readFileFn func(ctx context.Context, path string, offset, length int64) ([]byte, error)
	readLinkFn func(ctx context.Context, path string) (string, error)
}

func (m *mockClient) Stat(ctx context.Context, path string) (format.Stat, error) {
	return m.statFn(ctx, path)
}

func (m *mockClient) ReadDir(ctx context.Context, path string) ([]DirEntry, error) {
	return m.readDirFn(ctx, path)
}

func (m *mockClient) ReadFile(ctx context.Context, path string, offset, length int64) ([]byte, error) {
	return m.readFileFn(ctx, path, offset, length)
}

func (m *mockClient) ReadLink(ctx context.Context, path string) (string, error) {
	return m.readLinkFn(ctx, path)
}

// memFS is an in-memory filesystem for testing.
type memFS map[string]*memFile

type memFile struct {
	stat    format.Stat
	data    []byte // file content, or symlink target
	entries []DirEntry
}

func newMemFS() memFS { return make(memFS) }

func (fs memFS) addDir(path, parentPath string, mode uint64) {
	fs[path] = &memFile{
		stat: format.Stat{Mode: format.ModeIFDIR | mode},
	}
	if parent, ok := fs[parentPath]; ok {
		parent.entries = append(parent.entries, DirEntry{
			Name: baseName(path),
			Stat: format.Stat{Mode: format.ModeIFDIR | mode},
		})
	}
}

func (fs memFS) addFile(path, dirPath string, data []byte, mode uint64) {
	fs[path] = &memFile{stat: format.Stat{Mode: format.ModeIFREG | mode}, data: data}
	if dir, ok := fs[dirPath]; ok {
		dir.entries = append(dir.entries, DirEntry{
			Name: baseName(path),
			Stat: format.Stat{Mode: format.ModeIFREG | mode},
			Size: uint64(len(data)),
		})
	}
}

func (fs memFS) addSymlink(path, dirPath, target string, mode uint64) {
	fs[path] = &memFile{stat: format.Stat{Mode: format.ModeIFLNK | mode}, data: []byte(target)}
	if dir, ok := fs[dirPath]; ok {
		dir.entries = append(dir.entries, DirEntry{
			Name: baseName(path),
			Stat: format.Stat{Mode: format.ModeIFLNK | mode},
		})
	}
}

func baseName(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[i+1:]
		}
	}
	return path
}

func (fs memFS) provider() *mockClient {
	return &mockClient{
		statFn: func(_ context.Context, path string) (format.Stat, error) {
			f, ok := fs[path]
			if !ok {
				return format.Stat{}, fmt.Errorf("not found: %s", path)
			}
			return f.stat, nil
		},
		readDirFn: func(_ context.Context, path string) ([]DirEntry, error) {
			f, ok := fs[path]
			if !ok {
				return nil, fmt.Errorf("not found: %s", path)
			}
			if !f.stat.IsDir() {
				return nil, fmt.Errorf("not a dir: %s", path)
			}
			return f.entries, nil
		},
		readFileFn: func(_ context.Context, path string, offset, length int64) ([]byte, error) {
			f, ok := fs[path]
			if !ok {
				return nil, fmt.Errorf("not found: %s", path)
			}
			data := f.data
			if offset > int64(len(data)) {
				return nil, nil
			}
			data = data[offset:]
			if length >= 0 && length < int64(len(data)) {
				data = data[:length]
			}
			return data, nil
		},
		readLinkFn: func(_ context.Context, path string) (string, error) {
			f, ok := fs[path]
			if !ok {
				return "", fmt.Errorf("not found: %s", path)
			}
			return string(f.data), nil
		},
	}
}

func newTestServer(t *testing.T, fs memFS) (*Server, string) {
	t.Helper()
	client := fs.provider()
	dir := t.TempDir()
	config, _ := buzhash.NewConfig(4096)
	store, err := NewLocalStore(dir, config, false)
	if err != nil {
		t.Fatal(err)
	}
	return NewServer(client, store), dir
}

func TestServerEmptyDirectory(t *testing.T) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)

	srv, _ := newTestServer(t, fs)
	result, err := srv.RunBackup(context.Background(), "/root", BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   "test",
		BackupTime: 1700000000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.FileCount != 0 {
		t.Errorf("file count = %d, want 0", result.FileCount)
	}
}

func TestServerSingleFile(t *testing.T) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	fs.addFile("/root/hello.txt", "/root", []byte("hello world"), 0o644)

	srv, _ := newTestServer(t, fs)
	result, err := srv.RunBackup(context.Background(), "/root", BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.FileCount != 1 {
		t.Errorf("file count = %d, want 1", result.FileCount)
	}
	if result.Manifest == nil {
		t.Error("manifest should not be nil")
	}
}

func TestServerNestedDirectories(t *testing.T) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	fs.addDir("/root/subdir", "/root", 0o755)
	fs.addFile("/root/hello.txt", "/root", []byte("hello"), 0o644)
	fs.addFile("/root/subdir/nested.txt", "/root/subdir", []byte("nested"), 0o644)

	srv, _ := newTestServer(t, fs)
	result, err := srv.RunBackup(context.Background(), "/root", BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.FileCount != 2 {
		t.Errorf("file count = %d, want 2", result.FileCount)
	}
	if result.DirCount != 1 {
		t.Errorf("dir count = %d, want 1", result.DirCount)
	}
}

func TestServerSymlinks(t *testing.T) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	fs.addFile("/root/real.txt", "/root", []byte("content"), 0o644)
	fs.addSymlink("/root/link", "/root", "real.txt", 0o777)

	srv, _ := newTestServer(t, fs)
	result, err := srv.RunBackup(context.Background(), "/root", BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.FileCount != 1 {
		t.Errorf("file count = %d, want 1", result.FileCount)
	}
}

func TestServerMixedContent(t *testing.T) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	fs.addFile("/root/file.txt", "/root", []byte("data"), 0o644)
	fs.addDir("/root/subdir", "/root", 0o755)
	fs.addFile("/root/subdir/nested.txt", "/root/subdir", []byte("nested"), 0o644)
	fs.addSymlink("/root/link", "/root", "file.txt", 0o777)

	srv, _ := newTestServer(t, fs)
	result, err := srv.RunBackup(context.Background(), "/root", BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.FileCount != 2 {
		t.Errorf("file count = %d, want 2", result.FileCount)
	}
	if result.DirCount != 1 {
		t.Errorf("dir count = %d, want 1", result.DirCount)
	}
}

func TestServerLargeFile(t *testing.T) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	largeData := make([]byte, 100<<10)
	rand.Read(largeData)
	fs.addFile("/root/large.bin", "/root", largeData, 0o644)

	srv, _ := newTestServer(t, fs)
	result, err := srv.RunBackup(context.Background(), "/root", BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.FileCount != 1 {
		t.Errorf("file count = %d, want 1", result.FileCount)
	}
}

func TestServerContextCancellation(t *testing.T) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	fs.addFile("/root/file.txt", "/root", []byte("data"), 0o644)

	srv, _ := newTestServer(t, fs)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := srv.RunBackup(ctx, "/root", BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   "test",
	})
	if err == nil {
		t.Error("expected error from cancelled context")
	}
}

func TestServerClientError(t *testing.T) {
	client := &mockClient{
		statFn: func(_ context.Context, _ string) (format.Stat, error) {
			return format.Stat{Mode: format.ModeIFDIR | 0o755}, nil
		},
		readDirFn: func(_ context.Context, _ string) ([]DirEntry, error) {
			return nil, fmt.Errorf("permission denied")
		},
		readFileFn: func(_ context.Context, _ string, _, _ int64) ([]byte, error) {
			return nil, nil
		},
		readLinkFn: func(_ context.Context, _ string) (string, error) {
			return "", nil
		},
	}

	dir := t.TempDir()
	config, _ := buzhash.NewConfig(4096)
	store, _ := NewLocalStore(dir, config, false)
	srv := NewServer(client, store)

	_, err := srv.RunBackup(context.Background(), "/root", BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   "test",
	})
	if err == nil {
		t.Error("expected error from client")
	}
}

func TestServerArchiveRoundTrip(t *testing.T) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	fs.addFile("/root/hello.txt", "/root", []byte("hello world"), 0o644)
	fs.addDir("/root/subdir", "/root", 0o755)
	fs.addFile("/root/subdir/nested.txt", "/root/subdir", []byte("nested content"), 0o644)
	fs.addSymlink("/root/link", "/root", "hello.txt", 0o777)

	srv, dir := newTestServer(t, fs)
	result, err := srv.RunBackup(context.Background(), "/root", BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Reconstruct archive from stored chunks
	chunkStore, _ := datastore.NewChunkStore(dir)
	raw, err := os.ReadFile(dir + "/root.pxar.didx")
	if err != nil {
		t.Fatal(err)
	}

	reader, err := datastore.ReadDynamicIndex(raw)
	if err != nil {
		t.Fatal(err)
	}

	var archive bytes.Buffer
	for i := 0; i < reader.Count(); i++ {
		info, _ := reader.ChunkInfo(i)
		chunk, err := chunkStore.LoadChunk(info.Digest)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := datastore.DecodeBlob(chunk)
		if err != nil {
			t.Fatal(err)
		}
		archive.Write(decoded)
	}

	// Verify the archive is valid pxar
	acc := accessor.NewAccessor(bytes.NewReader(archive.Bytes()))
	root, err := acc.ReadRoot()
	if err != nil {
		t.Fatalf("read root: %v", err)
	}
	if !root.IsDir() {
		t.Error("root should be a directory")
	}

	_ = result
}

func TestDetectionModeString(t *testing.T) {
	tests := []struct {
		mode DetectionMode
		want string
	}{
		{DetectionLegacy, "legacy"},
		{DetectionData, "data"},
		{DetectionMetadata, "metadata"},
		{DetectionMode(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.mode.String(); got != tt.want {
			t.Errorf("DetectionMode(%d).String() = %q, want %q", tt.mode, got, tt.want)
		}
	}
}

func TestStatMetadataEqual(t *testing.T) {
	base := format.Stat{
		Mode:  format.ModeIFREG | 0o644,
		Flags: 0,
		UID:   1000,
		GID:   1000,
		Mtime: format.StatxTimestamp{Secs: 1700000000, Nanos: 0},
	}

	tests := []struct {
		name  string
		other format.Stat
		equal bool
	}{
		{"identical", base, true},
		{"diff_mode", format.Stat{Mode: format.ModeIFREG | 0o755, Flags: 0, UID: 1000, GID: 1000, Mtime: base.Mtime}, false},
		{"diff_uid", format.Stat{Mode: base.Mode, Flags: 0, UID: 0, GID: 1000, Mtime: base.Mtime}, false},
		{"diff_gid", format.Stat{Mode: base.Mode, Flags: 0, UID: 1000, GID: 0, Mtime: base.Mtime}, false},
		{"diff_mtime", format.Stat{Mode: base.Mode, Flags: 0, UID: 1000, GID: 1000, Mtime: format.StatxTimestamp{Secs: 1800000000}}, false},
		{"diff_type", format.Stat{Mode: format.ModeIFDIR | 0o755, Flags: 0, UID: 1000, GID: 1000, Mtime: base.Mtime}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := base.MetadataEqual(tt.other); got != tt.equal {
				t.Errorf("MetadataEqual() = %v, want %v", got, tt.equal)
			}
		})
	}
}

func TestEntryMatches(t *testing.T) {
	prevEntry := &CatalogEntry{
		Path:          "/root/file.txt",
		Stat:          format.Stat{Mode: format.ModeIFREG | 0o644, UID: 1000, GID: 1000, Mtime: format.StatxTimestamp{Secs: 1700000000}},
		FileSize:      100,
		IsRegularFile: true,
	}

	tests := []struct {
		name    string
		current DirEntry
		want    bool
	}{
		{
			"matching_file",
			DirEntry{Name: "file.txt", Stat: format.Stat{Mode: format.ModeIFREG | 0o644, UID: 1000, GID: 1000, Mtime: format.StatxTimestamp{Secs: 1700000000}}, Size: 100},
			true,
		},
		{
			"diff_size",
			DirEntry{Name: "file.txt", Stat: format.Stat{Mode: format.ModeIFREG | 0o644, UID: 1000, GID: 1000, Mtime: format.StatxTimestamp{Secs: 1700000000}}, Size: 200},
			false,
		},
		{
			"diff_mtime",
			DirEntry{Name: "file.txt", Stat: format.Stat{Mode: format.ModeIFREG | 0o644, UID: 1000, GID: 1000, Mtime: format.StatxTimestamp{Secs: 1800000000}}, Size: 100},
			false,
		},
		{
			"diff_type",
			DirEntry{Name: "file.txt", Stat: format.Stat{Mode: format.ModeIFDIR | 0o644, UID: 1000, GID: 1000, Mtime: format.StatxTimestamp{Secs: 1700000000}}, Size: 0},
			false,
		},
		{
			"nil_prev",
			DirEntry{Name: "file.txt", Stat: format.Stat{Mode: format.ModeIFREG | 0o644}, Size: 100},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EntryMatches(tt.current, prevEntry); got != tt.want {
				t.Errorf("EntryMatches() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestServerDataMode(t *testing.T) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	fs.addFile("/root/hello.txt", "/root", []byte("hello world"), 0o644)
	fs.addDir("/root/subdir", "/root", 0o755)
	fs.addFile("/root/subdir/nested.txt", "/root/subdir", []byte("nested content"), 0o644)

	srv, dir := newTestServer(t, fs)
	result, err := srv.RunSplitBackup(context.Background(), "/root", BackupConfig{
		BackupType:    datastore.BackupHost,
		BackupID:      "test",
		BackupTime:    1700000000,
		DetectionMode: DetectionData,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.FileCount != 2 {
		t.Errorf("file count = %d, want 2", result.FileCount)
	}
	if result.DirCount != 1 {
		t.Errorf("dir count = %d, want 1", result.DirCount)
	}

	// Verify both index files exist
	if _, err := os.Stat(dir + "/root.mpxar.didx"); os.IsNotExist(err) {
		t.Error("root.mpxar.didx should exist")
	}
	if _, err := os.Stat(dir + "/root.ppxar.didx"); os.IsNotExist(err) {
		t.Error("root.ppxar.didx should exist")
	}
}

func TestServerMetadataMode(t *testing.T) {
	// Phase 1: Create initial backup in data mode
	fs1 := newMemFS()
	fs1.addDir("/root", "", 0o755)
	fs1.addFile("/root/a.txt", "/root", []byte("alpha"), 0o644)
	fs1.addFile("/root/b.txt", "/root", []byte("beta"), 0o644)

	srv1, prevDir := newTestServer(t, fs1)
	prevResult, err := srv1.RunSplitBackup(context.Background(), "/root", BackupConfig{
		BackupType:    datastore.BackupHost,
		BackupID:      "test",
		BackupTime:    1700000000,
		DetectionMode: DetectionData,
	})
	if err != nil {
		t.Fatalf("initial backup: %v", err)
	}
	if prevResult.FileCount != 2 {
		t.Errorf("initial file count = %d, want 2", prevResult.FileCount)
	}

	// Phase 2: Create metadata mode backup with same files (should reuse payload)
	fs2 := newMemFS()
	fs2.addDir("/root", "", 0o755)
	fs2.addFile("/root/a.txt", "/root", []byte("alpha"), 0o644)         // unchanged
	fs2.addFile("/root/b.txt", "/root", []byte("beta modified"), 0o644) // modified

	srv2, currDir := newTestServer(t, fs2)
	metaResult, err := srv2.RunMetadataBackup(context.Background(), "/root", BackupConfig{
		BackupType:    datastore.BackupHost,
		BackupID:      "test",
		BackupTime:    1700000001,
		DetectionMode: DetectionMetadata,
		PreviousBackup: &PreviousBackupRef{
			BackupType: datastore.BackupHost,
			BackupID:   "test",
			BackupTime: 1700000000,
			Dir:        prevDir,
		},
	})
	if err != nil {
		t.Fatalf("metadata backup: %v", err)
	}
	if metaResult.FileCount != 2 {
		t.Errorf("metadata file count = %d, want 2", metaResult.FileCount)
	}

	// Verify the metadata backup produced split archives
	if _, err := os.Stat(currDir + "/root.mpxar.didx"); os.IsNotExist(err) {
		t.Error("root.mpxar.didx should exist")
	}
	if _, err := os.Stat(currDir + "/root.ppxar.didx"); os.IsNotExist(err) {
		t.Error("root.ppxar.didx should exist")
	}

	_ = currDir
}

func TestRunBackupWithMode(t *testing.T) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	fs.addFile("/root/file.txt", "/root", []byte("content"), 0o644)

	srv, _ := newTestServer(t, fs)

	// Test legacy mode dispatches correctly
	result, err := srv.RunBackupWithMode(context.Background(), "/root", BackupConfig{
		BackupType:    datastore.BackupHost,
		BackupID:      "test",
		DetectionMode: DetectionLegacy,
	})
	if err != nil {
		t.Fatalf("RunBackupWithMode legacy: %v", err)
	}
	if result.FileCount != 1 {
		t.Errorf("legacy file count = %d, want 1", result.FileCount)
	}
}
