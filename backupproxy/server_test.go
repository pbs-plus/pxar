package backupproxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/accessor"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
)

// mockClient implements ClientProvider using function fields.
type mockClient struct {
	statFn      func(ctx context.Context, path string) (format.Stat, error)
	readDirFn   func(ctx context.Context, path string) ([]DirEntry, error)
	readFileFn  func(ctx context.Context, path string, offset, length int64) ([]byte, error)
	readLinkFn  func(ctx context.Context, path string) (string, error)
	getXAttrsFn func(ctx context.Context, path string) ([]format.XAttr, error)
	getACLFn    func(ctx context.Context, path string) (pxar.ACL, error)
	getFCapsFn  func(ctx context.Context, path string) ([]byte, error)
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

func (m *mockClient) GetXAttrs(ctx context.Context, path string) ([]format.XAttr, error) {
	if m.getXAttrsFn != nil {
		return m.getXAttrsFn(ctx, path)
	}
	return nil, nil
}

func (m *mockClient) GetACL(ctx context.Context, path string) (pxar.ACL, error) {
	if m.getACLFn != nil {
		return m.getACLFn(ctx, path)
	}
	return pxar.ACL{}, nil
}

func (m *mockClient) GetFCaps(ctx context.Context, path string) ([]byte, error) {
	if m.getFCapsFn != nil {
		return m.getFCapsFn(ctx, path)
	}
	return nil, nil
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

func (fs memFS) addDirWithMtime(path, parentPath string, mode uint64, mtime format.StatxTimestamp) {
	fs[path] = &memFile{
		stat: format.Stat{Mode: format.ModeIFDIR | mode, Mtime: mtime},
	}
	if parent, ok := fs[parentPath]; ok {
		parent.entries = append(parent.entries, DirEntry{
			Name: baseName(path),
			Stat: format.Stat{Mode: format.ModeIFDIR | mode, Mtime: mtime},
		})
	}
}

func (fs memFS) addFile(path, dirPath string, data []byte, mode uint64) {
	fs.addFileWithMtime(path, dirPath, data, mode, format.StatxTimestamp{})
}

func (fs memFS) addFileWithMtime(path, dirPath string, data []byte, mode uint64, mtime format.StatxTimestamp) {
	fs[path] = &memFile{stat: format.Stat{Mode: format.ModeIFREG | mode, Mtime: mtime}, data: data}
	if dir, ok := fs[dirPath]; ok {
		dir.entries = append(dir.entries, DirEntry{
			Name: baseName(path),
			Stat: format.Stat{Mode: format.ModeIFREG | mode, Mtime: mtime},
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
		Metadata:      pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFREG | 0o644, UID: 1000, GID: 1000, Mtime: format.StatxTimestamp{Secs: 1700000000}}},
		FileSize:      100,
		IsRegularFile: true,
	}

	tests := []struct {
		name        string
		current     DirEntry
		currentMeta pxar.Metadata
		prev        *CatalogEntry
		want        bool
	}{
		{
			"matching_file",
			DirEntry{Name: "file.txt", Stat: format.Stat{Mode: format.ModeIFREG | 0o644, UID: 1000, GID: 1000, Mtime: format.StatxTimestamp{Secs: 1700000000}}, Size: 100},
			pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFREG | 0o644, UID: 1000, GID: 1000, Mtime: format.StatxTimestamp{Secs: 1700000000}}},
			prevEntry,
			true,
		},
		{
			"diff_size",
			DirEntry{Name: "file.txt", Stat: format.Stat{Mode: format.ModeIFREG | 0o644, UID: 1000, GID: 1000, Mtime: format.StatxTimestamp{Secs: 1700000000}}, Size: 200},
			pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFREG | 0o644, UID: 1000, GID: 1000, Mtime: format.StatxTimestamp{Secs: 1700000000}}},
			prevEntry,
			false,
		},
		{
			"diff_mtime",
			DirEntry{Name: "file.txt", Stat: format.Stat{Mode: format.ModeIFREG | 0o644, UID: 1000, GID: 1000, Mtime: format.StatxTimestamp{Secs: 1800000000}}, Size: 100},
			pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFREG | 0o644, UID: 1000, GID: 1000, Mtime: format.StatxTimestamp{Secs: 1800000000}}},
			prevEntry,
			false,
		},
		{
			"diff_type",
			DirEntry{Name: "file.txt", Stat: format.Stat{Mode: format.ModeIFDIR | 0o644, UID: 1000, GID: 1000, Mtime: format.StatxTimestamp{Secs: 1700000000}}, Size: 0},
			pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o644, UID: 1000, GID: 1000, Mtime: format.StatxTimestamp{Secs: 1700000000}}},
			prevEntry,
			false,
		},
		{
			"nil_prev",
			DirEntry{Name: "file.txt", Stat: format.Stat{Mode: format.ModeIFREG | 0o644}, Size: 100},
			pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFREG | 0o644, UID: 1000, GID: 1000, Mtime: format.StatxTimestamp{Secs: 1700000000}}},
			nil,
			false,
		},
		{
			"diff_xattr",
			DirEntry{Name: "file.txt", Stat: format.Stat{Mode: format.ModeIFREG | 0o644, UID: 1000, GID: 1000, Mtime: format.StatxTimestamp{Secs: 1700000000}}, Size: 100, XAttrs: []format.XAttr{format.NewXAttr([]byte("user.test"), []byte("changed"))}},
			pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFREG | 0o644, UID: 1000, GID: 1000, Mtime: format.StatxTimestamp{Secs: 1700000000}}, XAttrs: []format.XAttr{format.NewXAttr([]byte("user.test"), []byte("changed"))}},
			prevEntry,
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EntryMatches(tt.current, tt.currentMeta, tt.prev); got != tt.want {
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

func restoreLegacyArchive(t *testing.T, dir string) *accessor.Accessor {
	t.Helper()
	raw, err := os.ReadFile(dir + "/root.pxar.didx")
	if err != nil {
		t.Fatalf("read legacy index: %v", err)
	}
	idx, err := datastore.ReadDynamicIndex(raw)
	if err != nil {
		t.Fatalf("parse legacy index: %v", err)
	}
	chunkStore, err := datastore.NewChunkStore(dir)
	if err != nil {
		t.Fatalf("open chunk store: %v", err)
	}
	var archive bytes.Buffer
	for i := 0; i < idx.Count(); i++ {
		info, _ := idx.ChunkInfo(i)
		chunk, err := chunkStore.LoadChunk(info.Digest)
		if err != nil {
			t.Fatalf("load chunk %d: %v", i, err)
		}
		decoded, err := datastore.DecodeBlob(chunk)
		if err != nil {
			t.Fatalf("decode chunk %d: %v", i, err)
		}
		archive.Write(decoded)
	}
	acc := accessor.NewAccessor(bytes.NewReader(archive.Bytes()))
	return acc
}

func restoreSplitArchive(t *testing.T, dir string) (*accessor.Accessor, *accessor.Accessor) {
	t.Helper()
	metaRaw, err := os.ReadFile(dir + "/root.mpxar.didx")
	if err != nil {
		t.Fatalf("read metadata index: %v", err)
	}
	payloadRaw, err := os.ReadFile(dir + "/root.ppxar.didx")
	if err != nil {
		t.Fatalf("read payload index: %v", err)
	}
	metaIdx, err := datastore.ReadDynamicIndex(metaRaw)
	if err != nil {
		t.Fatalf("parse metadata index: %v", err)
	}
	payloadIdx, err := datastore.ReadDynamicIndex(payloadRaw)
	if err != nil {
		t.Fatalf("parse payload index: %v", err)
	}
	chunkStore, err := datastore.NewChunkStore(dir)
	if err != nil {
		t.Fatalf("open chunk store: %v", err)
	}
	var metaBuf, payloadBuf bytes.Buffer
	for _, idx := range []*datastore.DynamicIndexReader{metaIdx, payloadIdx} {
		dst := &metaBuf
		if idx == payloadIdx {
			dst = &payloadBuf
		}
		for i := 0; i < idx.Count(); i++ {
			info, _ := idx.ChunkInfo(i)
			chunk, err := chunkStore.LoadChunk(info.Digest)
			if err != nil {
				t.Fatalf("load chunk %d: %v", i, err)
			}
			decoded, err := datastore.DecodeBlob(chunk)
			if err != nil {
				t.Fatalf("decode chunk %d: %v", i, err)
			}
			dst.Write(decoded)
		}
	}
	acc := accessor.NewAccessor(bytes.NewReader(metaBuf.Bytes()), bytes.NewReader(payloadBuf.Bytes()))
	return acc, nil
}

func verifyFileContent(t *testing.T, acc *accessor.Accessor, path, expected string) {
	t.Helper()
	entry, err := acc.Lookup(path)
	if err != nil {
		t.Fatalf("lookup %q: %v", path, err)
	}
	if !entry.IsRegularFile() {
		t.Fatalf("expected %q to be a regular file, got kind=%v", path, entry.Kind)
	}
	content, err := acc.ReadFileContent(entry)
	if err != nil {
		t.Fatalf("read %q content: %v", path, err)
	}
	if string(content) != expected {
		t.Errorf("content of %q = %q, want %q", path, string(content), expected)
	}
}

func verifyIsSymlink(t *testing.T, acc *accessor.Accessor, path, expectedTarget string) {
	t.Helper()
	entry, err := acc.Lookup(path)
	if err != nil {
		t.Fatalf("lookup %q: %v", path, err)
	}
	if !entry.IsSymlink() {
		t.Fatalf("expected %q to be a symlink, got kind=%v", path, entry.Kind)
	}
	if entry.LinkTarget != expectedTarget {
		t.Errorf("symlink %q target = %q, want %q", path, entry.LinkTarget, expectedTarget)
	}
}

// --- Legacy mode edge cases ---

func TestLegacyEmptyDirectory(t *testing.T) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)

	srv, dir := newTestServer(t, fs)
	result, err := srv.RunBackup(context.Background(), "/root", BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.FileCount != 0 {
		t.Errorf("file count = %d, want 0", result.FileCount)
	}
	if result.DirCount != 0 {
		t.Errorf("dir count = %d, want 0", result.DirCount)
	}

	acc := restoreLegacyArchive(t, dir)
	root, err := acc.ReadRoot()
	if err != nil {
		t.Fatalf("read root: %v", err)
	}
	if !root.IsDir() {
		t.Error("root should be a directory")
	}
}

func TestLegacyEmptyFile(t *testing.T) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	fs.addFile("/root/empty.txt", "/root", []byte{}, 0o644)

	srv, dir := newTestServer(t, fs)
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

	acc := restoreLegacyArchive(t, dir)
	verifyFileContent(t, acc, "empty.txt", "")
}

func TestLegacyDeeplyNestedDirs(t *testing.T) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	fs.addDir("/root/a", "/root", 0o755)
	fs.addDir("/root/a/b", "/root/a", 0o755)
	fs.addDir("/root/a/b/c", "/root/a/b", 0o755)
	fs.addFile("/root/a/b/c/deep.txt", "/root/a/b/c", []byte("deep content"), 0o644)

	srv, dir := newTestServer(t, fs)
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
	if result.DirCount != 3 {
		t.Errorf("dir count = %d, want 3", result.DirCount)
	}

	acc := restoreLegacyArchive(t, dir)
	verifyFileContent(t, acc, "a/b/c/deep.txt", "deep content")
}

func TestLegacySymlinks(t *testing.T) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	fs.addFile("/root/real.txt", "/root", []byte("real content"), 0o644)
	fs.addSymlink("/root/link", "/root", "real.txt", 0o777)

	srv, dir := newTestServer(t, fs)
	result, err := srv.RunBackup(context.Background(), "/root", BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.FileCount != 1 {
		t.Errorf("file count = %d, want 1 (symlink not counted)", result.FileCount)
	}

	acc := restoreLegacyArchive(t, dir)
	verifyIsSymlink(t, acc, "link", "real.txt")
	verifyFileContent(t, acc, "real.txt", "real content")
}

func TestLegacyMultipleFilesWithSameContent(t *testing.T) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	fs.addFile("/root/a.txt", "/root", []byte("same content"), 0o644)
	fs.addFile("/root/b.txt", "/root", []byte("same content"), 0o644)

	srv, dir := newTestServer(t, fs)
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

	acc := restoreLegacyArchive(t, dir)
	verifyFileContent(t, acc, "a.txt", "same content")
	verifyFileContent(t, acc, "b.txt", "same content")
}

func TestLegacyMixedContent(t *testing.T) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	fs.addFile("/root/file.txt", "/root", []byte("data"), 0o644)
	fs.addDir("/root/subdir", "/root", 0o755)
	fs.addFile("/root/subdir/nested.txt", "/root/subdir", []byte("nested"), 0o644)
	fs.addFile("/root/subdir/empty.txt", "/root/subdir", []byte{}, 0o644)
	fs.addSymlink("/root/link", "/root", "file.txt", 0o777)

	srv, dir := newTestServer(t, fs)
	result, err := srv.RunBackup(context.Background(), "/root", BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.FileCount != 3 {
		t.Errorf("file count = %d, want 3", result.FileCount)
	}
	if result.DirCount != 1 {
		t.Errorf("dir count = %d, want 1", result.DirCount)
	}

	acc := restoreLegacyArchive(t, dir)
	verifyFileContent(t, acc, "file.txt", "data")
	verifyFileContent(t, acc, "subdir/nested.txt", "nested")
	verifyFileContent(t, acc, "subdir/empty.txt", "")
	verifyIsSymlink(t, acc, "link", "file.txt")
}

func TestLegacyLargeFile(t *testing.T) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	largeData := make([]byte, 100<<10) // 100KB
	rand.Read(largeData)
	fs.addFile("/root/large.bin", "/root", largeData, 0o644)

	srv, dir := newTestServer(t, fs)
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
	if result.TotalBytes == 0 {
		t.Error("total bytes should not be 0")
	}

	acc := restoreLegacyArchive(t, dir)
	entry, err := acc.Lookup("large.bin")
	if err != nil {
		t.Fatalf("lookup large.bin: %v", err)
	}
	content, err := acc.ReadFileContent(entry)
	if err != nil {
		t.Fatalf("read large.bin content: %v", err)
	}
	if !bytes.Equal(content, largeData) {
		t.Errorf("large.bin content mismatch: got %d bytes, want %d bytes", len(content), len(largeData))
	}
}

// --- Data mode edge cases ---

func TestDataModeEmptyDirectory(t *testing.T) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)

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
	if result.FileCount != 0 {
		t.Errorf("file count = %d, want 0", result.FileCount)
	}

	acc, _ := restoreSplitArchive(t, dir)
	root, err := acc.ReadRoot()
	if err != nil {
		t.Fatalf("read root: %v", err)
	}
	if !root.IsDir() {
		t.Error("root should be a directory")
	}
}

func TestDataModeEmptyFile(t *testing.T) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	fs.addFile("/root/empty.txt", "/root", []byte{}, 0o644)

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
	if result.FileCount != 1 {
		t.Errorf("file count = %d, want 1", result.FileCount)
	}

	acc, _ := restoreSplitArchive(t, dir)
	verifyFileContent(t, acc, "empty.txt", "")
}

func TestDataModeRoundTrip(t *testing.T) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	fs.addFile("/root/hello.txt", "/root", []byte("hello world"), 0o644)
	fs.addDir("/root/subdir", "/root", 0o755)
	fs.addFile("/root/subdir/nested.txt", "/root/subdir", []byte("nested content"), 0o644)
	fs.addFile("/root/subdir/empty.txt", "/root/subdir", []byte{}, 0o644)
	fs.addSymlink("/root/link", "/root", "hello.txt", 0o777)

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
	if result.FileCount != 3 {
		t.Errorf("file count = %d, want 3", result.FileCount)
	}
	if result.DirCount != 1 {
		t.Errorf("dir count = %d, want 1", result.DirCount)
	}

	acc, _ := restoreSplitArchive(t, dir)
	verifyFileContent(t, acc, "hello.txt", "hello world")
	verifyFileContent(t, acc, "subdir/nested.txt", "nested content")
	verifyFileContent(t, acc, "subdir/empty.txt", "")
	verifyIsSymlink(t, acc, "link", "hello.txt")
}

func TestDataModeDeeplyNested(t *testing.T) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	fs.addDir("/root/a", "/root", 0o755)
	fs.addDir("/root/a/b", "/root/a", 0o755)
	fs.addDir("/root/a/b/c", "/root/a/b", 0o755)
	fs.addFile("/root/a/b/c/deep.txt", "/root/a/b/c", []byte("deep content"), 0o644)

	srv, dir := newTestServer(t, fs)
	_, err := srv.RunSplitBackup(context.Background(), "/root", BackupConfig{
		BackupType:    datastore.BackupHost,
		BackupID:      "test",
		BackupTime:    1700000000,
		DetectionMode: DetectionData,
	})
	if err != nil {
		t.Fatal(err)
	}

	acc, _ := restoreSplitArchive(t, dir)
	verifyFileContent(t, acc, "a/b/c/deep.txt", "deep content")
}

func TestDataModeLargeFile(t *testing.T) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	largeData := make([]byte, 100<<10)
	rand.Read(largeData)
	fs.addFile("/root/large.bin", "/root", largeData, 0o644)

	srv, dir := newTestServer(t, fs)
	_, err := srv.RunSplitBackup(context.Background(), "/root", BackupConfig{
		BackupType:    datastore.BackupHost,
		BackupID:      "test",
		BackupTime:    1700000000,
		DetectionMode: DetectionData,
	})
	if err != nil {
		t.Fatal(err)
	}

	acc, _ := restoreSplitArchive(t, dir)
	entry, err := acc.Lookup("large.bin")
	if err != nil {
		t.Fatalf("lookup large.bin: %v", err)
	}
	content, err := acc.ReadFileContent(entry)
	if err != nil {
		t.Fatalf("read large.bin: %v", err)
	}
	if !bytes.Equal(content, largeData) {
		t.Errorf("large.bin content mismatch: got %d bytes, want %d bytes", len(content), len(largeData))
	}
}

func TestDataModeMultipleFilesSameContent(t *testing.T) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	fs.addFile("/root/a.txt", "/root", []byte("identical"), 0o644)
	fs.addFile("/root/b.txt", "/root", []byte("identical"), 0o644)

	srv, dir := newTestServer(t, fs)
	_, err := srv.RunSplitBackup(context.Background(), "/root", BackupConfig{
		BackupType:    datastore.BackupHost,
		BackupID:      "test",
		BackupTime:    1700000000,
		DetectionMode: DetectionData,
	})
	if err != nil {
		t.Fatal(err)
	}

	acc, _ := restoreSplitArchive(t, dir)
	verifyFileContent(t, acc, "a.txt", "identical")
	verifyFileContent(t, acc, "b.txt", "identical")
}

func TestDataModeFilesOnlyWithNoDirs(t *testing.T) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	fs.addFile("/root/only.txt", "/root", []byte("just a file"), 0o644)

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
	if result.FileCount != 1 {
		t.Errorf("file count = %d, want 1", result.FileCount)
	}
	if result.DirCount != 0 {
		t.Errorf("dir count = %d, want 0", result.DirCount)
	}

	acc, _ := restoreSplitArchive(t, dir)
	verifyFileContent(t, acc, "only.txt", "just a file")
}

// --- Metadata mode edge cases ---

func TestMetadataModeAllUnchanged(t *testing.T) {
	mtime := format.StatxTimestamp{Secs: 1700000000}

	// Phase 1: initial data backup
	fs1 := newMemFS()
	fs1.addDirWithMtime("/root", "", 0o755, mtime)
	fs1.addFileWithMtime("/root/a.txt", "/root", []byte("alpha"), 0o644, mtime)
	fs1.addFileWithMtime("/root/b.txt", "/root", []byte("beta"), 0o644, mtime)

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

	// Phase 2: no files changed
	fs2 := newMemFS()
	fs2.addDirWithMtime("/root", "", 0o755, mtime)
	fs2.addFileWithMtime("/root/a.txt", "/root", []byte("alpha"), 0o644, mtime)
	fs2.addFileWithMtime("/root/b.txt", "/root", []byte("beta"), 0o644, mtime)

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
		t.Fatalf("metadata backup (all unchanged): %v", err)
	}
	if metaResult.FileCount != 2 {
		t.Errorf("metadata file count = %d, want 2", metaResult.FileCount)
	}

	acc, _ := restoreSplitArchive(t, currDir)
	verifyFileContent(t, acc, "a.txt", "alpha")
	verifyFileContent(t, acc, "b.txt", "beta")
}

func TestMetadataModeAllChanged(t *testing.T) {
	mtime := format.StatxTimestamp{Secs: 1700000000}
	newMtime := format.StatxTimestamp{Secs: 1700000001}

	fs1 := newMemFS()
	fs1.addDirWithMtime("/root", "", 0o755, mtime)
	fs1.addFileWithMtime("/root/a.txt", "/root", []byte("old_a"), 0o644, mtime)
	fs1.addFileWithMtime("/root/b.txt", "/root", []byte("old_b"), 0o644, mtime)

	srv1, prevDir := newTestServer(t, fs1)
	_, err := srv1.RunSplitBackup(context.Background(), "/root", BackupConfig{
		BackupType:    datastore.BackupHost,
		BackupID:      "test",
		BackupTime:    1700000000,
		DetectionMode: DetectionData,
	})
	if err != nil {
		t.Fatalf("initial backup: %v", err)
	}

	// All files changed
	fs2 := newMemFS()
	fs2.addDirWithMtime("/root", "", 0o755, mtime)
	fs2.addFileWithMtime("/root/a.txt", "/root", []byte("new_a"), 0o644, newMtime)
	fs2.addFileWithMtime("/root/b.txt", "/root", []byte("new_b"), 0o644, newMtime)

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
		t.Fatalf("metadata backup (all changed): %v", err)
	}
	if metaResult.FileCount != 2 {
		t.Errorf("metadata file count = %d, want 2", metaResult.FileCount)
	}

	acc, _ := restoreSplitArchive(t, currDir)
	verifyFileContent(t, acc, "a.txt", "new_a")
	verifyFileContent(t, acc, "b.txt", "new_b")
}

func TestMetadataModeNewFileAdded(t *testing.T) {
	mtime := format.StatxTimestamp{Secs: 1700000000}

	fs1 := newMemFS()
	fs1.addDirWithMtime("/root", "", 0o755, mtime)
	fs1.addFileWithMtime("/root/existing.txt", "/root", []byte("old"), 0o644, mtime)

	srv1, prevDir := newTestServer(t, fs1)
	_, err := srv1.RunSplitBackup(context.Background(), "/root", BackupConfig{
		BackupType:    datastore.BackupHost,
		BackupID:      "test",
		BackupTime:    1700000000,
		DetectionMode: DetectionData,
	})
	if err != nil {
		t.Fatalf("initial backup: %v", err)
	}

	// New file added
	fs2 := newMemFS()
	fs2.addDirWithMtime("/root", "", 0o755, mtime)
	fs2.addFileWithMtime("/root/existing.txt", "/root", []byte("old"), 0o644, mtime)
	fs2.addFileWithMtime("/root/new.txt", "/root", []byte("new file"), 0o644, mtime)

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
		t.Fatalf("metadata backup (new file): %v", err)
	}
	if metaResult.FileCount != 2 {
		t.Errorf("metadata file count = %d, want 2", metaResult.FileCount)
	}

	acc, _ := restoreSplitArchive(t, currDir)
	verifyFileContent(t, acc, "existing.txt", "old")
	verifyFileContent(t, acc, "new.txt", "new file")
}

func TestMetadataModeFileSizeChange(t *testing.T) {
	mtime := format.StatxTimestamp{Secs: 1700000000}

	// Same mtime but different size should be detected as changed
	fs1 := newMemFS()
	fs1.addDirWithMtime("/root", "", 0o755, mtime)
	fs1.addFileWithMtime("/root/file.txt", "/root", []byte("short"), 0o644, mtime)

	srv1, prevDir := newTestServer(t, fs1)
	_, err := srv1.RunSplitBackup(context.Background(), "/root", BackupConfig{
		BackupType:    datastore.BackupHost,
		BackupID:      "test",
		BackupTime:    1700000000,
		DetectionMode: DetectionData,
	})
	if err != nil {
		t.Fatalf("initial backup: %v", err)
	}

	// Same mtime but different size (content grew)
	fs2 := newMemFS()
	fs2.addDirWithMtime("/root", "", 0o755, mtime)
	fs2.addFileWithMtime("/root/file.txt", "/root", []byte("much longer content now"), 0o644, mtime)

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
		t.Fatalf("metadata backup (size change): %v", err)
	}
	if metaResult.FileCount != 1 {
		t.Errorf("metadata file count = %d, want 1", metaResult.FileCount)
	}

	acc, _ := restoreSplitArchive(t, currDir)
	verifyFileContent(t, acc, "file.txt", "much longer content now")
}

func TestMetadataModeNestedChanges(t *testing.T) {
	mtime := format.StatxTimestamp{Secs: 1700000000}
	newMtime := format.StatxTimestamp{Secs: 1700000001}

	fs1 := newMemFS()
	fs1.addDirWithMtime("/root", "", 0o755, mtime)
	fs1.addDirWithMtime("/root/subdir", "/root", 0o755, mtime)
	fs1.addFileWithMtime("/root/top.txt", "/root", []byte("top unchanged"), 0o644, mtime)
	fs1.addFileWithMtime("/root/subdir/unchanged.txt", "/root/subdir", []byte("nested unchanged"), 0o644, mtime)
	fs1.addFileWithMtime("/root/subdir/changed.txt", "/root/subdir", []byte("nested old"), 0o644, mtime)

	srv1, prevDir := newTestServer(t, fs1)
	_, err := srv1.RunSplitBackup(context.Background(), "/root", BackupConfig{
		BackupType:    datastore.BackupHost,
		BackupID:      "test",
		BackupTime:    1700000000,
		DetectionMode: DetectionData,
	})
	if err != nil {
		t.Fatalf("initial backup: %v", err)
	}

	// Top-level unchanged, nested: one changed, one unchanged
	fs2 := newMemFS()
	fs2.addDirWithMtime("/root", "", 0o755, mtime)
	fs2.addDirWithMtime("/root/subdir", "/root", 0o755, mtime)
	fs2.addFileWithMtime("/root/top.txt", "/root", []byte("top unchanged"), 0o644, mtime)
	fs2.addFileWithMtime("/root/subdir/unchanged.txt", "/root/subdir", []byte("nested unchanged"), 0o644, mtime)
	fs2.addFileWithMtime("/root/subdir/changed.txt", "/root/subdir", []byte("nested new content"), 0o644, newMtime)

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
		t.Fatalf("metadata backup (nested changes): %v", err)
	}
	if metaResult.FileCount != 3 {
		t.Errorf("metadata file count = %d, want 3", metaResult.FileCount)
	}

	acc, _ := restoreSplitArchive(t, currDir)
	verifyFileContent(t, acc, "top.txt", "top unchanged")
	verifyFileContent(t, acc, "subdir/unchanged.txt", "nested unchanged")
	verifyFileContent(t, acc, "subdir/changed.txt", "nested new content")
}

func TestMetadataModeEmptyFileUnchanged(t *testing.T) {
	mtime := format.StatxTimestamp{Secs: 1700000000}

	fs1 := newMemFS()
	fs1.addDirWithMtime("/root", "", 0o755, mtime)
	fs1.addFileWithMtime("/root/empty.txt", "/root", []byte{}, 0o644, mtime)
	fs1.addFileWithMtime("/root/data.txt", "/root", []byte("some data"), 0o644, mtime)

	srv1, prevDir := newTestServer(t, fs1)
	_, err := srv1.RunSplitBackup(context.Background(), "/root", BackupConfig{
		BackupType:    datastore.BackupHost,
		BackupID:      "test",
		BackupTime:    1700000000,
		DetectionMode: DetectionData,
	})
	if err != nil {
		t.Fatalf("initial backup: %v", err)
	}

	// Empty file unchanged, data file changed
	newMtime := format.StatxTimestamp{Secs: 1700000001}
	fs2 := newMemFS()
	fs2.addDirWithMtime("/root", "", 0o755, mtime)
	fs2.addFileWithMtime("/root/empty.txt", "/root", []byte{}, 0o644, mtime)
	fs2.addFileWithMtime("/root/data.txt", "/root", []byte("new data"), 0o644, newMtime)

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
		t.Fatalf("metadata backup (empty file unchanged): %v", err)
	}
	if metaResult.FileCount != 2 {
		t.Errorf("metadata file count = %d, want 2", metaResult.FileCount)
	}

	acc, _ := restoreSplitArchive(t, currDir)
	verifyFileContent(t, acc, "empty.txt", "")
	verifyFileContent(t, acc, "data.txt", "new data")
}

func TestMetadataModePermissionChange(t *testing.T) {
	mtime := format.StatxTimestamp{Secs: 1700000000}

	fs1 := newMemFS()
	fs1.addDirWithMtime("/root", "", 0o755, mtime)
	fs1.addFileWithMtime("/root/file.txt", "/root", []byte("same content"), 0o644, mtime)

	srv1, prevDir := newTestServer(t, fs1)
	_, err := srv1.RunSplitBackup(context.Background(), "/root", BackupConfig{
		BackupType:    datastore.BackupHost,
		BackupID:      "test",
		BackupTime:    1700000000,
		DetectionMode: DetectionData,
	})
	if err != nil {
		t.Fatalf("initial backup: %v", err)
	}

	// Same content, same mtime, same size, but different mode (permissions)
	// This should be detected as changed because MetadataEqual checks mode
	fs2 := newMemFS()
	fs2.addDirWithMtime("/root", "", 0o755, mtime)
	fs2.addFileWithMtime("/root/file.txt", "/root", []byte("same content"), 0o755, mtime) // mode changed

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
		t.Fatalf("metadata backup (permission change): %v", err)
	}
	if metaResult.FileCount != 1 {
		t.Errorf("metadata file count = %d, want 1", metaResult.FileCount)
	}

	acc, _ := restoreSplitArchive(t, currDir)
	verifyFileContent(t, acc, "file.txt", "same content")
}

func TestMetadataModeSymlinkUnchanged(t *testing.T) {
	mtime := format.StatxTimestamp{Secs: 1700000000}

	fs1 := newMemFS()
	fs1.addDirWithMtime("/root", "", 0o755, mtime)
	fs1.addFileWithMtime("/root/real.txt", "/root", []byte("real"), 0o644, mtime)

	srv1, prevDir := newTestServer(t, fs1)
	_, err := srv1.RunSplitBackup(context.Background(), "/root", BackupConfig{
		BackupType:    datastore.BackupHost,
		BackupID:      "test",
		BackupTime:    1700000000,
		DetectionMode: DetectionData,
	})
	if err != nil {
		t.Fatalf("initial backup: %v", err)
	}

	// Add a symlink (new entry not in previous catalog)
	fs2 := newMemFS()
	fs2.addDirWithMtime("/root", "", 0o755, mtime)
	fs2.addFileWithMtime("/root/real.txt", "/root", []byte("real"), 0o644, mtime)
	fs2.addSymlink("/root/link", "/root", "real.txt", 0o777)

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
		t.Fatalf("metadata backup (symlink added): %v", err)
	}

	acc, _ := restoreSplitArchive(t, currDir)
	verifyFileContent(t, acc, "real.txt", "real")
	verifyIsSymlink(t, acc, "link", "real.txt")

	_ = metaResult
}

func TestRunBackupWithModeDataDispatch(t *testing.T) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	fs.addFile("/root/file.txt", "/root", []byte("data mode"), 0o644)

	srv, dir := newTestServer(t, fs)
	result, err := srv.RunBackupWithMode(context.Background(), "/root", BackupConfig{
		BackupType:    datastore.BackupHost,
		BackupID:      "test",
		BackupTime:    1700000000,
		DetectionMode: DetectionData,
	})
	if err != nil {
		t.Fatalf("RunBackupWithMode data: %v", err)
	}
	if result.FileCount != 1 {
		t.Errorf("file count = %d, want 1", result.FileCount)
	}

	acc, _ := restoreSplitArchive(t, dir)
	verifyFileContent(t, acc, "file.txt", "data mode")
}

func TestRunBackupWithModeMetadataDispatch(t *testing.T) {
	mtime := format.StatxTimestamp{Secs: 1700000000}

	fs1 := newMemFS()
	fs1.addDirWithMtime("/root", "", 0o755, mtime)
	fs1.addFileWithMtime("/root/file.txt", "/root", []byte("original"), 0o644, mtime)

	srv1, prevDir := newTestServer(t, fs1)
	_, err := srv1.RunSplitBackup(context.Background(), "/root", BackupConfig{
		BackupType:    datastore.BackupHost,
		BackupID:      "test",
		BackupTime:    1700000000,
		DetectionMode: DetectionData,
	})
	if err != nil {
		t.Fatalf("initial data backup: %v", err)
	}

	newMtime := format.StatxTimestamp{Secs: 1700000001}
	fs2 := newMemFS()
	fs2.addDirWithMtime("/root", "", 0o755, mtime)
	fs2.addFileWithMtime("/root/file.txt", "/root", []byte("updated"), 0o644, newMtime)

	srv2, currDir := newTestServer(t, fs2)
	result, err := srv2.RunBackupWithMode(context.Background(), "/root", BackupConfig{
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
		t.Fatalf("RunBackupWithMode metadata: %v", err)
	}
	if result.FileCount != 1 {
		t.Errorf("file count = %d, want 1", result.FileCount)
	}

	acc, _ := restoreSplitArchive(t, currDir)
	verifyFileContent(t, acc, "file.txt", "updated")
}
