package accessor

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
)

// ---------- Archive builders ----------

func buildDirArchive(b *testing.B, nFiles int) []byte {
	b.Helper()
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, benchDirMeta(0o755), nil)
	content := bytes.Repeat([]byte("x"), 64)
	for i := range nFiles {
		name := fmt.Sprintf("file_%04d.txt", i)
		if _, err := enc.AddFile(benchFileMeta(0o644, 1000, 1000), name, content); err != nil {
			b.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		b.Fatal(err)
	}
	return buf.Bytes()
}

func buildNestedArchive(b *testing.B, nDirs, nFilesPerDir int) []byte {
	b.Helper()
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, benchDirMeta(0o755), nil)
	content := bytes.Repeat([]byte("y"), 128)
	for d := range nDirs {
		dirName := fmt.Sprintf("dir_%03d", d)
		if err := enc.CreateDirectory(dirName, benchDirMeta(0o755)); err != nil {
			b.Fatal(err)
		}
		for f := range nFilesPerDir {
			name := fmt.Sprintf("file_%04d.txt", f)
			if _, err := enc.AddFile(benchFileMeta(0o644, 1000, 1000), name, content); err != nil {
				b.Fatal(err)
			}
		}
		if err := enc.Finish(); err != nil {
			b.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		b.Fatal(err)
	}
	return buf.Bytes()
}

func benchDirMeta(mode uint64) *pxar.Metadata {
	ts := format.NewStatxTimestampFromDuration(1430487000 * time.Second)
	return &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | mode, Mtime: ts}}
}

func benchFileMeta(mode uint64, uid, gid uint32) *pxar.Metadata {
	ts := format.NewStatxTimestampFromDuration(1430487000 * time.Second)
	return &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFREG | mode, UID: uid, GID: gid, Mtime: ts}}
}

// collectOffsets is a test helper that returns the FileOffset of every entry
// in the root directory.
func collectOffsets(b *testing.B, acc *Accessor, root *pxar.Entry) []int64 {
	b.Helper()
	var offsets []int64
	if err := acc.ListDirectory(int64(root.ContentOffset), ListOption{Minimal: true}, func(e *pxar.Entry) error {
		offsets = append(offsets, int64(e.FileOffset))
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	return offsets
}

// ---------- Core accessor benchmarks ----------

func BenchmarkReadRoot(b *testing.B) {
	archive := buildDirArchive(b, 10)
	b.SetBytes(int64(len(archive)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		acc := NewAccessor(bytes.NewReader(archive))
		if _, err := acc.ReadRoot(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkListDirectory100_Minimal(b *testing.B) {
	benchmarkListDirectory(b, 100, true)
}

func BenchmarkListDirectory100_Full(b *testing.B) {
	benchmarkListDirectory(b, 100, false)
}

func BenchmarkListDirectory1000_Minimal(b *testing.B) {
	benchmarkListDirectory(b, 1000, true)
}

func BenchmarkListDirectory1000_Full(b *testing.B) {
	benchmarkListDirectory(b, 1000, false)
}

func benchmarkListDirectory(b *testing.B, n int, minimal bool) {
	archive := buildDirArchive(b, n)
	acc := NewAccessor(bytes.NewReader(archive))
	root, err := acc.ReadRoot()
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		if err := acc.ListDirectory(int64(root.ContentOffset), ListOption{Minimal: minimal}, func(e *pxar.Entry) error {
			count++
			return nil
		}); err != nil {
			b.Fatal(err)
		}
		if count != n {
			b.Fatalf("expected %d entries, got %d", n, count)
		}
	}
}

// Pool warmup: run ListDirectory several times before measuring so the
// goodbyeItemPool is hot.
func BenchmarkListDirectory100_PoolWarm(b *testing.B) {
	archive := buildDirArchive(b, 100)
	acc := NewAccessor(bytes.NewReader(archive))
	root, err := acc.ReadRoot()
	if err != nil {
		b.Fatal(err)
	}
	// Warm up pool
	for range 10 {
		_ = acc.ListDirectory(int64(root.ContentOffset), ListOption{Minimal: true}, func(e *pxar.Entry) error { return nil })
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		if err := acc.ListDirectory(int64(root.ContentOffset), ListOption{Minimal: true}, func(e *pxar.Entry) error {
			count++
			return nil
		}); err != nil {
			b.Fatal(err)
		}
		if count != 100 {
			b.Fatalf("expected 100, got %d", count)
		}
	}
}

// ---------- Lookup benchmarks ----------

func BenchmarkLookup_SingleFile(b *testing.B) {
	archive := buildDirArchive(b, 100)
	acc := NewAccessor(bytes.NewReader(archive))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := acc.Lookup("/file_0050.txt"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLookup_Nested(b *testing.B) {
	archive := buildNestedArchive(b, 10, 50)
	acc := NewAccessor(bytes.NewReader(archive))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := acc.Lookup("/dir_005/file_0025.txt"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLookup_DeepPath(b *testing.B) {
	// Build 5-level deep directory tree: a/b/c/d/e/file.txt
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, benchDirMeta(0o755), nil)
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		if err := enc.CreateDirectory(name, benchDirMeta(0o755)); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := enc.AddFile(benchFileMeta(0o644, 0, 0), "file.txt", []byte("deep")); err != nil {
		b.Fatal(err)
	}
	for range []int{0, 1, 2, 3, 4} {
		if err := enc.Finish(); err != nil {
			b.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		b.Fatal(err)
	}

	acc := NewAccessor(bytes.NewReader(buf.Bytes()))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := acc.Lookup("/a/b/c/d/e/file.txt"); err != nil {
			b.Fatal(err)
		}
	}
}

// ---------- ReadEntryAt benchmarks ----------

func BenchmarkReadEntryAtMinimal(b *testing.B) {
	archive := buildDirArchive(b, 100)
	acc := NewAccessor(bytes.NewReader(archive))
	root, err := acc.ReadRoot()
	if err != nil {
		b.Fatal(err)
	}
	offsets := collectOffsets(b, acc, root)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := acc.ReadEntryAtMinimal(offsets[i%len(offsets)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadEntryAtFull(b *testing.B) {
	archive := buildDirArchive(b, 100)
	acc := NewAccessor(bytes.NewReader(archive))
	root, err := acc.ReadRoot()
	if err != nil {
		b.Fatal(err)
	}
	offsets := collectOffsets(b, acc, root)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := acc.ReadEntryAt(offsets[i%len(offsets)]); err != nil {
			b.Fatal(err)
		}
	}
}

// ---------- File content read benchmarks ----------

func BenchmarkReadFileContent_4K(b *testing.B) {
	benchmarkReadFileContent(b, 4096)
}

func BenchmarkReadFileContent_64K(b *testing.B) {
	benchmarkReadFileContent(b, 64*1024)
}

func BenchmarkReadFileContent_1M(b *testing.B) {
	benchmarkReadFileContent(b, 1024*1024)
}

func benchmarkReadFileContent(b *testing.B, size int) {
	content := bytes.Repeat([]byte("A"), size)
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, benchDirMeta(0o755), nil)
	if _, err := enc.AddFile(benchFileMeta(0o644, 0, 0), "data.bin", content); err != nil {
		b.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		b.Fatal(err)
	}

	acc := NewAccessor(bytes.NewReader(buf.Bytes()))
	root, _ := acc.ReadRoot()
	var entry *pxar.Entry
	_ = acc.ListDirectory(int64(root.ContentOffset), ListOption{Minimal: true}, func(e *pxar.Entry) error {
		entry = e
		return fmt.Errorf("stop") //nolint:err113 // sentinel
	})

	b.SetBytes(int64(size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rc, err := acc.ReadFileContentReader(entry)
		if err != nil {
			b.Fatal(err)
		}
		_, _ = io.ReadAll(rc)
		rc.Close() //nolint:errcheck
	}
}

// ---------- FUSE-like scenario benchmarks ----------

// BenchmarkFUSEReadDir simulates the FUSE mount readDir pattern from pxarmount:
// ListDirectory with minimal decode → extract slim entry fields.
func BenchmarkFUSEReadDir(b *testing.B) {
	archive := buildNestedArchive(b, 20, 50) // 20 dirs + 50 files = 70 root entries
	acc := NewAccessor(bytes.NewReader(archive))
	root, err := acc.ReadRoot()
	if err != nil {
		b.Fatal(err)
	}

	type slimEntry struct {
		name          string
		mode          uint64
		entryStart    uint64
		contentOffset uint64
		fileSize      uint64
		isDir         bool
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var entries []slimEntry
		if err := acc.ListDirectory(int64(root.ContentOffset), ListOption{Minimal: true}, func(e *pxar.Entry) error {
			entries = append(entries, slimEntry{
				name:          e.FileName(),
				mode:          e.Metadata.Stat.Mode,
				entryStart:    e.FileOffset,
				contentOffset: e.ContentOffset,
				fileSize:      e.FileSize,
				isDir:         e.IsDir(),
			})
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFUSEGetXAttr simulates the FUSE GetXAttr pattern:
// ReadEntryAt (full) → iterate xattrs.
func BenchmarkFUSEGetXAttr(b *testing.B) {
	// Build archive with xattrs
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, benchDirMeta(0o755), nil)
	meta := benchFileMeta(0o644, 0, 0)
	meta.XAttrs = []format.XAttr{
		format.NewXAttr([]byte("user.comment"), []byte("benchmark test value")),
		format.NewXAttr([]byte("security.label"), []byte("system_u:object_r:benchmark_t")),
	}
	if _, err := enc.AddFile(meta, "attr_file.txt", []byte("data")); err != nil {
		b.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		b.Fatal(err)
	}

	acc := NewAccessor(bytes.NewReader(buf.Bytes()))
	root, _ := acc.ReadRoot()
	var offset int64
	_ = acc.ListDirectory(int64(root.ContentOffset), ListOption{Minimal: true}, func(e *pxar.Entry) error {
		offset = int64(e.FileOffset)
		return fmt.Errorf("stop") //nolint:err113
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry, err := acc.ReadEntryAt(offset)
		if err != nil {
			b.Fatal(err)
		}
		for _, xa := range entry.Metadata.XAttrs {
			_ = xa.Name()
			_ = xa.Value()
		}
	}
}

// ---------- Format micro-benchmarks ----------

func BenchmarkHashFilename(b *testing.B) {
	name := []byte("this_is_a_typical_filename.txt")
	b.SetBytes(int64(len(name)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = format.HashFilename(name)
	}
}

func BenchmarkGoodbyeTableRead(b *testing.B) {
	// Measure just the goodbye table read (header + items), no entry decoding
	archive := buildDirArchive(b, 100)
	acc := NewAccessor(bytes.NewReader(archive))
	root, err := acc.ReadRoot()
	if err != nil {
		b.Fatal(err)
	}
	// Find goodbye offset by doing one ListDirectory
	_ = acc.ListDirectory(int64(root.ContentOffset), ListOption{Minimal: true}, func(e *pxar.Entry) error {
		return nil
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// The goodbye table is at the end of the directory content
		// We just do a full ListDirectory to measure the combined overhead
		_ = acc.ListDirectory(int64(root.ContentOffset), ListOption{Minimal: true}, func(e *pxar.Entry) error {
			return nil
		})
	}
}

// ---------- Allocation counting ----------

func BenchmarkListDirectory100_Allocs(b *testing.B) {
	archive := buildDirArchive(b, 100)
	acc := NewAccessor(bytes.NewReader(archive))
	root, err := acc.ReadRoot()
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := acc.ListDirectory(int64(root.ContentOffset), ListOption{Minimal: true}, func(e *pxar.Entry) error {
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkListDirectory100K_Minimal(b *testing.B) {
	archive := buildDirArchive(b, 100000)
	r := bytes.NewReader(archive)
	acc := NewAccessor(r)

	root, err := acc.ReadRoot()
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		err := acc.ListDirectory(int64(root.ContentOffset), ListOption{Minimal: true}, func(e *pxar.Entry) error {
			count++
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		if count != 100000 {
			b.Fatalf("expected 100000, got %d", count)
		}
	}
}

func BenchmarkListDirectory100K_Allocs(b *testing.B) {
	archive := buildDirArchive(b, 100000)
	r := bytes.NewReader(archive)
	acc := NewAccessor(r)

	root, err := acc.ReadRoot()
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		acc.ListDirectory(int64(root.ContentOffset), ListOption{Minimal: true}, func(e *pxar.Entry) error {
			count++
			return nil
		})
	}
}
