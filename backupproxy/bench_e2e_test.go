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

// benchFS generates a memFS with the given number of files, each of the
// specified size, spread across nested directories (3 levels deep, 5 dirs per
// level).
func benchFS(fileCount int, fileSize int) memFS {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)

	data := make([]byte, fileSize)
	rand.Read(data)

	for i := 0; i < fileCount; i++ {
		dirPath := fmt.Sprintf("/root/d%d", i%5)
		subPath := fmt.Sprintf("/root/d%d/sub%d", i%5, (i/5)%5)
		deepPath := fmt.Sprintf("/root/d%d/sub%d/dd%d", i%5, (i/5)%5, (i/25)%5)

		switch {
		case i < fileCount/3:
			fs.addDir(dirPath, "/root", 0o755)
			fs.addFile(fmt.Sprintf("/root/d%d/file%d.txt", i%5, i), dirPath, data, 0o644)
		case i < 2*fileCount/3:
			fs.addDir(dirPath, "/root", 0o755)
			fs.addDir(subPath, dirPath, 0o755)
			fs.addFile(fmt.Sprintf("/root/d%d/sub%d/file%d.txt", i%5, (i/5)%5, i), subPath, data, 0o644)
		default:
			fs.addDir(dirPath, "/root", 0o755)
			fs.addDir(subPath, dirPath, 0o755)
			fs.addDir(deepPath, subPath, 0o755)
			fs.addFile(fmt.Sprintf("/root/d%d/sub%d/dd%d/file%d.txt", i%5, (i/5)%5, (i/25)%5, i), deepPath, data, 0o644)
		}
	}

	return fs
}

// benchmarkLocalStore runs a backup benchmark with LocalStore (no network).
// It returns the result for validation.
func benchmarkLocalStore(b *testing.B, fs memFS, mode DetectionMode) {
	b.Helper()

	cfg, _ := buzhash.NewConfig(4096)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		store, err := NewLocalStore(dir, cfg, false)
		if err != nil {
			b.Fatal(err)
		}
		client := fs.provider()
		srv := NewServer(client, store)

		backupCfg := BackupConfig{
			BackupType:    datastore.BackupHost,
			BackupID:      fmt.Sprintf("bench-%d", i),
			BackupTime:    int64(1700000000 + i),
			DetectionMode: mode,
		}

		b.StartTimer()

		result, err := srv.RunBackupWithMode(context.Background(), "/root", backupCfg)
		if err != nil {
			b.Fatal(err)
		}

		_ = result
	}
}

// --- Legacy mode benchmarks ---

func BenchmarkLegacyEmptyDir(b *testing.B) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	benchmarkLocalStore(b, fs, DetectionLegacy)
}

func BenchmarkLegacySingleFile1KB(b *testing.B) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	data := make([]byte, 1024)
	rand.Read(data)
	fs.addFile("/root/file.txt", "/root", data, 0o644)
	benchmarkLocalStore(b, fs, DetectionLegacy)
}

func BenchmarkLegacySingleFile1MB(b *testing.B) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	data := make([]byte, 1<<20)
	rand.Read(data)
	fs.addFile("/root/file.txt", "/root", data, 0o644)
	benchmarkLocalStore(b, fs, DetectionLegacy)
}

func BenchmarkLegacy100Files4KB(b *testing.B) {
	fs := benchFS(100, 4096)
	benchmarkLocalStore(b, fs, DetectionLegacy)
}

func BenchmarkLegacy1000Files4KB(b *testing.B) {
	fs := benchFS(1000, 4096)
	benchmarkLocalStore(b, fs, DetectionLegacy)
}

func BenchmarkLegacy100Files16KB(b *testing.B) {
	fs := benchFS(100, 16<<10)
	benchmarkLocalStore(b, fs, DetectionLegacy)
}

// --- Data mode benchmarks ---

func BenchmarkDataEmptyDir(b *testing.B) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	benchmarkLocalStore(b, fs, DetectionData)
}

func BenchmarkDataSingleFile1KB(b *testing.B) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	data := make([]byte, 1024)
	rand.Read(data)
	fs.addFile("/root/file.txt", "/root", data, 0o644)
	benchmarkLocalStore(b, fs, DetectionData)
}

func BenchmarkDataSingleFile1MB(b *testing.B) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	data := make([]byte, 1<<20)
	rand.Read(data)
	fs.addFile("/root/file.txt", "/root", data, 0o644)
	benchmarkLocalStore(b, fs, DetectionData)
}

func BenchmarkData100Files4KB(b *testing.B) {
	fs := benchFS(100, 4096)
	benchmarkLocalStore(b, fs, DetectionData)
}

func BenchmarkData1000Files4KB(b *testing.B) {
	fs := benchFS(1000, 4096)
	benchmarkLocalStore(b, fs, DetectionData)
}

func BenchmarkData100Files16KB(b *testing.B) {
	fs := benchFS(100, 16<<10)
	benchmarkLocalStore(b, fs, DetectionData)
}

// --- Metadata mode benchmarks ---

func BenchmarkMetadataAllUnchanged(b *testing.B) {
	mtime := format.StatxTimestamp{Secs: 1700000000}
	cfg, _ := buzhash.NewConfig(4096)

	data := make([]byte, 4096)
	rand.Read(data)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Phase 1: initial data backup
		prevDir := b.TempDir()
		prevStore, err := NewLocalStore(prevDir, cfg, false)
		if err != nil {
			b.Fatal(err)
		}

		fs1 := newMemFS()
		fs1.addDirWithMtime("/root", "", 0o755, mtime)
		fs1.addFileWithMtime("/root/a.txt", "/root", data, 0o644, mtime)
		fs1.addFileWithMtime("/root/b.txt", "/root", data, 0o644, mtime)

		srv1 := NewServer(fs1.provider(), prevStore)
		_, err = srv1.RunSplitBackup(context.Background(), "/root", BackupConfig{
			BackupType:    datastore.BackupHost,
			BackupID:      fmt.Sprintf("bench-%d", i),
			BackupTime:    1700000000,
			DetectionMode: DetectionData,
		})
		if err != nil {
			b.Fatal(err)
		}

		// Phase 2: metadata backup (all unchanged)
		currDir := b.TempDir()
		currStore, err := NewLocalStore(currDir, cfg, false)
		if err != nil {
			b.Fatal(err)
		}

		fs2 := newMemFS()
		fs2.addDirWithMtime("/root", "", 0o755, mtime)
		fs2.addFileWithMtime("/root/a.txt", "/root", data, 0o644, mtime)
		fs2.addFileWithMtime("/root/b.txt", "/root", data, 0o644, mtime)

		b.StartTimer()

		srv2 := NewServer(fs2.provider(), currStore)
		_, err = srv2.RunMetadataBackup(context.Background(), "/root", BackupConfig{
			BackupType:    datastore.BackupHost,
			BackupID:      fmt.Sprintf("bench-%d", i),
			BackupTime:    1700000001,
			DetectionMode: DetectionMetadata,
			PreviousBackup: &PreviousBackupRef{
				BackupType: datastore.BackupHost,
				BackupID:   fmt.Sprintf("bench-%d", i),
				BackupTime: 1700000000,
				Dir:        prevDir,
			},
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMetadataAllChanged(b *testing.B) {
	mtime := format.StatxTimestamp{Secs: 1700000000}
	newMtime := format.StatxTimestamp{Secs: 1700000001}
	cfg, _ := buzhash.NewConfig(4096)

	data1 := make([]byte, 4096)
	rand.Read(data1)
	data2 := make([]byte, 4096)
	rand.Read(data2)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		prevDir := b.TempDir()
		prevStore, err := NewLocalStore(prevDir, cfg, false)
		if err != nil {
			b.Fatal(err)
		}

		fs1 := newMemFS()
		fs1.addDirWithMtime("/root", "", 0o755, mtime)
		fs1.addFileWithMtime("/root/a.txt", "/root", data1, 0o644, mtime)
		fs1.addFileWithMtime("/root/b.txt", "/root", data1, 0o644, mtime)

		srv1 := NewServer(fs1.provider(), prevStore)
		_, err = srv1.RunSplitBackup(context.Background(), "/root", BackupConfig{
			BackupType:    datastore.BackupHost,
			BackupID:      fmt.Sprintf("bench-%d", i),
			BackupTime:    1700000000,
			DetectionMode: DetectionData,
		})
		if err != nil {
			b.Fatal(err)
		}

		currDir := b.TempDir()
		currStore, err := NewLocalStore(currDir, cfg, false)
		if err != nil {
			b.Fatal(err)
		}

		fs2 := newMemFS()
		fs2.addDirWithMtime("/root", "", 0o755, mtime)
		fs2.addFileWithMtime("/root/a.txt", "/root", data2, 0o644, newMtime)
		fs2.addFileWithMtime("/root/b.txt", "/root", data2, 0o644, newMtime)

		b.StartTimer()

		srv2 := NewServer(fs2.provider(), currStore)
		_, err = srv2.RunMetadataBackup(context.Background(), "/root", BackupConfig{
			BackupType:    datastore.BackupHost,
			BackupID:      fmt.Sprintf("bench-%d", i),
			BackupTime:    1700000001,
			DetectionMode: DetectionMetadata,
			PreviousBackup: &PreviousBackupRef{
				BackupType: datastore.BackupHost,
				BackupID:   fmt.Sprintf("bench-%d", i),
				BackupTime: 1700000000,
				Dir:        prevDir,
			},
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMetadataMixed(b *testing.B) {
	mtime := format.StatxTimestamp{Secs: 1700000000}
	newMtime := format.StatxTimestamp{Secs: 1700000001}
	cfg, _ := buzhash.NewConfig(4096)

	unchanged := make([]byte, 4096)
	rand.Read(unchanged)
	changed := make([]byte, 4096)
	rand.Read(changed)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		prevDir := b.TempDir()
		prevStore, err := NewLocalStore(prevDir, cfg, false)
		if err != nil {
			b.Fatal(err)
		}

		fs1 := newMemFS()
		fs1.addDirWithMtime("/root", "", 0o755, mtime)
		fs1.addFileWithMtime("/root/unchanged.txt", "/root", unchanged, 0o644, mtime)
		fs1.addFileWithMtime("/root/changed.txt", "/root", unchanged, 0o644, mtime)

		srv1 := NewServer(fs1.provider(), prevStore)
		_, err = srv1.RunSplitBackup(context.Background(), "/root", BackupConfig{
			BackupType:    datastore.BackupHost,
			BackupID:      fmt.Sprintf("bench-%d", i),
			BackupTime:    1700000000,
			DetectionMode: DetectionData,
		})
		if err != nil {
			b.Fatal(err)
		}

		currDir := b.TempDir()
		currStore, err := NewLocalStore(currDir, cfg, false)
		if err != nil {
			b.Fatal(err)
		}

		fs2 := newMemFS()
		fs2.addDirWithMtime("/root", "", 0o755, mtime)
		fs2.addFileWithMtime("/root/unchanged.txt", "/root", unchanged, 0o644, mtime)
		fs2.addFileWithMtime("/root/changed.txt", "/root", changed, 0o644, newMtime)

		b.StartTimer()

		srv2 := NewServer(fs2.provider(), currStore)
		_, err = srv2.RunMetadataBackup(context.Background(), "/root", BackupConfig{
			BackupType:    datastore.BackupHost,
			BackupID:      fmt.Sprintf("bench-%d", i),
			BackupTime:    1700000001,
			DetectionMode: DetectionMetadata,
			PreviousBackup: &PreviousBackupRef{
				BackupType: datastore.BackupHost,
				BackupID:   fmt.Sprintf("bench-%d", i),
				BackupTime: 1700000000,
				Dir:        prevDir,
			},
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

// --- Round-trip verification benchmarks ---

func BenchmarkLegacyRoundTrip(b *testing.B) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	for i := 0; i < 50; i++ {
		data := make([]byte, 8192)
		rand.Read(data)
		fs.addFile(fmt.Sprintf("/root/file%d.txt", i), "/root", data, 0o644)
	}

	cfg, _ := buzhash.NewConfig(4096)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		store, _ := NewLocalStore(dir, cfg, false)
		client := fs.provider()
		srv := NewServer(client, store)

		b.StartTimer()

		result, err := srv.RunBackup(context.Background(), "/root", BackupConfig{
			BackupType: datastore.BackupHost,
			BackupID:   fmt.Sprintf("bench-%d", i),
			BackupTime: int64(1700000000 + i),
		})
		if err != nil {
			b.Fatal(err)
		}

		b.StopTimer()

		// Verify round-trip
		acc := restoreLegacyArchiveFromDir(b, dir)
		for j := 0; j < 50; j++ {
			entry, err := acc.Lookup(fmt.Sprintf("file%d.txt", j))
			if err != nil {
				b.Fatalf("lookup file%d.txt: %v", j, err)
			}
			if !entry.IsRegularFile() {
				b.Fatalf("file%d.txt should be a regular file", j)
			}
		}
		_ = result
		b.StartTimer()
	}
}

func BenchmarkDataRoundTrip(b *testing.B) {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	for i := 0; i < 50; i++ {
		data := make([]byte, 8192)
		rand.Read(data)
		fs.addFile(fmt.Sprintf("/root/file%d.txt", i), "/root", data, 0o644)
	}

	cfg, _ := buzhash.NewConfig(4096)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		store, _ := NewLocalStore(dir, cfg, false)
		client := fs.provider()
		srv := NewServer(client, store)

		b.StartTimer()

		result, err := srv.RunSplitBackup(context.Background(), "/root", BackupConfig{
			BackupType:    datastore.BackupHost,
			BackupID:      fmt.Sprintf("bench-%d", i),
			BackupTime:    int64(1700000000 + i),
			DetectionMode: DetectionData,
		})
		if err != nil {
			b.Fatal(err)
		}

		b.StopTimer()

		metaAcc, _ := restoreSplitArchiveFromDir(b, dir)
		for j := 0; j < 50; j++ {
			entry, err := metaAcc.Lookup(fmt.Sprintf("file%d.txt", j))
			if err != nil {
				b.Fatalf("lookup file%d.txt: %v", j, err)
			}
			if !entry.IsRegularFile() {
				b.Fatalf("file%d.txt should be a regular file", j)
			}
		}
		_ = result
		b.StartTimer()
	}
}

// restoreLegacyArchiveFromDir reconstructs a legacy pxar archive from stored chunks
// in the given LocalStore directory and returns an accessor for verification.
func restoreLegacyArchiveFromDir(tb testing.TB, dir string) *accessor.Accessor {
	tb.Helper()
	raw, err := os.ReadFile(dir + "/root.pxar.didx")
	if err != nil {
		tb.Fatalf("read legacy index: %v", err)
	}
	idx, err := datastore.ReadDynamicIndex(raw)
	if err != nil {
		tb.Fatalf("parse legacy index: %v", err)
	}
	chunkStore, err := datastore.NewChunkStore(dir)
	if err != nil {
		tb.Fatalf("open chunk store: %v", err)
	}
	var archive bytes.Buffer
	for i := 0; i < idx.Count(); i++ {
		info, _ := idx.ChunkInfo(i)
		chunk, err := chunkStore.LoadChunk(info.Digest)
		if err != nil {
			tb.Fatalf("load chunk %d: %v", i, err)
		}
		decoded, err := datastore.DecodeBlob(chunk)
		if err != nil {
			tb.Fatalf("decode chunk %d: %v", i, err)
		}
		archive.Write(decoded)
	}
	return accessor.NewAccessor(bytes.NewReader(archive.Bytes()))
}

// restoreSplitArchiveFromDir reconstructs a split pxar archive from stored chunks
// in the given LocalStore directory and returns accessors for metadata and payload.
func restoreSplitArchiveFromDir(tb testing.TB, dir string) (*accessor.Accessor, *accessor.Accessor) {
	tb.Helper()
	metaRaw, err := os.ReadFile(dir + "/root.mpxar.didx")
	if err != nil {
		tb.Fatalf("read metadata index: %v", err)
	}
	payloadRaw, err := os.ReadFile(dir + "/root.ppxar.didx")
	if err != nil {
		tb.Fatalf("read payload index: %v", err)
	}
	metaIdx, err := datastore.ReadDynamicIndex(metaRaw)
	if err != nil {
		tb.Fatalf("parse metadata index: %v", err)
	}
	payloadIdx, err := datastore.ReadDynamicIndex(payloadRaw)
	if err != nil {
		tb.Fatalf("parse payload index: %v", err)
	}
	chunkStore, err := datastore.NewChunkStore(dir)
	if err != nil {
		tb.Fatalf("open chunk store: %v", err)
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
				tb.Fatalf("load chunk %d: %v", i, err)
			}
			decoded, err := datastore.DecodeBlob(chunk)
			if err != nil {
				tb.Fatalf("decode chunk %d: %v", i, err)
			}
			dst.Write(decoded)
		}
	}
	acc := accessor.NewAccessor(bytes.NewReader(metaBuf.Bytes()), bytes.NewReader(payloadBuf.Bytes()))
	return acc, nil
}
