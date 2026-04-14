//go:build integration

package backupproxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
)

// --- Benchmark test helpers (testing.TB compatible) ---

func pbsConfigFromBench(tb testing.TB) PBSConfig {
	tb.Helper()
	url := os.Getenv("PBS_URL")
	datastoreName := os.Getenv("PBS_DATASTORE")
	token := os.Getenv("PBS_TOKEN")
	if url == "" || datastoreName == "" || token == "" {
		tb.Skip("PBS_URL, PBS_DATASTORE, PBS_TOKEN env vars required for PBS benchmarks")
	}
	return PBSConfig{
		BaseURL:       url,
		Datastore:     datastoreName,
		AuthToken:     token,
		SkipTLSVerify: true,
	}
}

func newBenchStore(tb testing.TB) *PBSRemoteStore {
	tb.Helper()
	cfg := pbsConfigFromBench(tb)
	chunkCfg, err := buzhash.NewConfig(4096)
	if err != nil {
		tb.Fatalf("create chunk config: %v", err)
	}
	return NewPBSRemoteStore(cfg, chunkCfg, false)
}

func cleanupBenchSnapshot(tb testing.TB, cfg PBSConfig, bc BackupConfig) {
	tb.Helper()
	url := fmt.Sprintf("%s/admin/datastore/%s/snapshots?backup-type=%s&backup-id=%s&backup-time=%d",
		cfg.BaseURL, cfg.Datastore, bc.BackupType.String(), bc.BackupID, bc.BackupTime)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodDelete, url, nil)
	if err != nil {
		tb.Logf("create delete request: %v", err)
		return
	}
	req.Header.Set("Authorization", "PBSAPIToken "+cfg.AuthToken)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig.InsecureSkipVerify = true
	client := &http.Client{Transport: transport}
	resp, err := client.Do(req)
	if err != nil {
		tb.Logf("delete snapshot: %v", err)
		return
	}
	resp.Body.Close()
}

func defaultBenchConfig(tb testing.TB) BackupConfig {
	tb.Helper()
	return BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   "benchmark",
		BackupTime: 1700000000,
		Namespace:  "",
	}
}

// --- Benchmarks ---

// BenchmarkPBSLegacy benchmarks legacy-mode backup against PBS (50 files × 8KB).
func BenchmarkPBSLegacy(b *testing.B) {
	pbsCfg := pbsConfigFromBench(b)
	store := newBenchStore(b)
	fs := benchPBSFS(50, 8192)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		cfg := defaultBenchConfig(b)
		cfg.DetectionMode = DetectionLegacy
		cfg.BackupID = fmt.Sprintf("bench-leg-%d", i)
		cfg.BackupTime = int64(1700000000 + i)
		cleanupBenchSnapshot(b, pbsCfg, cfg)
		srv := NewServer(fs.provider(), store)
		b.StartTimer()

		_, err := srv.RunBackupWithMode(context.Background(), "/root", cfg)
		if err != nil {
			b.Fatalf("RunBackupWithMode legacy: %v", err)
		}
	}
}

// BenchmarkPBSData benchmarks data-mode backup against PBS (50 files × 8KB).
func BenchmarkPBSData(b *testing.B) {
	pbsCfg := pbsConfigFromBench(b)
	store := newBenchStore(b)
	fs := benchPBSFS(50, 8192)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		cfg := defaultBenchConfig(b)
		cfg.DetectionMode = DetectionData
		cfg.BackupID = fmt.Sprintf("bench-data-%d", i)
		cfg.BackupTime = int64(1700000000 + i)
		cleanupBenchSnapshot(b, pbsCfg, cfg)
		srv := NewServer(fs.provider(), store)
		b.StartTimer()

		_, err := srv.RunBackupWithMode(context.Background(), "/root", cfg)
		if err != nil {
			b.Fatalf("RunBackupWithMode data: %v", err)
		}
	}
}

// BenchmarkPBSMetadata benchmarks metadata-mode incremental backup against PBS.
// Measures the full cycle: initial data backup + incremental metadata backup.
func BenchmarkPBSMetadata(b *testing.B) {
	pbsCfg := pbsConfigFromBench(b)
	store := newBenchStore(b)
	mtime := format.StatxTimestamp{Secs: 1700000000}
	newMtime := format.StatxTimestamp{Secs: 1700000001}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Phase 1: initial data backup
		prevCfg := defaultBenchConfig(b)
		prevCfg.DetectionMode = DetectionData
		prevCfg.BackupID = fmt.Sprintf("bench-meta-prev-%d", i)
		prevCfg.BackupTime = int64(1700000000 + i*2)
		cleanupBenchSnapshot(b, pbsCfg, prevCfg)

		fs1 := newMemFSUnchanged(mtime, 25, 8192)
		srv1 := NewServer(fs1.provider(), store)
		_, err := srv1.RunSplitBackup(context.Background(), "/root", prevCfg)
		if err != nil {
			b.Fatalf("initial data backup: %v", err)
		}

		// Phase 2: metadata backup (half changed)
		currCfg := defaultBenchConfig(b)
		currCfg.DetectionMode = DetectionMetadata
		currCfg.BackupID = fmt.Sprintf("bench-meta-curr-%d", i)
		currCfg.BackupTime = int64(1700000000 + i*2 + 1)
		currCfg.PreviousBackup = &PreviousBackupRef{
			BackupType: prevCfg.BackupType,
			BackupID:   prevCfg.BackupID,
			BackupTime: prevCfg.BackupTime,
			Namespace:  prevCfg.Namespace,
		}
		cleanupBenchSnapshot(b, pbsCfg, currCfg)

		fs2 := newMemFSMixed(mtime, newMtime, 25, 8192)
		srv2 := NewServer(fs2.provider(), store)
		b.StartTimer()

		_, err = srv2.RunMetadataBackup(context.Background(), "/root", currCfg)
		if err != nil {
			b.Fatalf("metadata backup: %v", err)
		}
	}
}

// BenchmarkPBSUploadRaw benchmarks raw 1MB upload throughput (no encoding).
func BenchmarkPBSUploadRaw(b *testing.B) {
	pbsCfg := pbsConfigFromBench(b)
	store := newBenchStore(b)
	data := make([]byte, 1<<20)
	rand.Read(data)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cfg := defaultBenchConfig(b)
		cfg.BackupID = fmt.Sprintf("bench-raw-%d", i)
		cfg.BackupTime = int64(1700000000 + i)
		cleanupBenchSnapshot(b, pbsCfg, cfg)

		sess, err := store.StartSession(context.Background(), cfg)
		if err != nil {
			b.Fatalf("StartSession: %v", err)
		}

		_, err = sess.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(data))
		if err != nil {
			b.Fatalf("UploadArchive: %v", err)
		}

		if _, err := sess.Finish(context.Background()); err != nil {
			b.Fatalf("Finish: %v", err)
		}
	}
}

// BenchmarkPBSUploadSplitRaw benchmarks raw split upload throughput (1MB+1MB, no encoding).
func BenchmarkPBSUploadSplitRaw(b *testing.B) {
	pbsCfg := pbsConfigFromBench(b)
	store := newBenchStore(b)
	metaData := make([]byte, 1<<20)
	payloadData := make([]byte, 1<<20)
	rand.Read(metaData)
	rand.Read(payloadData)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cfg := defaultBenchConfig(b)
		cfg.BackupID = fmt.Sprintf("bench-split-%d", i)
		cfg.BackupTime = int64(1700000000 + i)
		cleanupBenchSnapshot(b, pbsCfg, cfg)

		sess, err := store.StartSession(context.Background(), cfg)
		if err != nil {
			b.Fatalf("StartSession: %v", err)
		}

		_, err = sess.UploadSplitArchive(
			context.Background(),
			"root.mpxar.didx", bytes.NewReader(metaData),
			"root.ppxar.didx", bytes.NewReader(payloadData),
		)
		if err != nil {
			b.Fatalf("UploadSplitArchive: %v", err)
		}

		if _, err := sess.Finish(context.Background()); err != nil {
			b.Fatalf("Finish: %v", err)
		}
	}
}

// --- Helper memFS constructors ---

func benchPBSFS(fileCount, fileSize int) memFS {
	fs := newMemFS()
	fs.addDir("/root", "", 0o755)
	data := make([]byte, fileSize)
	rand.Read(data)
	for i := 0; i < fileCount; i++ {
		fs.addFile(fmt.Sprintf("/root/file%d.txt", i), "/root", data, 0o644)
	}
	return fs
}

func newMemFSUnchanged(mtime format.StatxTimestamp, fileCount, fileSize int) memFS {
	fs := newMemFS()
	fs.addDirWithMtime("/root", "", 0o755, mtime)
	data := make([]byte, fileSize)
	rand.Read(data)
	for i := 0; i < fileCount; i++ {
		fs.addFileWithMtime(fmt.Sprintf("/root/file%d.txt", i), "/root", data, 0o644, mtime)
	}
	return fs
}

func newMemFSMixed(mtime, newMtime format.StatxTimestamp, fileCount, fileSize int) memFS {
	fs := newMemFS()
	fs.addDirWithMtime("/root", "", 0o755, mtime)
	for i := 0; i < fileCount; i++ {
		data := make([]byte, fileSize)
		rand.Read(data)
		if i < fileCount/2 {
			fs.addFileWithMtime(fmt.Sprintf("/root/file%d.txt", i), "/root", data, 0o644, mtime)
		} else {
			fs.addFileWithMtime(fmt.Sprintf("/root/file%d.txt", i), "/root", data, 0o644, newMtime)
		}
	}
	return fs
}
