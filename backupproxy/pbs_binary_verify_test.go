//go:build integration

package backupproxy

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/pbs-plus/pxar/accessor"
	"github.com/pbs-plus/pxar/datastore"
)

func genBytes(n int, seed uint64) []byte {
	out := make([]byte, n)
	x := seed
	for i := range out {
		x = x*6364136223846793005 + 1442695040888963407
		out[i] = byte(x >> 33)
	}
	return out
}

func buildBinaryTree(t *testing.T) (memFS, map[string][]byte) {
	t.Helper()
	fs := newMemFS()
	expected := make(map[string][]byte)

	fs.addDir("/root", "", 0o755)
	add := func(path, dir string, data []byte) {
		fs.addFile(path, dir, data, 0o644)
		expected[path[len("/root/"):]] = data
	}

	add("/root/big.bin", "/root", genBytes(256*1024, 1))
	add("/root/small.bin", "/root", genBytes(100, 2))
	add("/root/empty.bin", "/root", nil)
	add("/root/zeros.bin", "/root", make([]byte, 64*1024))

	fs.addDir("/root/sub", "/root", 0o755)
	add("/root/sub/deep.bin", "/root/sub", genBytes(64*1024, 3))
	fs.addDir("/root/sub/nested", "/root/sub", 0o755)
	add("/root/sub/nested/deeper.bin", "/root/sub/nested", genBytes(128*1024, 4))

	return fs, expected
}

func assertArchivesBinaryEqual(t *testing.T, acc *accessor.Accessor, expected map[string][]byte) {
	t.Helper()
	for path, want := range expected {
		entry, err := acc.Lookup(path)
		if err != nil {
			t.Errorf("lookup %q: %v", path, err)
			continue
		}
		rc, err := acc.ReadFileContentReader(entry)
		if err != nil {
			t.Errorf("open content %q: %v", path, err)
			continue
		}
		got, readErr := io.ReadAll(rc)
		rc.Close()
		if readErr != nil {
			t.Errorf("read %q: %v", path, readErr)
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("binary mismatch for %q: got %d bytes, want %d (first diff at byte %d)",
				path, len(got), len(want), firstDiff(got, want))
		}
	}
}

func firstDiff(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

func TestIntegration_PBSBackupBinaryRoundTrip(t *testing.T) {
	ctx := context.Background()

	t.Run("legacy_v1", func(t *testing.T) {
		pbsCfg := pbsConfigFromEnv(t)
		store := newIntegrationStore(t)
		cfg := defaultBackupConfig(t)
		cfg.DetectionMode = DetectionLegacy
		cleanupSnapshot(t, pbsCfg, cfg)

		fs, expected := buildBinaryTree(t)
		result, err := NewServer(fs.provider(), store).RunBackupWithMode(ctx, "/root", cfg)
		if err != nil {
			t.Fatalf("RunBackupWithMode legacy: %v", err)
		}
		if result.FileCount != len(expected) {
			t.Errorf("file count = %d, want %d", result.FileCount, len(expected))
		}
		t.Logf("legacy backup: %d files, %d bytes", result.FileCount, result.TotalBytes)

		idxData := pbsDownload(t, pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime, "root.pxar.didx")
		idx, err := datastore.ParseDynamicIndex(idxData)
		if err != nil {
			t.Fatalf("parse didx: %v", err)
		}
		reader := NewPBSReader(pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime)
		if err := reader.Connect(ctx); err != nil {
			t.Fatalf("connect: %v", err)
		}
		defer reader.Close()
		if _, err := reader.DownloadFile("root.pxar.didx"); err != nil {
			t.Fatalf("download didx: %v", err)
		}
		var archive bytes.Buffer
		if err := reader.RestoreFile(idx, &archive); err != nil {
			t.Fatalf("restore archive: %v", err)
		}

		acc := accessor.NewAccessor(bytes.NewReader(archive.Bytes()))
		if _, err := acc.ReadRoot(); err != nil {
			t.Fatalf("read root: %v", err)
		}
		assertArchivesBinaryEqual(t, acc, expected)

		if s := pbsVerifySnapshot(t, pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime); s != "OK" {
			t.Errorf("PBS verify legacy: %q", s)
		} else {
			t.Log("PBS verify legacy binary round-trip: OK")
		}
	})

	t.Run("data_v2_split", func(t *testing.T) {
		pbsCfg := pbsConfigFromEnv(t)
		store := newIntegrationStore(t)
		cfg := defaultBackupConfig(t)
		cfg.DetectionMode = DetectionData
		cleanupSnapshot(t, pbsCfg, cfg)

		fs, expected := buildBinaryTree(t)
		result, err := NewServer(fs.provider(), store).RunBackupWithMode(ctx, "/root", cfg)
		if err != nil {
			t.Fatalf("RunBackupWithMode data: %v", err)
		}
		if result.FileCount != len(expected) {
			t.Errorf("file count = %d, want %d", result.FileCount, len(expected))
		}
		t.Logf("data backup: %d files, %d bytes", result.FileCount, result.TotalBytes)

		metaBytes, payloadBytes, _, _ := pbsRestoreSplitArchive(t, pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime)
		if len(metaBytes) == 0 || len(payloadBytes) == 0 {
			t.Fatalf("empty restored streams: meta=%d payload=%d", len(metaBytes), len(payloadBytes))
		}

		acc := accessor.NewAccessor(bytes.NewReader(metaBytes), bytes.NewReader(payloadBytes))
		if _, err := acc.ReadRoot(); err != nil {
			t.Fatalf("read root: %v", err)
		}
		assertArchivesBinaryEqual(t, acc, expected)

		if s := pbsVerifySnapshot(t, pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime); s != "OK" {
			t.Errorf("PBS verify data: %q", s)
		} else {
			t.Log("PBS verify data binary round-trip: OK")
		}
	})
}
