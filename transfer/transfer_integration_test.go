//go:build integration

package transfer_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
)

// pbsTransferConfigFromEnv reads PBS connection settings from environment variables.
func pbsTransferConfigFromEnv(t *testing.T) backupproxy.PBSConfig {
	t.Helper()

	url := os.Getenv("PBS_URL")
	ds := os.Getenv("PBS_DATASTORE")
	token := os.Getenv("PBS_TOKEN")
	if url == "" || ds == "" || token == "" {
		t.Skip("skipping integration test: set PBS_URL, PBS_DATASTORE, and PBS_TOKEN environment variables")
	}
	return backupproxy.PBSConfig{
		BaseURL:       url,
		Datastore:     ds,
		AuthToken:     token,
		SkipTLSVerify: true,
	}
}

// TestIntegration_PBSMultiSnapshotTransfer creates two PBS snapshots, then builds
// a new snapshot assembling files from both sources using the transfer API.
func TestIntegration_PBSMultiSnapshotTransfer(t *testing.T) {
	ctx := context.Background()
	pbsCfg := pbsTransferConfigFromEnv(t)
	chunkCfg, _ := buzhash.NewConfig(4096)

	// --- Phase 1: Create first snapshot (snap1) with /etc/hosts and /var/log/syslog ---
	store1 := backupproxy.NewPBSRemoteStore(pbsCfg, chunkCfg, false)
	snap1Cfg := backupproxy.BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   fmt.Sprintf("transfer-test-%d", time.Now().UnixMilli()),
		BackupTime: time.Now().Unix(),
	}

	sess1, err := store1.StartSession(ctx, snap1Cfg)
	if err != nil {
		t.Fatalf("StartSession snap1: %v", err)
	}

	dst1 := transfer.NewSplitSessionArchiveWriter(ctx, sess1, "root.mpxar.didx", "root.ppxar.didx")
	rootMeta := pxar.DirMetadata(0o755).Build()
	if err := dst1.Begin(&rootMeta, transfer.WriterOptions{Format: format.FormatVersion2}); err != nil {
		t.Fatalf("Begin snap1: %v", err)
	}

	// Create /etc directory with hosts file
	etcMeta := pxar.DirMetadata(0o755).Build()
	if err := dst1.BeginDirectory("etc", &etcMeta); err != nil {
		t.Fatalf("BeginDirectory etc: %v", err)
	}
	hostsMeta := pxar.FileMetadata(0o644).Owner(0, 0).Build()
	if err := dst1.WriteEntry(&pxar.Entry{
		Path:     "hosts",
		Kind:     pxar.KindFile,
		Metadata: hostsMeta,
		FileSize: 18,
	}, []byte("127.0.0.1 localhost")); err != nil {
		t.Fatalf("WriteEntry hosts: %v", err)
	}
	if err := dst1.EndDirectory(); err != nil {
		t.Fatalf("EndDirectory etc: %v", err)
	}

	// Create /var/log directory with syslog
	varMeta := pxar.DirMetadata(0o755).Build()
	if err := dst1.BeginDirectory("var", &varMeta); err != nil {
		t.Fatalf("BeginDirectory var: %v", err)
	}
	logMeta := pxar.DirMetadata(0o755).Build()
	if err := dst1.BeginDirectory("log", &logMeta); err != nil {
		t.Fatalf("BeginDirectory log: %v", err)
	}
	syslogMeta := pxar.FileMetadata(0o644).Owner(0, 0).Build()
	if err := dst1.WriteEntry(&pxar.Entry{
		Path:     "syslog",
		Kind:     pxar.KindFile,
		Metadata: syslogMeta,
		FileSize: 14,
	}, []byte("system booting")); err != nil {
		t.Fatalf("WriteEntry syslog: %v", err)
	}
	if err := dst1.EndDirectory(); err != nil {
		t.Fatalf("EndDirectory log: %v", err)
	}
	if err := dst1.EndDirectory(); err != nil {
		t.Fatalf("EndDirectory var: %v", err)
	}

	if err := dst1.Finish(); err != nil {
		t.Fatalf("Finish snap1: %v", err)
	}
	manifest1, err := sess1.Finish(ctx)
	if err != nil {
		t.Fatalf("Finish session 1: %v", err)
	}
	t.Logf("Snap1 created: %s/%s/%d, files=%d", manifest1.BackupType, manifest1.BackupID, manifest1.BackupTime, len(manifest1.Files))

	// --- Phase 2: Create second snapshot (snap2) with /opt/app/config.yml ---
	snap2Time := time.Now().Unix() + 1 // different timestamp
	snap2Cfg := backupproxy.BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   snap1Cfg.BackupID + "-snap2",
		BackupTime: snap2Time,
	}

	store2 := backupproxy.NewPBSRemoteStore(pbsCfg, chunkCfg, false)
	sess2, err := store2.StartSession(ctx, snap2Cfg)
	if err != nil {
		t.Fatalf("StartSession snap2: %v", err)
	}

	dst2 := transfer.NewSplitSessionArchiveWriter(ctx, sess2, "root.mpxar.didx", "root.ppxar.didx")
	if err := dst2.Begin(&rootMeta, transfer.WriterOptions{Format: format.FormatVersion2}); err != nil {
		t.Fatalf("Begin snap2: %v", err)
	}

	// Create /opt/app directory with config.yml
	optMeta := pxar.DirMetadata(0o755).Build()
	if err := dst2.BeginDirectory("opt", &optMeta); err != nil {
		t.Fatalf("BeginDirectory opt: %v", err)
	}
	appMeta := pxar.DirMetadata(0o755).Build()
	if err := dst2.BeginDirectory("app", &appMeta); err != nil {
		t.Fatalf("BeginDirectory app: %v", err)
	}
	configMeta := pxar.FileMetadata(0o644).Owner(0, 0).Build()
	if err := dst2.WriteEntry(&pxar.Entry{
		Path:     "config.yml",
		Kind:     pxar.KindFile,
		Metadata: configMeta,
		FileSize: 12,
	}, []byte("key: value\n")); err != nil {
		t.Fatalf("WriteEntry config: %v", err)
	}
	if err := dst2.EndDirectory(); err != nil {
		t.Fatalf("EndDirectory app: %v", err)
	}
	if err := dst2.EndDirectory(); err != nil {
		t.Fatalf("EndDirectory opt: %v", err)
	}

	if err := dst2.Finish(); err != nil {
		t.Fatalf("Finish snap2: %v", err)
	}
	manifest2, err := sess2.Finish(ctx)
	if err != nil {
		t.Fatalf("Finish session 2: %v", err)
	}
	t.Logf("Snap2 created: %s/%s/%d, files=%d", manifest2.BackupType, manifest2.BackupID, manifest2.BackupTime, len(manifest2.Files))

	// Cleanup both snapshots after the test
	t.Cleanup(func() {
		deletePBSSnapshot(t, pbsCfg, snap1Cfg)
		deletePBSSnapshot(t, pbsCfg, snap2Cfg)
		deletePBSSnapshot(t, pbsCfg, backupproxy.BackupConfig{
			BackupType: datastore.BackupHost,
			BackupID:   snap1Cfg.BackupID + "-merged",
			BackupTime: snap2Time + 1,
		})
	})

	// --- Phase 3: Read from both snapshots and build a new merged snapshot ---
	snap1Reader, err := transfer.NewPBSArchiveReader(ctx, transfer.PBSArchiveConfig{
		Config:       pbsCfg,
		BackupType:   "host",
		BackupID:     snap1Cfg.BackupID,
		BackupTime:   snap1Cfg.BackupTime,
		MetaName:     "root.mpxar.didx",
		PayloadName:  "root.ppxar.didx",
	})
	if err != nil {
		t.Fatalf("Open snap1 reader: %v", err)
	}
	defer snap1Reader.Close()

	snap2Reader, err := transfer.NewPBSArchiveReader(ctx, transfer.PBSArchiveConfig{
		Config:       pbsCfg,
		BackupType:   "host",
		BackupID:     snap2Cfg.BackupID,
		BackupTime:   snap2Cfg.BackupTime,
		MetaName:     "root.mpxar.didx",
		PayloadName:  "root.ppxar.didx",
	})
	if err != nil {
		t.Fatalf("Open snap2 reader: %v", err)
	}
	defer snap2Reader.Close()

	// Start a new merged snapshot session
	mergedTime := snap2Time + 1
	mergedCfg := backupproxy.BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   snap1Cfg.BackupID + "-merged",
		BackupTime: mergedTime,
	}

	store3 := backupproxy.NewPBSRemoteStore(pbsCfg, chunkCfg, false)
	sess3, err := store3.StartSession(ctx, mergedCfg)
	if err != nil {
		t.Fatalf("StartSession merged: %v", err)
	}

	dstMerged := transfer.NewSplitSessionArchiveWriter(ctx, sess3, "root.mpxar.didx", "root.ppxar.didx")
	if err := dstMerged.Begin(&rootMeta, transfer.WriterOptions{Format: format.FormatVersion2}); err != nil {
		t.Fatalf("Begin merged: %v", err)
	}

	// Copy /etc from snap1 and /opt from snap2 into the merged snapshot
	if err := transfer.CopyTree(snap1Reader, dstMerged, "/", "/", transfer.TransferOption{}); err != nil {
		t.Fatalf("Copy /etc from snap1: %v", err)
	}
	if err := transfer.CopyTree(snap2Reader, dstMerged, "/", "/", transfer.TransferOption{}); err != nil {
		t.Fatalf("Copy /opt from snap2: %v", err)
	}

	if err := dstMerged.Finish(); err != nil {
		t.Fatalf("Finish merged: %v", err)
	}

	manifest3, err := sess3.Finish(ctx)
	if err != nil {
		t.Fatalf("Finish merged session: %v", err)
	}
	t.Logf("Merged snapshot created: %s/%s/%d, files=%d", manifest3.BackupType, manifest3.BackupID, manifest3.BackupTime, len(manifest3.Files))

	// --- Phase 4: Read back the merged snapshot and verify content ---
	mergedReader, err := transfer.NewPBSArchiveReader(ctx, transfer.PBSArchiveConfig{
		Config:       pbsCfg,
		BackupType:   "host",
		BackupID:     mergedCfg.BackupID,
		BackupTime:   mergedCfg.BackupTime,
		MetaName:     "root.mpxar.didx",
		PayloadName:  "root.ppxar.didx",
	})
	if err != nil {
		t.Fatalf("Open merged reader: %v", err)
	}
	defer mergedReader.Close()

	// Verify /etc/hosts from snap1
	root, err := mergedReader.ReadRoot()
	if err != nil {
		t.Fatalf("ReadRoot: %v", err)
	}
	t.Logf("Merged root entries at offset %d", root.ContentOffset)

	hostsEntry, err := mergedReader.Lookup("/etc/hosts")
	if err != nil {
		t.Fatalf("Lookup /etc/hosts: %v", err)
	}
	hostsContent, err := mergedReader.ReadFileContent(hostsEntry)
	if err != nil {
		t.Fatalf("ReadFileContent /etc/hosts: %v", err)
	}
	if string(hostsContent) != "127.0.0.1 localhost" {
		t.Errorf("/etc/hosts content = %q, want %q", string(hostsContent), "127.0.0.1 localhost")
	}

	// Verify /opt/app/config.yml from snap2
	configEntry, err := mergedReader.Lookup("/opt/app/config.yml")
	if err != nil {
		t.Fatalf("Lookup /opt/app/config.yml: %v", err)
	}
	configContent, err := mergedReader.ReadFileContent(configEntry)
	if err != nil {
		t.Fatalf("ReadFileContent /opt/app/config.yml: %v", err)
	}
	if string(configContent) != "key: value\n" {
		t.Errorf("/opt/app/config.yml content = %q, want %q", string(configContent), "key: value\n")
	}

	// Verify /var/log/syslog from snap1
	syslogEntry, err := mergedReader.Lookup("/var/log/syslog")
	if err != nil {
		t.Fatalf("Lookup /var/log/syslog: %v", err)
	}
	syslogContent, err := mergedReader.ReadFileContent(syslogEntry)
	if err != nil {
		t.Fatalf("ReadFileContent /var/log/syslog: %v", err)
	}
	if string(syslogContent) != "system booting" {
		t.Errorf("/var/log/syslog content = %q, want %q", string(syslogContent), "system booting")
	}

	t.Log("Multi-snapshot transfer integration test PASSED")
}

// TestIntegration_PBSMultiSnapshotSelectiveTransfer creates two snapshots and
// selectively copies specific files (not entire directory trees) from each source.
func TestIntegration_PBSMultiSnapshotSelectiveTransfer(t *testing.T) {
	ctx := context.Background()
	pbsCfg := pbsTransferConfigFromEnv(t)
	chunkCfg, _ := buzhash.NewConfig(4096)

	// --- Phase 1: Create snap1 with /etc/hosts and /etc/resolv.conf ---
	store1 := backupproxy.NewPBSRemoteStore(pbsCfg, chunkCfg, false)
	snap1Cfg := backupproxy.BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   fmt.Sprintf("sel-test-%d", time.Now().UnixMilli()),
		BackupTime: time.Now().Unix(),
	}

	sess1, err := store1.StartSession(ctx, snap1Cfg)
	if err != nil {
		t.Fatalf("StartSession snap1: %v", err)
	}

	dst1 := transfer.NewSplitSessionArchiveWriter(ctx, sess1, "root.mpxar.didx", "root.ppxar.didx")
	rootMeta := pxar.DirMetadata(0o755).Build()
	if err := dst1.Begin(&rootMeta, transfer.WriterOptions{Format: format.FormatVersion2}); err != nil {
		t.Fatalf("Begin snap1: %v", err)
	}

	etcMeta := pxar.DirMetadata(0o755).Build()
	if err := dst1.BeginDirectory("etc", &etcMeta); err != nil {
		t.Fatalf("BeginDirectory etc: %v", err)
	}
	hostsMeta := pxar.FileMetadata(0o644).Owner(0, 0).Build()
	if err := dst1.WriteEntry(&pxar.Entry{
		Path:     "hosts",
		Kind:     pxar.KindFile,
		Metadata: hostsMeta,
		FileSize: 18,
	}, []byte("127.0.0.1 localhost")); err != nil {
		t.Fatalf("WriteEntry hosts: %v", err)
	}
	resolvMeta := pxar.FileMetadata(0o644).Owner(0, 0).Build()
	if err := dst1.WriteEntry(&pxar.Entry{
		Path:     "resolv.conf",
		Kind:     pxar.KindFile,
		Metadata: resolvMeta,
		FileSize: 18,
	}, []byte("nameserver 8.8.8.8")); err != nil {
		t.Fatalf("WriteEntry resolv.conf: %v", err)
	}
	if err := dst1.EndDirectory(); err != nil {
		t.Fatalf("EndDirectory etc: %v", err)
	}

	if err := dst1.Finish(); err != nil {
		t.Fatalf("Finish snap1: %v", err)
	}
	if _, err := sess1.Finish(ctx); err != nil {
		t.Fatalf("Finish session 1: %v", err)
	}

	// --- Phase 2: Create snap2 with /opt/app/config.yml ---
	snap2Time := snap1Cfg.BackupTime + 1
	snap2Cfg := backupproxy.BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   snap1Cfg.BackupID + "-snap2",
		BackupTime: snap2Time,
	}

	store2 := backupproxy.NewPBSRemoteStore(pbsCfg, chunkCfg, false)
	sess2, err := store2.StartSession(ctx, snap2Cfg)
	if err != nil {
		t.Fatalf("StartSession snap2: %v", err)
	}

	dst2 := transfer.NewSplitSessionArchiveWriter(ctx, sess2, "root.mpxar.didx", "root.ppxar.didx")
	if err := dst2.Begin(&rootMeta, transfer.WriterOptions{Format: format.FormatVersion2}); err != nil {
		t.Fatalf("Begin snap2: %v", err)
	}

	optMeta := pxar.DirMetadata(0o755).Build()
	if err := dst2.BeginDirectory("opt", &optMeta); err != nil {
		t.Fatalf("BeginDirectory opt: %v", err)
	}
	appMeta := pxar.DirMetadata(0o755).Build()
	if err := dst2.BeginDirectory("app", &appMeta); err != nil {
		t.Fatalf("BeginDirectory app: %v", err)
	}
	configMeta := pxar.FileMetadata(0o644).Owner(0, 0).Build()
	if err := dst2.WriteEntry(&pxar.Entry{
		Path:     "config.yml",
		Kind:     pxar.KindFile,
		Metadata: configMeta,
		FileSize: 12,
	}, []byte("key: value\n")); err != nil {
		t.Fatalf("WriteEntry config: %v", err)
	}
	if err := dst2.EndDirectory(); err != nil {
		t.Fatalf("EndDirectory app: %v", err)
	}
	if err := dst2.EndDirectory(); err != nil {
		t.Fatalf("EndDirectory opt: %v", err)
	}

	if err := dst2.Finish(); err != nil {
		t.Fatalf("Finish snap2: %v", err)
	}
	if _, err := sess2.Finish(ctx); err != nil {
		t.Fatalf("Finish session 2: %v", err)
	}

	// Cleanup
	t.Cleanup(func() {
		deletePBSSnapshot(t, pbsCfg, snap1Cfg)
		deletePBSSnapshot(t, pbsCfg, snap2Cfg)
		deletePBSSnapshot(t, pbsCfg, backupproxy.BackupConfig{
			BackupType: datastore.BackupHost,
			BackupID:   snap1Cfg.BackupID + "-sel-merged",
			BackupTime: snap2Time + 1,
		})
	})

	// --- Phase 3: Selectively copy specific files ---
	snap1Reader, err := transfer.NewPBSArchiveReader(ctx, transfer.PBSArchiveConfig{
		Config:      pbsCfg,
		BackupType:  "host",
		BackupID:    snap1Cfg.BackupID,
		BackupTime:  snap1Cfg.BackupTime,
		MetaName:    "root.mpxar.didx",
		PayloadName: "root.ppxar.didx",
	})
	if err != nil {
		t.Fatalf("Open snap1 reader: %v", err)
	}
	defer snap1Reader.Close()

	snap2Reader, err := transfer.NewPBSArchiveReader(ctx, transfer.PBSArchiveConfig{
		Config:      pbsCfg,
		BackupType:  "host",
		BackupID:    snap2Cfg.BackupID,
		BackupTime:  snap2Cfg.BackupTime,
		MetaName:    "root.mpxar.didx",
		PayloadName: "root.ppxar.didx",
	})
	if err != nil {
		t.Fatalf("Open snap2 reader: %v", err)
	}
	defer snap2Reader.Close()

	mergedTime := snap2Time + 1
	mergedCfg := backupproxy.BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   snap1Cfg.BackupID + "-sel-merged",
		BackupTime: mergedTime,
	}

	store3 := backupproxy.NewPBSRemoteStore(pbsCfg, chunkCfg, false)
	sess3, err := store3.StartSession(ctx, mergedCfg)
	if err != nil {
		t.Fatalf("StartSession merged: %v", err)
	}

	dstMerged := transfer.NewSplitSessionArchiveWriter(ctx, sess3, "root.mpxar.didx", "root.ppxar.didx")
	if err := dstMerged.Begin(&rootMeta, transfer.WriterOptions{Format: format.FormatVersion2}); err != nil {
		t.Fatalf("Begin merged: %v", err)
	}

	// Copy only /etc from snap1 (not any other dirs snap1 might have)
	// and /opt from snap2
	if err := transfer.Copy(snap1Reader, dstMerged, []transfer.PathMapping{
		{Src: "/etc", Dst: "/etc"},
	}, transfer.TransferOption{}); err != nil {
		t.Fatalf("Copy /etc from snap1: %v", err)
	}
	if err := transfer.Copy(snap2Reader, dstMerged, []transfer.PathMapping{
		{Src: "/opt", Dst: "/opt"},
	}, transfer.TransferOption{}); err != nil {
		t.Fatalf("Copy /opt from snap2: %v", err)
	}

	if err := dstMerged.Finish(); err != nil {
		t.Fatalf("Finish merged: %v", err)
	}
	if _, err := sess3.Finish(ctx); err != nil {
		t.Fatalf("Finish merged session: %v", err)
	}

	// --- Phase 4: Verify the merged snapshot ---
	mergedReader, err := transfer.NewPBSArchiveReader(ctx, transfer.PBSArchiveConfig{
		Config:      pbsCfg,
		BackupType:  "host",
		BackupID:    mergedCfg.BackupID,
		BackupTime:  mergedCfg.BackupTime,
		MetaName:    "root.mpxar.didx",
		PayloadName: "root.ppxar.didx",
	})
	if err != nil {
		t.Fatalf("Open merged reader: %v", err)
	}
	defer mergedReader.Close()

	// Check /etc/hosts from snap1
	hostsEntry, err := mergedReader.Lookup("/etc/hosts")
	if err != nil {
		t.Fatalf("Lookup /etc/hosts: %v", err)
	}
	hostsContent, err := mergedReader.ReadFileContent(hostsEntry)
	if err != nil {
		t.Fatalf("ReadFileContent /etc/hosts: %v", err)
	}
	if string(hostsContent) != "127.0.0.1 localhost" {
		t.Errorf("/etc/hosts = %q, want %q", string(hostsContent), "127.0.0.1 localhost")
	}

	// Check /opt/app/config.yml from snap2
	configEntry, err := mergedReader.Lookup("/opt/app/config.yml")
	if err != nil {
		t.Fatalf("Lookup /opt/app/config.yml: %v", err)
	}
	configContent, err := mergedReader.ReadFileContent(configEntry)
	if err != nil {
		t.Fatalf("ReadFileContent /opt/app/config.yml: %v", err)
	}
	if string(configContent) != "key: value\n" {
		t.Errorf("/opt/app/config.yml = %q, want %q", string(configContent), "key: value\n")
	}

	t.Log("Selective multi-snapshot transfer integration test PASSED")
}

// TestIntegration_PBSPathRemappingTransfer tests copying files from a source
// snapshot to a different destination path in the new snapshot.
// CopyTree(src, dst, "/etc", "/backup_etc") renames the directory from
// "etc" to "backup_etc" and remaps all child paths accordingly.
func TestIntegration_PBSPathRemappingTransfer(t *testing.T) {
	ctx := context.Background()
	pbsCfg := pbsTransferConfigFromEnv(t)
	chunkCfg, _ := buzhash.NewConfig(4096)

	// Create a snapshot with /etc/hosts
	store := backupproxy.NewPBSRemoteStore(pbsCfg, chunkCfg, false)
	srcCfg := backupproxy.BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   fmt.Sprintf("remap-test-%d", time.Now().UnixMilli()),
		BackupTime: time.Now().Unix(),
	}

	sess, err := store.StartSession(ctx, srcCfg)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	dst := transfer.NewSplitSessionArchiveWriter(ctx, sess, "root.mpxar.didx", "root.ppxar.didx")
	rootMeta := pxar.DirMetadata(0o755).Build()
	if err := dst.Begin(&rootMeta, transfer.WriterOptions{Format: format.FormatVersion2}); err != nil {
		t.Fatalf("Begin: %v", err)
	}

	etcMeta := pxar.DirMetadata(0o755).Build()
	if err := dst.BeginDirectory("etc", &etcMeta); err != nil {
		t.Fatalf("BeginDirectory etc: %v", err)
	}
	hostsMeta := pxar.FileMetadata(0o644).Owner(0, 0).Build()
	if err := dst.WriteEntry(&pxar.Entry{
		Path:     "hosts",
		Kind:     pxar.KindFile,
		Metadata: hostsMeta,
		FileSize: 18,
	}, []byte("127.0.0.1 localhost")); err != nil {
		t.Fatalf("WriteEntry hosts: %v", err)
	}
	if err := dst.EndDirectory(); err != nil {
		t.Fatalf("EndDirectory etc: %v", err)
	}

	if err := dst.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if _, err := sess.Finish(ctx); err != nil {
		t.Fatalf("Finish session: %v", err)
	}

	t.Cleanup(func() {
		deletePBSSnapshot(t, pbsCfg, srcCfg)
		deletePBSSnapshot(t, pbsCfg, backupproxy.BackupConfig{
			BackupType: datastore.BackupHost,
			BackupID:   srcCfg.BackupID + "-remap",
			BackupTime: srcCfg.BackupTime + 1,
		})
	})

	// Read source snapshot and copy /etc to /backup/etc in the new snapshot
	srcReader, err := transfer.NewPBSArchiveReader(ctx, transfer.PBSArchiveConfig{
		Config:      pbsCfg,
		BackupType:  "host",
		BackupID:    srcCfg.BackupID,
		BackupTime:  srcCfg.BackupTime,
		MetaName:    "root.mpxar.didx",
		PayloadName: "root.ppxar.didx",
	})
	if err != nil {
		t.Fatalf("Open source reader: %v", err)
	}
	defer srcReader.Close()

	remapCfg := backupproxy.BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   srcCfg.BackupID + "-remap",
		BackupTime: srcCfg.BackupTime + 1,
	}

	store2 := backupproxy.NewPBSRemoteStore(pbsCfg, chunkCfg, false)
	sess2, err := store2.StartSession(ctx, remapCfg)
	if err != nil {
		t.Fatalf("StartSession remap: %v", err)
	}

	dstRemap := transfer.NewSplitSessionArchiveWriter(ctx, sess2, "root.mpxar.didx", "root.ppxar.didx")
	if err := dstRemap.Begin(&rootMeta, transfer.WriterOptions{Format: format.FormatVersion2}); err != nil {
		t.Fatalf("Begin remap: %v", err)
	}

	// Copy /etc → /backup/etc (path remapping)
	if err := transfer.CopyTree(srcReader, dstRemap, "/etc", "/backup/etc", transfer.TransferOption{}); err != nil {
		t.Fatalf("CopyTree /etc → /backup/etc: %v", err)
	}

	if err := dstRemap.Finish(); err != nil {
		t.Fatalf("Finish remap: %v", err)
	}
	if _, err := sess2.Finish(ctx); err != nil {
		t.Fatalf("Finish remap session: %v", err)
	}

	// Verify the remapped snapshot
	remapReader, err := transfer.NewPBSArchiveReader(ctx, transfer.PBSArchiveConfig{
		Config:      pbsCfg,
		BackupType:  "host",
		BackupID:    remapCfg.BackupID,
		BackupTime:  remapCfg.BackupTime,
		MetaName:    "root.mpxar.didx",
		PayloadName: "root.ppxar.didx",
	})
	if err != nil {
		t.Fatalf("Open remap reader: %v", err)
	}
	defer remapReader.Close()

	// Should find /backup/etc/hosts, not /etc/hosts
	backupHosts, err := remapReader.Lookup("/backup/etc/hosts")
	if err != nil {
		t.Fatalf("Lookup /backup/etc/hosts: %v", err)
	}
	backupContent, err := remapReader.ReadFileContent(backupHosts)
	if err != nil {
		t.Fatalf("ReadFileContent /backup/etc/hosts: %v", err)
	}
	if string(backupContent) != "127.0.0.1 localhost" {
		t.Errorf("/backup/etc/hosts = %q, want %q", string(backupContent), "127.0.0.1 localhost")
	}

	// Should NOT find /etc/hosts in the remapped snapshot
	_, err = remapReader.Lookup("/etc/hosts")
	if err == nil {
		t.Error("expected /etc/hosts to NOT exist in remapped snapshot, but it was found")
	}

	t.Log("Path remapping transfer integration test PASSED")
}

// deletePBSSnapshot removes a backup snapshot from PBS (for cleanup).
func deletePBSSnapshot(t *testing.T, cfg backupproxy.PBSConfig, bc backupproxy.BackupConfig) {
	t.Helper()

	url := fmt.Sprintf("%s/admin/datastore/%s/snapshots?backup-type=%s&backup-id=%s&backup-time=%d",
		cfg.BaseURL, cfg.Datastore, bc.BackupType.String(), bc.BackupID, bc.BackupTime)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodDelete, url, nil)
	if err != nil {
		t.Logf("create delete request: %v", err)
		return
	}
	req.Header.Set("Authorization", "PBSAPIToken "+cfg.AuthToken)

	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	resp, err := client.Do(req)
	if err != nil {
		t.Logf("delete snapshot: %v", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Logf("delete snapshot: HTTP %d: %s", resp.StatusCode, body)
	}
}