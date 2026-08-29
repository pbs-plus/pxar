package backupproxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
)

func TestDatastoreStorePublishesExistingDynamicIndex(t *testing.T) {
	root := t.TempDir()
	chunkStore, err := datastore.NewChunkStore(root)
	if err != nil {
		t.Fatal(err)
	}
	chunks := [][]byte{[]byte("first chunk"), []byte("second chunk")}
	index := datastore.NewDynamicIndexWriter(0)
	var offset uint64
	for _, chunk := range chunks {
		digest := sha256.Sum256(chunk)
		blob, err := datastore.EncodeBlob(nil, chunk)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := chunkStore.InsertChunk(digest, blob); err != nil {
			t.Fatal(err)
		}
		offset += uint64(len(chunk))
		index.Add(offset, digest)
	}
	raw, err := index.Finish()
	if err != nil {
		t.Fatal(err)
	}
	sourcePath := filepath.Join(root, "source.didx")
	if err := os.WriteFile(sourcePath, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	snapshotDir := filepath.Join(root, "host", "target", "2026-08-28T17:00:00Z")
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg, err := buzhash.NewConfig(4 << 20)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewDatastoreStore(root, snapshotDir, cfg, DatastoreStoreOptions{UID: -1, GID: -1})
	if err != nil {
		t.Fatal(err)
	}
	var progress UploadProgress
	session, err := store.StartSession(context.Background(), BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   "target",
		CryptMode:  datastore.CryptModeNone,
		OnUploadProgress: func(current UploadProgress) {
			progress = current
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	publisher, ok := session.(DynamicIndexPublisher)
	if !ok {
		t.Fatal("local session does not publish dynamic indexes")
	}
	result, err := publisher.PublishDynamicIndex(context.Background(), "target.ppxar.didx", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if result.Size != offset || result.Digest != index.Csum() {
		t.Fatalf("unexpected result: %+v", result)
	}
	if progress.ProcessedChunks != uint64(len(chunks)) || progress.ProcessedBytes != offset || progress.UploadedChunks != 0 {
		t.Fatalf("unexpected progress: %+v", progress)
	}
	targetRaw, err := os.ReadFile(filepath.Join(snapshotDir, "target.ppxar.didx"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(targetRaw, raw) {
		t.Fatal("published index differs from source")
	}
	manifest, err := session.Finish(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Files) != 1 || manifest.Files[0].Filename != "target.ppxar.didx" {
		t.Fatalf("unexpected manifest: %+v", manifest.Files)
	}
}

func TestDatastoreStoreRejectsPublishedIndexWithMissingChunk(t *testing.T) {
	root := t.TempDir()
	if _, err := datastore.NewChunkStore(root); err != nil {
		t.Fatal(err)
	}
	index := datastore.NewDynamicIndexWriter(0)
	index.Add(4, sha256.Sum256([]byte("missing")))
	raw, err := index.Finish()
	if err != nil {
		t.Fatal(err)
	}
	sourcePath := filepath.Join(root, "source.didx")
	if err := os.WriteFile(sourcePath, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	snapshotDir := filepath.Join(root, "host", "target", "2026-08-28T17:00:00Z")
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg, err := buzhash.NewConfig(4 << 20)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewDatastoreStore(root, snapshotDir, cfg, DatastoreStoreOptions{UID: -1, GID: -1})
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.StartSession(context.Background(), BackupConfig{BackupType: datastore.BackupHost, BackupID: "target"})
	if err != nil {
		t.Fatal(err)
	}
	publisher := session.(DynamicIndexPublisher)
	if _, err := publisher.PublishDynamicIndex(context.Background(), "target.ppxar.didx", sourcePath); err == nil {
		t.Fatal("published an index with a missing chunk")
	}
	if _, err := os.Stat(filepath.Join(snapshotDir, "target.ppxar.didx")); !os.IsNotExist(err) {
		t.Fatalf("invalid target index remains: %v", err)
	}
}
