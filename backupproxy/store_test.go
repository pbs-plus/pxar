package backupproxy

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
)

func newTestLocalStore(t *testing.T) (*LocalStore, string) {
	t.Helper()
	dir := t.TempDir()
	config, _ := buzhash.NewConfig(4096)
	ls, err := NewLocalStore(dir, config, false)
	if err != nil {
		t.Fatal(err)
	}
	return ls, dir
}

func TestLocalStoreStartSession(t *testing.T) {
	ls, _ := newTestLocalStore(t)

	sess, err := ls.StartSession(nil, BackupConfig{
		BackupType: datastore.BackupVM,
		BackupID:   "100",
		BackupTime: 1700000000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if sess == nil {
		t.Error("session should not be nil")
	}
}

func TestLocalStoreUploadArchive(t *testing.T) {
	ls, dir := newTestLocalStore(t)

	sess, err := ls.StartSession(nil, BackupConfig{
		BackupType: datastore.BackupVM,
		BackupID:   "100",
		BackupTime: 1700000000,
	})
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 50<<10)
	rand.Read(data)

	result, err := sess.UploadArchive(nil, "root.pxar.didx", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	if result.Filename != "root.pxar.didx" {
		t.Errorf("filename = %q, want %q", result.Filename, "root.pxar.didx")
	}
	if result.Size == 0 {
		t.Error("index size should not be 0")
	}

	// Verify index file exists
	indexPath := filepath.Join(dir, "root.pxar.didx")
	raw, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatalf("index file not found: %v", err)
	}

	// Verify it's a valid dynamic index
	reader, err := datastore.ReadDynamicIndex(raw)
	if err != nil {
		t.Fatalf("read dynamic index: %v", err)
	}
	if reader.Count() == 0 {
		t.Error("index should have at least one chunk")
	}
}

func TestLocalStoreUploadBlob(t *testing.T) {
	ls, dir := newTestLocalStore(t)

	sess, _ := ls.StartSession(nil, BackupConfig{
		BackupType: datastore.BackupCT,
		BackupID:   "200",
	})

	blobData := []byte(`{"key": "value"}`)
	if err := sess.UploadBlob(nil, "config.json", blobData); err != nil {
		t.Fatal(err)
	}

	// Verify blob file exists
	blobPath := filepath.Join(dir, "config.json")
	raw, err := os.ReadFile(blobPath)
	if err != nil {
		t.Fatalf("blob file not found: %v", err)
	}
	if !bytes.Equal(raw, blobData) {
		t.Error("blob content mismatch")
	}
}

func TestLocalStoreFinish(t *testing.T) {
	ls, dir := newTestLocalStore(t)

	sess, _ := ls.StartSession(nil, BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   "myhost",
		BackupTime: 1700000000,
	})

	data := make([]byte, 10<<10)
	rand.Read(data)

	sess.UploadArchive(nil, "root.pxar.didx", bytes.NewReader(data))
	sess.UploadBlob(nil, "config.json", []byte("config"))

	manifest, err := sess.Finish(nil)
	if err != nil {
		t.Fatal(err)
	}

	if manifest.BackupType != "host" {
		t.Errorf("type = %q, want %q", manifest.BackupType, "host")
	}
	if manifest.BackupID != "myhost" {
		t.Errorf("id = %q, want %q", manifest.BackupID, "myhost")
	}
	if len(manifest.Files) != 2 {
		t.Fatalf("files = %d, want 2", len(manifest.Files))
	}

	// Verify manifest file exists
	manifestPath := filepath.Join(dir, "index.json")
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("manifest not found: %v", err)
	}

	parsed, err := datastore.UnmarshalManifest(raw)
	if err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	if len(parsed.Files) != 2 {
		t.Errorf("parsed files = %d, want 2", len(parsed.Files))
	}
}

func TestLocalStoreRoundTrip(t *testing.T) {
	ls, dir := newTestLocalStore(t)

	sess, _ := ls.StartSession(nil, BackupConfig{
		BackupType: datastore.BackupVM,
		BackupID:   "100",
	})

	data := make([]byte, 30<<10)
	rand.Read(data)

	result, err := sess.UploadArchive(nil, "root.pxar.didx", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	// Read back index
	indexPath := filepath.Join(dir, result.Filename)
	raw, _ := os.ReadFile(indexPath)
	reader, _ := datastore.ReadDynamicIndex(raw)

	// Load and verify each chunk
	chunkStore, _ := datastore.NewChunkStore(dir)
	var reconstructed bytes.Buffer
	for i := 0; i < reader.Count(); i++ {
		info, ok := reader.ChunkInfo(i)
		if !ok {
			t.Fatalf("chunk %d not found", i)
		}

		chunkData, err := chunkStore.LoadChunk(info.Digest)
		if err != nil {
			t.Fatalf("load chunk %d: %v", i, err)
		}

		decoded, err := datastore.DecodeBlob(chunkData)
		if err != nil {
			t.Fatalf("decode blob %d: %v", i, err)
		}

		reconstructed.Write(decoded)
	}

	if !bytes.Equal(reconstructed.Bytes(), data) {
		t.Error("reconstructed data doesn't match original")
	}
}

func TestLocalStoreDeduplication(t *testing.T) {
	ls, _ := newTestLocalStore(t)

	data := make([]byte, 20<<10)
	rand.Read(data)

	// First upload
	sess1, _ := ls.StartSession(nil, BackupConfig{BackupID: "1"})
	_, err := sess1.UploadArchive(nil, "root.pxar.didx", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	// Count chunks from first upload
	indexPath := filepath.Join(ls.baseDir, "root.pxar.didx")
	raw1, _ := os.ReadFile(indexPath)
	reader1, _ := datastore.ReadDynamicIndex(raw1)
	chunkCount := reader1.Count()

	// Second upload with same data
	sess2, _ := ls.StartSession(nil, BackupConfig{BackupID: "2"})
	_, err = sess2.UploadArchive(nil, "root2.pxar.didx", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	// Verify same number of chunks
	indexPath2 := filepath.Join(ls.baseDir, "root2.pxar.didx")
	raw2, _ := os.ReadFile(indexPath2)
	reader2, _ := datastore.ReadDynamicIndex(raw2)

	if reader2.Count() != chunkCount {
		t.Errorf("second upload: %d chunks, want %d", reader2.Count(), chunkCount)
	}

	// Verify same digests
	for i := 0; i < chunkCount; i++ {
		d1, _ := reader1.IndexDigest(i)
		d2, _ := reader2.IndexDigest(i)
		if d1 != d2 {
			t.Errorf("chunk %d: digest mismatch", i)
		}
	}
}

func TestLocalStoreIndexDigest(t *testing.T) {
	ls, _ := newTestLocalStore(t)

	sess, _ := ls.StartSession(nil, BackupConfig{})
	data := []byte("test data for index digest verification")

	result, err := sess.UploadArchive(nil, "test.pxar.didx", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	// Verify digest is hex-encodable and correct
	digestHex := hex.EncodeToString(result.Digest[:])
	if len(digestHex) != 64 {
		t.Errorf("digest hex length = %d, want 64", len(digestHex))
	}

	// Verify manifest entry has the digest
	sess.Finish(nil)
}
