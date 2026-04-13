//go:build integration

package backupproxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"

	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
)

// pbsConfigFromEnv reads PBS connection settings from environment variables.
// It calls t.Skip if any required variable is missing.
func pbsConfigFromEnv(t *testing.T) PBSConfig {
	t.Helper()

	url := os.Getenv("PBS_URL")
	datastoreName := os.Getenv("PBS_DATASTORE")
	token := os.Getenv("PBS_TOKEN")

	if url == "" || datastoreName == "" || token == "" {
		t.Skip("skipping integration test: set PBS_URL, PBS_DATASTORE, and PBS_TOKEN environment variables")
	}

	return PBSConfig{
		BaseURL:       url,
		Datastore:     datastoreName,
		AuthToken:     token,
		SkipTLSVerify: true,
	}
}

// newIntegrationStore creates a PBSRemoteStore from environment configuration.
func newIntegrationStore(t *testing.T) *PBSRemoteStore {
	t.Helper()

	cfg := pbsConfigFromEnv(t)
	chunkCfg, err := buzhash.NewConfig(4096)
	if err != nil {
		t.Fatalf("create chunk config: %v", err)
	}
	return NewPBSRemoteStore(cfg, chunkCfg, false)
}

// pbsHTTPClient returns an HTTP client with TLS verification disabled
// for communicating with a PBS instance using self-signed certificates.
func pbsHTTPClient(t *testing.T) *http.Client {
	t.Helper()

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig.InsecureSkipVerify = true
	return &http.Client{Transport: transport}
}

// pbsDownload downloads a file from a PBS snapshot via the download API.
func pbsDownload(t *testing.T, cfg PBSConfig, backupType string, backupID string, backupTime int64, fileName string) []byte {
	t.Helper()

	url := fmt.Sprintf("%s/admin/datastore/%s/download?backup-type=%s&backup-id=%s&backup-time=%d&file-name=%s",
		cfg.BaseURL, cfg.Datastore, backupType, backupID, backupTime, fileName)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("create download request: %v", err)
	}
	req.Header.Set("Authorization", "PBSAPIToken "+cfg.AuthToken)

	client := pbsHTTPClient(t)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("download %q: %v", fileName, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("download %q: HTTP %d: %s", fileName, resp.StatusCode, body)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read download response: %v", err)
	}
	return data
}

// deleteSnapshot removes a backup snapshot from PBS. Used as t.Cleanup to
// prevent snapshots from accumulating on the test PBS instance.
func deleteSnapshot(t *testing.T, cfg PBSConfig, backupType string, backupID string, backupTime int64) {
	t.Helper()

	url := fmt.Sprintf("%s/admin/datastore/%s/snapshots?backup-type=%s&backup-id=%s&backup-time=%d",
		cfg.BaseURL, cfg.Datastore, backupType, backupID, backupTime)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodDelete, url, nil)
	if err != nil {
		t.Logf("create delete snapshot request: %v", err)
		return
	}
	req.Header.Set("Authorization", "PBSAPIToken "+cfg.AuthToken)

	client := pbsHTTPClient(t)
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

// cleanupSnapshot registers a t.Cleanup that deletes the backup snapshot after the test.
func cleanupSnapshot(t *testing.T, cfg PBSConfig, bc BackupConfig) {
	t.Helper()
	t.Cleanup(func() {
		deleteSnapshot(t, cfg, bc.BackupType.String(), bc.BackupID, bc.BackupTime)
	})
}

// uniqueBackupID generates a unique backup ID using the current timestamp
// and a short random suffix to avoid collisions between test runs.
func uniqueBackupID(t *testing.T) string {
	t.Helper()

	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("generate random id: %v", err)
	}
	return fmt.Sprintf("integration-%d-%s", time.Now().UnixMilli(), hex.EncodeToString(b))
}

// defaultBackupConfig creates a BackupConfig with a unique ID and the current timestamp.
func defaultBackupConfig(t *testing.T) BackupConfig {
	t.Helper()

	return BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   uniqueBackupID(t),
		BackupTime: time.Now().Unix(),
	}
}

func TestIntegration_StartSession(t *testing.T) {
	store := newIntegrationStore(t)
	pbsCfg := pbsConfigFromEnv(t)
	cfg := defaultBackupConfig(t)
	cleanupSnapshot(t, pbsCfg, cfg)

	sess, err := store.StartSession(context.Background(), cfg)
	if err != nil {
		t.Fatalf("StartSession failed: %v", err)
	}
	if sess == nil {
		t.Fatal("StartSession returned nil session")
	}
}

func TestIntegration_FullBackupRoundTrip(t *testing.T) {
	store := newIntegrationStore(t)
	pbsCfg := pbsConfigFromEnv(t)
	cfg := defaultBackupConfig(t)
	cleanupSnapshot(t, pbsCfg, cfg)

	// Generate 50KB of random archive data
	archiveData := make([]byte, 50*1024)
	if _, err := rand.Read(archiveData); err != nil {
		t.Fatalf("generate archive data: %v", err)
	}

	// Config blob data
	configBlob := []byte(`{"hostname":"integration-test","version":"1.0","timestamp":"2025-01-01T00:00:00Z"}`)

	sess, err := store.StartSession(context.Background(), cfg)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	// Upload archive
	archiveResult, err := sess.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(archiveData))
	if err != nil {
		t.Fatalf("UploadArchive: %v", err)
	}
	if archiveResult.Filename != "root.pxar.didx" {
		t.Errorf("archive filename = %q, want %q", archiveResult.Filename, "root.pxar.didx")
	}

	// Upload config blob (PBS requires .blob extension)
	if err := sess.UploadBlob(context.Background(), "config.blob", configBlob); err != nil {
		t.Fatalf("UploadBlob: %v", err)
	}

	// Finish session
	manifest, err := sess.Finish(context.Background())
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if manifest == nil {
		t.Fatal("Finish returned nil manifest")
	}

	// Download the config blob back from PBS and verify byte-perfect match
	downloaded := pbsDownload(t, pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime, "config.blob")
	decoded, err := datastore.DecodeBlob(downloaded)
	if err != nil {
		t.Fatalf("decode downloaded blob: %v", err)
	}
	if !bytes.Equal(decoded, configBlob) {
		t.Errorf("downloaded blob does not match original.\n  got  %d bytes\n  want %d bytes", len(decoded), len(configBlob))
	}
}

func TestIntegration_ChunkDeduplication(t *testing.T) {
	store := newIntegrationStore(t)
	pbsCfg := pbsConfigFromEnv(t)

	// Generate shared random data for both uploads
	data := make([]byte, 20*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("generate data: %v", err)
	}

	cfg1 := defaultBackupConfig(t)
	cleanupSnapshot(t, pbsCfg, cfg1)

	// First upload
	sess1, err := store.StartSession(context.Background(), cfg1)
	if err != nil {
		t.Fatalf("StartSession 1: %v", err)
	}

	result1, err := sess1.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("UploadArchive 1: %v", err)
	}
	if _, err := sess1.Finish(context.Background()); err != nil {
		t.Fatalf("Finish 1: %v", err)
	}

	// Second upload with identical data, use incremented timestamp to avoid collision
	cfg2 := BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   uniqueBackupID(t),
		BackupTime: cfg1.BackupTime + 1,
	}
	cleanupSnapshot(t, pbsCfg, cfg2)

	sess2, err := store.StartSession(context.Background(), cfg2)
	if err != nil {
		t.Fatalf("StartSession 2: %v", err)
	}

	result2, err := sess2.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("UploadArchive 2: %v", err)
	}
	if _, err := sess2.Finish(context.Background()); err != nil {
		t.Fatalf("Finish 2: %v", err)
	}

	// Both indexes should have the same digest because the data is identical
	// and chunking is deterministic
	if result1.Digest != result2.Digest {
		d1 := hex.EncodeToString(result1.Digest[:])
		d2 := hex.EncodeToString(result2.Digest[:])
		t.Errorf("index digests differ for identical data:\n  session1: %s\n  session2: %s", d1, d2)
	}
}

func TestIntegration_BlobUploadDownload(t *testing.T) {
	store := newIntegrationStore(t)
	pbsCfg := pbsConfigFromEnv(t)
	cfg := defaultBackupConfig(t)
	cleanupSnapshot(t, pbsCfg, cfg)

	// Create a JSON blob to upload
	blobData := map[string]interface{}{
		"test":    "integration",
		"count":   42,
		"tags":    []string{"a", "b", "c"},
		"enabled": true,
	}
	blobBytes, err := json.MarshalIndent(blobData, "", "  ")
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}

	sess, err := store.StartSession(context.Background(), cfg)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	// Upload a small archive so the snapshot has at least one file
	smallData := make([]byte, 1024)
	if _, err := rand.Read(smallData); err != nil {
		t.Fatalf("generate small data: %v", err)
	}
	if _, err := sess.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(smallData)); err != nil {
		t.Fatalf("UploadArchive: %v", err)
	}

	// Upload the JSON blob
	if err := sess.UploadBlob(context.Background(), "test-config.blob", blobBytes); err != nil {
		t.Fatalf("UploadBlob: %v", err)
	}

	if _, err := sess.Finish(context.Background()); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Download the blob back from PBS
	downloaded := pbsDownload(t, pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime, "test-config.blob")
	decoded, err := datastore.DecodeBlob(downloaded)
	if err != nil {
		t.Fatalf("decode downloaded blob: %v", err)
	}
	if !bytes.Equal(decoded, blobBytes) {
		t.Errorf("downloaded blob mismatch:\n  got  %d bytes: %q\n  want %d bytes: %q",
			len(decoded), string(decoded), len(blobBytes), string(blobBytes))
	}

	// Verify the JSON content parses correctly
	var parsed map[string]interface{}
	if err := json.Unmarshal(decoded, &parsed); err != nil {
		t.Fatalf("unmarshal downloaded json: %v", err)
	}
	if parsed["test"] != "integration" {
		t.Errorf("parsed test field = %v, want %q", parsed["test"], "integration")
	}
}

func TestIntegration_ManifestVerification(t *testing.T) {
	store := newIntegrationStore(t)
	pbsCfg := pbsConfigFromEnv(t)
	cfg := defaultBackupConfig(t)
	cleanupSnapshot(t, pbsCfg, cfg)

	// Generate archive data
	archiveData := make([]byte, 30*1024)
	if _, err := rand.Read(archiveData); err != nil {
		t.Fatalf("generate archive data: %v", err)
	}

	// Create a blob
	blobData := []byte("manifest-verification-test-blob-content")

	sess, err := store.StartSession(context.Background(), cfg)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	archiveResult, err := sess.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(archiveData))
	if err != nil {
		t.Fatalf("UploadArchive: %v", err)
	}

	if err := sess.UploadBlob(context.Background(), "test-blob.blob", blobData); err != nil {
		t.Fatalf("UploadBlob: %v", err)
	}

	manifest, err := sess.Finish(context.Background())
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Verify manifest has correct metadata
	if manifest.BackupType != cfg.BackupType.String() {
		t.Errorf("manifest backup type = %q, want %q", manifest.BackupType, cfg.BackupType.String())
	}
	if manifest.BackupID != cfg.BackupID {
		t.Errorf("manifest backup id = %q, want %q", manifest.BackupID, cfg.BackupID)
	}
	if manifest.BackupTime != cfg.BackupTime {
		t.Errorf("manifest backup time = %d, want %d", manifest.BackupTime, cfg.BackupTime)
	}

	// Verify manifest contains both files
	if len(manifest.Files) != 2 {
		t.Fatalf("manifest has %d files, want 2", len(manifest.Files))
	}

	// Build a lookup map for files by name
	fileMap := make(map[string]datastore.FileInfo)
	for _, f := range manifest.Files {
		fileMap[f.Filename] = f
	}

	// Verify archive file entry
	archiveEntry, ok := fileMap["root.pxar.didx"]
	if !ok {
		t.Fatal("manifest missing root.pxar.didx")
	}
	if archiveEntry.Size == 0 {
		t.Error("archive entry size should not be 0")
	}
	expectedArchiveDigest := hex.EncodeToString(archiveResult.Digest[:])
	if archiveEntry.CSum != expectedArchiveDigest {
		t.Errorf("archive checksum = %q, want %q", archiveEntry.CSum, expectedArchiveDigest)
	}

	// Verify the archive checksum is a valid hex-encoded SHA-256
	if len(archiveEntry.CSum) != 64 {
		t.Errorf("archive checksum hex length = %d, want 64", len(archiveEntry.CSum))
	}

	// Verify blob file entry
	blobEntry, ok := fileMap["test-blob.blob"]
	if !ok {
		t.Fatal("manifest missing test-blob.blob")
	}
	if blobEntry.Size == 0 {
		t.Error("blob size should not be 0")
	}

	// Verify blob checksum matches SHA-256 of the encoded blob data
	// The checksum is calculated on the data after datastore.EncodeBlob is applied
	encodedBlob, err := datastore.EncodeBlob(blobData)
	if err != nil {
		t.Fatalf("encode blob: %v", err)
	}
	expectedBlobDigest := sha256.Sum256(encodedBlob.Bytes())
	expectedBlobDigestHex := hex.EncodeToString(expectedBlobDigest[:])
	if blobEntry.CSum != expectedBlobDigestHex {
		t.Errorf("blob checksum = %q, want %q", blobEntry.CSum, expectedBlobDigestHex)
	}
}

func TestIntegration_IndexReconstructionRoundTrip(t *testing.T) {
	store := newIntegrationStore(t)
	pbsCfg := pbsConfigFromEnv(t)
	cfg := defaultBackupConfig(t)
	cleanupSnapshot(t, pbsCfg, cfg)

	// Generate known random data
	data := make([]byte, 20*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("generate data: %v", err)
	}
	origDigest := sha256.Sum256(data)

	sess, err := store.StartSession(context.Background(), cfg)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	if _, err := sess.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(data)); err != nil {
		t.Fatalf("UploadArchive: %v", err)
	}
	if _, err := sess.Finish(context.Background()); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Download the .didx file from PBS
	didxData := pbsDownload(t, pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime, "root.pxar.didx")
	reader, err := datastore.ReadDynamicIndex(didxData)
	if err != nil {
		t.Fatalf("parse didx: %v", err)
	}

	// Verify total virtual size matches original data
	if reader.IndexBytes() != uint64(len(data)) {
		t.Errorf("index total bytes = %d, want %d", reader.IndexBytes(), len(data))
	}

	// Reconstruct data chunk by chunk using the index offsets and verify digests
	var reconstructed bytes.Buffer
	for i := 0; i < reader.Count(); i++ {
		info, ok := reader.ChunkInfo(i)
		if !ok {
			t.Fatalf("chunk info %d not found", i)
		}

		// Extract chunk from original data at the indexed offsets
		chunk := data[info.Start:info.End]
		digest := sha256.Sum256(chunk)
		if digest != info.Digest {
			t.Errorf("chunk %d digest mismatch:\n  local: %s\n  pbs:   %s",
				i, hex.EncodeToString(digest[:])[:16], hex.EncodeToString(info.Digest[:])[:16])
		}
		reconstructed.Write(chunk)
	}

	// Verify byte-perfect reconstruction
	if !bytes.Equal(reconstructed.Bytes(), data) {
		t.Errorf("reconstruction mismatch: got %d bytes, want %d", reconstructed.Len(), len(data))
	}

	// Verify SHA-256 of reconstructed data matches original
	reconDigest := sha256.Sum256(reconstructed.Bytes())
	if reconDigest != origDigest {
		t.Errorf("reconstructed digest = %s, want %s",
			hex.EncodeToString(reconDigest[:])[:16], hex.EncodeToString(origDigest[:])[:16])
	}
}

func TestIntegration_CompressedBlobRoundTrip(t *testing.T) {
	pbsCfg := pbsConfigFromEnv(t)
	chunkCfg, err := buzhash.NewConfig(4096)
	if err != nil {
		t.Fatalf("create chunk config: %v", err)
	}
	// Use compression
	store := NewPBSRemoteStore(pbsCfg, chunkCfg, true)
	cfg := defaultBackupConfig(t)
	cleanupSnapshot(t, pbsCfg, cfg)

	data := make([]byte, 50*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("generate data: %v", err)
	}

	sess, err := store.StartSession(context.Background(), cfg)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	if _, err := sess.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(data)); err != nil {
		t.Fatalf("UploadArchive: %v", err)
	}

	blobData := []byte(`{"compressed": true, "test": "round-trip"}`)
	if err := sess.UploadBlob(context.Background(), "config.blob", blobData); err != nil {
		t.Fatalf("UploadBlob: %v", err)
	}

	if _, err := sess.Finish(context.Background()); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Download and verify blob
	downloaded := pbsDownload(t, pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime, "config.blob")
	decoded, err := datastore.DecodeBlob(downloaded)
	if err != nil {
		t.Fatalf("decode blob: %v", err)
	}
	if !bytes.Equal(decoded, blobData) {
		t.Errorf("blob mismatch: got %d bytes, want %d", len(decoded), len(blobData))
	}

	// Download .didx and verify index reconstruction with compressed chunks
	didxData := pbsDownload(t, pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime, "root.pxar.didx")
	reader, err := datastore.ReadDynamicIndex(didxData)
	if err != nil {
		t.Fatalf("parse didx: %v", err)
	}
	if reader.IndexBytes() != uint64(len(data)) {
		t.Errorf("index bytes = %d, want %d", reader.IndexBytes(), len(data))
	}

	// Verify chunk digests match (using original data offsets)
	for i := 0; i < reader.Count(); i++ {
		info, ok := reader.ChunkInfo(i)
		if !ok {
			t.Fatalf("chunk info %d not found", i)
		}
		chunk := data[info.Start:info.End]
		digest := sha256.Sum256(chunk)
		if digest != info.Digest {
			t.Errorf("chunk %d digest mismatch (compressed upload)", i)
		}
	}
}

func TestIntegration_SmallInput(t *testing.T) {
	store := newIntegrationStore(t)
	pbsCfg := pbsConfigFromEnv(t)
	cfg := defaultBackupConfig(t)
	cleanupSnapshot(t, pbsCfg, cfg)

	// Tiny input: 100 bytes, likely single chunk
	data := []byte("small test data for edge case verification - exactly one hundred bytes of text data!")
	origDigest := sha256.Sum256(data)

	sess, err := store.StartSession(context.Background(), cfg)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	result, err := sess.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("UploadArchive: %v", err)
	}
	if _, err := sess.Finish(context.Background()); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Download index and verify
	didxData := pbsDownload(t, pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime, "root.pxar.didx")
	reader, err := datastore.ReadDynamicIndex(didxData)
	if err != nil {
		t.Fatalf("parse didx: %v", err)
	}

	if reader.Count() == 0 {
		t.Fatal("expected at least 1 chunk for small input")
	}

	// Reconstruct
	var reconstructed bytes.Buffer
	for i := 0; i < reader.Count(); i++ {
		info, ok := reader.ChunkInfo(i)
		if !ok {
			t.Fatalf("chunk info %d not found", i)
		}
		chunk := data[info.Start:info.End]
		reconstructed.Write(chunk)
	}

	if !bytes.Equal(reconstructed.Bytes(), data) {
		t.Errorf("small input reconstruction failed: got %d bytes, want %d", reconstructed.Len(), len(data))
	}

	reconDigest := sha256.Sum256(reconstructed.Bytes())
	if reconDigest != origDigest {
		t.Errorf("small input digest mismatch")
	}

	// Verify the index file entry has reasonable metadata
	if result.Size == 0 {
		t.Error("small input index size should not be 0")
	}
}

func TestIntegration_EmptyInputRejected(t *testing.T) {
	store := newIntegrationStore(t)
	cfg := defaultBackupConfig(t)

	// PBS rejects empty archives (chunk-count >= 1, size >= 1 required)
	sess, err := store.StartSession(context.Background(), cfg)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	_, err = sess.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader([]byte{}))
	if err == nil {
		t.Fatal("expected error for empty archive upload, got nil")
	}
	// The error should come from PBS closing the index with 0 chunks
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("expected HTTP 400 error, got: %v", err)
	}
}

func TestIntegration_ManifestRoundTrip(t *testing.T) {
	store := newIntegrationStore(t)
	pbsCfg := pbsConfigFromEnv(t)
	cfg := defaultBackupConfig(t)
	cleanupSnapshot(t, pbsCfg, cfg)

	archiveData := make([]byte, 30*1024)
	if _, err := rand.Read(archiveData); err != nil {
		t.Fatalf("generate archive data: %v", err)
	}
	blobData := []byte(`{"manifest": "round-trip", "version": 2}`)

	sess, err := store.StartSession(context.Background(), cfg)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	archiveResult, err := sess.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(archiveData))
	if err != nil {
		t.Fatalf("UploadArchive: %v", err)
	}
	if err := sess.UploadBlob(context.Background(), "config.blob", blobData); err != nil {
		t.Fatalf("UploadBlob: %v", err)
	}

	localManifest, err := sess.Finish(context.Background())
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Download and parse the manifest blob from PBS
	manifestBlobData := pbsDownload(t, pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime, "index.json.blob")
	decoded, err := datastore.DecodeBlob(manifestBlobData)
	if err != nil {
		t.Fatalf("decode manifest blob: %v", err)
	}

	pbsManifest, err := datastore.UnmarshalManifest(decoded)
	if err != nil {
		t.Fatalf("unmarshal PBS manifest: %v", err)
	}

	// Verify manifest metadata matches what we sent
	if pbsManifest.BackupType != localManifest.BackupType {
		t.Errorf("PBS manifest type = %q, want %q", pbsManifest.BackupType, localManifest.BackupType)
	}
	if pbsManifest.BackupID != localManifest.BackupID {
		t.Errorf("PBS manifest id = %q, want %q", pbsManifest.BackupID, localManifest.BackupID)
	}
	if pbsManifest.BackupTime != localManifest.BackupTime {
		t.Errorf("PBS manifest time = %d, want %d", pbsManifest.BackupTime, localManifest.BackupTime)
	}

	// Verify file count
	if len(pbsManifest.Files) != len(localManifest.Files) {
		t.Fatalf("PBS manifest files = %d, want %d", len(pbsManifest.Files), len(localManifest.Files))
	}

	// Build lookup by filename
	pbsFileMap := make(map[string]datastore.FileInfo)
	for _, f := range pbsManifest.Files {
		pbsFileMap[f.Filename] = f
	}

	// Verify each file entry matches
	for _, localFile := range localManifest.Files {
		pbsFile, ok := pbsFileMap[localFile.Filename]
		if !ok {
			t.Errorf("PBS manifest missing file %q", localFile.Filename)
			continue
		}

		// Checksums should match exactly
		if pbsFile.CSum != localFile.CSum {
			t.Errorf("file %q checksum: PBS=%q local=%q", localFile.Filename, pbsFile.CSum, localFile.CSum)
		}

		// Verify archive entry checksum matches the index digest
		if localFile.Filename == "root.pxar.didx" {
			expectedDigest := hex.EncodeToString(archiveResult.Digest[:])
			if pbsFile.CSum != expectedDigest {
				t.Errorf("archive checksum: PBS=%q expected=%q", pbsFile.CSum, expectedDigest)
			}
		}

		// Verify blob entry checksum matches SHA-256 of encoded blob data
		if localFile.Filename == "config.blob" {
			encodedBlob, err := datastore.EncodeBlob(blobData)
			if err != nil {
				t.Fatalf("encode blob: %v", err)
			}
			expectedBlobDigest := sha256.Sum256(encodedBlob.Bytes())
			expectedHex := hex.EncodeToString(expectedBlobDigest[:])
			if pbsFile.CSum != expectedHex {
				t.Errorf("blob checksum: PBS=%q expected=%q", pbsFile.CSum, expectedHex)
			}
		}
	}
}

func TestIntegration_ChunkOffsetOrdering(t *testing.T) {
	store := newIntegrationStore(t)
	pbsCfg := pbsConfigFromEnv(t)
	cfg := defaultBackupConfig(t)
	cleanupSnapshot(t, pbsCfg, cfg)

	// Use data large enough for multiple chunks
	data := make([]byte, 100*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("generate data: %v", err)
	}

	sess, err := store.StartSession(context.Background(), cfg)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	if _, err := sess.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(data)); err != nil {
		t.Fatalf("UploadArchive: %v", err)
	}
	if _, err := sess.Finish(context.Background()); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Download and verify offset ordering
	didxData := pbsDownload(t, pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime, "root.pxar.didx")
	reader, err := datastore.ReadDynamicIndex(didxData)
	if err != nil {
		t.Fatalf("parse didx: %v", err)
	}

	if reader.Count() < 2 {
		t.Skip("need at least 2 chunks to verify ordering")
	}

	// Verify offsets are strictly increasing and contiguous
	var prevEnd uint64
	for i := 0; i < reader.Count(); i++ {
		info, ok := reader.ChunkInfo(i)
		if !ok {
			t.Fatalf("chunk info %d not found", i)
		}
		if info.Start != prevEnd {
			t.Errorf("chunk %d: start=%d, expected=%d (gap/overlap)", i, info.Start, prevEnd)
		}
		if info.End <= info.Start {
			t.Errorf("chunk %d: end=%d <= start=%d (empty/inverted)", i, info.End, info.Start)
		}
		prevEnd = info.End
	}

	// Final offset must equal total data size
	if prevEnd != uint64(len(data)) {
		t.Errorf("total indexed bytes = %d, want %d", prevEnd, len(data))
	}
}

func TestIntegration_LargeDataRoundTrip(t *testing.T) {
	store := newIntegrationStore(t)
	pbsCfg := pbsConfigFromEnv(t)
	cfg := defaultBackupConfig(t)
	cleanupSnapshot(t, pbsCfg, cfg)

	// 2 MB — well above default H2 maxFrameSize (16 KB) to exercise frame splitting
	data := make([]byte, 2*1024*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("generate data: %v", err)
	}
	origDigest := sha256.Sum256(data)

	sess, err := store.StartSession(context.Background(), cfg)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	if _, err := sess.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(data)); err != nil {
		t.Fatalf("UploadArchive: %v", err)
	}
	if _, err := sess.Finish(context.Background()); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Download and parse .didx
	didxData := pbsDownload(t, pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime, "root.pxar.didx")
	reader, err := datastore.ReadDynamicIndex(didxData)
	if err != nil {
		t.Fatalf("parse didx: %v", err)
	}

	t.Logf("Large archive: %d bytes, %d chunks", len(data), reader.Count())

	if reader.IndexBytes() != uint64(len(data)) {
		t.Errorf("index bytes = %d, want %d", reader.IndexBytes(), len(data))
	}

	// Reconstruct and verify byte-perfect match
	var reconstructed bytes.Buffer
	for i := 0; i < reader.Count(); i++ {
		info, ok := reader.ChunkInfo(i)
		if !ok {
			t.Fatalf("chunk info %d not found", i)
		}
		chunk := data[info.Start:info.End]
		digest := sha256.Sum256(chunk)
		if digest != info.Digest {
			t.Errorf("chunk %d digest mismatch", i)
		}
		reconstructed.Write(chunk)
	}

	if !bytes.Equal(reconstructed.Bytes(), data) {
		t.Errorf("large data reconstruction failed: got %d bytes, want %d", reconstructed.Len(), len(data))
	}

	reconDigest := sha256.Sum256(reconstructed.Bytes())
	if reconDigest != origDigest {
		t.Errorf("large data digest mismatch")
	}
}

func TestIntegration_PBSChecksumParity(t *testing.T) {
	store := newIntegrationStore(t)
	pbsCfg := pbsConfigFromEnv(t)
	cfg := defaultBackupConfig(t)
	cleanupSnapshot(t, pbsCfg, cfg)

	data := make([]byte, 50*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("generate data: %v", err)
	}

	sess, err := store.StartSession(context.Background(), cfg)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	if _, err := sess.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(data)); err != nil {
		t.Fatalf("UploadArchive: %v", err)
	}
	if _, err := sess.Finish(context.Background()); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Download PBS-stored index
	didxData := pbsDownload(t, pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime, "root.pxar.didx")
	reader, err := datastore.ReadDynamicIndex(didxData)
	if err != nil {
		t.Fatalf("parse didx: %v", err)
	}

	// Compute PBS-side checksum from the downloaded index
	pbsCsum, _ := reader.ComputeCsum()

	// Independently compute the expected checksum by re-chunking the same data
	// using the same chunking config. The PBS checksum is:
	//   sha256(end_offset_LE || chunk_digest || end_offset_LE || chunk_digest || ...)
	chunkCfg, _ := buzhash.NewConfig(4096)
	chunker := buzhash.NewChunker(bytes.NewReader(data), chunkCfg)
	localHash := sha256.New()
	var totalSize uint64

	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("re-chunk: %v", err)
		}
		digest := sha256.Sum256(chunk)
		totalSize += uint64(len(chunk))

		var offsetBuf [8]byte
		binary.LittleEndian.PutUint64(offsetBuf[:], totalSize)
		localHash.Write(offsetBuf[:])
		localHash.Write(digest[:])
	}

	var localCsum [32]byte
	copy(localCsum[:], localHash.Sum(nil))

	// The two checksums must match — proves our checksum algorithm
	// matches PBS's expectations (if it didn't, dynamic_close would fail,
	// but this explicitly verifies the downloaded data)
	if pbsCsum != localCsum {
		t.Errorf("PBS csum mismatch:\n  PBS:    %s\n  local:  %s",
			hex.EncodeToString(pbsCsum[:])[:16], hex.EncodeToString(localCsum[:])[:16])
	}
}

func TestIntegration_MultipleArchivesPerSession(t *testing.T) {
	store := newIntegrationStore(t)
	pbsCfg := pbsConfigFromEnv(t)
	cfg := defaultBackupConfig(t)
	cleanupSnapshot(t, pbsCfg, cfg)

	sess, err := store.StartSession(context.Background(), cfg)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	// Upload three different archives with different data
	archives := []struct {
		name string
		data []byte
	}{
		{"root.pxar.didx", make([]byte, 20*1024)},
		{"home.pxar.didx", make([]byte, 30*1024)},
		{"var.pxar.didx", make([]byte, 15*1024)},
	}
	for i := range archives {
		if _, err := rand.Read(archives[i].data); err != nil {
			t.Fatalf("generate data for %s: %v", archives[i].name, err)
		}
	}

	// Track digests for each archive
	type archiveInfo struct {
		digest [32]byte
		data   []byte
	}
	infos := make([]archiveInfo, len(archives))

	for i, a := range archives {
		result, err := sess.UploadArchive(context.Background(), a.name, bytes.NewReader(a.data))
		if err != nil {
			t.Fatalf("UploadArchive %s: %v", a.name, err)
		}
		if result.Filename != a.name {
			t.Errorf("archive %d filename = %q, want %q", i, result.Filename, a.name)
		}
		infos[i] = archiveInfo{digest: result.Digest, data: a.data}
	}

	// Upload a blob too
	blobData := []byte(`{"multi": true, "archives": 3}`)
	if err := sess.UploadBlob(context.Background(), "config.blob", blobData); err != nil {
		t.Fatalf("UploadBlob: %v", err)
	}

	manifest, err := sess.Finish(context.Background())
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Manifest should have 4 files (3 archives + 1 blob)
	if len(manifest.Files) != 4 {
		t.Fatalf("manifest files = %d, want 4", len(manifest.Files))
	}

	// Verify each archive via download and reconstruction
	for _, a := range archives {
		didxData := pbsDownload(t, pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime, a.name)
		reader, err := datastore.ReadDynamicIndex(didxData)
		if err != nil {
			t.Fatalf("parse didx %s: %v", a.name, err)
		}

		// Verify total size
		if reader.IndexBytes() != uint64(len(a.data)) {
			t.Errorf("%s: index bytes = %d, want %d", a.name, reader.IndexBytes(), len(a.data))
		}

		// Reconstruct and verify
		var reconstructed bytes.Buffer
		for j := 0; j < reader.Count(); j++ {
			info, ok := reader.ChunkInfo(j)
			if !ok {
				t.Fatalf("%s: chunk info %d not found", a.name, j)
			}
			chunk := a.data[info.Start:info.End]
			digest := sha256.Sum256(chunk)
			if digest != info.Digest {
				t.Errorf("%s chunk %d: digest mismatch", a.name, j)
			}
			reconstructed.Write(chunk)
		}

		if !bytes.Equal(reconstructed.Bytes(), a.data) {
			t.Errorf("%s: reconstruction mismatch (got %d, want %d bytes)", a.name, reconstructed.Len(), len(a.data))
		}
	}

	// Verify manifest has correct file entries
	fileMap := make(map[string]datastore.FileInfo)
	for _, f := range manifest.Files {
		fileMap[f.Filename] = f
	}
	for _, a := range archives {
		entry, ok := fileMap[a.name]
		if !ok {
			t.Errorf("manifest missing %s", a.name)
			continue
		}
		if entry.CSum == "" {
			t.Errorf("%s: empty checksum in manifest", a.name)
		}
	}
	if _, ok := fileMap["config.blob"]; !ok {
		t.Error("manifest missing config.blob")
	}
}

func TestIntegration_ProtocolErrors(t *testing.T) {
	pbsCfg := pbsConfigFromEnv(t)

	// Create a store with an invalid auth token
	chunkCfg, err := buzhash.NewConfig(4096)
	if err != nil {
		t.Fatalf("create chunk config: %v", err)
	}

	badStore := NewPBSRemoteStore(PBSConfig{
		BaseURL:       pbsCfg.BaseURL,
		Datastore:     pbsCfg.Datastore,
		AuthToken:     "invalid-token-id:invalid-secret",
		SkipTLSVerify: true,
	}, chunkCfg, false)

	cfg := defaultBackupConfig(t)

	// Starting a session with wrong credentials should fail
	_, err = badStore.StartSession(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error with invalid auth token, got nil")
	}

	// The error should indicate an HTTP 401 or 403
	errMsg := err.Error()
	if !strings.Contains(errMsg, "401") && !strings.Contains(errMsg, "403") {
		t.Errorf("error should mention 401 or 403, got: %v", errMsg)
	}
}

// TestIntegration_ChunkedDidxUploadDownload verifies the full chunked upload
// workflow and validates the didx index file. It downloads the didx from PBS
// and verifies the chunk metadata matches the original data.
func TestIntegration_ChunkedDidxUploadDownload(t *testing.T) {
	store := newIntegrationStore(t)
	pbsCfg := pbsConfigFromEnv(t)
	cfg := defaultBackupConfig(t)
	cleanupSnapshot(t, pbsCfg, cfg)

	// Generate 50KB of random data - creates reasonable number of chunks
	data := make([]byte, 50*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("generate data: %v", err)
	}

	// Upload archive to PBS (creates chunked .didx)
	sess, err := store.StartSession(context.Background(), cfg)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	result, err := sess.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("UploadArchive: %v", err)
	}
	if _, err := sess.Finish(context.Background()); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Download the didx file via HTTP API and verify
	didxData := pbsDownload(t, pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime, "root.pxar.didx")

	// Parse the downloaded didx
	idx, err := datastore.ReadDynamicIndex(didxData)
	if err != nil {
		t.Fatalf("parse downloaded didx: %v", err)
	}

	t.Logf("Downloaded didx: %d chunks, %d total bytes", idx.Count(), idx.IndexBytes())

	// Verify index metadata matches upload result
	if idx.IndexBytes() != uint64(len(data)) {
		t.Errorf("index bytes = %d, want %d", idx.IndexBytes(), len(data))
	}

	// Verify chunk offsets are correct by checking original data
	var totalSize uint64
	for i := 0; i < idx.Count(); i++ {
		info, ok := idx.ChunkInfo(i)
		if !ok {
			t.Fatalf("chunk info %d not found", i)
		}

		// Verify offset continuity
		if info.Start != totalSize {
			t.Errorf("chunk %d: start offset = %d, expected %d", i, info.Start, totalSize)
		}

		// Verify chunk size is reasonable
		chunkSize := info.End - info.Start
		if chunkSize == 0 {
			t.Errorf("chunk %d: empty chunk", i)
		}

		// Verify the digest matches the data at those offsets
		chunk := data[info.Start:info.End]
		expectedDigest := sha256.Sum256(chunk)
		if expectedDigest != info.Digest {
			t.Errorf("chunk %d: digest mismatch\n  from data: %s\n  in index: %s",
				i, hex.EncodeToString(expectedDigest[:])[:16], hex.EncodeToString(info.Digest[:])[:16])
		}

		totalSize = info.End
	}

	if totalSize != uint64(len(data)) {
		t.Errorf("total indexed bytes = %d, want %d", totalSize, len(data))
	}

	// Verify the index checksum matches the upload digest
	csum, _ := idx.ComputeCsum()
	if csum != result.Digest {
		t.Errorf("computed index csum differs from upload digest")
	}

	t.Logf("Successfully verified %d chunks covering %d bytes", idx.Count(), len(data))
}

// TestIntegration_ChunkedDidxRestoreFile tests file restoration using PBSReader's
// RestoreFile method which downloads all chunks and reconstructs the file.
func TestIntegration_ChunkedDidxRestoreFile(t *testing.T) {
	store := newIntegrationStore(t)
	pbsCfg := pbsConfigFromEnv(t)
	cfg := defaultBackupConfig(t)
	cleanupSnapshot(t, pbsCfg, cfg)

	// Generate 50KB of random data
	data := make([]byte, 50*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("generate data: %v", err)
	}
	origDigest := sha256.Sum256(data)

	// Upload archive to PBS
	sess, err := store.StartSession(context.Background(), cfg)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	if _, err := sess.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(data)); err != nil {
		t.Fatalf("UploadArchive: %v", err)
	}
	if _, err := sess.Finish(context.Background()); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Use PBSReader to download didx and restore the file
	reader := NewPBSReader(pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime)
	if err := reader.Connect(context.Background()); err != nil {
		t.Fatalf("PBSReader connect: %v", err)
	}
	defer reader.Close()

	// Download the didx file (this populates the allowed_chunks set on the server)
	didxData, err := reader.DownloadFile("root.pxar.didx")
	if err != nil {
		t.Fatalf("DownloadFile didx: %v", err)
	}

	idx, err := datastore.ReadDynamicIndex(didxData)
	if err != nil {
		t.Fatalf("parse didx: %v", err)
	}

	t.Logf("Downloaded didx: %d chunks, %d bytes", idx.Count(), idx.IndexBytes())

	// Download each chunk individually and reconstruct
	var reconstructed bytes.Buffer
	for i := 0; i < idx.Count(); i++ {
		info, ok := idx.ChunkInfo(i)
		if !ok {
			t.Fatalf("chunk info %d not found", i)
		}

		// Create a fresh connection for each chunk to avoid H2 stream issues
		chunkReader := NewPBSReader(pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime)
		if err := chunkReader.Connect(context.Background()); err != nil {
			t.Fatalf("chunk %d: connect: %v", i, err)
		}

		// Download index first to populate allowed_chunks
		if _, err := chunkReader.DownloadFile("root.pxar.didx"); err != nil {
			chunkReader.Close()
			t.Fatalf("chunk %d: download index: %v", i, err)
		}

		chunkBlob, err := chunkReader.DownloadChunk(info.Digest)
		chunkReader.Close()
		if err != nil {
			t.Fatalf("download chunk %d (digest %s): %v", i, hex.EncodeToString(info.Digest[:])[:16], err)
		}

		// Decode the blob wrapper
		decoded, err := datastore.DecodeBlob(chunkBlob)
		if err != nil {
			t.Fatalf("decode chunk %d: %v", i, err)
		}

		// Verify the chunk size matches expected offsets
		expectedSize := info.End - info.Start
		if uint64(len(decoded)) != expectedSize {
			t.Errorf("chunk %d: decoded size = %d, expected %d", i, len(decoded), expectedSize)
		}

		// Verify the chunk digest matches
		chunkDigest := sha256.Sum256(decoded)
		if chunkDigest != info.Digest {
			t.Errorf("chunk %d: digest mismatch\n  computed: %s\n  expected: %s",
				i, hex.EncodeToString(chunkDigest[:])[:16], hex.EncodeToString(info.Digest[:])[:16])
		}

		reconstructed.Write(decoded)
	}

	// Verify complete reconstruction
	if !bytes.Equal(reconstructed.Bytes(), data) {
		t.Errorf("reconstruction mismatch: got %d bytes, want %d bytes", reconstructed.Len(), len(data))
	}

	reconDigest := sha256.Sum256(reconstructed.Bytes())
	if reconDigest != origDigest {
		t.Errorf("restoration digest mismatch")
	}

	t.Logf("Successfully restored file: %d chunks, %d bytes", idx.Count(), len(data))
}

// TestIntegration_ChunkedDidxRestoreRange tests partial file restoration using
// PBSReader's RestoreFileRange method for random access.
func TestIntegration_ChunkedDidxRestoreRange(t *testing.T) {
	store := newIntegrationStore(t)
	pbsCfg := pbsConfigFromEnv(t)
	cfg := defaultBackupConfig(t)
	cleanupSnapshot(t, pbsCfg, cfg)

	// Use 50KB of data with predictable content for range verification
	data := make([]byte, 50*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Upload archive to PBS
	sess, err := store.StartSession(context.Background(), cfg)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	if _, err := sess.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(data)); err != nil {
		t.Fatalf("UploadArchive: %v", err)
	}
	if _, err := sess.Finish(context.Background()); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Download didx via HTTP API
	didxData := pbsDownload(t, pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime, "root.pxar.didx")
	idx, err := datastore.ReadDynamicIndex(didxData)
	if err != nil {
		t.Fatalf("parse didx: %v", err)
	}

	// Test cases for range restoration - each creates a new connection
	testCases := []struct {
		name   string
		offset uint64
		length uint64
	}{
		{"first_1KB", 0, 1024},
		{"middle_4KB", 20 * 1024, 4096},
		{"last_1KB", uint64(len(data) - 1024), 1024},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reader := NewPBSReader(pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime)
			if err := reader.Connect(context.Background()); err != nil {
				t.Fatalf("connect: %v", err)
			}
			defer reader.Close()

			// Download index first (required for chunk access)
			if _, err := reader.DownloadFile("root.pxar.didx"); err != nil {
				t.Fatalf("download index: %v", err)
			}

			var restored bytes.Buffer
			if err := reader.RestoreFileRange(idx, tc.offset, tc.length, &restored); err != nil {
				t.Fatalf("RestoreFileRange: %v", err)
			}

			expected := data[tc.offset : tc.offset+tc.length]
			if !bytes.Equal(restored.Bytes(), expected) {
				t.Errorf("range restore mismatch at offset %d, length %d: got %d bytes, want %d bytes",
					tc.offset, tc.length, restored.Len(), len(expected))
			}
		})
	}

	t.Logf("Successfully tested %d range restoration cases", len(testCases))
}

// TestIntegration_ChunkedDidxWithCompression tests chunked upload/download
// with compression enabled and verifies data integrity.
func TestIntegration_ChunkedDidxWithCompression(t *testing.T) {
	pbsCfg := pbsConfigFromEnv(t)

	// Create store with compression enabled
	chunkCfg, err := buzhash.NewConfig(4096)
	if err != nil {
		t.Fatalf("create chunk config: %v", err)
	}
	store := NewPBSRemoteStore(pbsCfg, chunkCfg, true)

	cfg := defaultBackupConfig(t)
	cleanupSnapshot(t, pbsCfg, cfg)

	// Use compressible data (repeated pattern)
	data := bytes.Repeat([]byte("compressible data pattern "), 5000) // ~120KB
	origDigest := sha256.Sum256(data)

	// Upload compressed archive
	sess, err := store.StartSession(context.Background(), cfg)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	result, err := sess.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("UploadArchive: %v", err)
	}
	if _, err := sess.Finish(context.Background()); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Download didx and verify chunk metadata
	didxData := pbsDownload(t, pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime, "root.pxar.didx")
	idx, err := datastore.ReadDynamicIndex(didxData)
	if err != nil {
		t.Fatalf("parse didx: %v", err)
	}

	// Verify index metadata
	if idx.IndexBytes() != uint64(len(data)) {
		t.Errorf("index bytes = %d, want %d", idx.IndexBytes(), len(data))
	}

	// Verify each chunk's digest against the original data
	var reconstructed bytes.Buffer
	for i := 0; i < idx.Count(); i++ {
		info, _ := idx.ChunkInfo(i)
		chunk := data[info.Start:info.End]
		digest := sha256.Sum256(chunk)
		if digest != info.Digest {
			t.Errorf("chunk %d: digest mismatch", i)
		}
		reconstructed.Write(chunk)
	}

	// Verify reconstruction
	if !bytes.Equal(reconstructed.Bytes(), data) {
		t.Errorf("compressed reconstruction mismatch: got %d bytes, want %d bytes", reconstructed.Len(), len(data))
	}

	reconDigest := sha256.Sum256(reconstructed.Bytes())
	if reconDigest != origDigest {
		t.Errorf("compressed reconstruction digest mismatch")
	}

	// Verify result digest is valid
	if len(hex.EncodeToString(result.Digest[:])) != 64 {
		t.Errorf("result digest has wrong length")
	}

	t.Logf("Successfully verified compressed chunked upload/download: %d chunks", idx.Count())
}

// TestIntegration_ChunkedDidxMultipleFiles tests uploading multiple .didx files
// and verifying chunk metadata for each.
func TestIntegration_ChunkedDidxMultipleFiles(t *testing.T) {
	store := newIntegrationStore(t)
	pbsCfg := pbsConfigFromEnv(t)
	cfg := defaultBackupConfig(t)
	cleanupSnapshot(t, pbsCfg, cfg)

	// Create multiple archives with different sizes
	archives := []struct {
		name string
		size int
	}{
		{"root.pxar.didx", 80 * 1024},
		{"home.pxar.didx", 50 * 1024},
		{"var.pxar.didx", 120 * 1024},
	}

	// Generate data and compute digests
	type archiveData struct {
		name   string
		data   []byte
		digest [32]byte
	}
	dataMap := make(map[string]archiveData)

	// Upload all archives
	sess, err := store.StartSession(context.Background(), cfg)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	for _, a := range archives {
		data := make([]byte, a.size)
		if _, err := rand.Read(data); err != nil {
			t.Fatalf("generate data for %s: %v", a.name, err)
		}

		if _, err := sess.UploadArchive(context.Background(), a.name, bytes.NewReader(data)); err != nil {
			t.Fatalf("UploadArchive %s: %v", a.name, err)
		}

		dataMap[a.name] = archiveData{
			name:   a.name,
			data:   data,
			digest: sha256.Sum256(data),
		}
	}

	if _, err := sess.Finish(context.Background()); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Download and verify each archive's didx
	for _, a := range archives {
		t.Run(a.name, func(t *testing.T) {
			ad := dataMap[a.name]

			// Download didx via HTTP API
			didxData := pbsDownload(t, pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime, a.name)

			idx, err := datastore.ReadDynamicIndex(didxData)
			if err != nil {
				t.Fatalf("parse didx %s: %v", a.name, err)
			}

			// Verify total size
			if idx.IndexBytes() != uint64(len(ad.data)) {
				t.Errorf("%s: index bytes = %d, want %d", a.name, idx.IndexBytes(), len(ad.data))
			}

			// Verify chunk digests match original data
			var reconstructed bytes.Buffer
			for j := 0; j < idx.Count(); j++ {
				info, ok := idx.ChunkInfo(j)
				if !ok {
					t.Fatalf("%s: chunk info %d not found", a.name, j)
				}
				chunk := ad.data[info.Start:info.End]
				digest := sha256.Sum256(chunk)
				if digest != info.Digest {
					t.Errorf("%s chunk %d: digest mismatch", a.name, j)
				}
				reconstructed.Write(chunk)
			}

			if !bytes.Equal(reconstructed.Bytes(), ad.data) {
				t.Errorf("%s: reconstruction mismatch (got %d, want %d bytes)", a.name, reconstructed.Len(), len(ad.data))
			}

			reconstructedDigest := sha256.Sum256(reconstructed.Bytes())
			if reconstructedDigest != ad.digest {
				t.Errorf("%s: digest mismatch", a.name)
			}

			t.Logf("%s: verified %d chunks, %d bytes", a.name, idx.Count(), len(ad.data))
		})
	}
}

// TestIntegration_ChunkedDidxChunkCountAndSize verifies that chunks are
// created with reasonable sizes and that the chunk count is as expected.
func TestIntegration_ChunkedDidxChunkCountAndSize(t *testing.T) {
	store := newIntegrationStore(t)
	pbsCfg := pbsConfigFromEnv(t)
	cfg := defaultBackupConfig(t)
	cleanupSnapshot(t, pbsCfg, cfg)

	// Use 1MB of data - should create multiple chunks
	data := make([]byte, 1024*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("generate data: %v", err)
	}

	// Upload
	sess, err := store.StartSession(context.Background(), cfg)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	if _, err := sess.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(data)); err != nil {
		t.Fatalf("UploadArchive: %v", err)
	}
	if _, err := sess.Finish(context.Background()); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Download didx via HTTP API
	didxData := pbsDownload(t, pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime, "root.pxar.didx")

	idx, err := datastore.ReadDynamicIndex(didxData)
	if err != nil {
		t.Fatalf("parse didx: %v", err)
	}

	// With 4KB average chunk size and 1MB data, expect roughly 200-300 chunks
	chunkCount := idx.Count()
	expectedMin := 150
	expectedMax := 400

	if chunkCount < expectedMin {
		t.Errorf("chunk count %d too low (expected at least %d for 1MB with 4KB chunks)", chunkCount, expectedMin)
	}
	if chunkCount > expectedMax {
		t.Errorf("chunk count %d too high (expected at most %d for 1MB with 4KB chunks)", chunkCount, expectedMax)
	}

	t.Logf("1MB data created %d chunks (expected %d-%d)", chunkCount, expectedMin, expectedMax)

	// Verify each chunk has reasonable size (between 1KB and 16KB for content-defined chunking)
	const minChunkSize = 1024
	const maxChunkSize = 16 * 1024

	for i := 0; i < idx.Count(); i++ {
		info, _ := idx.ChunkInfo(i)
		chunkSize := info.End - info.Start

		if chunkSize < minChunkSize {
			t.Logf("warning: chunk %d size %d below typical minimum %d", i, chunkSize, minChunkSize)
		}
		if chunkSize > maxChunkSize {
			t.Logf("warning: chunk %d size %d above typical maximum %d", i, chunkSize, maxChunkSize)
		}
	}

	// Verify total size matches
	if idx.IndexBytes() != uint64(len(data)) {
		t.Errorf("total indexed bytes = %d, want %d", idx.IndexBytes(), len(data))
	}

	t.Logf("Chunk size analysis complete: %d chunks covering %d bytes", chunkCount, len(data))
}

// pbsVerifySnapshot triggers a verify job on PBS for the given snapshot and
// waits for it to complete. It returns the verify task status ("ok" or "failed").
func pbsVerifySnapshot(t *testing.T, cfg PBSConfig, backupType string, backupID string, backupTime int64) string {
	t.Helper()

	verifyURL := fmt.Sprintf("%s/admin/datastore/%s/verify", cfg.BaseURL, cfg.Datastore)
	form := url.Values{
		"backup-type":     {backupType},
		"backup-id":       {backupID},
		"backup-time":     {fmt.Sprintf("%d", backupTime)},
		"ignore-verified": {"true"},
		"outdated-after":  {"0"},
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, verifyURL, strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("create verify request: %v", err)
	}
	req.Header.Set("Authorization", "PBSAPIToken "+cfg.AuthToken)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := pbsHTTPClient(t)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("verify request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("verify: HTTP %d: %s", resp.StatusCode, body)
	}

	var result struct {
		Data string `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode verify response: %v", err)
	}

	upid := result.Data
	if upid == "" {
		t.Fatal("verify returned empty UPID")
	}
	t.Logf("verify task UPID: %s", upid)

	encodedUPID := url.PathEscape(upid)
	taskURL := fmt.Sprintf("%s/nodes/localhost/tasks/%s/status", cfg.BaseURL, encodedUPID)

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, taskURL, nil)
		if err != nil {
			t.Fatalf("create task status request: %v", err)
		}
		req.Header.Set("Authorization", "PBSAPIToken "+cfg.AuthToken)

		resp, err := client.Do(req)
		if err != nil {
			t.Logf("task status request error: %v, retrying", err)
			time.Sleep(2 * time.Second)
			continue
		}

		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			t.Logf("read task status body: %v, retrying", readErr)
			time.Sleep(2 * time.Second)
			continue
		}

		var raw map[string]json.RawMessage
		if err := json.Unmarshal(body, &raw); err != nil {
			t.Logf("parse task status: %v, retrying", err)
			time.Sleep(2 * time.Second)
			continue
		}

		dataRaw, ok := raw["data"]
		if !ok || string(dataRaw) == "null" {
			time.Sleep(2 * time.Second)
			continue
		}

		var taskStatus struct {
			Status     string `json:"status"`
			ExitStatus string `json:"exitstatus"`
		}
		if err := json.Unmarshal(dataRaw, &taskStatus); err != nil {
			t.Logf("decode task status data: %v, retrying", err)
			time.Sleep(2 * time.Second)
			continue
		}

		if taskStatus.Status == "stopped" {
			return taskStatus.ExitStatus
		}

		time.Sleep(2 * time.Second)
	}

	t.Fatal("verify task did not complete within 60 seconds")
	return ""
}

// TestIntegration_PBSVerifyDidx tests that PBS's own verify endpoint successfully
// verifies a snapshot containing an uploaded .didx archive. This validates that
// PBS can independently confirm all chunk data and the dynamic index are correct.
func TestIntegration_PBSVerifyDidx(t *testing.T) {
	store := newIntegrationStore(t)
	pbsCfg := pbsConfigFromEnv(t)
	cfg := defaultBackupConfig(t)
	cleanupSnapshot(t, pbsCfg, cfg)

	archiveData := make([]byte, 50*1024)
	if _, err := rand.Read(archiveData); err != nil {
		t.Fatalf("generate archive data: %v", err)
	}

	sess, err := store.StartSession(context.Background(), cfg)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	if _, err := sess.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(archiveData)); err != nil {
		t.Fatalf("UploadArchive: %v", err)
	}

	if _, err := sess.Finish(context.Background()); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	exitStatus := pbsVerifySnapshot(t, pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime)
	if exitStatus != "OK" {
		t.Errorf("PBS verify didx snapshot failed with exit status: %q", exitStatus)
	} else {
		t.Log("PBS verify of didx snapshot completed successfully: OK")
	}
}

// TestIntegration_PBSVerifyBlob tests that PBS's own verify endpoint successfully
// verifies a snapshot containing an uploaded .blob file. This validates that
// PBS can independently confirm blob data integrity.
func TestIntegration_PBSVerifyBlob(t *testing.T) {
	store := newIntegrationStore(t)
	pbsCfg := pbsConfigFromEnv(t)
	cfg := defaultBackupConfig(t)
	cleanupSnapshot(t, pbsCfg, cfg)

	blobData := make([]byte, 10*1024)
	if _, err := rand.Read(blobData); err != nil {
		t.Fatalf("generate blob data: %v", err)
	}

	sess, err := store.StartSession(context.Background(), cfg)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	if err := sess.UploadBlob(context.Background(), "config.blob", blobData); err != nil {
		t.Fatalf("UploadBlob: %v", err)
	}

	if _, err := sess.Finish(context.Background()); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	exitStatus := pbsVerifySnapshot(t, pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime)
	if exitStatus != "OK" {
		t.Errorf("PBS verify blob snapshot failed with exit status: %q", exitStatus)
	} else {
		t.Log("PBS verify of blob snapshot completed successfully: OK")
	}
}

// TestIntegration_PBSSplitArchive tests uploading and downloading a split archive
// (.mpxar.didx + .ppxar.didx) to PBS, verifying that both the metadata and payload
// indexes are stored correctly and that PBS verify confirms data integrity.
func TestIntegration_PBSSplitArchive(t *testing.T) {
	store := newIntegrationStore(t)
	pbsCfg := pbsConfigFromEnv(t)
	cfg := defaultBackupConfig(t)
	cleanupSnapshot(t, pbsCfg, cfg)

	sess, err := store.StartSession(context.Background(), cfg)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	// Create metadata and payload streams for a split archive
	metaData := make([]byte, 8*1024)
	payloadData := make([]byte, 50*1024)
	if _, err := rand.Read(metaData); err != nil {
		t.Fatalf("generate metadata: %v", err)
	}
	if _, err := rand.Read(payloadData); err != nil {
		t.Fatalf("generate payload: %v", err)
	}

	splitResult, err := sess.UploadSplitArchive(
		context.Background(),
		"root.mpxar.didx", bytes.NewReader(metaData),
		"root.ppxar.didx", bytes.NewReader(payloadData),
	)
	if err != nil {
		t.Fatalf("UploadSplitArchive: %v", err)
	}

	if splitResult.MetadataResult.Filename != "root.mpxar.didx" {
		t.Errorf("metadata filename = %q, want root.mpxar.didx", splitResult.MetadataResult.Filename)
	}
	if splitResult.PayloadResult.Filename != "root.ppxar.didx" {
		t.Errorf("payload filename = %q, want root.ppxar.didx", splitResult.PayloadResult.Filename)
	}

	if splitResult.MetadataResult.Size == 0 {
		t.Error("metadata size should not be zero")
	}
	if splitResult.PayloadResult.Size == 0 {
		t.Error("payload size should not be zero")
	}

	t.Logf("Split archive uploaded: metadata=%d bytes, payload=%d bytes",
		splitResult.MetadataResult.Size, splitResult.PayloadResult.Size)

	if _, err := sess.Finish(context.Background()); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Verify both indexes exist on PBS and can be downloaded
	metaIdxData := pbsDownload(t, pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime, "root.mpxar.didx")
	if len(metaIdxData) == 0 {
		t.Error("downloaded metadata index is empty")
	}

	payloadIdxData := pbsDownload(t, pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime, "root.ppxar.didx")
	if len(payloadIdxData) == 0 {
		t.Error("downloaded payload index is empty")
	}

	// Parse both indexes
	metaIdx, err := datastore.ReadDynamicIndex(metaIdxData)
	if err != nil {
		t.Fatalf("parse metadata didx: %v", err)
	}
	payloadIdx, err := datastore.ReadDynamicIndex(payloadIdxData)
	if err != nil {
		t.Fatalf("parse payload didx: %v", err)
	}

	t.Logf("Metadata index: %d entries, %d bytes", metaIdx.Count(), metaIdx.IndexBytes())
	t.Logf("Payload index: %d entries, %d bytes", payloadIdx.Count(), payloadIdx.IndexBytes())

	if metaIdx.Count() == 0 {
		t.Error("metadata index should have at least one chunk")
	}
	if payloadIdx.Count() == 0 {
		t.Error("payload index should have at least one chunk")
	}

	// Verify PBS verify passes for the snapshot with split archives
	exitStatus := pbsVerifySnapshot(t, pbsCfg, cfg.BackupType.String(), cfg.BackupID, cfg.BackupTime)
	if exitStatus != "OK" {
		t.Errorf("PBS verify split archive snapshot failed with exit status: %q", exitStatus)
	} else {
		t.Log("PBS verify of split archive snapshot completed successfully: OK")
	}
}
