//go:build integration

package backupproxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"

	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

	// Verify blob checksum matches SHA-256 of the uploaded data
	expectedBlobDigest := sha256.Sum256(blobData)
	expectedBlobDigestHex := hex.EncodeToString(expectedBlobDigest[:])
	if blobEntry.CSum != expectedBlobDigestHex {
		t.Errorf("blob checksum = %q, want %q", blobEntry.CSum, expectedBlobDigestHex)
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
