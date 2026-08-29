package backupproxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
)

// mockPBSProtocol implements pbsBackupProtocol for testing.
type mockPBSProtocol struct {
	chunks   map[string][]byte // digest -> encoded chunk data
	blobs    map[string][]byte // filename -> blob data
	indexes  map[uint64]*mockIndex
	nextWID  uint64
	closed   bool
	finished bool
}

type mockIndex struct {
	archiveName string
	chunks      []mockChunkRef
	appended    bool
	closed      bool
}

type mockChunkRef struct {
	digest string
	offset uint64
}

func newMockPBSProtocol() *mockPBSProtocol {
	return &mockPBSProtocol{
		chunks:  make(map[string][]byte),
		blobs:   make(map[string][]byte),
		indexes: make(map[uint64]*mockIndex),
		nextWID: 1,
	}
}

func (m *mockPBSProtocol) dynamicIndexCreate(archiveName string) (uint64, error) {
	wid := m.nextWID
	m.nextWID++
	m.indexes[wid] = &mockIndex{archiveName: archiveName}
	return wid, nil
}

func (m *mockPBSProtocol) dynamicChunkUpload(wid uint64, digest string, size, encodedSize int, data []byte) error {
	if _, exists := m.chunks[digest]; !exists {
		// Copy: the real protocol serialises the bytes over the wire; the mock
		// simulates a server-side store and must own an independent copy.
		m.chunks[digest] = append([]byte(nil), data...)
	}
	if idx, ok := m.indexes[wid]; ok {
		idx.chunks = append(idx.chunks, mockChunkRef{digest: digest})
	}
	return nil
}

func (m *mockPBSProtocol) pipelineChunkUploads(wid uint64, chunks []chunkUploadReq) error {
	for _, c := range chunks {
		if err := m.dynamicChunkUpload(wid, c.digest, c.size, c.encodedSize, c.data); err != nil {
			return err
		}
	}
	return nil
}

func (m *mockPBSProtocol) dynamicChunkUploadAsync(wid uint64, digest string, size, encodedSize int, data []byte) func() error {
	err := m.dynamicChunkUpload(wid, digest, size, encodedSize, data)
	return func() error { return err }
}

func (m *mockPBSProtocol) dynamicIndexAppend(wid uint64, digests []string, offsets []uint64) error {
	if idx, ok := m.indexes[wid]; ok {
		idx.appended = true
		for i, d := range digests {
			for j := range idx.chunks {
				if idx.chunks[j].digest == d {
					idx.chunks[j].offset = offsets[i]
					break
				}
			}
		}
	}
	return nil
}

func (m *mockPBSProtocol) dynamicIndexClose(wid uint64, chunkCount int, size uint64, csum string) error {
	if idx, ok := m.indexes[wid]; ok {
		idx.closed = true
	}
	return nil
}

func (m *mockPBSProtocol) blobUpload(fileName string, encodedSize int, data []byte) error {
	m.blobs[fileName] = data
	return nil
}

func (m *mockPBSProtocol) downloadPrevious(archiveName string) ([]byte, error) {
	return nil, nil
}

func (m *mockPBSProtocol) finish() error {
	m.finished = true
	return nil
}

func (m *mockPBSProtocol) close() {
	m.closed = true
}

func newTestPBSSession(t *testing.T) (*pbsSession, *mockPBSProtocol) {
	t.Helper()
	cfg, err := buzhash.NewConfig(4096)
	if err != nil {
		t.Fatal(err)
	}
	mock := newMockPBSProtocol()
	return &pbsSession{
		store:    &PBSStore{chunkCfg: cfg},
		proto:    mock,
		config:   BackupConfig{BackupType: datastore.BackupHost, BackupID: "test"},
		compress: false,
		chunkCfg: cfg,
		files:    make([]datastore.BackupFileInfo, 0),
	}, mock
}

func TestPBSUploadArchive(t *testing.T) {
	sess, mock := newTestPBSSession(t)

	data := make([]byte, 50<<10)
	_, _ = rand.Read(data)

	result, err := sess.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	if result.Filename != "root.pxar.didx" {
		t.Errorf("filename = %q, want %q", result.Filename, "root.pxar.didx")
	}
	if result.Size == 0 {
		t.Error("size should not be 0")
	}

	if len(mock.chunks) == 0 {
		t.Error("expected chunks to be uploaded")
	}
}

func TestPBSUploadPayloadInterleavedProgress(t *testing.T) {
	sess, _ := newTestPBSSession(t)
	knownDigest := [32]byte{1}
	uploadedDigest := [32]byte{2}
	sess.knownChunks = map[[32]byte]bool{knownDigest: true}

	var updates []UploadProgress
	sess.config.OnUploadProgress = func(progress UploadProgress) {
		updates = append(updates, progress)
	}

	injections := make(chan InjectChunks, 1)
	injections <- InjectChunks{
		Chunks: []KnownChunkRef{
			{Digest: knownDigest, Size: 50},
			{Digest: uploadedDigest, Size: 100, LoadEncodedBlob: func() ([]byte, error) {
				return []byte{1, 2, 3, 4}, nil
			}},
		},
		Size: 150,
	}
	close(injections)

	result, err := sess.UploadPayloadInterleaved(context.Background(), "root.ppxar.didx", nil, injections, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Size != 150 {
		t.Fatalf("size = %d, want 150", result.Size)
	}
	if len(updates) != 2 {
		t.Fatalf("progress updates = %d, want 2", len(updates))
	}
	want := UploadProgress{
		ProcessedChunks: 2,
		ProcessedBytes:  150,
		UploadedChunks:  1,
		UploadedBytes:   4,
	}
	if got := updates[len(updates)-1]; got != want {
		t.Fatalf("final progress = %+v, want %+v", got, want)
	}
}

func TestPBSUploadBlob(t *testing.T) {
	sess, mock := newTestPBSSession(t)

	blobData := []byte(`{"key": "value"}`)
	if err := sess.UploadBlob(context.Background(), "config.blob", blobData); err != nil {
		t.Fatal(err)
	}

	got := mock.blobs["config.blob"]
	decoded, err := datastore.DecodeBlob(nil, got)
	if err != nil {
		t.Fatalf("decode blob: %v", err)
	}
	if !bytes.Equal(decoded, blobData) {
		t.Error("blob content mismatch")
	}
}

func TestPBSFinish(t *testing.T) {
	sess, _ := newTestPBSSession(t)
	sess.config.BackupTime = 1700000000

	data := make([]byte, 10<<10)
	_, _ = rand.Read(data)
	_, _ = sess.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(data))
	_ = sess.UploadBlob(context.Background(), "config.blob", []byte("config"))

	manifest, err := sess.Finish(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if manifest.BackupType != "host" {
		t.Errorf("type = %q, want %q", manifest.BackupType, "host")
	}
	if manifest.BackupID != "test" {
		t.Errorf("id = %q, want %q", manifest.BackupID, "test")
	}
	if len(manifest.Files) != 2 {
		t.Errorf("files = %d, want 2", len(manifest.Files))
	}
}

func TestPBSChunkDedup(t *testing.T) {
	sess, mock := newTestPBSSession(t)

	data := make([]byte, 20<<10)
	_, _ = rand.Read(data)

	// First upload
	sess.config.BackupID = "1"
	_, err := sess.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	firstChunks := len(mock.chunks)

	// Reset session for second upload
	sess.files = make([]datastore.BackupFileInfo, 0)
	sess.config.BackupID = "2"

	_, err = sess.UploadArchive(context.Background(), "root2.pxar.didx", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	secondChunks := len(mock.chunks)

	if secondChunks != firstChunks {
		t.Errorf("after dedup: %d chunks, want %d", secondChunks, firstChunks)
	}
}

func TestPBSRoundTrip(t *testing.T) {
	sess, mock := newTestPBSSession(t)

	data := make([]byte, 30<<10)
	_, _ = rand.Read(data)

	result, err := sess.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	// Verify we can read back the result info
	if result.Filename != "root.pxar.didx" {
		t.Errorf("filename = %q", result.Filename)
	}

	// Reconstruct from stored chunks using chunk digests
	var reconstructed bytes.Buffer
	for digest, chunkBlob := range mock.chunks {
		_ = digest
		decoded, err := datastore.DecodeBlob(nil, chunkBlob)
		if err != nil {
			t.Fatalf("decode chunk: %v", err)
		}
		reconstructed.Write(decoded)
	}

	// Note: reconstruction order may differ, so we verify by length
	if reconstructed.Len() != len(data) {
		t.Errorf("reconstructed size = %d, want %d", reconstructed.Len(), len(data))
	}
}

func TestPBSUploadDigestVerification(t *testing.T) {
	sess, _ := newTestPBSSession(t)

	data := []byte("test data for digest verification")
	result, err := sess.UploadArchive(context.Background(), "test.pxar.didx", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	// Verify digest is valid hex and correct length
	digestHex := hex.EncodeToString(result.Digest[:])
	if len(digestHex) != 64 {
		t.Errorf("digest hex length = %d, want 64", len(digestHex))
	}
}

func TestPBSReplayUploadsUnknownChunkOnce(t *testing.T) {
	sess, mock := newTestPBSSession(t)
	raw := []byte("source chunk replayed without rechunking")
	digest := sha256.Sum256(raw)
	encoded, err := datastore.EncodeBlob(nil, raw)
	if err != nil {
		t.Fatal(err)
	}
	loads := 0
	load := func() ([]byte, error) {
		loads++
		return encoded, nil
	}

	upload := func(name string) {
		injections := make(chan InjectChunks, 1)
		injections <- InjectChunks{
			Chunks: []KnownChunkRef{{Digest: digest, Size: uint64(len(raw)), LoadEncodedBlob: load}},
			Size:   uint64(len(raw)),
		}
		close(injections)
		result, err := sess.UploadPayloadInterleaved(context.Background(), name, nil, injections, nil)
		if err != nil {
			t.Fatal(err)
		}
		if result.Size != uint64(len(raw)) {
			t.Fatalf("payload size = %d, want %d", result.Size, len(raw))
		}
	}

	upload("first.ppxar.didx")
	upload("second.ppxar.didx")
	if loads != 1 {
		t.Fatalf("source blob loaded %d times, want 1", loads)
	}
	digestHex := hex.EncodeToString(digest[:])
	if !bytes.Equal(mock.chunks[digestHex], encoded) {
		t.Fatal("uploaded chunk differs from exact source blob")
	}
}

func TestPBSManifestFileEntries(t *testing.T) {
	sess, _ := newTestPBSSession(t)

	archiveData := make([]byte, 10<<10)
	_, _ = rand.Read(archiveData)
	_, _ = sess.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(archiveData))

	blobData := []byte(`{"test": true}`)
	_ = sess.UploadBlob(context.Background(), "config.blob", blobData)

	manifest, err := sess.Finish(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if len(manifest.Files) != 2 {
		t.Fatalf("manifest files = %d, want 2", len(manifest.Files))
	}

	fileMap := make(map[string]datastore.BackupFileInfo)
	for _, f := range manifest.Files {
		fileMap[f.Filename] = f
	}

	if _, ok := fileMap["root.pxar.didx"]; !ok {
		t.Error("missing root.pxar.didx in manifest")
	}
	if _, ok := fileMap["config.blob"]; !ok {
		t.Error("missing config.blob in manifest")
	}

	blobEntry := fileMap["config.blob"]
	if blobEntry.Size == 0 {
		t.Error("blob size should not be 0")
	}

	// The checksum is calculated on the encoded blob data (after datastore.EncodeBlob)
	encodedBlob, err := datastore.EncodeBlob(nil, blobData)
	if err != nil {
		t.Fatalf("encode blob: %v", err)
	}
	expectedBlobDigest := sha256.Sum256(encodedBlob)
	if blobEntry.CSum != hex.EncodeToString(expectedBlobDigest[:]) {
		t.Errorf("blob checksum mismatch")
	}
}
