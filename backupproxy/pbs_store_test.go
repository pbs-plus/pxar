package backupproxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
)

// mockPBSServer simulates a PBS backup server for testing.
type mockPBSServer struct {
	server     *httptest.Server
	chunks     map[string][]byte
	files      map[string][]byte
	manifest   []byte
	mu         sync.Mutex
	token      string
}

func newMockPBSServer(t *testing.T) *mockPBSServer {
	t.Helper()
	m := &mockPBSServer{
		chunks: make(map[string][]byte),
		files:  make(map[string][]byte),
		token:  "test-token",
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/admin/datastore/test/backup", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("Authorization") != "PBSAPIToken "+m.token {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/admin/datastore/test/chunk", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "PBSAPIToken "+m.token {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		digest := r.URL.Query().Get("digest")
		if digest == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		defer m.mu.Unlock()

		switch r.Method {
		case http.MethodGet:
			if _, ok := m.chunks[digest]; ok {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		case http.MethodPut:
			data, _ := io.ReadAll(r.Body)
			m.chunks[digest] = data
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/admin/datastore/test/blob", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		file := r.URL.Query().Get("file")
		if file == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		data, _ := io.ReadAll(r.Body)
		m.mu.Lock()
		m.files[file] = data
		m.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/admin/datastore/test/finish", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		data, _ := io.ReadAll(r.Body)
		m.manifest = data
		w.WriteHeader(http.StatusOK)
	})

	m.server = httptest.NewServer(mux)
	t.Cleanup(m.server.Close)
	return m
}

func newTestPBSStore(t *testing.T, srv *mockPBSServer) *PBSRemoteStore {
	t.Helper()
	config, _ := buzhash.NewConfig(4096)
	return NewPBSRemoteStore(PBSConfig{
		BaseURL:   srv.server.URL,
		Datastore: "test",
		AuthToken: "test-token",
	}, config, false)
}

func TestPBSStartSession(t *testing.T) {
	srv := newMockPBSServer(t)
	store := newTestPBSStore(t, srv)

	sess, err := store.StartSession(context.Background(), BackupConfig{
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

func TestPBSStartSessionAuth(t *testing.T) {
	srv := newMockPBSServer(t)
	config, _ := buzhash.NewConfig(4096)
	store := NewPBSRemoteStore(PBSConfig{
		BaseURL:   srv.server.URL,
		Datastore: "test",
		AuthToken: "wrong-token",
	}, config, false)

	_, err := store.StartSession(context.Background(), BackupConfig{
		BackupType: datastore.BackupVM,
		BackupID:   "100",
	})
	if err == nil {
		t.Error("expected auth error")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("error should mention 401, got: %v", err)
	}
}

func TestPBSUploadArchive(t *testing.T) {
	srv := newMockPBSServer(t)
	store := newTestPBSStore(t, srv)

	sess, _ := store.StartSession(context.Background(), BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   "myhost",
	})

	data := make([]byte, 50<<10)
	rand.Read(data)

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

	// Verify chunks were uploaded
	srv.mu.Lock()
	chunkCount := len(srv.chunks)
	idxData := srv.files["root.pxar.didx"]
	srv.mu.Unlock()

	if chunkCount == 0 {
		t.Error("expected chunks to be uploaded")
	}

	// Verify index file was uploaded and is valid
	if len(idxData) == 0 {
		t.Fatal("expected index file to be uploaded")
	}
	reader, err := datastore.ReadDynamicIndex(idxData)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}
	if reader.Count() != chunkCount {
		t.Errorf("index has %d chunks, want %d", reader.Count(), chunkCount)
	}
}

func TestPBSUploadBlob(t *testing.T) {
	srv := newMockPBSServer(t)
	store := newTestPBSStore(t, srv)

	sess, _ := store.StartSession(context.Background(), BackupConfig{
		BackupType: datastore.BackupCT,
		BackupID:   "200",
	})

	blobData := []byte(`{"key": "value"}`)
	if err := sess.UploadBlob(context.Background(), "config.json", blobData); err != nil {
		t.Fatal(err)
	}

	srv.mu.Lock()
	got := srv.files["config.json"]
	srv.mu.Unlock()

	if !bytes.Equal(got, blobData) {
		t.Error("blob content mismatch")
	}
}

func TestPBSFinish(t *testing.T) {
	srv := newMockPBSServer(t)
	store := newTestPBSStore(t, srv)

	sess, _ := store.StartSession(context.Background(), BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   "myhost",
		BackupTime: 1700000000,
	})

	data := make([]byte, 10<<10)
	rand.Read(data)
	sess.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(data))
	sess.UploadBlob(context.Background(), "config.json", []byte("config"))

	manifest, err := sess.Finish(context.Background())
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
		t.Errorf("files = %d, want 2", len(manifest.Files))
	}

	if len(srv.manifest) == 0 {
		t.Error("manifest should have been sent to server")
	}
}

func TestPBSChunkDedup(t *testing.T) {
	srv := newMockPBSServer(t)
	store := newTestPBSStore(t, srv)

	data := make([]byte, 20<<10)
	rand.Read(data)

	// First upload
	sess1, _ := store.StartSession(context.Background(), BackupConfig{BackupID: "1"})
	_, err := sess1.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	srv.mu.Lock()
	firstChunks := len(srv.chunks)
	srv.mu.Unlock()

	// Second upload with same data — chunks should be deduped
	sess2, _ := store.StartSession(context.Background(), BackupConfig{BackupID: "2"})
	_, err = sess2.UploadArchive(context.Background(), "root2.pxar.didx", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	srv.mu.Lock()
	secondChunks := len(srv.chunks)
	srv.mu.Unlock()

	if secondChunks != firstChunks {
		t.Errorf("after dedup: %d chunks, want %d", secondChunks, firstChunks)
	}
}

func TestPBSErrorHandling(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	config, _ := buzhash.NewConfig(4096)
	store := NewPBSRemoteStore(PBSConfig{
		BaseURL:   srv.URL,
		Datastore: "test",
		AuthToken: "token",
	}, config, false)

	_, err := store.StartSession(context.Background(), BackupConfig{BackupID: "1"})
	if err == nil {
		t.Error("expected error from 500 response")
	}
}

func TestPBSRoundTrip(t *testing.T) {
	srv := newMockPBSServer(t)
	store := newTestPBSStore(t, srv)

	sess, _ := store.StartSession(context.Background(), BackupConfig{
		BackupType: datastore.BackupVM,
		BackupID:   "100",
	})

	data := make([]byte, 30<<10)
	rand.Read(data)

	result, err := sess.UploadArchive(context.Background(), "root.pxar.didx", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	// Read back index from mock server
	srv.mu.Lock()
	idxData := srv.files[result.Filename]
	srv.mu.Unlock()

	reader, err := datastore.ReadDynamicIndex(idxData)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}

	// Reconstruct from stored chunks
	var reconstructed bytes.Buffer
	srv.mu.Lock()
	for i := 0; i < reader.Count(); i++ {
		info, ok := reader.ChunkInfo(i)
		if !ok {
			t.Fatalf("chunk %d not found", i)
		}
		hexDigest := hex.EncodeToString(info.Digest[:])
		chunkBlob, ok := srv.chunks[hexDigest]
		if !ok {
			t.Fatalf("chunk %d (%s) not found on server", i, hexDigest)
		}
		decoded, err := datastore.DecodeBlob(chunkBlob)
		if err != nil {
			t.Fatalf("decode chunk %d: %v", i, err)
		}
		reconstructed.Write(decoded)
	}
	srv.mu.Unlock()

	if !bytes.Equal(reconstructed.Bytes(), data) {
		t.Error("reconstructed data doesn't match original")
	}
}

func TestPBSUploadDigestVerification(t *testing.T) {
	srv := newMockPBSServer(t)
	store := newTestPBSStore(t, srv)

	sess, _ := store.StartSession(context.Background(), BackupConfig{BackupID: "1"})

	data := []byte("test data for digest verification")
	result, err := sess.UploadArchive(context.Background(), "test.pxar.didx", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	// Verify digest is hex-encodable and correct length
	digestHex := hex.EncodeToString(result.Digest[:])
	if len(digestHex) != 64 {
		t.Errorf("digest hex length = %d, want 64", len(digestHex))
	}

	// Verify the digest matches SHA-256 of the stored index
	srv.mu.Lock()
	idxData := srv.files["test.pxar.didx"]
	srv.mu.Unlock()

	expected := sha256.Sum256(idxData)
	if result.Digest != expected {
		t.Error("digest doesn't match SHA-256 of stored index")
	}
}
