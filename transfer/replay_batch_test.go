package transfer

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
)

type replayBatchSession struct {
	mu      sync.Mutex
	batches []int
}

func (s *replayBatchSession) UploadPayloadInterleaved(_ context.Context, _ string, newData io.Reader, injections <-chan backupproxy.InjectChunks) (*backupproxy.UploadResult, error) {
	type copyResult struct {
		n   int64
		err error
	}
	copied := make(chan copyResult, 1)
	go func() {
		n, err := io.Copy(io.Discard, newData)
		copied <- copyResult{n: n, err: err}
	}()
	var size uint64
	for injection := range injections {
		s.mu.Lock()
		s.batches = append(s.batches, len(injection.Chunks))
		s.mu.Unlock()
		size += injection.Size
		if injection.Processed != nil {
			injection.Processed <- nil
		}
	}
	result := <-copied
	if result.err != nil {
		return nil, result.err
	}
	return &backupproxy.UploadResult{Size: size + uint64(result.n)}, nil
}

func (*replayBatchSession) UploadArchive(_ context.Context, _ string, data io.Reader) (*backupproxy.UploadResult, error) {
	n, err := io.Copy(io.Discard, data)
	return &backupproxy.UploadResult{Size: uint64(n)}, err
}

func (*replayBatchSession) UploadSplitArchive(context.Context, string, io.Reader, string, io.Reader) (*backupproxy.SplitArchiveResult, error) {
	return nil, nil
}

func (*replayBatchSession) UploadBlob(context.Context, string, []byte) error { return nil }

func (*replayBatchSession) Finish(context.Context) (*datastore.Manifest, error) {
	return &datastore.Manifest{}, nil
}

func (*replayBatchSession) Close() error { return nil }

func TestCopyBatchesInteriorChunkReplay(t *testing.T) {
	cfg, err := buzhash.NewConfig(4 << 10)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	createChunkedSelectionSource(t, dir, cfg, []byte("secret"), make([]byte, 2<<20))
	metaIndex, err := os.ReadFile(filepath.Join(dir, "root.mpxar.didx"))
	if err != nil {
		t.Fatal(err)
	}
	payloadIndex, err := os.ReadFile(filepath.Join(dir, "root.ppxar.didx"))
	if err != nil {
		t.Fatal(err)
	}
	chunkStore, err := datastore.NewChunkStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewSplitReader(metaIndex, payloadIndex, datastore.NewChunkStoreSource(chunkStore))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	session := &replayBatchSession{}
	writer, err := NewRemoteDedupWriter(context.Background(), session, "target.mpxar.didx", "target.ppxar.didx")
	if err != nil {
		t.Fatal(err)
	}
	root := pxar.DirMetadata(0o755).Build()
	if err := writer.Begin(&root, Options{Format: format.FormatVersion2}); err != nil {
		t.Fatal(err)
	}
	if err := Copy(source, writer, []PathMapping{{Src: "/selected.bin", Dst: "/selected.bin"}}, CopyOption{}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Finish(); err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	var replayed, largest int
	for _, size := range session.batches {
		replayed += size
		largest = max(largest, size)
		if size > replayChunkBatchSize {
			t.Fatalf("batch size = %d, max %d", size, replayChunkBatchSize)
		}
	}
	if replayed <= replayChunkBatchSize || largest <= 1 {
		t.Fatalf("replayed %d chunks in batches %+v", replayed, session.batches)
	}
}
