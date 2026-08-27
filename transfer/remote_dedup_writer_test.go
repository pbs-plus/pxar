package transfer

import (
	"context"
	"io"
	"testing"
	"time"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
)

type drainSession struct{}

func (drainSession) UploadPayloadInterleaved(_ context.Context, _ string, newData io.Reader, injections <-chan backupproxy.InjectChunks) (*backupproxy.UploadResult, error) {
	_, _ = io.Copy(io.Discard, newData)
	for range injections {
	}
	return &backupproxy.UploadResult{}, nil
}
func (drainSession) UploadArchive(context.Context, string, io.Reader) (*backupproxy.UploadResult, error) {
	return &backupproxy.UploadResult{}, nil
}
func (drainSession) UploadSplitArchive(context.Context, string, io.Reader, string, io.Reader) (*backupproxy.SplitArchiveResult, error) {
	return &backupproxy.SplitArchiveResult{}, nil
}
func (drainSession) UploadBlob(context.Context, string, []byte) error { return nil }
func (drainSession) Finish(context.Context) (*datastore.Manifest, error) {
	return &datastore.Manifest{}, nil
}
func (drainSession) Close() error { return nil }

func TestRemoteDedupWriterAbandonNoLeak(t *testing.T) {
	w, err := NewRemoteDedupWriter(context.Background(), drainSession{}, "root.mpxar.didx", "root.ppxar.didx")
	if err != nil {
		t.Fatalf("NewRemoteDedupWriter: %v", err)
	}
	rootMeta := &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755}}
	if err := w.Begin(rootMeta, Options{}); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestRemoteDedupWriterStreamsMoreThanInjectionBuffer(t *testing.T) {
	cfg, err := buzhash.NewConfig(4 << 10)
	if err != nil {
		t.Fatal(err)
	}
	store, err := backupproxy.NewLocalStore(t.TempDir(), cfg, false)
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.StartSession(context.Background(), backupproxy.BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   "injection-buffer",
		BackupTime: time.Now().Unix(),
	})
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		writer, err := NewRemoteDedupWriter(context.Background(), session, "root.mpxar.didx", "root.ppxar.didx")
		if err != nil {
			done <- err
			return
		}
		rootMeta := &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755}}
		if err := writer.Begin(rootMeta, Options{}); err != nil {
			done <- err
			return
		}
		for i := range 100 {
			if err := writer.InjectChunks([]backupproxy.KnownChunkRef{{Digest: [32]byte{byte(i)}, Size: 1}}); err != nil {
				done <- err
				return
			}
		}
		done <- writer.Finish()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("remote writer injection stream deadlocked")
	}
}

func TestRemoteDedupWriterFinishNoLeak(t *testing.T) {
	w, err := NewRemoteDedupWriter(context.Background(), drainSession{}, "root.mpxar.didx", "root.ppxar.didx")
	if err != nil {
		t.Fatalf("NewRemoteDedupWriter: %v", err)
	}
	rootMeta := &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755}}
	if err := w.Begin(rootMeta, Options{}); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
