package transfer

import (
	"context"
	"io"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
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
