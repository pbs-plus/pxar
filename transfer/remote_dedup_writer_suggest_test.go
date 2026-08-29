package transfer

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/format"
)

// suggestSession records the boundaries received on the suggestion channel
// the writer passes to UploadPayloadInterleaved.
type suggestSession struct {
	drainSession

	mu   sync.Mutex
	seen []uint64
}

func (s *suggestSession) UploadPayloadInterleaved(_ context.Context, _ string, newData io.Reader, injections <-chan backupproxy.InjectChunks, suggestions <-chan uint64) (*backupproxy.UploadResult, error) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for off := range suggestions {
			s.mu.Lock()
			s.seen = append(s.seen, off)
			s.mu.Unlock()
		}
	}()
	n, _ := io.Copy(io.Discard, newData)
	size := uint64(n)
	for injection := range injections {
		size += injection.Size
	}
	<-done
	return &backupproxy.UploadResult{Size: size}, nil
}

// TestRemoteDedupWriterSuggestsFileEnds writes two files and checks the
// suggestion channel receives exactly the payload offsets reached after each
// file's content, matching the Rust encoder's suggested_boundaries emission.
func TestRemoteDedupWriterSuggestsFileEnds(t *testing.T) {
	sess := &suggestSession{}
	w, err := NewRemoteDedupWriter(context.Background(), sess, "root.mpxar.didx", "root.ppxar.didx")
	if err != nil {
		t.Fatalf("NewRemoteDedupWriter: %v", err)
	}
	rootMeta := &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755}}
	if err := w.Begin(rootMeta, Options{}); err != nil {
		t.Fatalf("Begin: %v", err)
	}

	fileMeta := pxar.FileMetadata(0o644).Build()
	if err := w.BeginDirectory("dir", &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755}}); err != nil {
		t.Fatalf("BeginDirectory: %v", err)
	}
	if err := w.WriteEntryReader(&pxar.Entry{
		Path:     "a.txt",
		Kind:     pxar.KindFile,
		Metadata: fileMeta,
		FileSize: 4096,
	}, bytes.NewReader(make([]byte, 4096)), 4096); err != nil {
		t.Fatalf("WriteEntryReader: %v", err)
	}
	firstEnd := w.Encoder().PayloadPosition()
	if err := w.WriteEntry(&pxar.Entry{
		Path:     "b.txt",
		Kind:     pxar.KindFile,
		Metadata: fileMeta,
		FileSize: 8192,
	}, make([]byte, 8192)); err != nil {
		t.Fatalf("WriteEntry: %v", err)
	}
	secondEnd := w.Encoder().PayloadPosition()
	if err := w.EndDirectory(); err != nil {
		t.Fatalf("EndDirectory: %v", err)
	}

	sess.mu.Lock()
	before := len(sess.seen)
	sess.mu.Unlock()
	if before > 2 {
		t.Fatalf("suggestions before finish = %d, want at most 2", before)
	}

	if err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	sess.mu.Lock()
	defer sess.mu.Unlock()
	if len(sess.seen) != 2 {
		t.Fatalf("total suggestions = %d, want 2 (no emission for directories or finish)", len(sess.seen))
	}
	if sess.seen[0] != firstEnd || sess.seen[1] != secondEnd {
		t.Fatalf("suggestions = %v, want [%d %d]", sess.seen, firstEnd, secondEnd)
	}
}
