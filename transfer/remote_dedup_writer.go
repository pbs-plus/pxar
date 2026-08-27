package transfer

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/internal/payloadpipe"
)

// RemoteDedupWriter writes a split pxar archive (v2), uploading the payload
// stream to PBS with chunk-level deduplication.
//
// Architecture (faithful to Rust's pxar create + backup_writer pipeline):
//

// This replaces the previous push-based design (eventCh→dataCh→chanReader)
// which added ~66% idle time from channel synchronization overhead.
type RemoteDedupWriter struct {
	session     backupproxy.BackupSession
	ctx         context.Context
	inner       *StreamWriter
	metaName    string
	payloadName string
	metaBuf     bytes.Buffer
	dirDepth    int
	lastRefOff  *uint64

	payloadPipe *payloadpipe.Pipe

	// Injection channel for dedup boundary markers.
	injectCh chan backupproxy.InjectChunks

	// Result from the upload goroutine.
	uploadRes chan uploadResult

	// bufio writer wrapping the pipe for the encoder.
	payloadBuf *bufio.Writer

	mu      sync.Mutex
	encErr  error
	started bool
}

type uploadResult struct {
	result *backupproxy.UploadResult
	err    error
}

func NewRemoteDedupWriter(
	ctx context.Context,
	session backupproxy.BackupSession,
	metaName, payloadName string,
) (*RemoteDedupWriter, error) {
	return &RemoteDedupWriter{
		session:     session,
		ctx:         ctx,
		metaName:    metaName,
		payloadName: payloadName,
	}, nil
}

func (w *RemoteDedupWriter) Begin(rootMeta *pxar.Metadata, opts Options) error {
	w.metaBuf.Reset()
	w.dirDepth = 1
	opts.Format = format.FormatVersion2

	w.payloadPipe = payloadpipe.New()
	w.injectCh = make(chan backupproxy.InjectChunks, 64)
	w.uploadRes = make(chan uploadResult, 1)

	// bufio.Writer(256 KiB) reduces the number of pipe writes and gives the
	// encoder a larger atomic write unit, matching Rust's encoder behaviour.
	w.payloadBuf = bufio.NewWriterSize(w.payloadPipe, 256<<10)
	w.inner = NewSplitStreamWriter(&w.metaBuf, w.payloadBuf)

	go w.uploadPayload()
	w.started = true

	return w.inner.Begin(rootMeta, opts)
}

func (w *RemoteDedupWriter) flushPayload() error {
	if w.payloadBuf == nil {
		return nil
	}
	return w.payloadBuf.Flush()
}

// uploadPayload runs in a goroutine. It reads raw payload bytes from the pipe
// and injection markers from the channel, feeding them to interleavePayload.
func (w *RemoteDedupWriter) uploadPayload() {
	// interleavePayload reads from the pipe (new data) and the injection
	// channel (dedup boundaries). The pipe provides natural backpressure:
	// when the chunker is slow, pipe writes block, which blocks the encoder.
	result, err := w.session.UploadPayloadInterleaved(
		w.ctx,
		w.payloadName,
		w.payloadPipe,
		w.injectCh,
	)
	if err != nil {
		w.payloadPipe.CloseWithError(err)
	}
	w.uploadRes <- uploadResult{result: result, err: err}
}

// ----- entry writing (called from main goroutine) -----

func (w *RemoteDedupWriter) InjectChunks(chunks []backupproxy.KnownChunkRef) error {
	if len(chunks) == 0 {
		return nil
	}
	enc := w.inner.Encoder()
	if enc == nil {
		return fmt.Errorf("encoder not initialized")
	}
	boundary := enc.PayloadPosition()
	if err := w.flushPayload(); err != nil {
		return err
	}

	totalSize := uint64(0)
	for _, c := range chunks {
		totalSize += c.Size
	}

	injection := backupproxy.InjectChunks{
		Chunks:   chunks,
		Size:     totalSize,
		Boundary: boundary,
	}
	select {
	case w.injectCh <- injection:
		w.payloadPipe.Wake()
	case <-w.payloadPipe.Done():
		if err := w.payloadPipe.Err(); err != nil {
			return err
		}
		return io.ErrClosedPipe
	case <-w.ctx.Done():
		return w.ctx.Err()
	}
	return enc.Advance(totalSize)
}

func (w *RemoteDedupWriter) WriteEntry(entry *pxar.Entry, content []byte) error {
	return w.inner.WriteEntry(entry, content)
}

func (w *RemoteDedupWriter) WriteEntryReader(entry *pxar.Entry, r io.Reader, size uint64) error {
	return w.inner.WriteEntryReader(entry, r, size)
}

func (w *RemoteDedupWriter) WriteEntryRef(entry *pxar.Entry, payloadOffset uint64) error {
	if !RecordMax(&w.lastRefOff, payloadOffset) {
		return fmt.Errorf("payload offset %d is not strictly greater than last accepted offset %d", payloadOffset, *w.lastRefOff)
	}
	return w.inner.WriteEntryRef(entry, payloadOffset)
}

func (w *RemoteDedupWriter) WriteHardlink(name string, target string, targetOffset encoder.LinkOffset) error {
	return w.inner.WriteHardlink(name, target, targetOffset)
}

func (w *RemoteDedupWriter) BeginDirectory(name string, meta *pxar.Metadata) error {
	w.dirDepth++
	return w.inner.BeginDirectory(name, meta)
}

func (w *RemoteDedupWriter) EndDirectory() error {
	if w.dirDepth <= 1 {
		return fmt.Errorf("no directory to finish")
	}
	w.dirDepth--
	return w.inner.EndDirectory()
}

func (w *RemoteDedupWriter) cleanup(abort error) {
	if !w.started {
		return
	}
	w.started = false
	w.payloadPipe.CloseWithError(abort)
	close(w.injectCh)
	<-w.uploadRes
}

var ErrWriterAborted = fmt.Errorf("remote dedup writer: aborted before finish")

func (w *RemoteDedupWriter) Finish() error {
	for w.dirDepth > 1 {
		if err := w.inner.EndDirectory(); err != nil {
			w.setEncErr(err)
			_ = w.flushPayload()
			w.cleanup(err)
			return err
		}
		w.dirDepth--
	}
	if err := w.inner.Finish(); err != nil {
		w.setEncErr(err)
		_ = w.flushPayload()
		w.cleanup(err)
		return err
	}

	if err := w.flushPayload(); err != nil {
		w.cleanup(err)
		return err
	}
	w.payloadPipe.CloseWithError(nil)
	close(w.injectCh)

	res := <-w.uploadRes
	w.started = false
	if res.err != nil {
		return fmt.Errorf("upload payload: %w", res.err)
	}

	_, err := w.session.UploadArchive(w.ctx, w.metaName, bytes.NewReader(w.metaBuf.Bytes()))
	if err != nil {
		return fmt.Errorf("upload metadata: %w", err)
	}
	return nil
}

func (w *RemoteDedupWriter) Close() error {
	w.cleanup(ErrWriterAborted)
	return nil
}

func (w *RemoteDedupWriter) Encoder() *encoder.Encoder {
	return w.inner.Encoder()
}

func (w *RemoteDedupWriter) AdvancePayloadPosition(n uint64) error {
	if enc := w.inner.Encoder(); enc != nil {
		return enc.Advance(n)
	}
	return nil
}

func (w *RemoteDedupWriter) setEncErr(err error) {
	w.mu.Lock()
	if w.encErr == nil {
		w.encErr = err
	}
	w.mu.Unlock()
}
