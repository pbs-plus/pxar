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
)

// RemoteDedupWriter writes a split pxar archive (v2), uploading the payload
// stream to PBS with chunk-level deduplication.
//
// Architecture (faithful to Rust's pxar create + backup_writer pipeline):
//
//	Encoder ──(io.Pipe)──> interleavePayload ──> putRaw ──> appendCh ──> appendWorker ──> PBS
//	               ▲
//	         bufio.Writer (256 KiB)
//
// The encoder writes to a bufio.Writer that flushes into an io.Pipe. The
// upload goroutine reads from the pipe via interleavePayload. This is a
// pull-based stream: the chunker pulls bytes from the encoder through the
// pipe; the encoder blocks when the pipe buffer is full (natural backpressure).
// InjectChunks (dedup boundaries) are sent through a separate channel.
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

	// Pipe connecting encoder output to the chunker/uploader.
	pr *io.PipeReader
	pw *io.PipeWriter

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

	w.pr, w.pw = io.Pipe()
	w.injectCh = make(chan backupproxy.InjectChunks, 64)
	w.uploadRes = make(chan uploadResult, 1)

	// bufio.Writer(256 KiB) reduces the number of pipe writes and gives the
	// encoder a larger atomic write unit, matching Rust's encoder behaviour.
	w.payloadBuf = bufio.NewWriterSize(w.pw, 256<<10)
	w.inner = NewSplitStreamWriter(&w.metaBuf, w.payloadBuf)

	go w.uploadPayload()
	w.started = true

	return w.inner.Begin(rootMeta, opts)
}

func (w *RemoteDedupWriter) flushPayload() {
	if w.payloadBuf != nil {
		_ = w.payloadBuf.Flush()
	}
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
		w.pr,
		w.injectCh,
	)
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
	w.flushPayload()

	totalSize := uint64(0)
	for _, c := range chunks {
		totalSize += c.Size
	}

	w.injectCh <- backupproxy.InjectChunks{
		Chunks:   chunks,
		Size:     totalSize,
		Boundary: boundary,
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

func (w *RemoteDedupWriter) Finish() error {
	for w.dirDepth > 1 {
		if err := w.inner.EndDirectory(); err != nil {
			w.setEncErr(err)
			w.flushPayload()
			_ = w.pw.CloseWithError(err)
			<-w.uploadRes
			return err
		}
		w.dirDepth--
	}
	if err := w.inner.Finish(); err != nil {
		w.setEncErr(err)
		w.flushPayload()
		_ = w.pw.CloseWithError(err)
		<-w.uploadRes
		return err
	}

	w.flushPayload()
	_ = w.pw.Close()
	close(w.injectCh)

	res := <-w.uploadRes
	if res.err != nil {
		return fmt.Errorf("upload payload: %w", res.err)
	}

	_, err := w.session.UploadArchive(w.ctx, w.metaName, bytes.NewReader(w.metaBuf.Bytes()))
	if err != nil {
		return fmt.Errorf("upload metadata: %w", err)
	}
	return nil
}

func (w *RemoteDedupWriter) Close() error { return nil }

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
