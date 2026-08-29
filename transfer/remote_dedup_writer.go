package transfer

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"os"
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
	metaFile    *os.File
	metaPath    string
	metaBuf     *bufio.Writer
	dirDepth    int
	lastRefOff  *uint64
	requiredEnd uint64

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
	metaFile, err := os.CreateTemp("", "pxar-metadata-*.pxar")
	if err != nil {
		return fmt.Errorf("create metadata spool: %w", err)
	}
	w.metaFile = metaFile
	w.metaPath = metaFile.Name()
	w.metaBuf = bufio.NewWriterSize(metaFile, 256<<10)
	w.dirDepth = 1
	w.lastRefOff = nil
	w.requiredEnd = 0
	opts.Format = format.FormatVersion2

	w.payloadPipe = payloadpipe.New()
	w.injectCh = make(chan backupproxy.InjectChunks, 64)
	w.uploadRes = make(chan uploadResult, 1)
	w.payloadBuf = bufio.NewWriterSize(w.payloadPipe, 256<<10)
	w.inner = NewSplitStreamWriter(w.metaBuf, w.payloadBuf)

	go w.uploadPayload()
	w.started = true
	if err := w.inner.Begin(rootMeta, opts); err != nil {
		w.cleanup(err)
		return err
	}
	return nil
}

func (w *RemoteDedupWriter) flushPayload() error {
	if w.payloadBuf == nil {
		return nil
	}
	return w.payloadBuf.Flush()
}

func (w *RemoteDedupWriter) cleanupMetadata() {
	if w.metaFile != nil {
		_ = w.metaFile.Close()
		w.metaFile = nil
	}
	if w.metaPath != "" {
		_ = os.Remove(w.metaPath)
		w.metaPath = ""
	}
	w.metaBuf = nil
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
		if c.Size > math.MaxUint64-totalSize {
			return fmt.Errorf("injected chunk sizes overflow")
		}
		totalSize += c.Size
	}

	injection := backupproxy.InjectChunks{
		Chunks:   chunks,
		Size:     totalSize,
		Boundary: boundary,
	}
	for _, chunk := range chunks {
		if chunk.LoadEncodedBlob != nil {
			injection.Processed = make(chan error, 1)
			break
		}
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
	if injection.Processed != nil {
		select {
		case err := <-injection.Processed:
			if err != nil {
				return err
			}
		case <-w.payloadPipe.Done():
			if err := w.payloadPipe.Err(); err != nil {
				return err
			}
			return io.ErrClosedPipe
		case <-w.ctx.Done():
			return w.ctx.Err()
		}
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
	if payloadOffset > math.MaxUint64-format.HeaderSize || entry.FileSize > math.MaxUint64-format.HeaderSize-payloadOffset {
		return fmt.Errorf("payload range for %q overflows", entry.Path)
	}
	if !RecordMax(&w.lastRefOff, payloadOffset) {
		return fmt.Errorf("payload offset %d is not strictly greater than last accepted offset %d", payloadOffset, *w.lastRefOff)
	}
	if err := w.inner.WriteEntryRef(entry, payloadOffset); err != nil {
		return err
	}
	w.requiredEnd = max(w.requiredEnd, payloadOffset+format.HeaderSize+entry.FileSize)
	return nil
}

func (w *RemoteDedupWriter) WriteHardlink(name string, target string, targetOffset encoder.LinkOffset) error {
	return w.inner.WriteHardlink(name, target, targetOffset)
}

func (w *RemoteDedupWriter) LastEntryOffset() (encoder.LinkOffset, bool) {
	return w.inner.LastEntryOffset()
}

// WritePayload writes already-framed payload bytes without creating metadata.
func (w *RemoteDedupWriter) WritePayload(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	enc := w.inner.Encoder()
	if enc == nil || w.payloadBuf == nil {
		return fmt.Errorf("encoder not initialized")
	}
	if _, err := w.payloadBuf.Write(data); err != nil {
		return err
	}
	return enc.Advance(uint64(len(data)))
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
	w.cleanupMetadata()
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
	if w.inner.Encoder().PayloadPosition() > math.MaxUint64-format.HeaderSize {
		return fmt.Errorf("payload size overflows")
	}
	expectedPayloadSize := w.inner.Encoder().PayloadPosition() + format.HeaderSize
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
	if err := w.metaBuf.Flush(); err != nil {
		w.cleanup(err)
		return fmt.Errorf("flush metadata: %w", err)
	}
	w.payloadPipe.CloseWithError(nil)
	close(w.injectCh)

	res := <-w.uploadRes
	w.started = false
	if res.err != nil {
		return fmt.Errorf("upload payload: %w", res.err)
	}
	if res.result == nil {
		return fmt.Errorf("payload upload returned no result")
	}
	if res.result.Size != expectedPayloadSize {
		return fmt.Errorf("payload upload size %d does not match encoder position %d", res.result.Size, expectedPayloadSize)
	}
	if res.result.Size < w.requiredEnd {
		return fmt.Errorf("payload references end at %d but upload ends at %d", w.requiredEnd, res.result.Size)
	}

	if _, err := w.metaFile.Seek(0, io.SeekStart); err != nil {
		w.cleanupMetadata()
		return fmt.Errorf("rewind metadata: %w", err)
	}
	_, err := w.session.UploadArchive(w.ctx, w.metaName, w.metaFile)
	w.cleanupMetadata()
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
