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

type RemoteDedupWriter struct {
	session     backupproxy.BackupSession
	ctx         context.Context
	inner       *StreamWriter
	metaName    string
	payloadName string
	metaBuf     bytes.Buffer
	dirDepth    int
	lastRefOff  *uint64

	eventCh   chan streamEvent
	encErr    error
	encMu     sync.Mutex
	uploadRes chan uploadResult
}

type streamEvent struct {
	injection *backupproxy.InjectChunks
	data      []byte
	err       error
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

type eventWriter struct {
	ch chan<- streamEvent
}

func (ew *eventWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	buf := make([]byte, len(p))
	copy(buf, p)
	ew.ch <- streamEvent{data: buf}
	return len(p), nil
}

func (w *RemoteDedupWriter) Begin(rootMeta *pxar.Metadata, opts Options) error {
	w.metaBuf.Reset()
	w.dirDepth = 1
	opts.Format = format.FormatVersion2

	w.eventCh = make(chan streamEvent, 10)
	w.uploadRes = make(chan uploadResult, 1)

	go w.uploadPayload()

	const bufSize = 256 * 1024
	ew := &eventWriter{ch: w.eventCh}
	payloadOut := bufio.NewWriterSize(ew, bufSize)

	w.inner = NewSplitStreamWriter(&w.metaBuf, payloadOut)

	return w.inner.Begin(rootMeta, opts)
}

func (w *RemoteDedupWriter) flushPayload() {
	if sw, ok := w.inner.payloadOut.(*bufio.Writer); ok {
		_ = sw.Flush()
	}
}

func (w *RemoteDedupWriter) setEncErr(err error) {
	w.encMu.Lock()
	if w.encErr == nil {
		w.encErr = err
	}
	w.encMu.Unlock()
}

func (w *RemoteDedupWriter) uploadPayload() {
	injectCh := make(chan backupproxy.InjectChunks, 64)
	dataCh := make(chan streamEvent, 10)

	go func() {
		defer close(injectCh)
		defer close(dataCh)
		for ev := range w.eventCh {
			if ev.injection != nil {
				injectCh <- *ev.injection
			} else {
				dataCh <- ev
			}
		}
	}()

	cr := &chanReader{ch: dataCh}

	result, err := w.session.UploadPayloadInterleaved(
		w.ctx,
		w.payloadName,
		cr,
		injectCh,
	)
	w.uploadRes <- uploadResult{result: result, err: err}
}

type chanReader struct {
	ch  <-chan streamEvent
	buf []byte
	err error
}

func (cr *chanReader) Read(p []byte) (int, error) {
	if cr.err != nil {
		return 0, cr.err
	}
	if len(cr.buf) > 0 {
		n := copy(p, cr.buf)
		cr.buf = cr.buf[n:]
		return n, nil
	}
	for ev := range cr.ch {
		if ev.err != nil {
			cr.err = ev.err
			return 0, ev.err
		}
		if len(ev.data) == 0 {
			continue
		}
		n := copy(p, ev.data)
		if n < len(ev.data) {
			cr.buf = ev.data[n:]
		}
		return n, nil
	}
	return 0, io.EOF
}

func (w *RemoteDedupWriter) InjectChunks(chunks []backupproxy.KnownChunkRef) error {
	if len(chunks) == 0 {
		return nil
	}

	totalSize := uint64(0)
	for _, c := range chunks {
		totalSize += c.Size
	}

	enc := w.inner.Encoder()
	if enc == nil {
		return fmt.Errorf("encoder not initialized")
	}

	// Boundary is the absolute payload offset at which the injection occurs.
	// It must be captured from the encoder BEFORE advancing, matching the
	// Rust encoder's `injection_boundary = encoder.payload_position()` taken
	// before `encoder.advance(size)`. The payload chunker uses it to splice
	// injected chunks into the stream at the right place so offsets stay in
	// sync with the rest of the archive.
	boundary := enc.PayloadPosition()

	w.flushPayload()

	w.eventCh <- streamEvent{
		injection: &backupproxy.InjectChunks{
			Chunks:   chunks,
			Size:     totalSize,
			Boundary: boundary,
		},
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
			close(w.eventCh)
			<-w.uploadRes
			return err
		}
		w.dirDepth--
	}
	if err := w.inner.Finish(); err != nil {
		w.setEncErr(err)
		w.flushPayload()
		close(w.eventCh)
		<-w.uploadRes
		return err
	}

	w.flushPayload()
	close(w.eventCh)

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

func (w *RemoteDedupWriter) Close() error {
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
