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
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
)

// RemoteDedupWriter writes a split archive to PBS with chunk-level dedup.
//
// For files that are unchanged from the original archive (pxar-only entries),
// it uses AddPayloadRef to reference original payload offsets without reading
// file content. The original payload chunks are injected into the new DIDX directly.
//
// For new/modified files (backed entries), it writes payload data normally.
//
// Architecture mirrors the Rust PBS client (pxar_backup_stream.rs):
// the encoder writes to a bufio.Writer wrapping a bounded channel sender.
// A separate goroutine reads the channel and presents an io.Reader to
// UploadPayloadWithInjection. This decouples encoding from uploading with
// bounded memory (~10 × bufioSize = ~2.5 MB in-flight payload data).
type RemoteDedupWriter struct {
	session     backupproxy.BackupSession
	ctx         context.Context
	inner       *StreamWriter
	metaName    string
	payloadName string
	origChunks  []backupproxy.KnownChunkRef
	metaBuf     bytes.Buffer
	dirDepth    int
	origSize    uint64
	lastRefOff  *uint64 // monotonic offset tracker for WriteEntryRef (nil = no offset yet)
	alignDone   bool    // true once payloadWritePos has been aligned to origSize

	// Channel-based payload stream (mirrors Rust's sync_channel(10)).
	encCh   chan payloadChunk
	encDone chan struct{}   // closed when encoder finishes
	encErr  error          // set by encoder before closing encDone
	encMu   sync.Mutex     // guards encErr

	uploadRes chan uploadResult // upload goroutine result
}

type payloadChunk struct {
	data []byte
	err  error // non-nil on encoder error
}

type uploadResult struct {
	result *backupproxy.UploadResult
	err    error
}

// NewRemoteDedupWriter creates a dedup writer for PBS uploads.
// origPayloadIndex is the raw DIDX bytes from the original .ppxar.didx.
func NewRemoteDedupWriter(
	ctx context.Context,
	session backupproxy.BackupSession,
	metaName, payloadName string,
	origPayloadIndex []byte,
) (*RemoteDedupWriter, error) {
	w := &RemoteDedupWriter{
		session:     session,
		ctx:         ctx,
		metaName:    metaName,
		payloadName: payloadName,
	}

	if len(origPayloadIndex) > 0 {
		idx, err := datastore.ParseDynamicIndex(origPayloadIndex)
		if err != nil {
			return nil, fmt.Errorf("read original payload index: %w", err)
		}
		w.origSize = idx.IndexBytes()
		w.origChunks = make([]backupproxy.KnownChunkRef, idx.Count())
		for i := 0; i < idx.Count(); i++ {
			info, ok := idx.ChunkInfo(i)
			if !ok {
				break
			}
			w.origChunks[i] = backupproxy.KnownChunkRef{
				Digest: info.Digest,
				Size:   info.End - info.Start,
			}
		}
	}

	return w, nil
}

// channelWriter is an io.Writer that sends flushed buffers through a channel.
// This mirrors Rust's StdChannelWriter that wraps a sync_channel sender.
type channelWriter struct {
	ch chan<- payloadChunk
}

func (cw *channelWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	// Copy because bufio may reuse the underlying buffer after Write returns.
	buf := make([]byte, len(p))
	copy(buf, p)
	cw.ch <- payloadChunk{data: buf}
	return len(p), nil
}

func (w *RemoteDedupWriter) Begin(rootMeta *pxar.Metadata, opts Options) error {
	w.metaBuf.Reset()
	w.dirDepth = 1
	opts.Format = format.FormatVersion2

	// Bounded channel — matches Rust's sync_channel(10).
	// Encoder can get ~10 flushes ahead of the uploader before blocking.
	w.encCh = make(chan payloadChunk, 10)
	w.encDone = make(chan struct{})
	w.uploadRes = make(chan uploadResult, 1)

	// Start upload goroutine that reads from the channel and presents
	// an io.Reader to UploadPayloadWithInjection.
	go w.uploadPayload()

	// Encoder writes to bufio.Writer (256KB buffer, matches Rust's buffer_size)
	// wrapping the channel sender.
	const bufSize = 256 * 1024
	cw := &channelWriter{ch: w.encCh}
	payloadOut := bufio.NewWriterSize(cw, bufSize)

	w.inner = NewSplitStreamWriter(&w.metaBuf, payloadOut)

	return w.inner.Begin(rootMeta, opts)
}

// flushPayload flushes the bufio.Writer, sending any buffered data through
// the channel. Called before closing the channel to ensure all data is sent.
func (w *RemoteDedupWriter) flushPayload() {
	if sw, ok := w.inner.payloadOut.(*bufio.Writer); ok {
		_ = sw.Flush()
	}
}

// setEncError records an encoder error for the upload goroutine.
func (w *RemoteDedupWriter) setEncErr(err error) {
	w.encMu.Lock()
	if w.encErr == nil {
		w.encErr = err
	}
	w.encMu.Unlock()
}

// uploadPayload runs in a goroutine. It reads payload chunks from the channel
// and presents them as an io.Reader to UploadPayloadWithInjection.
func (w *RemoteDedupWriter) uploadPayload() {
	// Ensure the upload result is always sent.
	defer func() {
		close(w.encCh) // release any blocked encoder writes
	}()

	cr := &channelReader{ch: w.encCh, done: w.encDone}

	// When there are original chunks, skip the 16-byte START_MARKER
	// from the encoder's payload stream (the combined stream already
	// has one from the original). For init mode, keep it as-is.
	var reader io.Reader = cr
	if len(w.origChunks) > 0 {
		reader = &skipHeaderReader{source: cr, skip: int(format.HeaderSize)}
	}

	result, err := w.session.UploadPayloadWithInjection(
		w.ctx,
		w.payloadName,
		w.origChunks,
		reader,
		w.origSize,
	)
	w.uploadRes <- uploadResult{result: result, err: err}
}

// channelReader presents an io.Reader interface over the payload channel.
// This mirrors how the Rust client's PxarBackupStream implements Stream
// by receiving from the sync_channel.
type channelReader struct {
	ch   <-chan payloadChunk
	done <-chan struct{} // closed when encoder is finished
	buf  []byte          // leftover data from previous chunk
	err  error
}

func (cr *channelReader) Read(p []byte) (int, error) {
	if cr.err != nil {
		return 0, cr.err
	}

	// Use leftover data from previous chunk first.
	if len(cr.buf) > 0 {
		n := copy(p, cr.buf)
		cr.buf = cr.buf[n:]
		return n, nil
	}

	// Read next chunk from channel.
	for {
		select {
		case chunk, ok := <-cr.ch:
			if !ok {
				// Channel closed — check for encoder error.
				<-cr.done // wait for encoder to report status
				return 0, io.EOF
			}
			if chunk.err != nil {
				cr.err = chunk.err
				return 0, chunk.err
			}
			if len(chunk.data) == 0 {
				continue
			}
			n := copy(p, chunk.data)
			if n < len(chunk.data) {
				cr.buf = chunk.data[n:]
			}
			return n, nil
		case <-cr.done:
			// Encoder finished. Drain remaining channel data.
			for {
				select {
				case chunk, ok := <-cr.ch:
					if !ok {
						return 0, io.EOF
					}
					if chunk.err != nil {
						cr.err = chunk.err
						return 0, chunk.err
					}
					if len(chunk.data) == 0 {
						continue
					}
					n := copy(p, chunk.data)
					if n < len(chunk.data) {
						cr.buf = chunk.data[n:]
					}
					return n, nil
				default:
					return 0, io.EOF
				}
			}
		}
	}
}

// skipHeaderReader skips the first N bytes from the underlying reader,
// then passes through everything else.
type skipHeaderReader struct {
	source io.Reader
	skip   int
	done   bool
}

func (s *skipHeaderReader) Read(p []byte) (int, error) {
	if !s.done {
		// Drain the skip bytes.
		buf := make([]byte, s.skip)
		if _, err := io.ReadFull(s.source, buf); err != nil {
			return 0, err
		}
		s.done = true
	}
	return s.source.Read(p)
}

// alignPayload advances the encoder's payloadWritePos so that new file offsets
// match their actual positions in the combined payload stream.
func (w *RemoteDedupWriter) alignPayload() error {
	if w.alignDone {
		return nil
	}
	w.alignDone = true
	enc := w.inner.Encoder()
	if enc == nil || w.origSize == 0 {
		return nil
	}
	curPos := enc.PayloadPosition()
	if w.origSize > curPos {
		if err := enc.Advance(w.origSize - curPos); err != nil {
			return fmt.Errorf("align payload position: %w", err)
		}
	}
	return nil
}

func (w *RemoteDedupWriter) WriteEntry(entry *pxar.Entry, content []byte) error {
	if err := w.alignPayload(); err != nil {
		return err
	}
	return w.inner.WriteEntry(entry, content)
}

func (w *RemoteDedupWriter) WriteEntryReader(entry *pxar.Entry, r io.Reader, size uint64) error {
	if err := w.alignPayload(); err != nil {
		return err
	}
	return w.inner.WriteEntryReader(entry, r, size)
}

// WriteEntryRef writes an entry referencing existing payload data.
// Returns an error if payloadOffset is not strictly greater than the last accepted
// offset (mirrors Rust's try_record_strictly_greater validation).
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
			close(w.encDone)
			<-w.uploadRes // drain upload goroutine
			return err
		}
		w.dirDepth--
	}
	if err := w.inner.Finish(); err != nil {
		w.setEncErr(err)
		w.flushPayload()
		close(w.encDone)
		<-w.uploadRes
		return err
	}

	// Flush remaining buffered payload data through the channel.
	w.flushPayload()

	// Signal that encoding is complete (no error).
	close(w.encDone)

	// Wait for upload to finish.
	res := <-w.uploadRes
	if res.err != nil {
		return fmt.Errorf("upload payload: %w", res.err)
	}

	// Upload metadata (small, buffered in memory — always last).
	_, err := w.session.UploadArchive(w.ctx, w.metaName, bytes.NewReader(w.metaBuf.Bytes()))
	if err != nil {
		return fmt.Errorf("upload metadata: %w", err)
	}

	return nil
}

func (w *RemoteDedupWriter) Close() error {
	return nil
}

// Encoder returns the underlying encoder.
func (w *RemoteDedupWriter) Encoder() *encoder.Encoder {
	return w.inner.Encoder()
}

// AdvancePayloadPosition advances the encoder's payload write position.
// Call after all AddPayloadRef calls to account for the original stream's
// TAIL_MARKER before writing new files.
func (w *RemoteDedupWriter) AdvancePayloadPosition(n uint64) error {
	if enc := w.inner.Encoder(); enc != nil {
		return enc.Advance(n)
	}
	return nil
}
