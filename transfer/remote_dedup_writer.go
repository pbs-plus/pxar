package transfer

import (
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
// Payload data is streamed to PBS via an io.Pipe — no full-archive buffering
// in memory. Metadata is buffered (it is small). Only the pipe buffer (typically
// 64 KB) is held in memory for payload at any given time.
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

	// Streaming payload — encoder writes to pw, upload goroutine reads from pr.
	payloadMu   sync.Mutex
	payloadPr   *io.PipeReader
	payloadPw   *io.PipeWriter
	payloadOnce sync.Once
	payloadRes  chan payloadUploadResult
}

type payloadUploadResult struct {
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

// startPayloadUpload lazily creates the pipe and starts the upload goroutine.
// Called on first actual payload write or in Finish if no new data was written.
func (w *RemoteDedupWriter) startPayloadUpload() {
	w.payloadOnce.Do(func() {
		pr, pw := io.Pipe()
		w.payloadPr = pr
		w.payloadPw = pw
		w.payloadRes = make(chan payloadUploadResult, 1)

		go func() {
			var reader io.Reader = pr

			// When there are original chunks, the encoder writes a START_MARKER
			// (16 bytes) at the beginning of the payload stream. The combined
			// stream already has one from the original, so skip it.
			if len(w.origChunks) > 0 {
				discard := make([]byte, format.HeaderSize)
				if _, err := io.ReadFull(pr, discard); err != nil {
					_ = pr.CloseWithError(err)
					w.payloadRes <- payloadUploadResult{err: fmt.Errorf("skip header: %w", err)}
					return
				}
			}

			result, err := w.session.UploadPayloadWithInjection(
				w.ctx,
				w.payloadName,
				w.origChunks,
				reader,
				w.origSize,
			)
			w.payloadRes <- payloadUploadResult{result: result, err: err}
		}()
	})
}

func (w *RemoteDedupWriter) Begin(rootMeta *pxar.Metadata, opts Options) error {
	w.metaBuf.Reset()
	w.dirDepth = 1
	opts.Format = format.FormatVersion2

	// Don't start the pipe yet — delay until first payload write.
	// Create a deferredPayloadWriter that starts the pipe on first Write.
	dw := &deferredWriter{}
	dw.start = func() {
		w.startPayloadUpload()
		dw.w = w.payloadPw
	}
	w.inner = NewSplitStreamWriter(&w.metaBuf, dw)

	return w.inner.Begin(rootMeta, opts)
}

// deferredWriter wraps an io.Writer that is nil until start() is called.
// All writes go through the pipe once started.
type deferredWriter struct {
	start func()
	w     io.Writer // set by start()
}

func (d *deferredWriter) Write(p []byte) (int, error) {
	if d.w == nil {
		d.start()
	}
	return d.w.Write(p)
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
			return err
		}
		w.dirDepth--
	}
	if err := w.inner.Finish(); err != nil {
		// Encoder failed — close pipe to unblock upload goroutine.
		if w.payloadPw != nil {
			_ = w.payloadPw.CloseWithError(err)
		}
		return err
	}

	// Close payload pipe (signals EOF to upload goroutine).
	if w.payloadPw != nil {
		_ = w.payloadPw.Close()
	}

	// Wait for payload upload to finish.
	if w.payloadRes != nil {
		res := <-w.payloadRes
		if res.err != nil {
			return fmt.Errorf("upload payload: %w", res.err)
		}
	}

	// If no payload data was written at all (empty archive with no origChunks),
	// upload an empty payload.
	if w.payloadPw == nil {
		_, err := w.session.UploadPayloadWithInjection(
			w.ctx,
			w.payloadName,
			w.origChunks,
			nil,
			w.origSize,
		)
		if err != nil {
			return fmt.Errorf("upload empty payload: %w", err)
		}
	}

	// Upload metadata (small, buffered — always uploaded last).
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
