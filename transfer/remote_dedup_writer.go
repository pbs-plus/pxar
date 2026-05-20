package transfer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

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
// Payload data is spilled to a temporary file instead of buffered in memory.
// Metadata is buffered (it is small). This avoids OOM on large archives while
// keeping encoding and upload decoupled for throughput.
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

	// Payload spills to a temp file instead of a bytes.Buffer.
	payloadFile *os.File
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

func (w *RemoteDedupWriter) Begin(rootMeta *pxar.Metadata, opts Options) error {
	w.metaBuf.Reset()
	w.dirDepth = 1
	opts.Format = format.FormatVersion2

	// Create temp file for payload spilling.
	f, err := os.CreateTemp("", "pxar-payload-*.tmp")
	if err != nil {
		return fmt.Errorf("create payload temp file: %w", err)
	}
	w.payloadFile = f

	w.inner = NewSplitStreamWriter(&w.metaBuf, w.payloadFile)

	return w.inner.Begin(rootMeta, opts)
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
		return err
	}

	// Sync temp file to disk before uploading.
	if err := w.payloadFile.Sync(); err != nil {
		return fmt.Errorf("sync payload temp file: %w", err)
	}

	// Upload metadata (small, always buffered in memory).
	_, err := w.session.UploadArchive(w.ctx, w.metaName, bytes.NewReader(w.metaBuf.Bytes()))
	if err != nil {
		return fmt.Errorf("upload metadata: %w", err)
	}

	// Upload payload from temp file.
	return w.uploadPayload()
}

// uploadPayload builds the combined payload DIDX by streaming from the temp file.
//
// When there are original chunks, the encoder's payload stream starts with a
// 16-byte START_MARKER that must be stripped (the combined stream already has
// one from the original). When there are no original chunks (init mode), the
// full stream is uploaded as-is.
func (w *RemoteDedupWriter) uploadPayload() error {
	enc := w.inner.Encoder()

	if enc == nil {
		// Fallback: no encoder, upload raw payload file.
		if _, err := w.payloadFile.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("seek payload temp file: %w", err)
		}
		_, err := w.session.UploadArchive(w.ctx, w.payloadName, w.payloadFile)
		return err
	}

	fi, err := w.payloadFile.Stat()
	if err != nil {
		return fmt.Errorf("stat payload temp file: %w", err)
	}
	payloadSize := fi.Size()

	// Seek to start of payload data (skip header if needed).
	offset := int64(0)
	if len(w.origChunks) > 0 && payloadSize > int64(format.HeaderSize) {
		offset = int64(format.HeaderSize)
	} else if len(w.origChunks) > 0 && payloadSize <= int64(format.HeaderSize) {
		// Only header data — nothing to upload for new data portion.
		_, err := w.session.UploadPayloadWithInjection(
			w.ctx,
			w.payloadName,
			w.origChunks,
			nil,
			w.origSize,
		)
		return err
	}

	if _, err := w.payloadFile.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("seek payload temp file: %w", err)
	}

	_, err = w.session.UploadPayloadWithInjection(
		w.ctx,
		w.payloadName,
		w.origChunks,
		w.payloadFile,
		w.origSize,
	)
	return err
}

func (w *RemoteDedupWriter) Close() error {
	if w.payloadFile != nil {
		name := w.payloadFile.Name()
		_ = w.payloadFile.Close()
		_ = os.Remove(name)
		w.payloadFile = nil
	}
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
