package transfer

import (
	"bytes"
	"context"
	"fmt"
	"io"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
)

// RemoteDedupSplitArchiveWriter writes a split archive to PBS with chunk-level dedup.
//
// For files that are unchanged from the original archive (pxar-only entries),
// it uses AddPayloadRef to reference original payload offsets without reading
// file content. The original payload chunks are injected into the new DIDX directly.
//
// For new/modified files (backed entries), it writes payload data normally.
//
// Only metadata and new file content are buffered in memory. Unchanged file
// content is never read — the original payload chunks are referenced directly.
type RemoteDedupSplitArchiveWriter struct {
	session      backupproxy.BackupSession
	ctx          context.Context
	inner        *StreamArchiveWriter
	metaName     string
	payloadName  string
	origChunks   []backupproxy.KnownChunkRef
	metaBuf      bytes.Buffer
	payloadBuf   bytes.Buffer
	dirDepth     int
	origSize     uint64
	lastRefOff   *uint64 // monotonic offset tracker for WriteEntryRef (nil = no offset yet)
	payloadAlign bool    // true once payloadWritePos has been aligned to origSize
}

// NewRemoteDedupSplitArchiveWriter creates a dedup writer for PBS uploads.
// origPayloadIndex is the raw DIDX bytes from the original .ppxar.didx.
func NewRemoteDedupSplitArchiveWriter(
	ctx context.Context,
	session backupproxy.BackupSession,
	metaName, payloadName string,
	origPayloadIndex []byte,
) (*RemoteDedupSplitArchiveWriter, error) {
	w := &RemoteDedupSplitArchiveWriter{
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

func (w *RemoteDedupSplitArchiveWriter) Begin(rootMeta *pxar.Metadata, opts WriterOptions) error {
	w.metaBuf.Reset()
	w.payloadBuf.Reset()
	w.inner = NewSplitStreamArchiveWriter(&w.metaBuf, &w.payloadBuf)
	w.dirDepth = 1
	opts.Format = format.FormatVersion2
	return w.inner.Begin(rootMeta, opts)
}

// alignPayload advances the encoder's payloadWritePos so that new file offsets
// match their actual positions in the combined payload stream.
//
// Combined stream: [0, origSize) = original chunks, [origSize, ...) = new data.
// The encoder tracks a virtual payloadWritePos. After all AddPayloadRef calls,
// payloadWritePos = 16 + sum(16+fileSize for ref'd files), which may be less
// than origSize (missing the original stream's TAIL_MARKER and any skipped files).
// We advance by the difference so that CreateFile generates correct offsets.
func (w *RemoteDedupSplitArchiveWriter) alignPayload() error {
	if w.payloadAlign {
		return nil
	}
	w.payloadAlign = true
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

func (w *RemoteDedupSplitArchiveWriter) WriteEntry(entry *pxar.Entry, content []byte) error {
	if err := w.alignPayload(); err != nil {
		return err
	}
	return w.inner.WriteEntry(entry, content)
}

func (w *RemoteDedupSplitArchiveWriter) WriteEntryReader(entry *pxar.Entry, r io.Reader, size uint64) error {
	if err := w.alignPayload(); err != nil {
		return err
	}
	return w.inner.WriteEntryReader(entry, r, size)
}

// WriteEntryRef writes an entry referencing existing payload data.
// Returns an error if payloadOffset is not strictly greater than the last accepted
// offset (mirrors Rust's try_record_strictly_greater validation).
func (w *RemoteDedupSplitArchiveWriter) WriteEntryRef(entry *pxar.Entry, payloadOffset uint64) error {
	if !TryRecordStrictlyGreater(&w.lastRefOff, payloadOffset) {
		return fmt.Errorf("payload offset %d is not strictly greater than last accepted offset %d", payloadOffset, *w.lastRefOff)
	}
	return w.inner.WriteEntryRef(entry, payloadOffset)
}

func (w *RemoteDedupSplitArchiveWriter) BeginDirectory(name string, meta *pxar.Metadata) error {
	w.dirDepth++
	return w.inner.BeginDirectory(name, meta)
}

func (w *RemoteDedupSplitArchiveWriter) EndDirectory() error {
	if w.dirDepth <= 1 {
		return fmt.Errorf("no directory to finish")
	}
	w.dirDepth--
	return w.inner.EndDirectory()
}

func (w *RemoteDedupSplitArchiveWriter) Finish() error {
	for w.dirDepth > 1 {
		if err := w.inner.EndDirectory(); err != nil {
			return err
		}
		w.dirDepth--
	}
	if err := w.inner.Finish(); err != nil {
		return err
	}

	// Upload metadata (small, always uploaded)
	_, err := w.session.UploadArchive(w.ctx, w.metaName, bytes.NewReader(w.metaBuf.Bytes()))
	if err != nil {
		return fmt.Errorf("upload metadata: %w", err)
	}

	// Upload payload with chunk injection
	return w.uploadPayload()
}

// uploadPayload builds the combined payload DIDX.
//
// alignPayload (called before the first WriteEntry/WriteEntryReader) advances
// the encoder's virtual payloadWritePos to origSize so that PAYLOAD_REF offsets
// for new files match their actual positions in the combined stream.
//
// Combined DIDX layout:
//
//	[0, origSize)                  — original chunks (injected)
//	[origSize, origSize + newData) — new file data (uploaded)
func (w *RemoteDedupSplitArchiveWriter) uploadPayload() error {
	enc := w.inner.Encoder()
	if enc == nil {
		_, err := w.session.UploadArchive(w.ctx, w.payloadName, bytes.NewReader(w.payloadBuf.Bytes()))
		return err
	}

	// The encoder wrote payloadBuf = [START_MARKER] [new file entries] [TAIL_MARKER]
	// When there are original chunks, strip the START_MARKER (first 16 bytes)
	// because the combined stream already has one from the original.
	// When there are no original chunks (init mode), keep the START_MARKER
	// because it IS the start of the combined stream.
	newData := w.payloadBuf.Bytes()
	if len(w.origChunks) > 0 && len(newData) > int(format.HeaderSize) {
		newData = newData[format.HeaderSize:]
	} else if len(w.origChunks) > 0 {
		newData = nil
	}

	_, err := w.session.UploadPayloadWithInjection(
		w.ctx,
		w.payloadName,
		w.origChunks,
		bytes.NewReader(newData),
		w.origSize,
	)
	return err
}

func (w *RemoteDedupSplitArchiveWriter) Close() error {
	return nil
}

// Encoder returns the underlying encoder.
func (w *RemoteDedupSplitArchiveWriter) Encoder() *encoder.Encoder {
	return w.inner.Encoder()
}

// AdvancePayloadPosition advances the encoder's payload write position.
// Call after all AddPayloadRef calls to account for the original stream's
// TAIL_MARKER before writing new files.
func (w *RemoteDedupSplitArchiveWriter) AdvancePayloadPosition(n uint64) error {
	if enc := w.inner.Encoder(); enc != nil {
		return enc.Advance(n)
	}
	return nil
}
