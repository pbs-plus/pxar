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
	session     backupproxy.BackupSession
	ctx         context.Context
	inner       *StreamArchiveWriter
	metaName    string
	payloadName string
	metaBuf     bytes.Buffer
	payloadBuf  bytes.Buffer
	dirDepth    int

	// Original payload index for chunk injection
	origPayloadIdx *datastore.DynamicIndexReader
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
		idx, err := datastore.ReadDynamicIndex(origPayloadIndex)
		if err != nil {
			return nil, fmt.Errorf("read original payload index: %w", err)
		}
		w.origPayloadIdx = idx
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

func (w *RemoteDedupSplitArchiveWriter) WriteEntry(entry *pxar.Entry, content []byte) error {
	return w.inner.WriteEntry(entry, content)
}

func (w *RemoteDedupSplitArchiveWriter) WriteEntryReader(entry *pxar.Entry, r io.Reader, size uint64) error {
	return w.inner.WriteEntryReader(entry, r, size)
}

// WriteEntryRef writes an entry referencing existing payload data.
func (w *RemoteDedupSplitArchiveWriter) WriteEntryRef(entry *pxar.Entry, payloadOffset uint64) error {
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
// The encoder's payloadWritePos after all AddPayloadRef calls is at
// origSize - HeaderSize (missing the original stream's TAIL_MARKER).
// We call Advance(HeaderSize) to account for it, making new file data
// start at origSize in the combined stream.
//
// Combined DIDX layout:
//   [0, origSize)                — original chunks (injected)
//   [origSize, origSize + newData) — new file data (uploaded)
func (w *RemoteDedupSplitArchiveWriter) uploadPayload() error {
	enc := w.inner.Encoder()
	if enc == nil {
		_, err := w.session.UploadArchive(w.ctx, w.payloadName, bytes.NewReader(w.payloadBuf.Bytes()))
		return err
	}

	origSize := uint64(0)
	if w.origPayloadIdx != nil {
		origSize = w.origPayloadIdx.IndexBytes()
	}

	// Collect original chunk references
	var origChunks []backupproxy.KnownChunkRef
	if w.origPayloadIdx != nil {
		origChunks = make([]backupproxy.KnownChunkRef, w.origPayloadIdx.Count())
		for i := 0; i < w.origPayloadIdx.Count(); i++ {
			info, ok := w.origPayloadIdx.ChunkInfo(i)
			if !ok {
				break
			}
			origChunks[i] = backupproxy.KnownChunkRef{
				Digest: info.Digest,
				Size:   info.End - info.Start,
			}
		}
	}

	// The encoder wrote payloadBuf = [START_MARKER] [new file entries] [TAIL_MARKER]
	// The START_MARKER belongs at position 0 (already covered by original chunks).
	// New file data starts after START_MARKER in the buffer.
	// In the combined stream, new data should be at offset origSize.
	//
	// We need to call Advance(HeaderSize) BEFORE the new files are written
	// (to make payloadWritePos = origSize). But Finish() has already been called.
	//
	// Instead, we just place the new data at the correct offset.
	// Strip the START_MARKER (first 16 bytes) from payloadBuf.
	// The remaining bytes are: [file PAYLOAD_HEADER + data] [TAIL_MARKER]
	newData := w.payloadBuf.Bytes()
	if len(newData) > int(format.HeaderSize) {
		newData = newData[format.HeaderSize:]
	} else {
		newData = nil
	}

	// Use UploadPayloadWithInjection with newDataOffset = origSize
	_, err := w.session.UploadPayloadWithInjection(
		w.ctx,
		w.payloadName,
		origChunks,
		bytes.NewReader(newData),
		origSize,
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
