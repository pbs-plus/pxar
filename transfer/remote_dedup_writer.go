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
// For unchanged files (pxar-only entries), it uses AddPayloadRef to reference
// original payload offsets without reading file content. The original payload
// chunks are injected into the new DIDX by reference.
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
// The new payload stream layout:
//
//	[original payload bytes 0..origSize] [new file data]
//
// Original chunks are injected by reference (no data read/uploaded).
// New file data is chunked and uploaded normally.
func (w *RemoteDedupSplitArchiveWriter) uploadPayload() error {
	enc := w.inner.Encoder()
	if enc == nil {
		_, err := w.session.UploadArchive(w.ctx, w.payloadName, bytes.NewReader(w.payloadBuf.Bytes()))
		return err
	}

	_ = enc
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

	// Extract new data from payloadBuf.
	// The encoder wrote:
	//   [PAYLOAD_START_MARKER] [new file entries...] [PAYLOAD_TAIL_MARKER]
	// The new file entries have PayloadRef offsets in metadata that point to
	// virtual positions >= origSize. But the actual bytes in payloadBuf are
	// contiguous (no gap for reused files).
	//
	// The payloadBuf bytes need to be placed at offset origSize in the
	// combined stream. The PAYLOAD_START_MARKER in payloadBuf is wrong for the
	// combined stream (original payload already has its own start). Similarly,
	// the PAYLOAD_TAIL_MARKER needs to be at the end of the combined stream.
	//
	// So we extract just the file data (skip markers) and rely on the chunk
	// injection to handle the original portion.
	//
	// The encoder's payloadBuf layout:
	//   [format.HeaderSize bytes: PAYLOAD_START_MARKER]
	//   [new file payload entries: PAYLOAD_HEADER + data]
	//   [format.HeaderSize bytes: PAYLOAD_TAIL_MARKER]
	//
	// We strip the markers and just upload the new file data portion.
	newData := w.payloadBuf.Bytes()
	if len(newData) > int(format.HeaderSize)*2 {
		newData = newData[format.HeaderSize : len(newData)-format.HeaderSize]
	} else {
		newData = nil
	}

	// Use UploadPayloadWithInjection to build the combined DIDX
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
