package transfer

import (
	"bytes"
	"context"
	"fmt"
	"io"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
)

// SplitSessionArchiveWriter writes a split (v2) archive by uploading
// both metadata and payload streams through a BackupSession.
type SplitSessionArchiveWriter struct {
	session     backupproxy.BackupSession
	ctx         context.Context
	inner       *StreamArchiveWriter
	SplitResult *backupproxy.SplitArchiveResult
	metaName    string
	payloadName string
	closers     []io.Closer
	metaBuf     bytes.Buffer
	payloadBuf  bytes.Buffer
	dirDepth    int
}

// NewSplitSessionArchiveWriter creates a split writer that uploads via a BackupSession.
func NewSplitSessionArchiveWriter(ctx context.Context, session backupproxy.BackupSession, metaName, payloadName string) *SplitSessionArchiveWriter {
	return &SplitSessionArchiveWriter{
		session:     session,
		ctx:         ctx,
		metaName:    metaName,
		payloadName: payloadName,
	}
}

func (w *SplitSessionArchiveWriter) Begin(rootMeta *pxar.Metadata, opts WriterOptions) error {
	w.metaBuf.Reset()
	w.payloadBuf.Reset()
	w.inner = NewSplitStreamArchiveWriter(&w.metaBuf, &w.payloadBuf)
	w.dirDepth = 1
	opts.Format = format.FormatVersion2
	return w.inner.Begin(rootMeta, opts)
}

func (w *SplitSessionArchiveWriter) WriteEntry(entry *pxar.Entry, content []byte) error {
	return w.inner.WriteEntry(entry, content)
}

func (w *SplitSessionArchiveWriter) WriteEntryRef(entry *pxar.Entry, payloadOffset uint64) error {
	return w.inner.WriteEntryRef(entry, payloadOffset)
}

func (w *SplitSessionArchiveWriter) WriteEntryReader(entry *pxar.Entry, r io.Reader, size uint64) error {
	return w.inner.WriteEntryReader(entry, r, size)
}

func (w *SplitSessionArchiveWriter) BeginDirectory(name string, meta *pxar.Metadata) error {
	w.dirDepth++
	return w.inner.BeginDirectory(name, meta)
}

func (w *SplitSessionArchiveWriter) EndDirectory() error {
	if w.dirDepth <= 1 {
		return fmt.Errorf("no directory to finish")
	}
	w.dirDepth--
	return w.inner.EndDirectory()
}

func (w *SplitSessionArchiveWriter) Finish() error {
	for w.dirDepth > 1 {
		if err := w.inner.EndDirectory(); err != nil {
			return err
		}
		w.dirDepth--
	}
	if err := w.inner.Finish(); err != nil {
		return err
	}

	result, err := w.session.UploadSplitArchive(
		w.ctx,
		w.metaName,
		bytes.NewReader(w.metaBuf.Bytes()),
		w.payloadName,
		bytes.NewReader(w.payloadBuf.Bytes()),
	)
	if err != nil {
		return fmt.Errorf("upload split archive: %w", err)
	}

	w.SplitResult = result
	return nil
}

// Encoder returns the underlying encoder for advanced operations.
func (w *SplitSessionArchiveWriter) Encoder() *encoder.Encoder {
	return w.inner.Encoder()
}

func (w *SplitSessionArchiveWriter) Close() error {
	var err error
	for _, c := range w.closers {
		if closeErr := c.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}
