package transfer

import (
	"bytes"
	"context"
	"fmt"
	"io"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
)

// ChunkedArchiveWriter writes an archive by encoding it to a buffer,
// then chunking and storing via a local ChunkStore, producing a .didx index.
type ChunkedArchiveWriter struct {
	store    *datastore.ChunkStore
	config   buzhash.Config
	compress bool
	buf      bytes.Buffer
	inner    *StreamArchiveWriter
	dirDepth int
	closers  []io.Closer
	// Result after Finish
	IndexData []byte
	ChunkResults []datastore.ChunkResult
}

// NewChunkedArchiveWriter creates a chunked writer backed by a local chunk store.
func NewChunkedArchiveWriter(store *datastore.ChunkStore, config buzhash.Config, compress bool) *ChunkedArchiveWriter {
	return &ChunkedArchiveWriter{
		store:    store,
		config:   config,
		compress: compress,
	}
}

func (w *ChunkedArchiveWriter) Begin(rootMeta *pxar.Metadata, opts WriterOptions) error {
	w.buf.Reset()
	w.inner = NewStreamArchiveWriter(&w.buf)
	if opts.Format == format.FormatVersion2 {
		w.inner.payloadOut = &bytes.Buffer{}
	}
	w.dirDepth = 1
	return w.inner.Begin(rootMeta, opts)
}

func (w *ChunkedArchiveWriter) WriteEntry(entry *pxar.Entry, content []byte) error {
	return w.inner.WriteEntry(entry, content)
}

func (w *ChunkedArchiveWriter) BeginDirectory(name string, meta *pxar.Metadata) error {
	w.dirDepth++
	return w.inner.BeginDirectory(name, meta)
}

func (w *ChunkedArchiveWriter) EndDirectory() error {
	if w.dirDepth <= 1 {
		return fmt.Errorf("no directory to finish")
	}
	w.dirDepth--
	return w.inner.EndDirectory()
}

func (w *ChunkedArchiveWriter) Finish() error {
	// Close remaining directories
	for w.dirDepth > 1 {
		if err := w.inner.EndDirectory(); err != nil {
			return err
		}
		w.dirDepth--
	}
	if err := w.inner.Finish(); err != nil {
		return err
	}

	// Now chunk and store the encoded archive
	chunker := datastore.NewStoreChunker(w.store, w.config, w.compress)
	results, idx, err := chunker.ChunkStream(bytes.NewReader(w.buf.Bytes()))
	if err != nil {
		return fmt.Errorf("chunk archive: %w", err)
	}

	// Handle split archive payload
	if w.inner.payloadOut != nil {
		payloadBuf := w.inner.payloadOut.(*bytes.Buffer)
		_, payloadIdx, err := chunker.ChunkStream(bytes.NewReader(payloadBuf.Bytes()))
		if err != nil {
			return fmt.Errorf("chunk payload: %w", err)
		}
		_ = payloadIdx // caller can access via PayloadIndexData
	}

	idxData, err := idx.Finish()
	if err != nil {
		return fmt.Errorf("finish index: %w", err)
	}

	w.IndexData = idxData
	w.ChunkResults = results
	return nil
}

// PayloadIndexData returns the .didx index data for the payload stream.
// Only valid for v2 split archives after Finish.
func (w *ChunkedArchiveWriter) PayloadIndexData() ([]byte, error) {
	if w.inner.payloadOut == nil {
		return nil, fmt.Errorf("not a split archive")
	}
	payloadBuf := w.inner.payloadOut.(*bytes.Buffer)
	chunker := datastore.NewStoreChunker(w.store, w.config, w.compress)
	_, idx, err := chunker.ChunkStream(bytes.NewReader(payloadBuf.Bytes()))
	if err != nil {
		return nil, fmt.Errorf("chunk payload: %w", err)
	}
	return idx.Finish()
}

func (w *ChunkedArchiveWriter) Close() error {
	var err error
	for _, c := range w.closers {
		if closeErr := c.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

// SessionArchiveWriter writes an archive by uploading it through a BackupSession.
// This works with both local stores and PBS remote stores.
type SessionArchiveWriter struct {
	session  backupproxy.BackupSession
	ctx      context.Context
	name     string
	buf      bytes.Buffer
	inner    *StreamArchiveWriter
	dirDepth int
	closers  []io.Closer
	// Result after Finish
	UploadResult *backupproxy.UploadResult
}

// NewSessionArchiveWriter creates a writer that uploads via a BackupSession.
func NewSessionArchiveWriter(ctx context.Context, session backupproxy.BackupSession, name string) *SessionArchiveWriter {
	return &SessionArchiveWriter{
		session: session,
		ctx:     ctx,
		name:    name,
	}
}

func (w *SessionArchiveWriter) Begin(rootMeta *pxar.Metadata, opts WriterOptions) error {
	w.buf.Reset()
	w.inner = NewStreamArchiveWriter(&w.buf)
	w.dirDepth = 1
	return w.inner.Begin(rootMeta, opts)
}

func (w *SessionArchiveWriter) WriteEntry(entry *pxar.Entry, content []byte) error {
	return w.inner.WriteEntry(entry, content)
}

func (w *SessionArchiveWriter) BeginDirectory(name string, meta *pxar.Metadata) error {
	w.dirDepth++
	return w.inner.BeginDirectory(name, meta)
}

func (w *SessionArchiveWriter) EndDirectory() error {
	if w.dirDepth <= 1 {
		return fmt.Errorf("no directory to finish")
	}
	w.dirDepth--
	return w.inner.EndDirectory()
}

func (w *SessionArchiveWriter) Finish() error {
	// Close remaining directories
	for w.dirDepth > 1 {
		if err := w.inner.EndDirectory(); err != nil {
			return err
		}
		w.dirDepth--
	}
	if err := w.inner.Finish(); err != nil {
		return err
	}

	// Upload through the session
	result, err := w.session.UploadArchive(w.ctx, w.name, bytes.NewReader(w.buf.Bytes()))
	if err != nil {
		return fmt.Errorf("upload archive: %w", err)
	}

	w.UploadResult = result
	return nil
}

func (w *SessionArchiveWriter) Close() error {
	var err error
	for _, c := range w.closers {
		if closeErr := c.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

// SplitSessionArchiveWriter writes a split (v2) archive by uploading
// both metadata and payload streams through a BackupSession.
type SplitSessionArchiveWriter struct {
	session     backupproxy.BackupSession
	ctx         context.Context
	metaName    string
	payloadName string
	metaBuf     bytes.Buffer
	payloadBuf  bytes.Buffer
	inner       *StreamArchiveWriter
	dirDepth    int
	closers     []io.Closer
	// Result after Finish
	SplitResult *backupproxy.SplitArchiveResult
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
	// Close remaining directories
	for w.dirDepth > 1 {
		if err := w.inner.EndDirectory(); err != nil {
			return err
		}
		w.dirDepth--
	}
	if err := w.inner.Finish(); err != nil {
		return err
	}

	// Upload both streams
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

func (w *SplitSessionArchiveWriter) Close() error {
	var err error
	for _, c := range w.closers {
		if closeErr := c.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

