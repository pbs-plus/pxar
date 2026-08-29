// Package transfer provides utilities for transferring files between pxar archives.
package transfer

import (
	"fmt"
	"io"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/accessor"
	"github.com/pbs-plus/pxar/datastore"
)

// ArchiveReader provides unified read access to any pxar archive format.
type ArchiveReader interface {
	ReadRoot() (*pxar.Entry, error)

	ReadRootFull() (*pxar.Entry, error)

	// Lookup finds an entry by archive-internal path.
	Lookup(path string) (*pxar.Entry, error)

	// ListDirectory streams directory entries without materializing a slice.
	// For each entry, fn is called with a pointer valid only during the callback.
	// If fn returns a non-nil error, iteration stops.
	ListDirectory(dirOffset int64, opts accessor.ListOption, fn func(*pxar.Entry) error) error

	// ReadFileContentReader returns a streaming reader for file content.
	// The caller must close the reader. Use this for large files to avoid
	// buffering the entire content in memory.
	ReadFileContentReader(entry *pxar.Entry) (io.ReadCloser, error)

	// ReadEntryAt reads a full pxar entry at the given archive byte offset.
	// Used for re-reading entries with full metadata after a minimal ListDirectory.
	ReadEntryAt(offset int64) (*pxar.Entry, error)

	// ReadCatalog streams the full directory tree via a callback with
	// minimal decoding. For each entry, fn is called. If fn returns a
	// non-nil error, iteration stops and the error is returned.
	ReadCatalog(fn func(CatalogEntry) error) error

	// Close releases resources.
	Close() error
}

// FileReader reads from a standalone .pxar file using an io.ReadSeeker.
// For split archives (v2), provide both the metadata and payload readers.
type FileReader struct {
	accessor *accessor.Accessor
	closers  []io.Closer
}

// NewFileReader creates a reader for a standalone .pxar file.
func NewFileReader(reader io.ReadSeeker) *FileReader {
	return &FileReader{
		accessor: accessor.NewAccessor(reader),
	}
}

// NewSplitFileReader creates a reader for a split (v2) archive
// with separate metadata and payload streams.
func NewSplitFileReader(metaReader, payloadReader io.ReadSeeker) *FileReader {
	return &FileReader{
		accessor: accessor.NewAccessor(metaReader, payloadReader),
	}
}

func (r *FileReader) ReadRoot() (*pxar.Entry, error) {
	return r.accessor.ReadRoot()
}

func (r *FileReader) ReadRootFull() (*pxar.Entry, error) {
	return r.accessor.ReadRootFull()
}

func (r *FileReader) Lookup(path string) (*pxar.Entry, error) {
	return r.accessor.Lookup(path)
}

func (r *FileReader) ListDirectory(dirOffset int64, opts accessor.ListOption, fn func(*pxar.Entry) error) error {
	return r.accessor.ListDirectory(dirOffset, opts, fn)
}

// ReadEntryAt reads a full pxar entry at the given archive byte offset.

func (r *FileReader) ReadFileContentReader(entry *pxar.Entry) (io.ReadCloser, error) {
	return r.accessor.ReadFileContentReader(entry)
}
func (r *FileReader) ReadEntryAt(offset int64) (*pxar.Entry, error) {
	return r.accessor.ReadEntryAt(offset)
}

// ReadEntryAtMinimal reads a pxar entry with minimal decoding (stat only).
func (r *FileReader) ReadEntryAtMinimal(offset int64) (*pxar.Entry, error) {
	return r.accessor.ReadEntryAtMinimal(offset)
}

func (r *FileReader) ReadCatalog(fn func(CatalogEntry) error) error {
	return readCatalog(r, fn)
}

func (r *FileReader) Close() error {
	var err error
	for _, c := range r.closers {
		if closeErr := c.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

// ChunkedReader reads from a chunked archive (.pxar.didx).
// It lazily loads chunks on demand using a ReadSeeker, avoiding
// full-stream-in-memory reconstruction.
type ChunkedReader struct {
	source  datastore.ChunkSource
	inner   *FileReader
	idx     *datastore.DynamicIndexReader
	lazy    *ReadSeeker
	closers []io.Closer
}

// NewChunkedReader creates a reader for a chunked .pxar.didx archive
// using lazy on-demand chunk loading. This avoids reconstructing the entire
// stream into memory — only chunks needed for Lookups and ReadFileContent
// calls are loaded.
func NewChunkedReader(idxData []byte, source datastore.ChunkSource) (*ChunkedReader, error) {
	idx, err := datastore.ParseDynamicIndex(idxData)
	if err != nil {
		return nil, fmt.Errorf("read dynamic index: %w", err)
	}

	// Use lazy read-seeker instead of full reconstruction
	lazyReader := NewReadSeeker(idx, source, 64)
	return &ChunkedReader{
		inner:  NewFileReader(lazyReader),
		idx:    idx,
		source: source,
		lazy:   lazyReader,
	}, nil
}

// OpenChunkedReader opens a .pxar.didx archive from disk, mapping the index instead of loading it, and unmaps it on Close.
func OpenChunkedReader(idxPath string, source datastore.ChunkSource) (*ChunkedReader, error) {
	idx, err := datastore.OpenDynamicIndex(idxPath)
	if err != nil {
		return nil, fmt.Errorf("open dynamic index: %w", err)
	}

	lazyReader := NewReadSeeker(idx, source, 64)
	return &ChunkedReader{
		inner:   NewFileReader(lazyReader),
		idx:     idx,
		source:  source,
		lazy:    lazyReader,
		closers: []io.Closer{idx},
	}, nil
}

func (r *ChunkedReader) ReaderAt() io.ReaderAt {
	if r.lazy != nil {
		return r.lazy
	}
	return nil
}

func (r *ChunkedReader) ReadRoot() (*pxar.Entry, error) {
	return r.inner.ReadRoot()
}

func (r *ChunkedReader) ReadRootFull() (*pxar.Entry, error) {
	return r.inner.ReadRootFull()
}

func (r *ChunkedReader) Lookup(path string) (*pxar.Entry, error) {
	return r.inner.Lookup(path)
}

func (r *ChunkedReader) ListDirectory(dirOffset int64, opts accessor.ListOption, fn func(*pxar.Entry) error) error {
	return r.inner.ListDirectory(dirOffset, opts, fn)
}

func (r *ChunkedReader) ReadFileContentReader(entry *pxar.Entry) (io.ReadCloser, error) {
	return r.inner.ReadFileContentReader(entry)
}
func (r *ChunkedReader) ReadEntryAt(offset int64) (*pxar.Entry, error) {
	return r.inner.ReadEntryAt(offset)
}

func (r *ChunkedReader) ReadEntryAtMinimal(offset int64) (*pxar.Entry, error) {
	return r.inner.ReadEntryAtMinimal(offset)
}

func (r *ChunkedReader) ReadCatalog(fn func(CatalogEntry) error) error {
	return readCatalog(r.inner, fn)
}

func (r *ChunkedReader) Close() error {
	var err error
	if r.lazy != nil {
		if closeErr := r.lazy.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	if closeErr := r.inner.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	for _, c := range r.closers {
		if closeErr := c.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

// SplitReader reads from a split chunked archive (.mpxar.didx + .ppxar.didx).
// It uses lazy on-demand chunk loading for both metadata and payload streams,
// avoiding full-stream-in-memory reconstruction.
type SplitReader struct {
	source      datastore.ChunkSource
	inner       *FileReader
	metaIdx     *datastore.DynamicIndexReader
	payloadIdx  *datastore.DynamicIndexReader
	metaLazy    *ReadSeeker
	payloadLazy *ReadSeeker
	closers     []io.Closer
}

// NewSplitReader creates a reader for a split chunked archive using
// lazy on-demand chunk loading. Only chunks needed for Lookups and
// ReadFileContent calls are loaded, which is critical for same-datastore
// PBS transfers where downloading the entire payload stream is expensive.
func NewSplitReader(metaIdxData, payloadIdxData []byte, source datastore.ChunkSource) (*SplitReader, error) {
	metaIdx, err := datastore.ParseDynamicIndex(metaIdxData)
	if err != nil {
		return nil, fmt.Errorf("read metadata index: %w", err)
	}

	payloadIdx, err := datastore.ParseDynamicIndex(payloadIdxData)
	if err != nil {
		return nil, fmt.Errorf("read payload index: %w", err)
	}

	// Use lazy read-seekers for both streams
	metaLazy := NewReadSeeker(metaIdx, source, 32)
	payloadLazy := NewReadSeeker(payloadIdx, source, 64)

	return &SplitReader{
		inner:       NewSplitFileReader(metaLazy, payloadLazy),
		metaIdx:     metaIdx,
		payloadIdx:  payloadIdx,
		source:      source,
		metaLazy:    metaLazy,
		payloadLazy: payloadLazy,
	}, nil
}

// OpenSplitReader opens a split archive from disk, mapping both indexes instead of loading them, and unmaps them on Close.
func OpenSplitReader(metaIdxPath, payloadIdxPath string, source datastore.ChunkSource) (*SplitReader, error) {
	metaIdx, err := datastore.OpenDynamicIndex(metaIdxPath)
	if err != nil {
		return nil, fmt.Errorf("open metadata index: %w", err)
	}

	payloadIdx, err := datastore.OpenDynamicIndex(payloadIdxPath)
	if err != nil {
		_ = metaIdx.Close()
		return nil, fmt.Errorf("open payload index: %w", err)
	}

	metaLazy := NewReadSeeker(metaIdx, source, 32)
	payloadLazy := NewReadSeeker(payloadIdx, source, 64)

	return &SplitReader{
		inner:       NewSplitFileReader(metaLazy, payloadLazy),
		metaIdx:     metaIdx,
		payloadIdx:  payloadIdx,
		source:      source,
		metaLazy:    metaLazy,
		payloadLazy: payloadLazy,
		closers:     []io.Closer{metaIdx, payloadIdx},
	}, nil
}

// only downloads and uses the metadata stream. The payload stream is never
// fetched. ReadFileContent/ReadFileContentReader will return errors for files
// stored in the payload stream (PayloadOffset > 0).
func NewSplitReaderMetaOnly(metaIdxData []byte, source datastore.ChunkSource) (*SplitReader, error) {
	metaIdx, err := datastore.ParseDynamicIndex(metaIdxData)
	if err != nil {
		return nil, fmt.Errorf("read metadata index: %w", err)
	}

	metaLazy := NewReadSeeker(metaIdx, source, 32)

	return &SplitReader{
		inner:    NewFileReader(metaLazy),
		metaIdx:  metaIdx,
		source:   source,
		metaLazy: metaLazy,
	}, nil
}

func (r *SplitReader) PayloadReaderAt() io.ReaderAt {
	if r.payloadLazy != nil {
		return r.payloadLazy
	}
	return nil
}

// PayloadIndex returns the payload dynamic index, or nil for metadata-only readers.
func (r *SplitReader) PayloadIndex() *datastore.DynamicIndexReader {
	return r.payloadIdx
}

func (r *SplitReader) ReadRoot() (*pxar.Entry, error) {
	return r.inner.ReadRoot()
}

func (r *SplitReader) ReadRootFull() (*pxar.Entry, error) {
	return r.inner.ReadRootFull()
}

func (r *SplitReader) Lookup(path string) (*pxar.Entry, error) {
	return r.inner.Lookup(path)
}

func (r *SplitReader) ListDirectory(dirOffset int64, opts accessor.ListOption, fn func(*pxar.Entry) error) error {
	return r.inner.ListDirectory(dirOffset, opts, fn)
}

func (r *SplitReader) ReadFileContentReader(entry *pxar.Entry) (io.ReadCloser, error) {
	return r.inner.ReadFileContentReader(entry)
}
func (r *SplitReader) ReadEntryAt(offset int64) (*pxar.Entry, error) {
	return r.inner.ReadEntryAt(offset)
}

func (r *SplitReader) ReadEntryAtMinimal(offset int64) (*pxar.Entry, error) {
	return r.inner.ReadEntryAtMinimal(offset)
}

func (r *SplitReader) ReadCatalog(fn func(CatalogEntry) error) error {
	return readCatalog(r.inner, fn)
}

func (r *SplitReader) SetPayloadCacheSize(n int) {
	if r.payloadLazy != nil {
		r.payloadLazy.SetCacheSize(n)
	}
}

func (r *SplitReader) SetMetaCacheSize(n int) {
	if r.metaLazy != nil {
		r.metaLazy.SetCacheSize(n)
	}
}

func (r *SplitReader) Close() error {
	var err error
	if r.metaLazy != nil {
		if closeErr := r.metaLazy.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	if r.payloadLazy != nil {
		if closeErr := r.payloadLazy.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	if closeErr := r.inner.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	for _, c := range r.closers {
		if closeErr := c.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}
