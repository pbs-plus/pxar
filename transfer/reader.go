// Package transfer provides utilities for transferring files between pxar archives.
package transfer

import (
	"bytes"
	"fmt"
	"io"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/accessor"
	"github.com/pbs-plus/pxar/datastore"
)

// ArchiveReader provides unified read access to any pxar archive format.
type ArchiveReader interface {
	// ReadRoot returns the root directory entry.
	ReadRoot() (*pxar.Entry, error)

	// Lookup finds an entry by archive-internal path.
	Lookup(path string) (*pxar.Entry, error)

	// LookupBatch looks up multiple paths in a single pass. More efficient
	// than N separate Lookup calls because it shares directory traversals
	// for common prefixes. Returns entries in the same order as paths;
	// nil means not found.
	LookupBatch(paths []string) ([]*pxar.Entry, error)

	// ListDirectory lists entries in a directory.
	ListDirectory(dirOffset int64) ([]pxar.Entry, error)

	// ListDirectoryWithOptions lists entries with selective metadata decoding.
	ListDirectoryWithOptions(dirOffset int64, opts ListOption) ([]pxar.Entry, error)

	// ReadFileContent reads the complete content of a regular file.
	ReadFileContent(entry *pxar.Entry) ([]byte, error)

	// ReadFileContentReader returns a streaming reader for file content.
	// The caller must close the reader. Use this for large files to avoid
	// buffering the entire content in memory.
	ReadFileContentReader(entry *pxar.Entry) (io.ReadCloser, error)

	// ReadCatalog extracts the full directory tree as a flat list of
	// CatalogEntry values with minimal decoding. Significantly faster
	// than WalkTree for indexing.
	ReadCatalog() ([]CatalogEntry, error)

	// Close releases resources.
	Close() error
}

// FileArchiveReader reads from a standalone .pxar file using an io.ReadSeeker.
// For split archives (v2), provide both the metadata and payload readers.
type FileArchiveReader struct {
	accessor *accessor.Accessor
	closers  []io.Closer
}

// NewFileArchiveReader creates a reader for a standalone .pxar file.
func NewFileArchiveReader(reader io.ReadSeeker) *FileArchiveReader {
	return &FileArchiveReader{
		accessor: accessor.NewAccessor(reader),
	}
}

// NewSplitFileArchiveReader creates a reader for a split (v2) archive
// with separate metadata and payload streams.
func NewSplitFileArchiveReader(metaReader, payloadReader io.ReadSeeker) *FileArchiveReader {
	return &FileArchiveReader{
		accessor: accessor.NewAccessor(metaReader, payloadReader),
	}
}

func (r *FileArchiveReader) ReadRoot() (*pxar.Entry, error) {
	return r.accessor.ReadRoot()
}

func (r *FileArchiveReader) Lookup(path string) (*pxar.Entry, error) {
	return r.accessor.Lookup(path)
}

func (r *FileArchiveReader) LookupBatch(paths []string) ([]*pxar.Entry, error) {
	return r.accessor.LookupBatch(paths)
}

func (r *FileArchiveReader) ListDirectory(dirOffset int64) ([]pxar.Entry, error) {
	return r.accessor.ListDirectory(dirOffset)
}

func (r *FileArchiveReader) ListDirectoryWithOptions(dirOffset int64, opts ListOption) ([]pxar.Entry, error) {
	return r.accessor.ListDirectoryWithOptions(dirOffset, accessor.ListOption{Minimal: opts.Minimal})
}

func (r *FileArchiveReader) ReadFileContent(entry *pxar.Entry) ([]byte, error) {
	return r.accessor.ReadFileContent(entry)
}

func (r *FileArchiveReader) ReadFileContentReader(entry *pxar.Entry) (io.ReadCloser, error) {
	return r.accessor.ReadFileContentReader(entry)
}

func (r *FileArchiveReader) ReadCatalog() ([]CatalogEntry, error) {
	return readCatalog(r)
}

func (r *FileArchiveReader) Close() error {
	var err error
	for _, c := range r.closers {
		if closeErr := c.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

// ChunkedArchiveReader reads from a chunked archive (.pxar.didx).
// It lazily loads chunks on demand using a ChunkedReadSeeker, avoiding
// full-stream-in-memory reconstruction. For small archives where full
// reconstruction is acceptable, use NewChunkedArchiveReaderEager.
type ChunkedArchiveReader struct {
	inner   *FileArchiveReader
	idx     *datastore.DynamicIndexReader
	source  datastore.ChunkSource
	closers []io.Closer
	lazy    *ChunkedReadSeeker // track for cleanup
}

// NewChunkedArchiveReader creates a reader for a chunked .pxar.didx archive
// using lazy on-demand chunk loading. This avoids reconstructing the entire
// stream into memory — only chunks needed for Lookups and ReadFileContent
// calls are loaded.
func NewChunkedArchiveReader(idxData []byte, source datastore.ChunkSource) (*ChunkedArchiveReader, error) {
	idx, err := datastore.ReadDynamicIndex(idxData)
	if err != nil {
		return nil, fmt.Errorf("read dynamic index: %w", err)
	}

	// Use lazy read-seeker instead of full reconstruction
	lazyReader := NewChunkedReadSeeker(idx, source, 64)
	return &ChunkedArchiveReader{
		inner:  NewFileArchiveReader(lazyReader),
		idx:    idx,
		source: source,
		lazy:   lazyReader,
	}, nil
}

// NewChunkedArchiveReaderEager creates a reader that reconstructs the entire
// stream into memory upfront. Use this for small archives or when you need
// guaranteed sequential access performance.
func NewChunkedArchiveReaderEager(idxData []byte, source datastore.ChunkSource) (*ChunkedArchiveReader, error) {
	idx, err := datastore.ReadDynamicIndex(idxData)
	if err != nil {
		return nil, fmt.Errorf("read dynamic index: %w", err)
	}

	// Reconstruct the full stream into a buffer
	var buf bytes.Buffer
	restorer := datastore.NewRestorer(source)
	if err := restorer.RestoreFile(idx, &buf); err != nil {
		return nil, fmt.Errorf("restore archive stream: %w", err)
	}

	reader := bytes.NewReader(buf.Bytes())
	return &ChunkedArchiveReader{
		inner:  NewFileArchiveReader(reader),
		idx:    idx,
		source: source,
	}, nil
}

func (r *ChunkedArchiveReader) ReadRoot() (*pxar.Entry, error) {
	return r.inner.ReadRoot()
}

func (r *ChunkedArchiveReader) Lookup(path string) (*pxar.Entry, error) {
	return r.inner.Lookup(path)
}

func (r *ChunkedArchiveReader) LookupBatch(paths []string) ([]*pxar.Entry, error) {
	return r.inner.LookupBatch(paths)
}

func (r *ChunkedArchiveReader) ListDirectory(dirOffset int64) ([]pxar.Entry, error) {
	return r.inner.ListDirectory(dirOffset)
}

func (r *ChunkedArchiveReader) ListDirectoryWithOptions(dirOffset int64, opts ListOption) ([]pxar.Entry, error) {
	return r.inner.ListDirectoryWithOptions(dirOffset, opts)
}

func (r *ChunkedArchiveReader) ReadFileContent(entry *pxar.Entry) ([]byte, error) {
	return r.inner.ReadFileContent(entry)
}

func (r *ChunkedArchiveReader) ReadFileContentReader(entry *pxar.Entry) (io.ReadCloser, error) {
	return r.inner.ReadFileContentReader(entry)
}

func (r *ChunkedArchiveReader) ReadCatalog() ([]CatalogEntry, error) {
	return r.inner.ReadCatalog()
}

func (r *ChunkedArchiveReader) Close() error {
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

// SplitArchiveReader reads from a split chunked archive (.mpxar.didx + .ppxar.didx).
// It uses lazy on-demand chunk loading for both metadata and payload streams,
// avoiding full-stream-in-memory reconstruction. For small archives, use
// NewSplitArchiveReaderEager.
type SplitArchiveReader struct {
	inner       *FileArchiveReader
	metaIdx     *datastore.DynamicIndexReader
	payloadIdx  *datastore.DynamicIndexReader
	source      datastore.ChunkSource
	closers     []io.Closer
	metaLazy    *ChunkedReadSeeker
	payloadLazy *ChunkedReadSeeker
}

// NewSplitArchiveReader creates a reader for a split chunked archive using
// lazy on-demand chunk loading. Only chunks needed for Lookups and
// ReadFileContent calls are loaded, which is critical for same-datastore
// PBS transfers where downloading the entire payload stream is expensive.
func NewSplitArchiveReader(metaIdxData, payloadIdxData []byte, source datastore.ChunkSource) (*SplitArchiveReader, error) {
	metaIdx, err := datastore.ReadDynamicIndex(metaIdxData)
	if err != nil {
		return nil, fmt.Errorf("read metadata index: %w", err)
	}

	payloadIdx, err := datastore.ReadDynamicIndex(payloadIdxData)
	if err != nil {
		return nil, fmt.Errorf("read payload index: %w", err)
	}

	// Use lazy read-seekers for both streams
	metaLazy := NewChunkedReadSeeker(metaIdx, source, 32)
	payloadLazy := NewChunkedReadSeeker(payloadIdx, source, 64)

	return &SplitArchiveReader{
		inner:       NewSplitFileArchiveReader(metaLazy, payloadLazy),
		metaIdx:     metaIdx,
		payloadIdx:  payloadIdx,
		source:      source,
		metaLazy:    metaLazy,
		payloadLazy: payloadLazy,
	}, nil
}

// NewSplitArchiveReaderMetaOnly creates a reader for a split archive that
// only downloads and uses the metadata stream. The payload stream is never
// fetched. ReadFileContent/ReadFileContentReader will return errors for files
// stored in the payload stream (PayloadOffset > 0).
func NewSplitArchiveReaderMetaOnly(metaIdxData []byte, source datastore.ChunkSource) (*SplitArchiveReader, error) {
	metaIdx, err := datastore.ReadDynamicIndex(metaIdxData)
	if err != nil {
		return nil, fmt.Errorf("read metadata index: %w", err)
	}

	metaLazy := NewChunkedReadSeeker(metaIdx, source, 32)

	return &SplitArchiveReader{
		inner:    NewFileArchiveReader(metaLazy),
		metaIdx:  metaIdx,
		source:   source,
		metaLazy: metaLazy,
	}, nil
}

// NewSplitArchiveReaderEager creates a reader that reconstructs both streams
// into memory upfront. Use for small archives or when you need guaranteed
// sequential access performance.
func NewSplitArchiveReaderEager(metaIdxData, payloadIdxData []byte, source datastore.ChunkSource) (*SplitArchiveReader, error) {
	metaIdx, err := datastore.ReadDynamicIndex(metaIdxData)
	if err != nil {
		return nil, fmt.Errorf("read metadata index: %w", err)
	}

	payloadIdx, err := datastore.ReadDynamicIndex(payloadIdxData)
	if err != nil {
		return nil, fmt.Errorf("read payload index: %w", err)
	}

	restorer := datastore.NewRestorer(source)

	// Reconstruct metadata stream
	var metaBuf bytes.Buffer
	if err := restorer.RestoreFile(metaIdx, &metaBuf); err != nil {
		return nil, fmt.Errorf("restore metadata stream: %w", err)
	}

	// Reconstruct payload stream
	var payloadBuf bytes.Buffer
	if err := restorer.RestoreFile(payloadIdx, &payloadBuf); err != nil {
		return nil, fmt.Errorf("restore payload stream: %w", err)
	}

	metaReader := bytes.NewReader(metaBuf.Bytes())
	payloadReader := bytes.NewReader(payloadBuf.Bytes())

	return &SplitArchiveReader{
		inner:      NewSplitFileArchiveReader(metaReader, payloadReader),
		metaIdx:    metaIdx,
		payloadIdx: payloadIdx,
		source:     source,
	}, nil
}

func (r *SplitArchiveReader) ReadRoot() (*pxar.Entry, error) {
	return r.inner.ReadRoot()
}

func (r *SplitArchiveReader) Lookup(path string) (*pxar.Entry, error) {
	return r.inner.Lookup(path)
}

func (r *SplitArchiveReader) LookupBatch(paths []string) ([]*pxar.Entry, error) {
	return r.inner.LookupBatch(paths)
}

func (r *SplitArchiveReader) ListDirectory(dirOffset int64) ([]pxar.Entry, error) {
	return r.inner.ListDirectory(dirOffset)
}

func (r *SplitArchiveReader) ListDirectoryWithOptions(dirOffset int64, opts ListOption) ([]pxar.Entry, error) {
	return r.inner.ListDirectoryWithOptions(dirOffset, opts)
}

func (r *SplitArchiveReader) ReadFileContent(entry *pxar.Entry) ([]byte, error) {
	return r.inner.ReadFileContent(entry)
}

func (r *SplitArchiveReader) ReadFileContentReader(entry *pxar.Entry) (io.ReadCloser, error) {
	return r.inner.ReadFileContentReader(entry)
}

func (r *SplitArchiveReader) ReadCatalog() ([]CatalogEntry, error) {
	return r.inner.ReadCatalog()
}

func (r *SplitArchiveReader) Close() error {
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
