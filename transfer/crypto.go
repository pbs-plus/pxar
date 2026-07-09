package transfer

import (
	"fmt"
	"io"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/accessor"
	"github.com/pbs-plus/pxar/datastore"
)

// DecryptSource wraps a ChunkSource and decrypts/decompresses chunks
// on the fly. This is used when reading from an encrypted archive where the raw
// chunks are encrypted blobs that need to be decoded before restoration.
//
// When a CryptConfig is provided, encrypted blobs are decrypted. All blobs are
// decoded (uncompressed/decrypted) before being returned, producing the raw
// chunk data that the Restorer expects.
type DecryptSource struct {
	inner datastore.ChunkSource
	cc    *datastore.CryptConfig
}

// NewDecryptSource creates a chunk source that decrypts chunks after retrieval.
// Pass nil for cc if the archive is not encrypted (only decompression is needed).
func NewDecryptSource(inner datastore.ChunkSource, cc *datastore.CryptConfig) *DecryptSource {
	return &DecryptSource{
		inner: inner,
		cc:    cc,
	}
}

func (d *DecryptSource) GetChunk(digest [32]byte) ([]byte, error) {
	raw, err := d.inner.GetChunk(digest)
	if err != nil {
		return nil, fmt.Errorf("get chunk %x: %w", digest[:8], err)
	}

	// Check if this is an encrypted blob
	if len(raw) >= 8 {
		var magic [8]byte
		copy(magic[:], raw[:8])

		if datastore.IsEncryptedMagic(magic) {
			if d.cc == nil {
				return nil, fmt.Errorf("encrypted chunk %x but no CryptConfig provided", digest[:8])
			}
			decrypted, err := datastore.DecodeEncryptedBlob(raw, d.cc)
			if err != nil {
				return nil, fmt.Errorf("decrypt chunk %x: %w", digest[:8], err)
			}
			return decrypted, nil
		}
	}

	// Non-encrypted blob, just decode normally (handles uncompressed and compressed)
	decoded, err := datastore.DecodeBlob(raw)
	if err != nil {
		return nil, fmt.Errorf("decode chunk %x: %w", digest[:8], err)
	}
	return decoded, nil
}

// DecryptingReader is a placeholder for per-file decryption support.
// For most cases, using DecryptSource when constructing the underlying
// reader (ChunkedReader/SplitReader) is preferred since it
// handles decryption at the chunk level before stream reconstruction.
// This type simply delegates to the inner reader.
type DecryptingReader struct {
	inner ArchiveReader
}

// NewDecryptingReader wraps an ArchiveReader for transparent access.
// The underlying reader should already be configured with a DecryptSource
// if decryption is needed.
func NewDecryptingReader(inner ArchiveReader) *DecryptingReader {
	return &DecryptingReader{inner: inner}
}

func (r *DecryptingReader) ReadRoot() (*pxar.Entry, error) {
	return r.inner.ReadRoot()
}

func (r *DecryptingReader) ReadRootFull() (*pxar.Entry, error) {
	return r.inner.ReadRootFull()
}

func (r *DecryptingReader) Lookup(path string) (*pxar.Entry, error) {
	return r.inner.Lookup(path)
}

func (r *DecryptingReader) ListDirectory(dirOffset int64, opts accessor.ListOption, fn func(*pxar.Entry) error) error {
	return r.inner.ListDirectory(dirOffset, opts, fn)
}

func (r *DecryptingReader) ReadFileContentReader(entry *pxar.Entry) (io.ReadCloser, error) {
	return r.inner.ReadFileContentReader(entry)
}

func (r *DecryptingReader) ReadEntryAt(offset int64) (*pxar.Entry, error) {
	return r.inner.ReadEntryAt(offset)
}

func (r *DecryptingReader) ReadCatalog(fn func(CatalogEntry) error) error {
	return r.inner.ReadCatalog(fn)
}

func (r *DecryptingReader) Close() error {
	return r.inner.Close()
}
