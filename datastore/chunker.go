package datastore

import (
	"crypto/sha256"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/pbs-plus/pxar/buzhash"
)

// ChunkResult describes a single chunk produced by the chunker pipeline.
type ChunkResult struct {
	Digest [32]byte // SHA-256 of raw chunk data
	Offset uint64   // start offset in the original stream
	Size   int      // chunk data size in bytes
	Exists bool     // true if chunk was already in the store
}

// StoreChunker splits a data stream into variable-size chunks using buzhash
// content-defined chunking, computes digests, stores chunks via ChunkStore,
// and builds a DynamicIndexWriter.
type StoreChunker struct {
	store    *ChunkStore
	config   buzhash.Config
	compress bool
}

// NewStoreChunker creates a chunker pipeline. If compress is true, chunks are
// stored as compressed DataBlobs; otherwise as uncompressed blobs.
func NewStoreChunker(store *ChunkStore, config buzhash.Config, compress bool) *StoreChunker {
	return &StoreChunker{
		store:    store,
		config:   config,
		compress: compress,
	}
}

// ChunkStream reads all data from r, splits it into chunks, stores each chunk,
// and builds a dynamic index. Returns the chunk results and the completed index
// writer (Finish has NOT been called on it yet).
func (sc *StoreChunker) ChunkStream(r io.Reader) ([]ChunkResult, *DynamicIndexWriter, error) {
	return sc.ChunkStreamCallback(r, nil)
}

var blobBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 4<<20)
		return &buf
	},
}

// ChunkStreamCallback is like ChunkStream but calls fn for each chunk after it
// is stored. If fn returns a non-nil error, chunking stops and the error is
// returned. If fn is nil, no callback is made.
func (sc *StoreChunker) ChunkStreamCallback(r io.Reader, fn func(ChunkResult) error) ([]ChunkResult, *DynamicIndexWriter, error) {
	index := NewDynamicIndexWriter(time.Now().Unix())
	chunker := buzhash.NewChunker(r, sc.config)

	results := make([]ChunkResult, 0, 64)
	var offset uint64
	var blobBuf *[]byte

	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("chunker read: %w", err)
		}

		digest := sha256.Sum256(chunk)

		var storeData []byte
		if sc.compress {
			if blobBuf == nil {
				blobBuf = blobBufPool.Get().(*[]byte)
			}
			encoded, err := EncodeCompressedBlobTo((*blobBuf)[:0], chunk)
			if err != nil {
				putBlobBuf(blobBuf)
				return nil, nil, fmt.Errorf("compress chunk at offset %d: %w", offset, err)
			}
			// Copy out since blobBuf will be reused
			storeData = make([]byte, len(encoded))
			copy(storeData, encoded)
		} else {
			if blobBuf == nil {
				blobBuf = blobBufPool.Get().(*[]byte)
			}
			encoded, err := EncodeBlobTo((*blobBuf)[:0], chunk)
			if err != nil {
				putBlobBuf(blobBuf)
				return nil, nil, fmt.Errorf("encode chunk at offset %d: %w", offset, err)
			}
			storeData = make([]byte, len(encoded))
			copy(storeData, encoded)
		}

		exists, _, err := sc.store.InsertChunk(digest, storeData)
		if err != nil {
			putBlobBuf(blobBuf)
			return nil, nil, fmt.Errorf("store chunk at offset %d: %w", offset, err)
		}

		endOffset := offset + uint64(len(chunk))
		index.Add(endOffset, digest)

		result := ChunkResult{
			Digest: digest,
			Offset: offset,
			Size:   len(chunk),
			Exists: exists,
		}
		results = append(results, result)
		offset = endOffset

		if fn != nil {
			if err := fn(result); err != nil {
				putBlobBuf(blobBuf)
				return results, index, err
			}
		}
	}

	if blobBuf != nil {
		putBlobBuf(blobBuf)
	}

	return results, index, nil
}

func putBlobBuf(bp *[]byte) {
	*bp = (*bp)[:0]
	blobBufPool.Put(bp)
}
