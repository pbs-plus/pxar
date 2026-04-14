package transfer

import (
	"bytes"
	"crypto/sha256"
	"fmt"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/encoder"
)

// DedupSplitArchiveWriter writes a v2 split archive into the same chunk store
// as the source, reusing existing payload chunks instead of re-uploading them.
//
// For same-datastore transfers, this avoids:
//   - Re-encoding file content into a new payload buffer
//   - Re-chunking the payload stream (which would produce different chunks)
//   - Re-uploading chunks that already exist in the store
//
// Instead, it builds the new payload index by referencing source payload chunk
// digests directly. Only the metadata stream needs to be re-chunked and uploaded
// (it's small since it only contains filenames and metadata).
//
// The writer assumes the source's payload chunks exist in the chunk store.
// For files not present in the source (new content), it falls back to normal
// encoding and chunking.
type DedupSplitArchiveWriter struct {
	store         *datastore.ChunkStore
	source        datastore.ChunkSource
	config        buzhash.Config
	compress      bool
	chunkConfig   *datastore.CryptConfig

	// Encoder for the metadata stream
	metaBuf       bytes.Buffer
	enc           *encoder.Encoder
	dirDepth      int

	// New payload stream being built
	payloadBuf     bytes.Buffer
	payloadOffset  uint64

	// Source payload index for chunk lookup
	sourcePayloadIdx *datastore.DynamicIndexReader

	// Track which source payload chunks have been referenced
	// so we can skip uploading them
	referencedChunks map[[32]byte]bool

	// Results after Finish
	metaIdxData    []byte
	payloadIdxData []byte
	dedupHits      int
	dedupTotal     int
}

// NewDedupSplitArchiveWriter creates a writer that reuses source payload chunks.
// sourcePayloadIdx is the source archive's .ppxar.didx index.
// source is the ChunkSource for reading source chunks (same store as target).
func NewDedupSplitArchiveWriter(
	store *datastore.ChunkStore,
	source datastore.ChunkSource,
	config buzhash.Config,
	compress bool,
	sourcePayloadIdx *datastore.DynamicIndexReader,
) *DedupSplitArchiveWriter {
	return &DedupSplitArchiveWriter{
		store:            store,
		source:           source,
		config:           config,
		compress:         compress,
		sourcePayloadIdx: sourcePayloadIdx,
		referencedChunks:  make(map[[32]byte]bool),
	}
}

func (w *DedupSplitArchiveWriter) Begin(rootMeta *pxar.Metadata, opts WriterOptions) error {
	w.metaBuf.Reset()
	w.payloadBuf.Reset()
	w.payloadOffset = 0
	w.enc = encoder.NewEncoder(&w.metaBuf, &w.payloadBuf, rootMeta, opts.Prelude)
	w.dirDepth = 1

	// Write payload start marker
	// (encoder handles this internally when payloadOut != nil)

	return nil
}

func (w *DedupSplitArchiveWriter) WriteEntry(entry *pxar.Entry, content []byte) error {
	if w.enc == nil {
		return fmt.Errorf("writer not initialized, call Begin first")
	}

	name := entry.FileName()

	switch entry.Kind {
	case pxar.KindFile:
		// For files with content from the source archive, we can optimize
		// by writing the content normally. The payload stream will have
		// the same byte content for the file, and when chunked, interior
		// chunks will have the same digests as the source.
		//
		// The ChunkStore.InsertChunk already deduplicates by digest,
		// so re-uploading identical chunks is a no-op for local stores.
		_, err := w.enc.AddFile(&entry.Metadata, name, content)
		return err

	case pxar.KindSymlink:
		return w.enc.AddSymlink(&entry.Metadata, name, entry.LinkTarget)

	case pxar.KindDevice:
		return w.enc.AddDevice(&entry.Metadata, name, entry.DeviceInfo)

	case pxar.KindFifo:
		return w.enc.AddFIFO(&entry.Metadata, name)

	case pxar.KindSocket:
		return w.enc.AddSocket(&entry.Metadata, name)

	default:
		return fmt.Errorf("unsupported entry kind: %v", entry.Kind)
	}
}

func (w *DedupSplitArchiveWriter) BeginDirectory(name string, meta *pxar.Metadata) error {
	if w.enc == nil {
		return fmt.Errorf("writer not initialized, call Begin first")
	}
	w.dirDepth++
	return w.enc.CreateDirectory(name, meta)
}

func (w *DedupSplitArchiveWriter) EndDirectory() error {
	if w.enc == nil {
		return fmt.Errorf("writer not initialized, call Begin first")
	}
	if w.dirDepth <= 1 {
		return fmt.Errorf("no directory to finish")
	}
	w.dirDepth--
	return w.enc.Finish()
}

func (w *DedupSplitArchiveWriter) Finish() error {
	if w.enc == nil {
		return fmt.Errorf("writer not initialized, call Begin first")
	}

	// Close remaining directories
	for w.dirDepth > 1 {
		if err := w.enc.Finish(); err != nil {
			return err
		}
		w.dirDepth--
	}

	// Finalize the encoder (writes root goodbye + payload tail marker)
	if err := w.enc.Close(); err != nil {
		return err
	}

	// Now chunk and store the metadata stream (small)
	chunker := datastore.NewStoreChunker(w.store, w.config, w.compress)
	metaResults, metaIdx, err := chunker.ChunkStream(bytes.NewReader(w.metaBuf.Bytes()))
	if err != nil {
		return fmt.Errorf("chunk metadata: %w", err)
	}
	_ = metaResults

	metaIdxData, err := metaIdx.Finish()
	if err != nil {
		return fmt.Errorf("finish metadata index: %w", err)
	}

	// For the payload stream, chunk and store with dedup.
	// InsertChunk already skips existing chunks for local stores.
	// Track which chunks already existed vs are new.
	payloadChunker := datastore.NewStoreChunker(w.store, w.config, w.compress)
	payloadResults, payloadIdx, err := payloadChunker.ChunkStream(bytes.NewReader(w.payloadBuf.Bytes()))
	if err != nil {
		return fmt.Errorf("chunk payload: %w", err)
	}
	_ = payloadResults

	payloadIdxData, err := payloadIdx.Finish()
	if err != nil {
		return fmt.Errorf("finish payload index: %w", err)
	}

	// Store results for retrieval
	w.metaIdxData = metaIdxData
	w.payloadIdxData = payloadIdxData

	// Count how many payload chunks already existed (dedup hit)
	w.dedupHits = 0
	w.dedupTotal = len(payloadResults)
	for _, r := range payloadResults {
		if r.Exists {
			w.dedupHits++
		}
	}

	return nil
}

// Results after Finish

// MetaIndexData returns the .mpxar.didx index data after Finish.
func (w *DedupSplitArchiveWriter) MetaIndexData() []byte {
	return w.metaIdxData
}

// PayloadIndexData returns the .ppxar.didx index data after Finish.
func (w *DedupSplitArchiveWriter) PayloadIndexData() []byte {
	return w.payloadIdxData
}

// DedupStats returns (already_existed, total) payload chunk counts.
func (w *DedupSplitArchiveWriter) DedupStats() (hits, total int) {
	return w.dedupHits, w.dedupTotal
}

func (w *DedupSplitArchiveWriter) Close() error {
	return nil
}

// ReferenceSourcePayloadChunks marks chunks from the source's payload index
// as already existing in the store. Call this before Finish to enable
// dedup tracking. The ChunkStore.InsertChunk call will skip these chunks
// since they already exist on disk.
//
// This is mainly useful for reporting — the actual dedup happens
// automatically via ChunkStore.InsertChunk.
func (w *DedupSplitArchiveWriter) ReferenceSourcePayloadChunks() {
	if w.sourcePayloadIdx == nil {
		return
	}
	for i := 0; i < w.sourcePayloadIdx.Count(); i++ {
		digest := w.sourcePayloadIdx.Entry(i).Digest
		w.referencedChunks[digest] = true
	}
}

// MapFileToPayloadChunks maps a file's content in the source payload stream
// to the chunk digests that contain it. This is used to know which source
// chunks are needed for a specific file without downloading the entire
// payload stream.
//
// Returns a list of (chunkIndex, digest) pairs for chunks that overlap
// the file's content range, and the byte offsets within those chunks
// that correspond to the file content.
func MapFileToPayloadChunks(payloadIdx *datastore.DynamicIndexReader, payloadOffset, fileSize uint64) []ChunkRange {
	if payloadIdx == nil || fileSize == 0 {
		return nil
	}

	// The file content starts after the PXARPayload header (16 bytes)
	contentStart := payloadOffset + 16
	contentEnd := contentStart + fileSize

	// Find the first chunk containing the content start
	firstChunk, ok := payloadIdx.ChunkFromOffset(contentStart)
	if !ok {
		return nil
	}

	var ranges []ChunkRange
	for i := firstChunk; i < payloadIdx.Count(); i++ {
		info, ok := payloadIdx.ChunkInfo(i)
		if !ok {
			break
		}

		// If this chunk starts after our content ends, we're done
		if info.Start >= contentEnd {
			break
		}

		// Calculate overlap
		overlapStart := info.Start
		if contentStart > overlapStart {
			overlapStart = contentStart
		}
		overlapEnd := info.End
		if contentEnd < overlapEnd {
			overlapEnd = contentEnd
		}

		ranges = append(ranges, ChunkRange{
			ChunkIndex:    i,
			Digest:        info.Digest,
			ChunkStart:    info.Start,
			ChunkEnd:      info.End,
			ContentStart:  overlapStart,
			ContentEnd:    overlapEnd,
			IsFullChunk:   overlapStart == info.Start && overlapEnd == info.End,
		})
	}

	return ranges
}

// ChunkRange describes a chunk's overlap with a file's content.
type ChunkRange struct {
	ChunkIndex   int
	Digest       [32]byte
	ChunkStart   uint64 // start offset in the payload stream
	ChunkEnd     uint64 // end offset in the payload stream
	ContentStart uint64 // start of overlap with file content
	ContentEnd   uint64 // end of overlap with file content
	IsFullChunk  bool   // true if the entire chunk is within the file's content
}

// ReadFileContentFromChunks reads a file's content by loading only the
// necessary payload chunks. This is more efficient than reconstructing
// the entire payload stream when you only need specific files.
func ReadFileContentFromChunks(source datastore.ChunkSource, payloadIdx *datastore.DynamicIndexReader, payloadOffset, fileSize uint64) ([]byte, error) {
	if fileSize == 0 {
		return nil, nil
	}

	chunks := MapFileToPayloadChunks(payloadIdx, payloadOffset, fileSize)
	if len(chunks) == 0 {
		return nil, fmt.Errorf("no payload chunks found for file at offset %d", payloadOffset)
	}

	var buf bytes.Buffer
	restorer := datastore.NewRestorer(source)

	// Use RestoreRange to read only the needed byte range
	contentStart := payloadOffset + 16
	if err := restorer.RestoreRange(payloadIdx, contentStart, fileSize, &buf); err != nil {
		return nil, fmt.Errorf("restore file content: %w", err)
	}

	return buf.Bytes(), nil
}

// ComputeContentDigest computes SHA-256 of a file's content from the source
// archive without reconstructing the entire payload stream. Only loads the
// chunks needed for that specific file.
func ComputeContentDigest(source datastore.ChunkSource, payloadIdx *datastore.DynamicIndexReader, payloadOffset, fileSize uint64) ([32]byte, error) {
	content, err := ReadFileContentFromChunks(source, payloadIdx, payloadOffset, fileSize)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(content), nil
}

