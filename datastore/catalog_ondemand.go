package datastore

import (
	"encoding/binary"
	"fmt"

	"github.com/pbs-plus/pxar/format"
)

// dirLocation records where a directory's children start in the metadata stream.
type dirLocation struct {
	chunkIdx int // index into DynamicIndexReader
	offset   int // byte offset within the decoded chunk
}

// DirIndex is a lightweight mapping from directory paths to their positions
// in the metadata stream. Use with OnDemandCatalog for lazy directory loading.
//
// Memory: ~80 bytes per directory (path string + location struct).
// For 1M directories: ~80 MB (vs 2+ GB for a full Catalog).
type DirIndex struct {
	entries map[string]dirLocation
}

// HasDir reports whether a directory path exists in the index.
func (idx *DirIndex) HasDir(path string) bool {
	_, ok := idx.entries[path]
	return ok
}

// DirPaths returns all known directory paths in unspecified order.
func (idx *DirIndex) DirPaths() []string {
	paths := make([]string, 0, len(idx.entries))
	for p := range idx.entries {
		paths = append(paths, p)
	}
	return paths
}

// NumDirs returns the number of directories in the index.
func (idx *DirIndex) NumDirs() int {
	return len(idx.entries)
}

// BuildDirIndex builds a lightweight directory location index by scanning
// metadata chunks. Unlike BuildCatalogFast, it does not store child entries —
// only path → (chunk, offset) mappings for each directory.
//
// Peak memory: O(directory_count × avg_path_length) for the index + transient
// chunk data during download (released after indexing).
func BuildDirIndex(
	metaIdx *DynamicIndexReader,
	source ChunkSource,
	opts CatalogOptions,
) (*DirIndex, error) {
	if metaIdx.Count() == 0 {
		return &DirIndex{entries: make(map[string]dirLocation)}, nil
	}

	maxWorkers := opts.MaxWorkers
	if maxWorkers <= 0 {
		maxWorkers = 4
	}
	if maxWorkers > metaIdx.Count() {
		maxWorkers = metaIdx.Count()
	}

	// Phase 1: Parallel chunk download + decode (transient).
	decodedChunks, chunkErrs := downloadChunks(metaIdx, source, maxWorkers)
	for i := range chunkErrs {
		if chunkErrs[i] != nil {
			return nil, chunkErrs[i]
		}
	}

	// Phase 2: Sequential scan for directory locations only.
	r := &chunkReader{chunks: decodedChunks}
	index := &DirIndex{entries: make(map[string]dirLocation)}

	// Skip FORMAT_VERSION + PRELUDE headers.
	for r.remaining() >= format.HeaderSize {
		h := r.readHeader()
		if h.Type == format.PXARFormatVersion || h.Type == format.PXARPrelude {
			if r.remaining() < int(h.ContentSize()) {
				return nil, fmt.Errorf("not enough bytes for %s content", h.String())
			}
			r.skip(int(h.ContentSize()))
			continue
		}
		if err := indexDirTree(r, h, index); err != nil {
			return nil, err
		}
		return index, nil
	}

	return index, nil
}

// indexDirTree scans the metadata stream and records directory locations.
// Reuses chunkReader and scanEntryAttributes from catalog_fast.go.
func indexDirTree(r *chunkReader, first format.Header, index *DirIndex) error {
	if first.Type != format.PXAREntry {
		return fmt.Errorf("expected ENTRY header, got %s", first.String())
	}

	if r.remaining() < 40 {
		return fmt.Errorf("not enough bytes for root stat")
	}
	stat := format.UnmarshalStatBytes(r.read(40))

	if !stat.IsDir() {
		return fmt.Errorf("root entry is not a directory")
	}

	if err := skipUntilStructural(r); err != nil {
		return fmt.Errorf("scanning root attributes: %w", err)
	}

	// Record root directory location.
	index.entries["/"] = dirLocation{chunkIdx: r.ci, offset: r.pos}

	dirStack := make([]string, 0, 32)
	dirStack = append(dirStack, "/")
	var pathBuf []byte

	for r.remaining() >= format.HeaderSize {
		h := r.readHeader()

		switch h.Type {
		case format.PXARFilename:
			if r.remaining() < int(h.ContentSize()) {
				return fmt.Errorf("not enough bytes for filename content")
			}
			name := readFilename(r, h)
			if len(dirStack) == 0 {
				return fmt.Errorf("filename %q outside of directory", name)
			}
			parentPath := dirStack[len(dirStack)-1]

			if r.remaining() < format.HeaderSize {
				return fmt.Errorf("not enough bytes for entry after filename %q", name)
			}
			h2 := r.readHeader()

			switch h2.Type {
			case format.PXARHardlink:
				if r.remaining() < int(h2.ContentSize()) {
					return fmt.Errorf("not enough bytes for hardlink content of %q", name)
				}
				r.skip(int(h2.ContentSize()))

			case format.PXAREntry:
				if r.remaining() < 40 {
					return fmt.Errorf("not enough bytes for stat of %q", name)
				}
				es := format.UnmarshalStatBytes(r.read(40))

				kind, _, err := scanEntryAttributes(r, es)
				if err != nil {
					return fmt.Errorf("scanning attributes for %q: %w", name, err)
				}

				if kind == KindDirectory {
					pathBuf = buildChildPath(pathBuf[:0], parentPath, name)
					childPath := string(append([]byte(nil), pathBuf...))
					// Current position is at the directory's first FILENAME
					// or GOODBYE child — record it.
					index.entries[childPath] = dirLocation{chunkIdx: r.ci, offset: r.pos}
					dirStack = append(dirStack, childPath)
				}

			default:
				return fmt.Errorf("expected ENTRY or HARDLINK after filename %q, got %s", name, h2.String())
			}

		case format.PXARGoodbye:
			if r.remaining() < int(h.ContentSize()) {
				return fmt.Errorf("not enough bytes for goodbye content")
			}
			r.skip(int(h.ContentSize()))
			if len(dirStack) > 1 {
				dirStack = dirStack[:len(dirStack)-1]
			}

		default:
			if r.remaining() < int(h.ContentSize()) {
				return fmt.Errorf("not enough bytes for unknown header content")
			}
			r.skip(int(h.ContentSize()))
		}
	}

	return nil
}

// --- OnDemandCatalog ---

// OnDemandCatalog provides lazy directory listing over a metadata stream.
// Only the lightweight DirIndex (~80 MB for 1M dirs) is held in memory;
// chunks are fetched from the source when ListDir is called. A single-chunk
// cache avoids re-fetching the most recently used chunk.
//
// Not safe for concurrent use. Caller-side synchronization is required
// if multiple goroutines access the catalog simultaneously.
//
// Memory: DirIndex + 1 cached chunk (~4 MB).
type OnDemandCatalog struct {
	index   *DirIndex
	source  ChunkSource
	metaIdx *DynamicIndexReader

	cachedIdx  int
	cachedData []byte
}

// NewOnDemandCatalog creates a lazy catalog backed by the given index and source.
func NewOnDemandCatalog(index *DirIndex, metaIdx *DynamicIndexReader, source ChunkSource) *OnDemandCatalog {
	return &OnDemandCatalog{
		index:     index,
		source:    source,
		metaIdx:   metaIdx,
		cachedIdx: -1,
	}
}

// HasDir reports whether a directory exists in the index.
func (c *OnDemandCatalog) HasDir(path string) bool {
	return c.index.HasDir(path)
}

// DirPaths returns all known directory paths.
func (c *OnDemandCatalog) DirPaths() []string {
	return c.index.DirPaths()
}

// NumDirs returns the total number of directories.
func (c *OnDemandCatalog) NumDirs() int {
	return c.index.NumDirs()
}

// ListDir fetches and parses a single directory's children on demand.
// Only the chunk(s) containing that directory's entries are fetched.
// Returned children own their data — safe to retain across calls.
//
// If the directory has nested subdirectories, their subtrees are skipped
// without being parsed. Only the direct children of the requested directory
// are returned.
func (c *OnDemandCatalog) ListDir(path string) ([]CatalogChild, error) {
	loc, ok := c.index.entries[path]
	if !ok {
		return nil, fmt.Errorf("directory %q not found in index", path)
	}

	// Fetch the first chunk containing this directory.
	data, err := c.fetchChunk(loc.chunkIdx)
	if err != nil {
		return nil, fmt.Errorf("fetching chunk %d for dir %q: %w", loc.chunkIdx, path, err)
	}

	lr := &lazyChunkReader{
		cat:       c,
		baseChunk: loc.chunkIdx,
		chunks:    [][]byte{data},
		pos:       loc.offset,
	}

	children := make([]CatalogChild, 0, 16)
	depth := 0 // nesting depth for skipping subtrees
	var pending format.Header
	hasPending := false

	for {
		var h format.Header
		if hasPending {
			h = pending
			hasPending = false
		} else {
			var err error
			h, err = lr.readHeader()
			if err != nil {
				return children, nil // EOF
			}
		}

		switch h.Type {
		case format.PXARFilename:
			name, err := lr.readFilename(h)
			if err != nil {
				return nil, fmt.Errorf("reading filename in dir %q: %w", path, err)
			}

			h2, err := lr.readHeader()
			if err != nil {
				return nil, fmt.Errorf("reading entry after filename %q: %w", name, err)
			}

			switch h2.Type {
			case format.PXARHardlink:
				if err := lr.skip(int(h2.ContentSize())); err != nil {
					return nil, err
				}
				if depth == 0 {
					children = append(children, CatalogChild{Name: name, Kind: KindHardlink})
				}

			case format.PXAREntry:
				statBytes, err := lr.read(40)
				if err != nil {
					return nil, fmt.Errorf("reading stat for %q: %w", name, err)
				}
				stat := format.UnmarshalStatBytes(statBytes)

				kind, size, peeked, err := lr.scanAttributes(stat)
				if err != nil {
					return nil, fmt.Errorf("scanning attributes for %q: %w", name, err)
				}

				if depth == 0 {
					children = append(children, CatalogChild{Size: size, Name: name, Kind: kind})
				}
				if kind == KindDirectory {
					depth++
				}
				if peeked != nil {
					pending = *peeked
					hasPending = true
				}

			default:
				return nil, fmt.Errorf("unexpected %s after filename %q in dir %q", h2.String(), name, path)
			}

		case format.PXARGoodbye:
			if err := lr.skip(int(h.ContentSize())); err != nil {
				return nil, err
			}
			if depth > 0 {
				depth-- // closing a nested subdirectory
			} else {
				return children, nil // closing our directory — done
			}

		default:
			if err := lr.skip(int(h.ContentSize())); err != nil {
				return nil, err
			}
		}
	}
}

// fetchChunk returns decoded chunk data using a single-entry cache.
func (c *OnDemandCatalog) fetchChunk(idx int) ([]byte, error) {
	if idx == c.cachedIdx && c.cachedData != nil {
		return c.cachedData, nil
	}
	if idx < 0 || idx >= c.metaIdx.Count() {
		return nil, fmt.Errorf("chunk index %d out of range [0, %d)", idx, c.metaIdx.Count())
	}
	entry := c.metaIdx.Entry(idx)
	raw, err := c.source.GetChunk(entry.Digest)
	if err != nil {
		return nil, err
	}
	decoded, err := DecodeBlob(raw)
	if err != nil {
		return nil, err
	}
	c.cachedIdx = idx
	c.cachedData = decoded
	return decoded, nil
}

// --- lazyChunkReader ---

// lazyChunkReader reads across chunks, fetching new ones on demand from
// the OnDemandCatalog's chunk source. Unlike chunkReader, it does not
// support pushback — peeked headers are returned to the caller instead.
type lazyChunkReader struct {
	cat       *OnDemandCatalog
	baseChunk int      // chunks[0] corresponds to this metaIdx index
	chunks    [][]byte // fetched chunks, lazily grown
	ci        int      // current index in chunks slice
	pos       int      // offset in chunks[ci]
}

func (r *lazyChunkReader) ensureChunk() error {
	for r.ci >= len(r.chunks) {
		nextIdx := r.baseChunk + len(r.chunks)
		if nextIdx >= r.cat.metaIdx.Count() {
			return fmt.Errorf("no more chunks (need chunk %d, have %d fetched)", nextIdx, len(r.chunks))
		}
		data, err := r.cat.fetchChunk(nextIdx)
		if err != nil {
			return err
		}
		r.chunks = append(r.chunks, data)
	}
	return nil
}

func (r *lazyChunkReader) read(n int) ([]byte, error) {
	if err := r.ensureChunk(); err != nil {
		return nil, err
	}
	cur := r.chunks[r.ci]
	avail := len(cur) - r.pos
	if n <= avail {
		b := cur[r.pos : r.pos+n]
		r.pos += n
		if r.pos >= len(cur) {
			r.ci++
			r.pos = 0
		}
		return b, nil
	}

	// Cross-chunk span: assemble into a new buffer.
	buf := make([]byte, 0, n)
	buf = append(buf, cur[r.pos:]...)
	need := n - len(buf)
	r.ci++
	r.pos = 0
	for need > 0 {
		if err := r.ensureChunk(); err != nil {
			return nil, err
		}
		cur = r.chunks[r.ci]
		take := min(need, len(cur))
		buf = append(buf, cur[:take]...)
		need -= take
		r.pos = take
		if r.pos >= len(cur) {
			r.ci++
			r.pos = 0
		}
	}
	return buf, nil
}

func (r *lazyChunkReader) readHeader() (format.Header, error) {
	b, err := r.read(format.HeaderSize)
	if err != nil {
		return format.Header{}, err
	}
	return format.Header{
		Type: binary.LittleEndian.Uint64(b[0:8]),
		Size: binary.LittleEndian.Uint64(b[8:16]),
	}, nil
}

// readFilename returns the filename as a copied string (safe across cache evictions).
func (r *lazyChunkReader) readFilename(h format.Header) (string, error) {
	data, err := r.read(int(h.ContentSize()))
	if err != nil {
		return "", err
	}
	if len(data) > 0 && data[len(data)-1] == 0 {
		data = data[:len(data)-1]
	}
	return string(data), nil
}

func (r *lazyChunkReader) skip(n int) error {
	for n > 0 {
		if err := r.ensureChunk(); err != nil {
			return err
		}
		cur := r.chunks[r.ci]
		avail := len(cur) - r.pos
		if n < avail {
			r.pos += n
			return nil
		}
		n -= avail
		r.ci++
		r.pos = 0
	}
	return nil
}

// scanAttributes reads attribute headers after an ENTRY+Stat and returns
// the entry kind, file size, and a peeked header if the scan terminated at
// a structural header (FILENAME or GOODBYE) without consuming it.
// Unlike chunkReader's scanEntryAttributes, this does NOT push back —
// the caller is responsible for processing the peeked header.
func (r *lazyChunkReader) scanAttributes(stat format.Stat) (EntryKind, int64, *format.Header, error) {
	fileType := stat.Mode & format.ModeIFMT

	for {
		h, err := r.readHeader()
		if err != nil {
			return 0, 0, nil, err
		}
		contentSize := int(h.ContentSize())

		switch h.Type {
		case format.PXARSymlink:
			if err := r.skip(contentSize); err != nil {
				return 0, 0, nil, err
			}
			return KindSymlink, 0, nil, nil

		case format.PXARDevice:
			if err := r.skip(contentSize); err != nil {
				return 0, 0, nil, err
			}
			return KindDevice, 0, nil, nil

		case format.PXARPayload:
			if err := r.skip(contentSize); err != nil {
				return 0, 0, nil, err
			}
			return KindFile, int64(contentSize), nil, nil

		case format.PXARPayloadRef:
			data, err := r.read(contentSize)
			if err != nil {
				return 0, 0, nil, err
			}
			var sz int64
			if len(data) >= 16 {
				sz = int64(binary.LittleEndian.Uint64(data[8:16]))
			}
			return KindFile, sz, nil, nil

		case format.PXARFilename, format.PXARGoodbye:
			// Non-terminal — peeked but not consumed.
			switch fileType {
			case format.ModeIFIFO:
				return KindFifo, 0, &h, nil
			case format.ModeIFSOCK:
				return KindSocket, 0, &h, nil
			default:
				return KindDirectory, 0, &h, nil
			}

		default:
			if err := r.skip(contentSize); err != nil {
				return 0, 0, nil, err
			}
		}
	}
}
