package datastore

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/pbs-plus/pxar/format"
)

// dirLocation records where a directory's children live in the metadata
// stream and where they end (the byte AFTER the directory's GOODBYE).
type dirLocation struct {
	chunkIdx    int // where children start
	offset      int // byte offset within decoded chunk
	endChunkIdx int // byte after GOODBYE — next sibling or parent continuation
	endOffset   int
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

// BuildResult is returned by BuildDirIndex. It contains the lightweight
// DirIndex and, as a free side effect of the scan, the root directory's
// direct children.
type BuildResult struct {
	Index        *DirIndex
	RootChildren []CatalogChild
}

// BuildDirIndex builds a lightweight directory location index by scanning
// metadata chunks. It records start and end positions for every directory
// and returns root children as a side effect.
//
// Peak memory: O(directory_count × avg_path_length) for the index + one
// chunk at a time during download/scan (streaming).
func BuildDirIndex(
	metaIdx *DynamicIndexReader,
	source ChunkSource,
	opts CatalogOptions,
) (*BuildResult, error) {
	if metaIdx.Count() == 0 {
		return &BuildResult{
			Index:        &DirIndex{entries: make(map[string]dirLocation)},
			RootChildren: nil,
		}, nil
	}

	maxWorkers := resolveMaxWorkers(opts.MaxWorkers, metaIdx.Count())

	// Phase 1: Parallel chunk download + decode.
	decodedChunks, chunkErrs := downloadChunks(metaIdx, source, maxWorkers)
	for i := range chunkErrs {
		if chunkErrs[i] != nil {
			return nil, chunkErrs[i]
		}
	}

	// Phase 2: Single-pass scan using the unified tree scanner.
	r := &chunkReader{chunks: decodedChunks}
	result := &BuildResult{
		Index: &DirIndex{entries: make(map[string]dirLocation)},
	}

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
		if err := scanTree(r, h, &treeVisitor{
			Child: func(parentPath, name string, kind EntryKind, size int64) {
				if parentPath == "/" {
					result.RootChildren = append(result.RootChildren, CatalogChild{
						Name: name,
						Kind: kind,
						Size: size,
					})
				}
			},
			EnterDir: func(path string, chunkIdx, offset int) {
				result.Index.entries[path] = dirLocation{
					chunkIdx: chunkIdx,
					offset:   offset,
				}
			},
			ExitDir: func(path string, endChunkIdx, endOffset int) {
				if loc, ok := result.Index.entries[path]; ok {
					loc.endChunkIdx = endChunkIdx
					loc.endOffset = endOffset
					result.Index.entries[path] = loc
				}
			},
		}); err != nil {
			return nil, err
		}
		return result, nil
	}

	return result, nil
}

// --- OnDemandCatalog ---

// OnDemandCatalog provides lazy directory listing over a metadata stream.
// Only the lightweight DirIndex (~80 MB for 1M dirs) is held in memory;
// chunks are fetched from the source when ListDir is called. An LRU cache
// of decoded chunks avoids re-fetching recently used chunks.
//
// Safe for concurrent use. Internal LRU cache is mutex-protected.
type OnDemandCatalog struct {
	index   *DirIndex
	source  ChunkSource
	metaIdx *DynamicIndexReader

	mu    sync.Mutex
	cache lruChunkCache
}

// NewOnDemandCatalog creates a lazy catalog backed by the given index and source.
func NewOnDemandCatalog(index *DirIndex, metaIdx *DynamicIndexReader, source ChunkSource) *OnDemandCatalog {
	return &OnDemandCatalog{
		index:   index,
		source:  source,
		metaIdx: metaIdx,
		cache:   newLRUChunkCache(16),
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
// using end offsets from the DirIndex (when available) or depth tracking
// (fallback). Only the direct children of the requested directory are
// returned.
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

	for {
		h, err := lr.readHeader()
		if err != nil {
			return children, nil // EOF
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
					if depth == 0 {
						// Try to skip the entire subtree using end offsets.
						childPath := buildChildPathStr(path, name)
						if childLoc, ok := c.index.entries[childPath]; ok && childLoc.endChunkIdx > 0 {
							if err := lr.seekTo(childLoc.endChunkIdx, childLoc.endOffset); err == nil {
								continue // subtree skipped
							}
						}
					}
					depth++
				}
				if peeked != nil {
					// Rewind: re-process the peeked header in the next iteration.
					if err := lr.pushbackHeader(*peeked); err != nil {
						return nil, err
					}
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

// fetchChunk returns decoded chunk data using the LRU cache.
func (c *OnDemandCatalog) fetchChunk(idx int) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if data, ok := c.cache.Get(idx); ok {
		return data, nil
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
	c.cache.Put(idx, decoded)
	return decoded, nil
}

// buildChildPathStr constructs a child directory path. Used by ListDir
// to look up subdirectory end offsets.
func buildChildPathStr(parentPath, name string) string {
	if parentPath == "/" {
		return "/" + name
	}
	return parentPath + "/" + name
}

// --- LRU chunk cache ---

// lruChunkCache is a simple LRU cache for decoded chunk data.
// Not goroutine-safe — callers must hold external locks.
type lruChunkCache struct {
	capacity int
	items    map[int]*list.Element
	order    *list.List // front = most recent
}

type cacheEntry struct {
	idx  int
	data []byte
}

func newLRUChunkCache(capacity int) lruChunkCache {
	return lruChunkCache{
		capacity: capacity,
		items:    make(map[int]*list.Element, capacity),
		order:    list.New(),
	}
}

func (c *lruChunkCache) Get(idx int) ([]byte, bool) {
	if e, ok := c.items[idx]; ok {
		c.order.MoveToFront(e)
		return e.Value.(*cacheEntry).data, true
	}
	return nil, false
}

func (c *lruChunkCache) Put(idx int, data []byte) {
	if e, ok := c.items[idx]; ok {
		c.order.MoveToFront(e)
		e.Value.(*cacheEntry).data = data
		return
	}
	if c.order.Len() >= c.capacity {
		oldest := c.order.Back()
		if oldest != nil {
			c.order.Remove(oldest)
			delete(c.items, oldest.Value.(*cacheEntry).idx)
		}
	}
	e := c.order.PushFront(&cacheEntry{idx: idx, data: data})
	c.items[idx] = e
}

// --- lazyChunkReader ---

// lazyChunkReader reads across chunks, fetching new ones on demand from
// the OnDemandCatalog's chunk source. Supports seeking for subtree skip.
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

// seekTo jumps to an absolute (metaIdx chunk index, offset) position.
// Fetches the target chunk if not already cached. Chunks between the
// current position and target may be fetched but their data is discarded
// from the local chunks slice (still in the LRU cache).
func (r *lazyChunkReader) seekTo(metaChunkIdx, offset int) error {
	localIdx := metaChunkIdx - r.baseChunk
	if localIdx < 0 {
		return fmt.Errorf("cannot seek backwards past base chunk")
	}

	// Fetch chunks up to the target if not already fetched.
	for len(r.chunks) <= localIdx {
		nextIdx := r.baseChunk + len(r.chunks)
		if nextIdx >= r.cat.metaIdx.Count() {
			return fmt.Errorf("seek target chunk %d beyond stream", metaChunkIdx)
		}
		data, err := r.cat.fetchChunk(nextIdx)
		if err != nil {
			return err
		}
		r.chunks = append(r.chunks, data)
	}

	r.ci = localIdx
	r.pos = offset
	return nil
}

// pushbackHeader "unreads" a single header by rewinding 16 bytes.
// Only valid if the current position allows it.
func (r *lazyChunkReader) pushbackHeader(h format.Header) error {
	// Try to rewind within current chunk.
	if r.pos >= format.HeaderSize {
		r.pos -= format.HeaderSize
		return nil
	}
	// Cross-chunk pushback — rare but possible when a structural header
	// lands at the start of a new chunk.
	n := format.HeaderSize - r.pos
	if r.ci == 0 {
		return fmt.Errorf("pushback at start of stream")
	}
	r.ci--
	r.pos = len(r.chunks[r.ci]) - n
	return nil
}

// scanAttributes reads attribute headers after an ENTRY+Stat and returns
// the entry kind, file size, and a peeked header if the scan terminated at
// a structural header (FILENAME or GOODBYE) without consuming it.
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
