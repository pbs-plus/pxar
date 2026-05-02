package datastore

import (
	"encoding/binary"
	"fmt"
	"sync"
	"unsafe"

	"github.com/pbs-plus/pxar/format"
)

// EntryKind identifies the type of a pxar archive entry in the catalog.
type EntryKind byte

const (
	KindSymlink   EntryKind = 2
	KindHardlink  EntryKind = 3
	KindDevice    EntryKind = 4
	KindSocket    EntryKind = 5
	KindFifo      EntryKind = 6
	KindFile      EntryKind = 7
	KindDirectory EntryKind = 8
)

// CatalogChild is a lightweight directory entry.
// Field order: largest first to minimize padding (Size int64, Name string, Kind byte).
// Total: 8 + 16 + 1 + 7pad = 32 bytes.
type CatalogChild struct {
	Size int64
	Name string
	Kind EntryKind
}

// Catalog is a lightweight directory tree: parentPath → children.
//
// The chunks field retains decoded chunk data so that filename strings
// (created via unsafe.String during parsing) remain valid for the
// Catalog's lifetime. Callers should not retain a Catalog longer than needed.
type Catalog struct {
	Dirs   map[string][]CatalogChild
	chunks [][]byte // keeps chunk data alive for unsafe.String filenames
}

// CatalogOptions configures BuildCatalogFast behavior.
type CatalogOptions struct {
	MaxWorkers int // parallel chunk downloads (default 4)
}

// resolveMaxWorkers clamps opts.MaxWorkers to a valid range.
func resolveMaxWorkers(maxWorkers, count int) int {
	if maxWorkers <= 0 {
		maxWorkers = 4
	}
	if maxWorkers > count {
		maxWorkers = count
	}
	return maxWorkers
}

// treeVisitor receives callbacks during scanTree. Set only the callbacks
// you need; nil callbacks are skipped.
type treeVisitor struct {
	// Child is called for every entry (file, dir, symlink, hardlink, etc.).
	Child func(parentPath, name string, kind EntryKind, size int64)

	// EnterDir is called when a directory's first child is about to be
	// scanned. chunkIdx/offset point to where the children start.
	EnterDir func(path string, chunkIdx, offset int)

	// ExitDir is called when a directory's GOODBYE marker has been
	// consumed. chunkIdx/offset point to the byte AFTER the GOODBYE
	// content — the position of the next sibling or the parent's
	// continuation.
	ExitDir func(path string, endChunkIdx, endOffset int)
}

// scanTree performs a single sequential pass over the pxar metadata stream,
// calling visitor callbacks at each structural boundary. It replaces the
// duplicated scanning loops previously in parseRootAndChildren and
// indexDirTree.
func scanTree(r *chunkReader, first format.Header, visit *treeVisitor) error {
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

	// Record root directory.
	if visit.EnterDir != nil {
		visit.EnterDir("/", r.ci, r.pos)
	}

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
				if visit.Child != nil {
					visit.Child(parentPath, name, KindHardlink, 0)
				}

			case format.PXAREntry:
				if r.remaining() < 40 {
					return fmt.Errorf("not enough bytes for stat of %q", name)
				}
				es := format.UnmarshalStatBytes(r.read(40))

				kind, size, err := scanEntryAttributes(r, es)
				if err != nil {
					return fmt.Errorf("scanning attributes for %q: %w", name, err)
				}

				if visit.Child != nil {
					visit.Child(parentPath, name, kind, size)
				}

				if kind == KindDirectory {
					pathBuf = buildChildPath(pathBuf[:0], parentPath, name)
					childPath := string(append([]byte(nil), pathBuf...))
					if visit.EnterDir != nil {
						visit.EnterDir(childPath, r.ci, r.pos)
					}
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

			if len(dirStack) > 0 && visit.ExitDir != nil {
				visit.ExitDir(dirStack[len(dirStack)-1], r.ci, r.pos)
			}
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

// chunkReader reads sequentially across an ordered slice of byte chunks
// without concatenating them. Zero-copy for in-chunk reads; small allocation
// only at chunk boundaries (rare — headers are 16 bytes, chunks are typically 4 MB).
type chunkReader struct {
	chunks [][]byte
	ci     int // current chunk index
	pos    int // offset within chunks[ci]
}

// read returns n contiguous bytes. For the common case (data fully within
// the current chunk), this is zero-copy — the returned slice points directly
// into the chunk's backing array. For cross-chunk spans (rare), a new buffer
// is allocated.
func (r *chunkReader) read(n int) []byte {
	cur := r.chunks[r.ci]
	avail := len(cur) - r.pos
	if n <= avail {
		b := cur[r.pos : r.pos+n]
		r.pos += n
		if r.pos == len(cur) && r.ci < len(r.chunks)-1 {
			r.ci++
			r.pos = 0
		}
		return b
	}

	// Cross-chunk span: assemble into a new buffer.
	buf := make([]byte, 0, n)
	buf = append(buf, cur[r.pos:]...)
	need := n - len(buf)
	r.ci++
	r.pos = 0
	for need > 0 {
		cur = r.chunks[r.ci]
		take := min(need, len(cur))
		buf = append(buf, cur[:take]...)
		need -= take
		r.pos = take
		if take == len(cur) && r.ci < len(r.chunks)-1 {
			r.ci++
			r.pos = 0
		}
	}
	return buf
}

// readHeader reads a 16-byte pxar header.
func (r *chunkReader) readHeader() format.Header {
	b := r.read(format.HeaderSize)
	return format.Header{
		Type: binary.LittleEndian.Uint64(b[0:8]),
		Size: binary.LittleEndian.Uint64(b[8:16]),
	}
}

// skip advances the reader position by n bytes across chunk boundaries.
func (r *chunkReader) skip(n int) {
	for n > 0 {
		cur := r.chunks[r.ci]
		avail := len(cur) - r.pos
		if n < avail {
			r.pos += n
			return
		}
		n -= avail
		r.ci++
		r.pos = 0
	}
}

// remaining returns the total unread bytes across all remaining chunks.
func (r *chunkReader) remaining() int {
	if r.ci >= len(r.chunks) {
		return 0
	}
	total := len(r.chunks[r.ci]) - r.pos
	for i := r.ci + 1; i < len(r.chunks); i++ {
		total += len(r.chunks[i])
	}
	return total
}

// pushback rewinds by n bytes (used for header pushback after peeking).
// Only valid for small n (typically format.HeaderSize = 16) within the
// current chunk. Panics if n exceeds current chunk position.
func (r *chunkReader) pushback(n int) {
	if r.pos >= n {
		r.pos -= n
		return
	}
	// Cross-chunk pushback: step back through chunks to find the position.
	// This is rare and only happens when a structural header (FILENAME/GOODBYE)
	// lands at the start of a new chunk.
	n -= r.pos
	r.ci--
	for r.ci >= 0 {
		cur := r.chunks[r.ci]
		if n <= len(cur) {
			r.pos = len(cur) - n
			return
		}
		n -= len(cur)
		r.ci--
	}
	panic("chunkReader.pushback: rewound past start of data")
}

// BuildCatalogFast downloads metadata chunks in parallel and performs a
// single sequential pass to build a directory catalog — no concatenation.
// Chunks are parsed in-place; filename strings are zero-copy views into
// chunk data kept alive by Catalog.chunks.
func BuildCatalogFast(
	metaIdx *DynamicIndexReader,
	source ChunkSource,
	opts CatalogOptions,
) (*Catalog, error) {
	if metaIdx.Count() == 0 {
		return &Catalog{Dirs: make(map[string][]CatalogChild)}, nil
	}

	maxWorkers := resolveMaxWorkers(opts.MaxWorkers, metaIdx.Count())

	// Phase 1: Parallel chunk download + decode.
	decodedChunks, chunkErrs := downloadChunks(metaIdx, source, maxWorkers)
	for i := range chunkErrs {
		if chunkErrs[i] != nil {
			return nil, chunkErrs[i]
		}
	}

	// Phase 2: Sequential single-pass parse directly from chunk slices.
	// No concatenation — the chunkReader reads across [][]byte boundaries.
	catalog := &Catalog{
		Dirs:   make(map[string][]CatalogChild),
		chunks: decodedChunks, // keep alive for unsafe.String filenames
	}
	r := &chunkReader{chunks: decodedChunks}

	// Skip FORMAT_VERSION header + content and optional PRELUDE.
	for r.remaining() >= format.HeaderSize {
		h := r.readHeader()
		if h.Type == format.PXARFormatVersion || h.Type == format.PXARPrelude {
			if r.remaining() < int(h.ContentSize()) {
				return nil, fmt.Errorf("not enough bytes for %s content", h.String())
			}
			r.skip(int(h.ContentSize()))
			continue
		}
		// Not a skip-able header — process it below.
		if err := parseRootAndChildren(r, h, catalog); err != nil {
			return nil, err
		}
		return catalog, nil
	}

	return catalog, nil
}

// parseRootAndChildren handles the root ENTRY and all subsequent children.
func parseRootAndChildren(r *chunkReader, first format.Header, catalog *Catalog) error {
	return scanTree(r, first, &treeVisitor{
		Child: func(parentPath, name string, kind EntryKind, size int64) {
			catalog.addChild(parentPath, name, kind, size)
		},
	})
}

// scanEntryAttributes reads attribute headers after an ENTRY+Stat to determine
// the entry kind and size. Returns the kind, file size, and any error.
// After this function returns, the reader is positioned right after the
// terminal attribute, ready for the next structural header.
func scanEntryAttributes(r *chunkReader, stat format.Stat) (EntryKind, int64, error) {
	fileType := stat.Mode & format.ModeIFMT

	for r.remaining() >= format.HeaderSize {
		h := r.readHeader()
		contentSize := int(h.ContentSize())

		switch h.Type {
		case format.PXARSymlink:
			if r.remaining() < contentSize {
				return 0, 0, fmt.Errorf("not enough bytes for symlink content")
			}
			r.skip(contentSize)
			return KindSymlink, 0, nil

		case format.PXARDevice:
			if r.remaining() < contentSize {
				return 0, 0, fmt.Errorf("not enough bytes for device content")
			}
			r.skip(contentSize)
			return KindDevice, 0, nil

		case format.PXARPayload:
			if r.remaining() < contentSize {
				return 0, 0, fmt.Errorf("not enough bytes for payload content")
			}
			r.skip(contentSize)
			return KindFile, int64(contentSize), nil

		case format.PXARPayloadRef:
			if r.remaining() < contentSize {
				return 0, 0, fmt.Errorf("not enough bytes for PAYLOAD_REF")
			}
			refBytes := r.read(contentSize)
			if len(refBytes) >= 16 {
				return KindFile, int64(binary.LittleEndian.Uint64(refBytes[8:16])), nil
			}
			return KindFile, 0, nil

		case format.PXARFilename, format.PXARGoodbye:
			// Non-terminal entries (directories, FIFOs, sockets) have no
			// PAYLOAD/SYMLINK/DEVICE. The next FILENAME or GOODBYE belongs
			// to the parent structure. Do NOT consume it — push back.
			r.pushback(format.HeaderSize)
			switch fileType {
			case format.ModeIFIFO:
				return KindFifo, 0, nil
			case format.ModeIFSOCK:
				return KindSocket, 0, nil
			default:
				return KindDirectory, 0, nil
			}

		default:
			// Skip unknown attributes (xattrs, ACLs, fcaps, etc.)
			if r.remaining() < contentSize {
				return 0, 0, fmt.Errorf("not enough bytes for unknown attribute")
			}
			r.skip(contentSize)
		}
	}

	// EOF during attribute scan — treat as directory (no terminal found).
	return KindDirectory, 0, nil
}

// skipUntilStructural skips attribute headers until a FILENAME or GOODBYE
// is found. Does NOT consume the structural header.
func skipUntilStructural(r *chunkReader) error {
	for r.remaining() >= format.HeaderSize {
		h := r.readHeader()
		if h.Type == format.PXARFilename || h.Type == format.PXARGoodbye {
			r.pushback(format.HeaderSize)
			return nil
		}
		if r.remaining() < int(h.ContentSize()) {
			return fmt.Errorf("not enough bytes for attribute content")
		}
		r.skip(int(h.ContentSize()))
	}
	return nil
}

// readFilename reads the content of a FILENAME header and strips the trailing null.
// Returns a zero-copy string backed by the chunk data. Safe because Catalog.chunks
// keeps the backing data alive for the catalog's lifetime.
func readFilename(r *chunkReader, h format.Header) string {
	data := r.read(int(h.ContentSize()))
	if len(data) > 0 && data[len(data)-1] == 0 {
		data = data[:len(data)-1]
	}
	return unsafe.String(unsafe.SliceData(data), len(data))
}

// addChild appends a child to the catalog under the given parent path.
func (c *Catalog) addChild(parentPath, name string, kind EntryKind, size int64) {
	children := c.Dirs[parentPath]
	if children == nil {
		// First child for this directory — preallocate a small slice
		// to reduce the number of grows from append.
		children = make([]CatalogChild, 0, 16)
	}
	children = append(children, CatalogChild{
		Size: size,
		Name: name,
		Kind: kind,
	})
	c.Dirs[parentPath] = children
}

// downloadChunks downloads and decodes all metadata chunks in parallel.
// Returns the decoded chunks and any per-chunk errors.
func downloadChunks(metaIdx *DynamicIndexReader, source ChunkSource, maxWorkers int) ([][]byte, []error) {
	decodedChunks := make([][]byte, metaIdx.Count())
	chunkErrs := make([]error, metaIdx.Count())

	work := make(chan int, metaIdx.Count())
	for i := 0; i < metaIdx.Count(); i++ {
		work <- i
	}
	close(work)

	var wg sync.WaitGroup
	for range maxWorkers {
		wg.Go(func() {
			for i := range work {
				entry := metaIdx.Entry(i)
				raw, err := source.GetChunk(entry.Digest)
				if err != nil {
					chunkErrs[i] = fmt.Errorf("chunk %d/%d (digest %x): %w", i+1, metaIdx.Count(), entry.Digest[:8], err)
					continue
				}
				decoded, err := DecodeBlob(raw)
				if err != nil {
					chunkErrs[i] = fmt.Errorf("decode chunk %d: %w", i, err)
					continue
				}
				decodedChunks[i] = decoded
			}
		})
	}
	wg.Wait()

	return decodedChunks, chunkErrs
}

// buildChildPath constructs a child path into dst, avoiding per-call allocation.
// Returns the written slice (no copy).
func buildChildPath(dst []byte, parentPath, name string) []byte {
	if parentPath == "/" {
		dst = append(dst, '/')
	} else {
		dst = append(dst, parentPath...)
		dst = append(dst, '/')
	}
	dst = append(dst, name...)
	return dst
}
