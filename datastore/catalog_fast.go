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
// Total: 8 + 16 + 1 + 7pad = 32 bytes (same as before, but Kind is now byte-sized).
type CatalogChild struct {
	Size int64
	Name string
	Kind EntryKind
}

// Catalog is a lightweight directory tree: parentPath → children.
type Catalog struct {
	Dirs     map[string][]CatalogChild
	Estimate int // hint: estimated total child count for preallocation
}

// CatalogOptions configures BuildCatalogFast behavior.
type CatalogOptions struct {
	MaxWorkers int // parallel chunk downloads (default 4)
}

// byteReader is a simple sequential reader over a byte slice.
// Kept as a value type (no pointer indirection) for inline-friendly hot path.
type byteReader struct {
	data []byte
	pos  int
}

// read returns a slice into the underlying data. No allocation.
func (r *byteReader) read(n int) []byte {
	b := r.data[r.pos : r.pos+n]
	r.pos += n
	return b
}

// readHeader reads a 16-byte pxar header. Inlined by the compiler.
func (r *byteReader) readHeader() format.Header {
	b := r.data[r.pos : r.pos+format.HeaderSize]
	r.pos += format.HeaderSize
	return format.Header{
		Type: binary.LittleEndian.Uint64(b[0:8]),
		Size: binary.LittleEndian.Uint64(b[8:16]),
	}
}

func (r *byteReader) skip(n int) {
	r.pos += n
}

func (r *byteReader) remaining() int { return len(r.data) - r.pos }

// BuildCatalogFast downloads metadata chunks in parallel, concatenates them,
// and performs a single sequential pass to build a directory catalog.
func BuildCatalogFast(
	metaIdx *DynamicIndexReader,
	source ChunkSource,
	opts CatalogOptions,
) (*Catalog, error) {
	if metaIdx.Count() == 0 {
		return &Catalog{Dirs: make(map[string][]CatalogChild)}, nil
	}

	maxWorkers := opts.MaxWorkers
	if maxWorkers <= 0 {
		maxWorkers = 4
	}
	if maxWorkers > metaIdx.Count() {
		maxWorkers = metaIdx.Count()
	}

	totalSize := metaIdx.IndexBytes()

	// Phase 1: Parallel chunk download + decode.
	// Each slot holds: decoded data OR error. Using separate slices avoids
	// struct padding and false sharing between cache lines.
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

	// Concatenate decoded chunks into a single contiguous buffer using copy
	// (avoids append's bounds checking on every iteration).
	meta := make([]byte, totalSize)
	written := 0
	for i := range decodedChunks {
		if chunkErrs[i] != nil {
			return nil, chunkErrs[i]
		}
		written += copy(meta[written:], decodedChunks[i])
	}

	// Phase 2: Sequential single-pass parse.
	catalog := &Catalog{Dirs: make(map[string][]CatalogChild)}
	r := &byteReader{data: meta}

	// Skip FORMAT_VERSION header + content and optional PRELUDE.
	for r.remaining() >= format.HeaderSize {
		h := r.readHeader()
		if h.Type == format.PXARFormatVersion || h.Type == format.PXARPrelude {
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
func parseRootAndChildren(r *byteReader, first format.Header, catalog *Catalog) error {
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

	// Skip any remaining root ENTRY content (xattrs etc.) and scan for
	// the first structural header (FILENAME or GOODBYE).
	if err := skipUntilStructural(r); err != nil {
		return fmt.Errorf("scanning root attributes: %w", err)
	}

	// Preallocate dir stack for typical depth.
	dirStack := make([]string, 0, 32)
	dirStack = append(dirStack, "/")

	// Scratch buffer for building child paths. Reused across iterations
	// to avoid per-entry string concatenation allocations.
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
				r.skip(int(h2.ContentSize()))
				catalog.addChild(parentPath, name, KindHardlink, 0)

			case format.PXAREntry:
				if r.remaining() < 40 {
					return fmt.Errorf("not enough bytes for stat of %q", name)
				}
				es := format.UnmarshalStatBytes(r.read(40))

				kind, size, err := scanEntryAttributes(r, es)
				if err != nil {
					return fmt.Errorf("scanning attributes for %q: %w", name, err)
				}
				catalog.addChild(parentPath, name, kind, size)

				if kind == KindDirectory {
					// Build child path using reusable buffer to avoid
					// per-entry string concatenation allocations.
					pathBuf = buildChildPath(pathBuf[:0], parentPath, name)
					// Copy pathBuf bytes into an immutable string so
					// pathBuf can be reused in the next iteration.
					dirStack = append(dirStack, string(append([]byte(nil), pathBuf...)))
				}

			default:
				return fmt.Errorf("expected ENTRY or HARDLINK after filename %q, got %s", name, h2.String())
			}

		case format.PXARGoodbye:
			r.skip(int(h.ContentSize()))
			if len(dirStack) > 1 {
				dirStack = dirStack[:len(dirStack)-1]
			}

		default:
			r.skip(int(h.ContentSize()))
		}
	}

	return nil
}

// scanEntryAttributes reads attribute headers after an ENTRY+Stat to determine
// the entry kind and size. Returns the kind, file size, and any error.
// After this function returns, the reader is positioned right after the
// terminal attribute, ready for the next structural header.
func scanEntryAttributes(r *byteReader, stat format.Stat) (EntryKind, int64, error) {
	fileType := stat.Mode & format.ModeIFMT

	for r.remaining() >= format.HeaderSize {
		h := r.readHeader()
		contentSize := int(h.ContentSize())

		switch h.Type {
		case format.PXARSymlink:
			r.skip(contentSize)
			return KindSymlink, 0, nil

		case format.PXARDevice:
			r.skip(contentSize)
			return KindDevice, 0, nil

		case format.PXARPayload:
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
			r.pos -= format.HeaderSize
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
			r.skip(contentSize)
		}
	}

	// EOF during attribute scan — treat as directory (no terminal found).
	return KindDirectory, 0, nil
}

// skipUntilStructural skips attribute headers until a FILENAME or GOODBYE
// is found. Does NOT consume the structural header.
func skipUntilStructural(r *byteReader) error {
	for r.remaining() >= format.HeaderSize {
		h := r.readHeader()
		if h.Type == format.PXARFilename || h.Type == format.PXARGoodbye {
			r.pos -= format.HeaderSize
			return nil
		}
		r.skip(int(h.ContentSize()))
	}
	return nil
}

// readFilename reads the content of a FILENAME header and strips the trailing null.
// Returns a zero-copy string backed by the reader's data. The returned string is
// safe to use as long as the underlying meta slice is alive.
func readFilename(r *byteReader, h format.Header) string {
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
