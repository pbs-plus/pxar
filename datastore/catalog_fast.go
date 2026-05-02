package datastore

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/pbs-plus/pxar/format"
)

// EntryKind identifies the type of a pxar archive entry in the catalog.
type EntryKind int

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
type CatalogChild struct {
	Name string
	Kind EntryKind
	Size int64
}

// Catalog is a lightweight directory tree: parentPath → children.
type Catalog struct {
	Dirs map[string][]CatalogChild
}

// CatalogOptions configures BuildCatalogFast behavior.
type CatalogOptions struct {
	MaxWorkers int // parallel chunk downloads (default 4)
}

// byteReader is a simple sequential reader over a byte slice.
type byteReader struct {
	data []byte
	pos  int
}

func (r *byteReader) read(n int) ([]byte, error) {
	if r.pos+n > len(r.data) {
		return nil, fmt.Errorf("byteReader: need %d bytes at pos %d, have %d", n, r.pos, len(r.data)-r.pos)
	}
	b := r.data[r.pos : r.pos+n]
	r.pos += n
	return b, nil
}

func (r *byteReader) readHeader() (format.Header, error) {
	b, err := r.read(format.HeaderSize)
	if err != nil {
		return format.Header{}, err
	}
	return format.Header{
		Type: binary.LittleEndian.Uint64(b[0:8]),
		Size: binary.LittleEndian.Uint64(b[8:16]),
	}, nil
}

func (r *byteReader) skip(n int) error {
	if r.pos+n > len(r.data) {
		return fmt.Errorf("byteReader: skip %d bytes at pos %d, have %d", n, r.pos, len(r.data)-r.pos)
	}
	r.pos += n
	return nil
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

	// Phase 1: Parallel chunk download + decode.
	type chunkResult struct {
		index int
		data  []byte
		err   error
	}

	results := make([]chunkResult, metaIdx.Count())
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
					results[i] = chunkResult{index: i, err: fmt.Errorf("chunk %d/%d (digest %x): %w", i+1, metaIdx.Count(), entry.Digest[:8], err)}
					continue
				}
				decoded, err := DecodeBlob(raw)
				if err != nil {
					results[i] = chunkResult{index: i, err: fmt.Errorf("decode chunk %d: %w", i, err)}
					continue
				}
				results[i] = chunkResult{index: i, data: decoded}
			}
		})
	}
	wg.Wait()

	// Concatenate in order.
	totalSize := metaIdx.IndexBytes()
	meta := make([]byte, 0, totalSize)
	for i := range results {
		if results[i].err != nil {
			return nil, results[i].err
		}
		meta = append(meta, results[i].data...)
	}

	// Phase 2: Sequential single-pass parse.
	catalog := &Catalog{Dirs: make(map[string][]CatalogChild)}
	r := &byteReader{data: meta}

	// Skip FORMAT_VERSION header + content and optional PRELUDE.
	for r.remaining() >= format.HeaderSize {
		h, err := r.readHeader()
		if err != nil {
			return nil, fmt.Errorf("reading header: %w", err)
		}
		if h.Type == format.PXARFormatVersion || h.Type == format.PXARPrelude {
			if err := r.skip(int(h.ContentSize())); err != nil {
				return nil, fmt.Errorf("skipping %s content: %w", h.String(), err)
			}
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

	// Read root stat (40 bytes).
	statBytes, err := r.read(40)
	if err != nil {
		return fmt.Errorf("reading root stat: %w", err)
	}
	stat := format.UnmarshalStatBytes(statBytes)

	if !stat.IsDir() {
		return fmt.Errorf("root entry is not a directory")
	}

	// Skip any remaining root ENTRY content (xattrs etc.) and scan for
	// the first structural header (FILENAME or GOODBYE).
	if err := skipUntilStructural(r); err != nil {
		return fmt.Errorf("scanning root attributes: %w", err)
	}

	dirStack := []string{"/"}
	for r.remaining() >= format.HeaderSize {
		h, err := r.readHeader()
		if err != nil {
			return fmt.Errorf("reading header: %w", err)
		}

		switch h.Type {
		case format.PXARFilename:
			name, err := readFilename(r, h)
			if err != nil {
				return err
			}
			if len(dirStack) == 0 {
				return fmt.Errorf("filename %q outside of directory", name)
			}
			parentPath := dirStack[len(dirStack)-1]

			// Read next header to determine what follows the filename.
			h2, err := r.readHeader()
			if err != nil {
				return fmt.Errorf("reading entry after filename %q: %w", name, err)
			}

			switch h2.Type {
			case format.PXARHardlink:
				// Hardlink: skip content, record as hardlink child.
				if err := r.skip(int(h2.ContentSize())); err != nil {
					return fmt.Errorf("skipping hardlink content: %w", err)
				}
				catalog.addChild(parentPath, name, KindHardlink, 0)

			case format.PXAREntry:
				// Regular entry: read stat, then scan attributes.
				sb, err := r.read(40)
				if err != nil {
					return fmt.Errorf("reading stat for %q: %w", name, err)
				}
				es := format.UnmarshalStatBytes(sb)

				kind, size, err := scanEntryAttributes(r, es)
				if err != nil {
					return fmt.Errorf("scanning attributes for %q: %w", name, err)
				}
				catalog.addChild(parentPath, name, kind, size)

				if kind == KindDirectory {
					childPath := parentPath
					if childPath == "/" {
						childPath = "/" + name
					} else {
						childPath = childPath + "/" + name
					}
					dirStack = append(dirStack, childPath)
				}

			default:
				return fmt.Errorf("expected ENTRY or HARDLINK after filename %q, got %s", name, h2.String())
			}

		case format.PXARGoodbye:
			// End of current directory's children.
			if err := r.skip(int(h.ContentSize())); err != nil {
				return fmt.Errorf("skipping goodbye content: %w", err)
			}
			if len(dirStack) > 1 {
				dirStack = dirStack[:len(dirStack)-1]
			}
			// If stack is at root, we might still have more (shouldn't, but be safe).

		default:
			// Unexpected header at top level — skip content.
			if err := r.skip(int(h.ContentSize())); err != nil {
				return fmt.Errorf("skipping unknown header content: %w", err)
			}
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
		h, err := r.readHeader()
		if err != nil {
			return 0, 0, err
		}

		switch h.Type {
		case format.PXARSymlink:
			// Terminal for symlinks.
			if err := r.skip(int(h.ContentSize())); err != nil {
				return 0, 0, err
			}
			return KindSymlink, 0, nil

		case format.PXARDevice:
			// Terminal for devices.
			if err := r.skip(int(h.ContentSize())); err != nil {
				return 0, 0, err
			}
			return KindDevice, 0, nil

		case format.PXARPayload:
			// Terminal for files (inline payload).
			if err := r.skip(int(h.ContentSize())); err != nil {
				return 0, 0, err
			}
			return KindFile, int64(h.ContentSize()), nil

		case format.PXARPayloadRef:
			// Terminal for files (split payload ref — 16 bytes: offset + size).
			refBytes, err := r.read(int(h.ContentSize()))
			if err != nil {
				return 0, 0, err
			}
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
			if err := r.skip(int(h.ContentSize())); err != nil {
				return 0, 0, err
			}
		}
	}

	// EOF during attribute scan — treat as directory (no terminal found).
	return KindDirectory, 0, nil
}

// skipUntilStructural skips attribute headers until a FILENAME or GOODBYE
// is found. Does NOT consume the structural header.
func skipUntilStructural(r *byteReader) error {
	for r.remaining() >= format.HeaderSize {
		h, err := r.readHeader()
		if err != nil {
			return err
		}
		if h.Type == format.PXARFilename || h.Type == format.PXARGoodbye {
			// Push back — don't consume.
			r.pos -= format.HeaderSize
			return nil
		}
		if err := r.skip(int(h.ContentSize())); err != nil {
			return err
		}
	}
	return nil
}

// readFilename reads the content of a FILENAME header and strips the trailing null.
func readFilename(r *byteReader, h format.Header) (string, error) {
	data, err := r.read(int(h.ContentSize()))
	if err != nil {
		return "", fmt.Errorf("reading filename: %w", err)
	}
	if len(data) > 0 && data[len(data)-1] == 0 {
		data = data[:len(data)-1]
	}
	return string(data), nil
}

// addChild appends a child to the catalog under the given parent path.
func (c *Catalog) addChild(parentPath, name string, kind EntryKind, size int64) {
	c.Dirs[parentPath] = append(c.Dirs[parentPath], CatalogChild{
		Name: name,
		Kind: kind,
		Size: size,
	})
}
