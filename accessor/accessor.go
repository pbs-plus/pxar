// Package accessor provides random access to pxar archives.
package accessor

import (
	"encoding/binary"
	"fmt"
	"io"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/binarytree"
	"github.com/pbs-plus/pxar/format"
)

// Accessor provides random access to entries in a pxar archive.
type Accessor struct {
	reader        io.ReadSeeker
	payloadReader io.ReadSeeker // optional, for split archives (v2 format)
}

// NewAccessor creates an accessor for random access to a pxar archive.
// For split archives (v2 format), provide the payload reader as the second argument.
func NewAccessor(reader io.ReadSeeker, payloadReader ...io.ReadSeeker) *Accessor {
	a := &Accessor{reader: reader}
	if len(payloadReader) > 0 {
		a.payloadReader = payloadReader[0]
	}
	return a
}

// ReadRoot reads the root entry of the archive.
func (a *Accessor) ReadRoot() (*pxar.Entry, error) {
	if _, err := a.reader.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	h, err := a.readHeader()
	if err != nil {
		return nil, err
	}

	// Skip optional format version
	if h.Type == format.PXARFormatVersion {
		if _, err := a.reader.Seek(int64(h.ContentSize()), io.SeekCurrent); err != nil {
			return nil, err
		}
		h, err = a.readHeader()
		if err != nil {
			return nil, err
		}

		// Skip optional prelude
		if h.Type == format.PXARPrelude {
			if _, err := a.reader.Seek(int64(h.ContentSize()), io.SeekCurrent); err != nil {
				return nil, err
			}
			h, err = a.readHeader()
			if err != nil {
				return nil, err
			}
		}
	}

	if h.Type != format.PXAREntry {
		return nil, fmt.Errorf("expected ENTRY header, got %s", h.String())
	}

	stat, err := a.readStat()
	if err != nil {
		return nil, err
	}

	entry := &pxar.Entry{
		Path:     "/",
		Metadata: pxar.Metadata{Stat: stat},
		Kind:     pxar.KindDirectory,
	}

	// Skip past the ENTRY content (stat) to find the content area
	if _, err := a.reader.Seek(int64(h.ContentSize())-40, io.SeekCurrent); err != nil {
		return nil, err
	}

	// Scan past directory attributes to find first child FILENAME/GOODBYE
	for {
		posBefore, _ := a.reader.Seek(0, io.SeekCurrent)
		h2, err := a.readHeader()
		if err != nil {
			return nil, err
		}
		switch h2.Type {
		case format.PXARFilename, format.PXARGoodbye:
			entry.ContentOffset = uint64(posBefore)
			return entry, nil
		default:
			if _, err := a.reader.Seek(int64(h2.ContentSize()), io.SeekCurrent); err != nil {
				return nil, err
			}
		}
	}
}

// ListDirectory lists entries in a directory at the given offset.
// The offset should point to the start of the directory's FILENAME range.
func (a *Accessor) ListDirectory(dirOffset int64) ([]pxar.Entry, error) {
	// Seek to directory content area
	if _, err := a.reader.Seek(dirOffset, io.SeekStart); err != nil {
		return nil, err
	}

	// Read goodbye table first to get all entries
	goodbyeOffset, err := a.findGoodbyeOffset(dirOffset)
	if err != nil {
		return nil, fmt.Errorf("finding goodbye table: %w", err)
	}

	items, err := a.readGoodbyeTable(goodbyeOffset)
	if err != nil {
		return nil, err
	}

	var entries []pxar.Entry
	for _, item := range items {
		if item.Hash == format.PXARGoodbyeTailMarker {
			continue
		}

		// item.Offset is relative to goodbye table start
		entryOffset := goodbyeOffset - int64(item.Offset)
		entry, err := a.ReadEntryAt(entryOffset)
		if err != nil {
			return nil, fmt.Errorf("reading entry at %d: %w", entryOffset, err)
		}
		entries = append(entries, *entry)
	}

	return entries, nil
}

// Lookup finds an entry by path in the archive.
func (a *Accessor) Lookup(path string) (*pxar.Entry, error) {
	root, err := a.ReadRoot()
	if err != nil {
		return nil, err
	}

	if path == "/" || path == "" {
		return root, nil
	}

	// Find the root directory content area
	rootOffset, err := a.getRootContentOffset()
	if err != nil {
		return nil, err
	}

	return a.lookupPath(rootOffset, path)
}

func (a *Accessor) lookupPath(dirOffset int64, path string) (*pxar.Entry, error) {
	// Split path into first component and remainder
	parts := splitPath(path)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty path")
	}

	name := parts[0]
	rest := ""
	if len(parts) > 1 {
		rest = parts[1]
		for _, p := range parts[2:] {
			rest = rest + "/" + p
		}
	}

	// Find goodbye table
	goodbyeOffset, err := a.findGoodbyeOffset(dirOffset)
	if err != nil {
		return nil, err
	}

	items, err := a.readGoodbyeTable(goodbyeOffset)
	if err != nil {
		return nil, err
	}

	// Search for the name
	hash := format.HashFilename([]byte(name))
	idx, found := binarytree.SearchBy(items, 0, 0, func(item format.GoodbyeItem) int {
		if hash < item.Hash {
			return -1
		} else if hash > item.Hash {
			return 1
		}
		return 0
	})
	if !found {
		return nil, fmt.Errorf("entry %q not found", name)
	}

	// Resolve entry
	entryOffset := goodbyeOffset - int64(items[idx].Offset)
	entry, err := a.ReadEntryAt(entryOffset)
	if err != nil {
		return nil, err
	}

	// If there's more path to resolve, recurse into directory
	if rest != "" {
		if !entry.IsDir() {
			return nil, fmt.Errorf("%q is not a directory", name)
		}

		// Find this directory's content area
		subDirOffset, err := a.findDirContentOffset(entryOffset)
		if err != nil {
			return nil, err
		}
		return a.lookupPath(subDirOffset, rest)
	}

	return entry, nil
}

func (a *Accessor) readHeader() (format.Header, error) {
	var h format.Header
	err := binary.Read(a.reader, binary.LittleEndian, &h)
	if err != nil {
		return h, err
	}
	if err := h.CheckHeaderSize(); err != nil {
		return h, err
	}
	return h, nil
}

func (a *Accessor) readStat() (format.Stat, error) {
	var data [40]byte
	if _, err := io.ReadFull(a.reader, data[:]); err != nil {
		return format.Stat{}, err
	}
	return unmarshalStat(data[:]), nil
}

func (a *Accessor) findGoodbyeOffset(dirOffset int64) (int64, error) {
	if _, err := a.reader.Seek(dirOffset, io.SeekStart); err != nil {
		return 0, err
	}

	// We scan through all items in this directory. Each FILENAME starts a child
	// entry. For non-directory children, we just skip to the next item.
	// For directory children, we recursively skip their content including their
	// GOODBYE. The first GOODBYE we see at our level is ours.
	for {
		pos, _ := a.reader.Seek(0, io.SeekCurrent)
		h, err := a.readHeader()
		if err != nil {
			return 0, err
		}

		switch h.Type {
		case format.PXARGoodbye:
			return pos, nil

		case format.PXARFilename:
			// Skip filename content
			if _, err := a.reader.Seek(int64(h.ContentSize()), io.SeekCurrent); err != nil {
				return 0, err
			}
			// Skip the child entry completely (including nested content)
			if err := a.skipChildEntry(); err != nil {
				return 0, err
			}

		default:
			if _, err := a.reader.Seek(int64(h.ContentSize()), io.SeekCurrent); err != nil {
				return 0, err
			}
		}
	}
}

// skipChildEntry skips a complete child entry (ENTRY header + stat + all content).
// For directories, this includes recursively skipping all children and the GOODBYE.
// For non-directories (files, symlinks, devices), it skips the terminal item.
// For FIFOs/sockets (no terminal item), it just returns without consuming anything.
func (a *Accessor) skipChildEntry() error {
	h, err := a.readHeader()
	if err != nil {
		return err
	}

	switch h.Type {
	case format.PXARHardlink:
		_, err := a.reader.Seek(int64(h.ContentSize()), io.SeekCurrent)
		return err

	case format.PXAREntry, format.PXAREntryV1:
		// Read stat to determine entry type
		stat, err := a.readStat()
		if err != nil {
			return err
		}

		isDir := stat.IsDir()
		// Now skip the entry's content items
		return a.skipEntryItems(isDir)

	default:
		return fmt.Errorf("expected ENTRY or HARDLINK, got %s", h.String())
	}
}

// skipEntryItems skips items belonging to an entry.
// isDir indicates whether the entry is a directory (determined from stat).
// For directories: reads children recursively until GOODBYE, then skips it.
// For non-directories: reads until a terminal item (PAYLOAD, SYMLINK, DEVICE, PAYLOAD_REF),
// or until FILENAME/GOODBYE (which means FIFO/socket with no terminal item).
func (a *Accessor) skipEntryItems(isDir bool) error {
	for {
		h, err := a.readHeader()
		if err != nil {
			return err
		}

		switch h.Type {
		case format.PXARFilename:
			if isDir {
				// Child entry in a directory
				if _, err := a.reader.Seek(int64(h.ContentSize()), io.SeekCurrent); err != nil {
					return err
				}
				if err := a.skipChildEntry(); err != nil {
					return err
				}
			} else {
				// FIFO/socket: FILENAME belongs to parent, rewind
				a.reader.Seek(-format.HeaderSize, io.SeekCurrent)
				return nil
			}

		case format.PXARGoodbye:
			if isDir {
				// End of directory - skip goodbye content
				if _, err := a.reader.Seek(int64(h.ContentSize()), io.SeekCurrent); err != nil {
					return err
				}
				return nil
			}
			// FIFO/socket: GOODBYE belongs to parent, rewind
			a.reader.Seek(-format.HeaderSize, io.SeekCurrent)
			return nil

		case format.PXARPayload, format.PXARSymlink, format.PXARDevice, format.PXARPayloadRef:
			if _, err := a.reader.Seek(int64(h.ContentSize()), io.SeekCurrent); err != nil {
				return err
			}
			return nil

		default:
			if _, err := a.reader.Seek(int64(h.ContentSize()), io.SeekCurrent); err != nil {
				return err
			}
		}
	}
}

func (a *Accessor) readGoodbyeTable(offset int64) ([]format.GoodbyeItem, error) {
	if _, err := a.reader.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}

	h, err := a.readHeader()
	if err != nil {
		return nil, err
	}
	if h.Type != format.PXARGoodbye {
		return nil, fmt.Errorf("expected GOODBYE at offset %d, got %s", offset, h.String())
	}

	contentSize := int64(h.ContentSize())
	if contentSize%24 != 0 {
		return nil, fmt.Errorf("invalid goodbye table size: %d", contentSize)
	}

	nItems := contentSize / 24
	items := make([]format.GoodbyeItem, nItems)
	for i := range items {
		var data [24]byte
		if _, err := io.ReadFull(a.reader, data[:]); err != nil {
			return nil, err
		}
		items[i] = format.GoodbyeItem{
			Hash:   binary.LittleEndian.Uint64(data[0:]),
			Offset: binary.LittleEndian.Uint64(data[8:]),
			Size:   binary.LittleEndian.Uint64(data[16:]),
		}
	}

	return items, nil
}

// ReadEntryAt reads a pxar entry at the given archive offset.
func (a *Accessor) ReadEntryAt(offset int64) (*pxar.Entry, error) {
	if _, err := a.reader.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}

	// Read FILENAME
	h, err := a.readHeader()
	if err != nil {
		return nil, err
	}
	if h.Type != format.PXARFilename {
		return nil, fmt.Errorf("expected FILENAME at %d, got %s", offset, h.String())
	}

	nameData := make([]byte, h.ContentSize())
	if _, err := io.ReadFull(a.reader, nameData); err != nil {
		return nil, err
	}
	// Remove null terminator
	if len(nameData) > 0 && nameData[len(nameData)-1] == 0 {
		nameData = nameData[:len(nameData)-1]
	}
	name := string(nameData)

	// Read ENTRY
	h, err = a.readHeader()
	if err != nil {
		return nil, err
	}

	if h.Type == format.PXARHardlink {
		data := make([]byte, h.ContentSize())
		if _, err := io.ReadFull(a.reader, data); err != nil {
			return nil, err
		}
		target := data[8:]
		if len(target) > 0 && target[len(target)-1] == 0 {
			target = target[:len(target)-1]
		}
		return &pxar.Entry{
			Kind:       pxar.KindHardlink,
			Path:       name,
			LinkTarget: string(target),
		}, nil
	}

	if h.Type != format.PXAREntry {
		return nil, fmt.Errorf("expected ENTRY, got %s", h.String())
	}

	stat, err := a.readStat()
	if err != nil {
		return nil, err
	}

	entry := &pxar.Entry{
		Path:       name,
		Metadata:   pxar.Metadata{Stat: stat},
		FileOffset: uint64(offset),
	}

	// Read attributes
	for {
		posBefore, _ := a.reader.Seek(0, io.SeekCurrent)
		h2, err := a.readHeader()
		if err != nil {
			return nil, err
		}

		switch h2.Type {
		case format.PXARSymlink:
			data := make([]byte, h2.ContentSize())
			if _, err := io.ReadFull(a.reader, data); err != nil {
				return nil, err
			}
			if len(data) > 0 && data[len(data)-1] == 0 {
				data = data[:len(data)-1]
			}
			entry.Kind = pxar.KindSymlink
			entry.LinkTarget = string(data)
			return entry, nil

		case format.PXARDevice:
			data := make([]byte, h2.ContentSize())
			if _, err := io.ReadFull(a.reader, data); err != nil {
				return nil, err
			}
			entry.Kind = pxar.KindDevice
			entry.DeviceInfo = format.Device{
				Major: binary.LittleEndian.Uint64(data[0:]),
				Minor: binary.LittleEndian.Uint64(data[8:]),
			}
			return entry, nil

		case format.PXARPayload:
			posAfter, _ := a.reader.Seek(0, io.SeekCurrent)
			entry.Kind = pxar.KindFile
			entry.FileSize = h2.ContentSize()
			entry.ContentOffset = uint64(posAfter)
			return entry, nil

		case format.PXARPayloadRef:
			data := make([]byte, h2.ContentSize())
			if _, err := io.ReadFull(a.reader, data); err != nil {
				return nil, err
			}
			entry.Kind = pxar.KindFile
			entry.PayloadOffset = binary.LittleEndian.Uint64(data[0:])
			entry.FileSize = binary.LittleEndian.Uint64(data[8:])
			return entry, nil

		case format.PXARFilename, format.PXARGoodbye:
			if stat.IsFIFO() {
				entry.Kind = pxar.KindFifo
			} else if stat.IsSocket() {
				entry.Kind = pxar.KindSocket
			} else {
				entry.Kind = pxar.KindDirectory
			}
			entry.ContentOffset = uint64(posBefore)
			return entry, nil

		default:
			// Skip attribute content
			if _, err := a.reader.Seek(int64(h2.ContentSize()), io.SeekCurrent); err != nil {
				return nil, err
			}
		}
	}
}

func (a *Accessor) getRootContentOffset() (int64, error) {
	if _, err := a.reader.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	h, err := a.readHeader()
	if err != nil {
		return 0, err
	}

	// Skip format version
	if h.Type == format.PXARFormatVersion {
		if _, err := a.reader.Seek(int64(h.ContentSize()), io.SeekCurrent); err != nil {
			return 0, err
		}
		h, err = a.readHeader()
		if err != nil {
			return 0, err
		}
		// Skip prelude
		if h.Type == format.PXARPrelude {
			if _, err := a.reader.Seek(int64(h.ContentSize()), io.SeekCurrent); err != nil {
				return 0, err
			}
			h, err = a.readHeader()
			if err != nil {
				return 0, err
			}
		}
	}

	// Skip ENTRY + stat
	if _, err := a.reader.Seek(int64(h.ContentSize()), io.SeekCurrent); err != nil {
		return 0, err
	}

	return a.reader.Seek(0, io.SeekCurrent)
}

func (a *Accessor) findDirContentOffset(entryOffset int64) (int64, error) {
	if _, err := a.reader.Seek(entryOffset, io.SeekStart); err != nil {
		return 0, err
	}

	// Read FILENAME
	h, err := a.readHeader()
	if err != nil {
		return 0, err
	}
	if _, err := a.reader.Seek(int64(h.ContentSize()), io.SeekCurrent); err != nil {
		return 0, err
	}

	// Read ENTRY
	h, err = a.readHeader()
	if err != nil {
		return 0, err
	}
	if _, err := a.reader.Seek(int64(h.ContentSize()), io.SeekCurrent); err != nil {
		return 0, err
	}

	// Skip all attributes until we hit FILENAME or GOODBYE
	for {
		h2, err := a.readHeader()
		if err != nil {
			return 0, err
		}

		switch h2.Type {
		case format.PXARFilename, format.PXARGoodbye:
			// Rewind to before this header
			a.reader.Seek(-format.HeaderSize, io.SeekCurrent)
			return a.reader.Seek(0, io.SeekCurrent)
		default:
			if _, err := a.reader.Seek(int64(h2.ContentSize()), io.SeekCurrent); err != nil {
				return 0, err
			}
		}
	}
}

func splitPath(path string) []string {
	// Remove leading slashes
	for len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}
	if path == "" {
		return nil
	}

	var parts []string
	start := 0
	for i := 0; i <= len(path); i++ {
		if i == len(path) || path[i] == '/' {
			if i > start {
				parts = append(parts, path[start:i])
			}
			start = i + 1
		}
	}
	return parts
}

func unmarshalStat(data []byte) format.Stat { return format.UnmarshalStatBytes(data) }

// ReadFileContent reads the content of a file entry from the archive.
func (a *Accessor) ReadFileContent(entry *pxar.Entry) ([]byte, error) {
	if !entry.IsRegularFile() {
		return nil, fmt.Errorf("entry is not a regular file")
	}

	// For split archives (v2 format), read from payload stream
	if entry.PayloadOffset > 0 {
		if a.payloadReader == nil {
			return nil, fmt.Errorf("split archive requires payload reader")
		}
		// Seek to payload offset (after the PXARPayload header)
		if _, err := a.payloadReader.Seek(int64(entry.PayloadOffset)+format.HeaderSize, io.SeekStart); err != nil {
			return nil, err
		}
		data := make([]byte, entry.FileSize)
		_, err := io.ReadFull(a.payloadReader, data)
		return data, err
	}

	// For unified archives (v1 format), read inline payload
	// Seek to the entry start (FILENAME header)
	if _, err := a.reader.Seek(int64(entry.FileOffset), io.SeekStart); err != nil {
		return nil, err
	}

	// Skip FILENAME header + content
	h, err := a.readHeader()
	if err != nil {
		return nil, err
	}
	if h.Type != format.PXARFilename {
		return nil, fmt.Errorf("expected FILENAME, got %s", h.String())
	}
	if _, err := a.reader.Seek(int64(h.ContentSize()), io.SeekCurrent); err != nil {
		return nil, err
	}

	// Skip ENTRY header + stat
	h, err = a.readHeader()
	if err != nil {
		return nil, err
	}
	if _, err := a.reader.Seek(int64(h.ContentSize()), io.SeekCurrent); err != nil {
		return nil, err
	}

	// Now scan for PAYLOAD header (skipping attributes)
	for {
		h, err := a.readHeader()
		if err != nil {
			return nil, err
		}

		switch h.Type {
		case format.PXARPayload:
			data := make([]byte, h.ContentSize())
			_, err := io.ReadFull(a.reader, data)
			return data, err
		case format.PXARFilename, format.PXARGoodbye:
			return nil, fmt.Errorf("PAYLOAD not found for entry")
		default:
			if _, err := a.reader.Seek(int64(h.ContentSize()), io.SeekCurrent); err != nil {
				return nil, err
			}
		}
	}
}
