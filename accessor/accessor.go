// Package accessor provides random access to pxar archives.
package accessor

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/binarytree"
	"github.com/pbs-plus/pxar/format"
)

// Accessor provides random access to entries in a pxar archive.
//
// Thread safety: Accessor methods that read from the metadata stream
// (ReadRoot, Lookup, ListDirectory, ReadEntryAt, ReadEntryAtMinimal,
// ReadFileContent, and the v1 path of ReadFileContentReader) are safe
// for concurrent use. The payload path of ReadFileContentReader
// (split archives) returns an independent io.SectionReader that can
// be read concurrently without additional synchronization.
type Accessor struct {
	reader        io.ReadSeeker
	payloadReader io.ReadSeeker // optional, for split archives (v2 format)
	readBuf       []byte        // reusable buffer for variable-size reads

	// metaMu serializes access to the metadata stream reader.
	// io.ReadSeeker implementations like bytes.Reader and ChunkedReadSeeker
	// are not safe for concurrent Seek+Read (only ReadAt is safe).
	metaMu sync.Mutex

	goodbyeMu    sync.RWMutex
	goodbyeCache map[int64]int64 // dirOffset → goodbyeOffset
}

// ListOption controls which metadata is decoded during ListDirectory.
type ListOption struct {
	// Minimal skips decoding xattrs, fcaps, ACLs, and other extended
	// metadata. Only stat basics (mode, uid, gid, times) are populated.
	// Significantly reduces per-entry decode cost.
	Minimal bool
}

// NewAccessor creates an accessor for random access to a pxar archive.
// For split archives (v2 format), provide the payload reader as the second argument.
func NewAccessor(reader io.ReadSeeker, payloadReader ...io.ReadSeeker) *Accessor {
	a := &Accessor{
		reader:       reader,
		readBuf:      make([]byte, 0, 4096),
		goodbyeCache: make(map[int64]int64),
	}
	if len(payloadReader) > 0 {
		a.payloadReader = payloadReader[0]
	}
	return a
}

// growBuf returns a byte slice of at least n bytes backed by the internal
// reusable buffer. The returned slice aliases the internal buffer and is
// invalidated by subsequent growBuf calls.
func (a *Accessor) growBuf(n int) []byte {
	if cap(a.readBuf) < n {
		a.readBuf = make([]byte, n*2)
	}
	return a.readBuf[:n]
}

// ReadRoot reads the root entry of the archive.
func (a *Accessor) ReadRoot() (*pxar.Entry, error) {
	a.metaMu.Lock()
	defer a.metaMu.Unlock()
	return a.readRootLocked()
}

// readRootLocked reads the root entry without acquiring metaMu.
func (a *Accessor) readRootLocked() (*pxar.Entry, error) {
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

// ListDirectory streams directory entries without materializing a slice.
// For each entry, fn is called with a pointer that is only valid during the
// callback. Callers must copy the Entry if they need to retain it beyond fn's
// return. If fn returns a non-nil error, iteration stops and the error is returned.
func (a *Accessor) ListDirectory(dirOffset int64, opts ListOption, fn func(*pxar.Entry) error) error {
	return a.listDirectoryStream(dirOffset, opts, fn)
}

func (a *Accessor) listDirectoryStream(dirOffset int64, opts ListOption, fn func(*pxar.Entry) error) error {
	// Lock only the goodbye table scan, then release before iterating
	// entries to allow reentrant calls from callbacks.
	a.metaMu.Lock()
	// Seek to directory content area
	if _, err := a.reader.Seek(dirOffset, io.SeekStart); err != nil {
		a.metaMu.Unlock()
		return err
	}

	// Check goodbye table cache
	a.goodbyeMu.RLock()
	goodbyeOffset, cached := a.goodbyeCache[dirOffset]
	a.goodbyeMu.RUnlock()

	if !cached {
		var err error
		goodbyeOffset, err = a.findGoodbyeOffset(dirOffset)
		if err != nil {
			a.metaMu.Unlock()
			return fmt.Errorf("finding goodbye table: %w", err)
		}
		a.goodbyeMu.Lock()
		a.goodbyeCache[dirOffset] = goodbyeOffset
		a.goodbyeMu.Unlock()
	}

	items, err := a.readGoodbyeTable(goodbyeOffset)
	a.metaMu.Unlock()
	if err != nil {
		return err
	}

	for _, item := range items {
		if item.Hash == format.PXARGoodbyeTailMarker {
			continue
		}

		// item.Offset is relative to goodbye table start
		entryOffset := goodbyeOffset - int64(item.Offset)

		var entry *pxar.Entry
		if opts.Minimal {
			entry, err = a.ReadEntryAtMinimal(entryOffset)
		} else {
			entry, err = a.ReadEntryAt(entryOffset)
		}
		if err != nil {
			return fmt.Errorf("reading entry at %d: %w", entryOffset, err)
		}
		if err := fn(entry); err != nil {
			return err
		}
	}

	return nil
}

// Lookup finds an entry by path in the archive.
func (a *Accessor) Lookup(path string) (*pxar.Entry, error) {
	a.metaMu.Lock()
	defer a.metaMu.Unlock()
	root, err := a.readRootLocked()
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
	parts := pxar.SplitPath(path)
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
	entry, err := a.readEntryAtLocked(entryOffset)
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
	var buf [16]byte
	if _, err := io.ReadFull(a.reader, buf[:]); err != nil {
		return format.Header{}, err
	}
	h := format.Header{
		Type: binary.LittleEndian.Uint64(buf[0:]),
		Size: binary.LittleEndian.Uint64(buf[8:]),
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
	return format.UnmarshalStatBytes(data[:]), nil
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
				if _, err := a.reader.Seek(-format.HeaderSize, io.SeekCurrent); err != nil {
					return err
				}
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
			if _, err := a.reader.Seek(-format.HeaderSize, io.SeekCurrent); err != nil {
				return err
			}
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
	if nItems == 0 {
		return nil, nil
	}
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

// ReadEntryAtMinimal reads a pxar entry with minimal decoding. It only
// populates stat basics and structural fields. Use for indexing/browsing
// where full metadata is unnecessary.
func (a *Accessor) ReadEntryAtMinimal(offset int64) (*pxar.Entry, error) {
	a.metaMu.Lock()
	defer a.metaMu.Unlock()
	return a.readEntryAtLocked(offset)
}

// ReadEntryAt reads a pxar entry at the given archive offset.
func (a *Accessor) ReadEntryAt(offset int64) (*pxar.Entry, error) {
	a.metaMu.Lock()
	defer a.metaMu.Unlock()
	return a.readEntryAtFullLocked(offset)
}

// readEntryAtFullLocked reads a pxar entry including all extended metadata
// (xattrs, fcaps, ACLs). It is the same as readEntryAtLocked except that
// the default case decodes instead of skipping.
func (a *Accessor) readEntryAtFullLocked(offset int64) (*pxar.Entry, error) {
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

	nameData := a.growBuf(int(h.ContentSize()))
	if _, err := io.ReadFull(a.reader, nameData); err != nil {
		return nil, err
	}
	if len(nameData) > 0 && nameData[len(nameData)-1] == 0 {
		nameData = nameData[:len(nameData)-1]
	}
	name := string(nameData)

	// Read ENTRY header
	h, err = a.readHeader()
	if err != nil {
		return nil, err
	}

	if h.Type == format.PXARHardlink {
		data := a.growBuf(int(h.ContentSize()))
		if _, err := io.ReadFull(a.reader, data); err != nil {
			return nil, err
		}
		if len(data) < 8 {
			return nil, fmt.Errorf("hardlink entry too small")
		}
		relOffset := binary.LittleEndian.Uint64(data[:8])
		target := data[8:]
		if len(target) > 0 && target[len(target)-1] == 0 {
			target = target[:len(target)-1]
		}
		return &pxar.Entry{
			Kind:       pxar.KindHardlink,
			Path:       name,
			LinkTarget: string(target),
			LinkOffset: relOffset,
			FileOffset: uint64(offset),
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

	// Scan for terminal item — decode extended metadata
	for {
		posBefore, _ := a.reader.Seek(0, io.SeekCurrent)
		h2, err := a.readHeader()
		if err != nil {
			return nil, err
		}

		switch h2.Type {
		case format.PXARSymlink:
			data := a.growBuf(int(h2.ContentSize()))
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
			data := a.growBuf(int(h2.ContentSize()))
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
			data := a.growBuf(int(h2.ContentSize()))
			if _, err := io.ReadFull(a.reader, data); err != nil {
				return nil, err
			}
			entry.Kind = pxar.KindFile
			entry.PayloadOffset = binary.LittleEndian.Uint64(data[0:])
			entry.FileSize = binary.LittleEndian.Uint64(data[8:])
			entry.ContentOffset = entry.PayloadOffset
			return entry, nil

		case format.PXARFilename, format.PXARGoodbye:
			if stat.IsFIFO() {
				entry.Kind = pxar.KindFIFO
			} else if stat.IsSocket() {
				entry.Kind = pxar.KindSocket
			} else {
				entry.Kind = pxar.KindDirectory
			}
			entry.ContentOffset = uint64(posBefore)
			return entry, nil

		case format.PXARXAttr:
			data := a.growBuf(int(h2.ContentSize()))
			if _, err := io.ReadFull(a.reader, data); err != nil {
				return nil, err
			}
			cp := make([]byte, len(data))
			copy(cp, data)
			data = cp
			nameLen := 0
			for i, b := range data {
				if b == 0 {
					nameLen = i
					break
				}
			}
			entry.Metadata.XAttrs = append(entry.Metadata.XAttrs, format.XAttr{Data: data, NameLen: nameLen})

		case format.PXARFCaps:
			data := a.growBuf(int(h2.ContentSize()))
			if _, err := io.ReadFull(a.reader, data); err != nil {
				return nil, err
			}
			cp := make([]byte, len(data))
			copy(cp, data)
			entry.Metadata.FCaps = cp

		case format.PXARACLUser:
			data := a.growBuf(int(h2.ContentSize()))
			if _, err := io.ReadFull(a.reader, data); err != nil {
				return nil, err
			}
			if len(data) >= 16 {
				entry.Metadata.ACL.Users = append(entry.Metadata.ACL.Users, format.ACLUser{
					UID:         binary.LittleEndian.Uint64(data[0:]),
					Permissions: format.ACLPermissions(binary.LittleEndian.Uint64(data[8:])),
				})
			}

		case format.PXARACLGroup:
			data := a.growBuf(int(h2.ContentSize()))
			if _, err := io.ReadFull(a.reader, data); err != nil {
				return nil, err
			}
			if len(data) >= 16 {
				entry.Metadata.ACL.Groups = append(entry.Metadata.ACL.Groups, format.ACLGroup{
					GID:         binary.LittleEndian.Uint64(data[0:]),
					Permissions: format.ACLPermissions(binary.LittleEndian.Uint64(data[8:])),
				})
			}

		case format.PXARACLGroupObj:
			data := a.growBuf(int(h2.ContentSize()))
			if _, err := io.ReadFull(a.reader, data); err != nil {
				return nil, err
			}
			if len(data) >= 8 {
				obj := format.ACLGroupObject{
					Permissions: format.ACLPermissions(binary.LittleEndian.Uint64(data[0:])),
				}
				entry.Metadata.ACL.GroupObj = &obj
			}

		case format.PXARACLDefault:
			data := a.growBuf(int(h2.ContentSize()))
			if _, err := io.ReadFull(a.reader, data); err != nil {
				return nil, err
			}
			if len(data) >= 32 {
				def := format.ACLDefault{
					UserObjPermissions:  format.ACLPermissions(binary.LittleEndian.Uint64(data[0:])),
					GroupObjPermissions: format.ACLPermissions(binary.LittleEndian.Uint64(data[8:])),
					OtherPermissions:    format.ACLPermissions(binary.LittleEndian.Uint64(data[16:])),
					MaskPermissions:     format.ACLPermissions(binary.LittleEndian.Uint64(data[24:])),
				}
				entry.Metadata.ACL.Default = &def
			}

		case format.PXARACLDefaultUser:
			data := a.growBuf(int(h2.ContentSize()))
			if _, err := io.ReadFull(a.reader, data); err != nil {
				return nil, err
			}
			if len(data) >= 16 {
				entry.Metadata.ACL.DefaultUsers = append(entry.Metadata.ACL.DefaultUsers, format.ACLUser{
					UID:         binary.LittleEndian.Uint64(data[0:]),
					Permissions: format.ACLPermissions(binary.LittleEndian.Uint64(data[8:])),
				})
			}

		case format.PXARACLDefaultGroup:
			data := a.growBuf(int(h2.ContentSize()))
			if _, err := io.ReadFull(a.reader, data); err != nil {
				return nil, err
			}
			if len(data) >= 16 {
				entry.Metadata.ACL.DefaultGroups = append(entry.Metadata.ACL.DefaultGroups, format.ACLGroup{
					GID:         binary.LittleEndian.Uint64(data[0:]),
					Permissions: format.ACLPermissions(binary.LittleEndian.Uint64(data[8:])),
				})
			}

		case format.PXARQuotaProjID:
			data := a.growBuf(int(h2.ContentSize()))
			if _, err := io.ReadFull(a.reader, data); err != nil {
				return nil, err
			}
			if len(data) >= 8 {
				v := binary.LittleEndian.Uint64(data[0:])
				entry.Metadata.QuotaProjectID = &v
			}

		default:
			// Unknown metadata — skip
			if _, err := a.reader.Seek(int64(h2.ContentSize()), io.SeekCurrent); err != nil {
				return nil, err
			}
		}
	}
}

// readEntryAtLocked reads a pxar entry without acquiring metaMu.
// ReadEntryAtMinimal calls this; it returns minimal metadata (no xattrs/ACLs).
func (a *Accessor) readEntryAtLocked(offset int64) (*pxar.Entry, error) {
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

	nameData := a.growBuf(int(h.ContentSize()))
	if _, err := io.ReadFull(a.reader, nameData); err != nil {
		return nil, err
	}
	if len(nameData) > 0 && nameData[len(nameData)-1] == 0 {
		nameData = nameData[:len(nameData)-1]
	}
	name := string(nameData)

	// Read ENTRY header
	h, err = a.readHeader()
	if err != nil {
		return nil, err
	}

	if h.Type == format.PXARHardlink {
		data := a.growBuf(int(h.ContentSize()))
		if _, err := io.ReadFull(a.reader, data); err != nil {
			return nil, err
		}
		if len(data) < 8 {
			return nil, fmt.Errorf("hardlink entry too small")
		}
		relOffset := binary.LittleEndian.Uint64(data[:8])
		target := data[8:]
		if len(target) > 0 && target[len(target)-1] == 0 {
			target = target[:len(target)-1]
		}
		return &pxar.Entry{
			Kind:       pxar.KindHardlink,
			Path:       name,
			LinkTarget: string(target),
			LinkOffset: relOffset,
			FileOffset: uint64(offset),
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

	// Scan for terminal item — skip all extended metadata
	for {
		posBefore, _ := a.reader.Seek(0, io.SeekCurrent)
		h2, err := a.readHeader()
		if err != nil {
			return nil, err
		}

		switch h2.Type {
		case format.PXARSymlink:
			data := a.growBuf(int(h2.ContentSize()))
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
			data := a.growBuf(int(h2.ContentSize()))
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
			data := a.growBuf(int(h2.ContentSize()))
			if _, err := io.ReadFull(a.reader, data); err != nil {
				return nil, err
			}
			entry.Kind = pxar.KindFile
			entry.PayloadOffset = binary.LittleEndian.Uint64(data[0:])
			entry.FileSize = binary.LittleEndian.Uint64(data[8:])
			entry.ContentOffset = entry.PayloadOffset
			return entry, nil

		case format.PXARFilename, format.PXARGoodbye:
			if stat.IsFIFO() {
				entry.Kind = pxar.KindFIFO
			} else if stat.IsSocket() {
				entry.Kind = pxar.KindSocket
			} else {
				entry.Kind = pxar.KindDirectory
			}
			entry.ContentOffset = uint64(posBefore)
			return entry, nil

		default:
			// Skip extended metadata (xattrs, fcaps, ACLs, etc.)
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
			if _, err := a.reader.Seek(-format.HeaderSize, io.SeekCurrent); err != nil {
				return 0, err
			}
			return a.reader.Seek(0, io.SeekCurrent)
		default:
			if _, err := a.reader.Seek(int64(h2.ContentSize()), io.SeekCurrent); err != nil {
				return 0, err
			}
		}
	}
}

// ReadFileContentReader returns a streaming reader for file content.
// The caller must close the returned reader when done. This avoids
// materializing the entire file in memory.
//
// When the underlying reader implements io.ReaderAt, the returned reader
// is backed by an io.SectionReader and is safe for concurrent use across
// multiple goroutines (each call returns an independent reader).
func (a *Accessor) ReadFileContentReader(entry *pxar.Entry) (io.ReadCloser, error) {
	if !entry.IsRegularFile() {
		return nil, fmt.Errorf("entry is not a regular file")
	}

	// For split archives (v2 format), read from payload stream.
	// No lock needed: payload reader is independent and uses ReadAt.
	if entry.PayloadOffset > 0 {
		if a.payloadReader == nil {
			return nil, fmt.Errorf("split archive requires payload reader")
		}
		start := int64(entry.PayloadOffset) + format.HeaderSize
		size := int64(entry.FileSize)

		// Use ReaderAt path when available — each SectionReader is independent
		// so concurrent file reads don't race on the shared seek position.
		if ra, ok := a.payloadReader.(io.ReaderAt); ok {
			return io.NopCloser(io.NewSectionReader(ra, start, size)), nil
		}

		if _, err := a.payloadReader.Seek(start, io.SeekStart); err != nil {
			return nil, err
		}
		return io.NopCloser(io.LimitReader(a.payloadReader, size)), nil
	}

	// For unified archives (v1 format), read inline payload.
	// Lock the metadata stream to scan for the PAYLOAD header.
	a.metaMu.Lock()
	defer a.metaMu.Unlock()
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

	// Scan for PAYLOAD header
	for {
		h, err = a.readHeader()
		if err != nil {
			return nil, err
		}

		switch h.Type {
		case format.PXARPayload:
			if ra, ok := a.reader.(io.ReaderAt); ok {
				pos, _ := a.reader.Seek(0, io.SeekCurrent)
				return io.NopCloser(io.NewSectionReader(ra, pos, int64(h.ContentSize()))), nil
			}
			return io.NopCloser(io.LimitReader(a.reader, int64(h.ContentSize()))), nil
		case format.PXARFilename, format.PXARGoodbye:
			return nil, fmt.Errorf("PAYLOAD not found for entry")
		default:
			if _, err := a.reader.Seek(int64(h.ContentSize()), io.SeekCurrent); err != nil {
				return nil, err
			}
		}
	}
}

// FollowHardlink resolves a hardlink entry to its target file entry.
// Mirrors Rust's Accessor::follow_hardlink: uses the relative offset stored in the
// hardlink wire format to seek back to the target's FILENAME header and re-reads
// the full entry from that position.
func (a *Accessor) FollowHardlink(entry *pxar.Entry) (*pxar.Entry, error) {
	if !entry.IsHardlink() {
		return nil, fmt.Errorf("cannot resolve a non-hardlink")
	}

	// FileOffset is the FILENAME header position of the hardlink entry.
	filenameOffset := int64(entry.FileOffset)
	if filenameOffset == 0 {
		return nil, fmt.Errorf("cannot follow hardlink without file entry offset")
	}

	// LinkOffset is relative back from the hardlink's FILENAME to the target's FILENAME.
	relOffset := int64(entry.LinkOffset)
	if relOffset > filenameOffset {
		return nil, fmt.Errorf("invalid offset in hardlink")
	}

	targetOffset := filenameOffset - relOffset

	a.metaMu.Lock()
	defer a.metaMu.Unlock()

	resolved, err := a.readEntryAtLocked(targetOffset)
	if err != nil {
		return nil, fmt.Errorf("follow hardlink: %w", err)
	}

	if !resolved.IsRegularFile() {
		return nil, fmt.Errorf("hardlink does not point to a regular file")
	}
	return resolved, nil
}
