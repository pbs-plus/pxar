package datastore

import (
	"encoding/binary"
	"io"
)

var CatalogMagic = [8]byte{145, 253, 96, 249, 196, 103, 88, 213}

const (
	CatalogEntryDir      byte = 'd'
	CatalogEntryFile     byte = 'f'
	CatalogEntrySymlink  byte = 'l'
	CatalogEntryHardlink byte = 'h'
	CatalogEntryBlockDev byte = 'b'
	CatalogEntryCharDev  byte = 'c'
	CatalogEntryFifo     byte = 'p'
	CatalogEntrySocket   byte = 's'
)

type catalogDirEntry struct {
	entryType byte
	name      string
	size      uint64
	mtime     int64
	offset    int64
}

type catalogDirInfo struct {
	name    string
	entries []catalogDirEntry
}

// CatalogWriter produces a pcat1 binary catalog stream compatible with
// Proxmox Backup Server. Directories are written bottom-up: leaf directories
// before their parents. Each directory block contains entries followed by a
// trailer with metadata. The stream ends with an 8-byte little-endian root
// directory offset.
//
// Block format: [entries...][entry_count(u64)][name_len(u64)][name(bytes)]
// Entry format: [name_len(u64)][type(1)][name(bytes)][type-specific data...]
// File extra:   [size(u64)][mtime(i64)]
// Dir extra:    [offset(u64)] (offset from dir start in the stream)
type CatalogWriter struct {
	w        io.Writer
	dirstack []catalogDirInfo
	pos      int64
	err      error
}

// NewCatalogWriter creates a catalog writer and writes the pcat1 magic header.
func NewCatalogWriter(w io.Writer) *CatalogWriter {
	cw := &CatalogWriter{w: w, pos: 0}
	cw.write(CatalogMagic[:])
	return cw
}

func (cw *CatalogWriter) StartDirectory(name string) {
	cw.dirstack = append(cw.dirstack, catalogDirInfo{name: name})
}

func (cw *CatalogWriter) AddFile(name string, size uint64, mtime int64) {
	top := &cw.dirstack[len(cw.dirstack)-1]
	top.entries = append(top.entries, catalogDirEntry{
		entryType: CatalogEntryFile,
		name:      name,
		size:      size,
		mtime:     mtime,
	})
}

func (cw *CatalogWriter) AddSymlink(name string) {
	top := &cw.dirstack[len(cw.dirstack)-1]
	top.entries = append(top.entries, catalogDirEntry{
		entryType: CatalogEntrySymlink,
		name:      name,
	})
}

func (cw *CatalogWriter) AddHardlink(name string) {
	top := &cw.dirstack[len(cw.dirstack)-1]
	top.entries = append(top.entries, catalogDirEntry{
		entryType: CatalogEntryHardlink,
		name:      name,
	})
}

func (cw *CatalogWriter) AddBlockDevice(name string) {
	top := &cw.dirstack[len(cw.dirstack)-1]
	top.entries = append(top.entries, catalogDirEntry{
		entryType: CatalogEntryBlockDev,
		name:      name,
	})
}

func (cw *CatalogWriter) AddCharDevice(name string) {
	top := &cw.dirstack[len(cw.dirstack)-1]
	top.entries = append(top.entries, catalogDirEntry{
		entryType: CatalogEntryCharDev,
		name:      name,
	})
}

func (cw *CatalogWriter) AddFifo(name string) {
	top := &cw.dirstack[len(cw.dirstack)-1]
	top.entries = append(top.entries, catalogDirEntry{
		entryType: CatalogEntryFifo,
		name:      name,
	})
}

func (cw *CatalogWriter) AddSocket(name string) {
	top := &cw.dirstack[len(cw.dirstack)-1]
	top.entries = append(top.entries, catalogDirEntry{
		entryType: CatalogEntrySocket,
		name:      name,
	})
}

// EndDirectory encodes the current directory block and adds a Dir entry
// to the parent directory.
func (cw *CatalogWriter) EndDirectory() {
	if cw.err != nil {
		return
	}
	dir := cw.dirstack[len(cw.dirstack)-1]
	cw.dirstack = cw.dirstack[:len(cw.dirstack)-1]

	dirStart := cw.pos

	catalogEncodeU64(cw, uint64(len(dir.entries)))
	catalogEncodeU64(cw, uint64(len(dir.name)))
	cw.writeString(dir.name)

	cw.encodeEntries(dir.entries)

	if len(cw.dirstack) > 0 {
		parent := &cw.dirstack[len(cw.dirstack)-1]
		parent.entries = append(parent.entries, catalogDirEntry{
			entryType: CatalogEntryDir,
			name:      dir.name,
			offset:    dirStart,
		})
	}
}

// Finish encodes the root directory block and writes the 8-byte
// little-endian root directory offset at the end of the stream.
func (cw *CatalogWriter) Finish() error {
	if cw.err != nil {
		return cw.err
	}
	root := cw.dirstack[len(cw.dirstack)-1]
	cw.dirstack = cw.dirstack[:len(cw.dirstack)-1]

	rootStart := cw.pos

	catalogEncodeU64(cw, uint64(len(root.entries)))
	catalogEncodeU64(cw, uint64(len(root.name)))
	cw.writeString(root.name)

	cw.encodeEntries(root.entries)

	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(rootStart))
	cw.write(buf[:])

	return cw.err
}

func (cw *CatalogWriter) encodeEntries(entries []catalogDirEntry) {
	for _, e := range entries {
		catalogEncodeU64(cw, uint64(len(e.name)))
		cw.writeByte(e.entryType)
		cw.writeString(e.name)

		switch e.entryType {
		case CatalogEntryFile:
			catalogEncodeU64(cw, e.size)
			catalogEncodeI64(cw, e.mtime)
		case CatalogEntryDir:
			catalogEncodeU64(cw, uint64(e.offset))
		}
	}
}

func (cw *CatalogWriter) write(data []byte) {
	if cw.err != nil {
		return
	}
	n, err := cw.w.Write(data)
	cw.pos += int64(n)
	if err != nil {
		cw.err = err
	}
}

func (cw *CatalogWriter) writeByte(b byte) {
	var buf [1]byte
	buf[0] = b
	cw.write(buf[:])
}

func (cw *CatalogWriter) writeString(s string) {
	if len(s) > 0 {
		cw.write([]byte(s))
	}
}

// catalogEncodeU64 writes a u64 in custom variable-length encoding:
// each byte carries 7 bits of data; the high bit signals more bytes follow.
func catalogEncodeU64(cw *CatalogWriter, v uint64) {
	if cw.err != nil {
		return
	}
	for {
		b := byte(v & 0x7f)
		v >>= 7
		if v != 0 {
			b |= 0x80
		}
		cw.writeByte(b)
		if v == 0 {
			break
		}
	}
}

// catalogEncodeI64 writes an i64. Positive values use the same encoding as u64.
// Negative values OR each byte with 0x80 and terminate with 0x00.
func catalogEncodeI64(cw *CatalogWriter, v int64) {
	if cw.err != nil {
		return
	}
	if v >= 0 {
		catalogEncodeU64(cw, uint64(v))
		return
	}
	enc := uint64(-v)
	for {
		b := byte(enc & 0x7f)
		enc >>= 7
		b |= 0x80
		cw.writeByte(b)
		if enc == 0 {
			break
		}
	}
	cw.writeByte(0x00)
}
