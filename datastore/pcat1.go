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
	name       string
	size       uint64
	mtime      int64
	childStart int64
	entryType  byte
}

type catalogDirInfo struct {
	name    string
	entries []catalogDirEntry
}

type CatalogWriter struct {
	w        io.Writer
	err      error
	dirstack []catalogDirInfo
	pos      int64
}

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

func (cw *CatalogWriter) AddFIFO(name string) {
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

func (cw *CatalogWriter) EndDirectory() {
	if cw.err != nil {
		return
	}
	dir := cw.dirstack[len(cw.dirstack)-1]
	cw.dirstack = cw.dirstack[:len(cw.dirstack)-1]

	start := cw.pos
	cw.writeDirBlock(dir, start)

	if len(cw.dirstack) > 0 {
		parent := &cw.dirstack[len(cw.dirstack)-1]
		parent.entries = append(parent.entries, catalogDirEntry{
			entryType:  CatalogEntryDir,
			name:       dir.name,
			childStart: start,
		})
	}
}

func (cw *CatalogWriter) Finish() error {
	if cw.err != nil {
		return cw.err
	}
	root := cw.dirstack[len(cw.dirstack)-1]
	cw.dirstack = cw.dirstack[:len(cw.dirstack)-1]

	rootStart := cw.pos
	cw.writeDirBlock(root, rootStart)

	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(rootStart))
	cw.write(buf[:])

	return cw.err
}

func (cw *CatalogWriter) writeDirBlock(dir catalogDirInfo, start int64) {
	table := appendCatalogVarintU64(nil, uint64(len(dir.entries)))
	for _, e := range dir.entries {
		table = append(table, e.entryType)
		table = appendCatalogVarintU64(table, uint64(len(e.name)))
		table = append(table, e.name...)
		switch e.entryType {
		case CatalogEntryFile:
			table = appendCatalogVarintU64(table, e.size)
			table = appendCatalogVarintI64(table, e.mtime)
		case CatalogEntryDir:
			table = appendCatalogVarintU64(table, uint64(start-e.childStart))
		}
	}
	cw.write(appendCatalogVarintU64(nil, uint64(len(table))))
	cw.write(table)
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

func appendCatalogVarintU64(buf []byte, v uint64) []byte {
	for {
		b := byte(v & 0x7f)
		v >>= 7
		if v != 0 {
			b |= 0x80
		}
		buf = append(buf, b)
		if v == 0 {
			return buf
		}
	}
}

func appendCatalogVarintI64(buf []byte, v int64) []byte {
	if v >= 0 {
		return appendCatalogVarintU64(buf, uint64(v))
	}
	enc := uint64(-v)
	for {
		b := byte(enc & 0x7f)
		enc >>= 7
		b |= 0x80
		buf = append(buf, b)
		if enc == 0 {
			break
		}
	}
	buf = append(buf, 0x00)
	return buf
}
