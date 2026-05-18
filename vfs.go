package pxar

import (
	"os"
	"sync"
	"time"
	"unsafe"
)

// FileType identifies the kind of file system entry, matching
// the wire-protocol values used in CBOR serialization.
type FileType uint8

const (
	FileTypeFile FileType = iota
	FileTypeDirectory
	FileTypeSymlink
	FileTypeHardlink
	FileTypeDevice
	FileTypeFifo
	FileTypeSocket
)

// FileInfo implements os.FileInfo for a pxar archive entry.
// It carries all POSIX stat fields, archive offsets, xattrs,
// and link targets needed for restore, FUSE, display, and
// CBOR wire serialization.
//
// Exported fields use CBOR tags for wire compatibility.
// The os.FileInfo interface methods access derived values.
type FileInfo struct {
	// --- CBOR wire fields ---
	FileName        []byte            `cbor:"file_name,omitempty"`
	FileType        FileType          `cbor:"file_type"`
	EntryRangeStart uint64            `cbor:"entry_range_start"`
	EntryRangeEnd   uint64            `cbor:"entry_range_end"`
	ContentRange    []uint64          `cbor:"content_range,omitempty"`
	ContentOffset   uint64            `cbor:"content_offset"`
	LinkTarget      string            `cbor:"link_target,omitempty"`
	Xattrs          map[string][]byte `cbor:"xattrs,omitempty"`
	RawMode         uint64            `cbor:"mode"`
	RawUID          uint32            `cbor:"uid"`
	RawGID          uint32            `cbor:"gid"`
	RawSize         uint64            `cbor:"size"`
	MtimeSecs       int64             `cbor:"mtime_secs"`
	MtimeNsecs      uint32            `cbor:"mtime_nsecs"`

	// --- derived / convenience (not serialized) ---
	modTime   time.Time
	mode      os.FileMode
	isDir     bool
	isSymlink bool
	isDevice  bool
	isFifo    bool
	isSocket  bool
}

var fileInfoPool = sync.Pool{
	New: func() any { return &FileInfo{} },
}

// NewFileInfo constructs a FileInfo from explicit fields.
func NewFileInfo(name string, size int64, mode os.FileMode, modTime time.Time, uid, gid uint32) *FileInfo {
	fi := fileInfoPool.Get().(*FileInfo)
	fi.FileName = unsafe.Slice(unsafe.StringData(name), len(name))
	fi.RawSize = uint64(size)
	fi.mode = mode
	fi.modTime = modTime
	fi.RawUID = uid
	fi.RawGID = gid
	fi.RawMode = uint64(mode)
	fi.MtimeSecs = modTime.Unix()
	fi.MtimeNsecs = uint32(modTime.Nanosecond())
	fi.isDir = mode&os.ModeDir != 0
	fi.isSymlink = mode&os.ModeSymlink != 0
	fi.isDevice = mode&os.ModeDevice != 0
	fi.isFifo = mode&os.ModeNamedPipe != 0
	fi.isSocket = mode&os.ModeSocket != 0
	return fi
}

// os.FileInfo interface
func (fi *FileInfo) Name() string       { return string(fi.FileName) }
func (fi *FileInfo) Size() int64        { return int64(fi.RawSize) }
func (fi *FileInfo) Mode() os.FileMode  { return fi.mode }
func (fi *FileInfo) ModTime() time.Time { return fi.modTime }
func (fi *FileInfo) IsDir() bool        { return fi.isDir }
func (fi *FileInfo) Sys() any           { return fi }

// Extended accessors
func (fi *FileInfo) UID() uint32     { return fi.RawUID }
func (fi *FileInfo) GID() uint32     { return fi.RawGID }
func (fi *FileInfo) IsSymlink() bool { return fi.isSymlink }
func (fi *FileInfo) IsDevice() bool  { return fi.isDevice }
func (fi *FileInfo) IsFifo() bool    { return fi.isFifo }
func (fi *FileInfo) IsSocket() bool  { return fi.isSocket }
func (fi *FileInfo) IsFile() bool {
	return !fi.isDir && !fi.isSymlink && !fi.isDevice && !fi.isFifo && !fi.isSocket
}

// ReleaseFileInfo returns a FileInfo to the pool.
// Callers must not use fi after calling ReleaseFileInfo.
func ReleaseFileInfo(fi *FileInfo) {
	*fi = FileInfo{}
	fileInfoPool.Put(fi)
}

// EntryToFileInfo converts a low-level Entry to a FileInfo.
// This is the canonical conversion — consumers should use this instead
// of reimplementing it.
func EntryToFileInfo(e *Entry) *FileInfo {
	fi := fileInfoPool.Get().(*FileInfo)
	baseName := e.FileName()
	fi.FileName = unsafe.Slice(unsafe.StringData(baseName), len(baseName))
	fi.RawSize = e.FileSize
	fi.EntryRangeStart = e.FileOffset
	fi.EntryRangeEnd = e.FileOffset + e.FileSize
	fi.ContentOffset = e.ContentOffset
	fi.RawMode = e.Metadata.Stat.Mode
	fi.RawUID = e.Metadata.Stat.UID
	fi.RawGID = e.Metadata.Stat.GID
	fi.MtimeSecs = e.Metadata.Stat.Mtime.Secs
	fi.MtimeNsecs = e.Metadata.Stat.Mtime.Nanos
	fi.modTime = time.Unix(e.Metadata.Stat.Mtime.Secs, int64(e.Metadata.Stat.Mtime.Nanos))
	fi.LinkTarget = e.LinkTarget

	// Derive os.FileMode from raw mode (permission bits only)
	fi.mode = os.FileMode(e.Metadata.Stat.Mode & 0o7777)

	fi.isDir = false
	fi.isSymlink = false
	fi.isDevice = false
	fi.isFifo = false
	fi.isSocket = false

	switch e.Kind {
	case KindDirectory:
		fi.FileType = FileTypeDirectory
		fi.isDir = true
		fi.mode |= os.ModeDir
	case KindSymlink:
		fi.FileType = FileTypeSymlink
		fi.isSymlink = true
		fi.mode |= os.ModeSymlink
	case KindDevice:
		fi.FileType = FileTypeDevice
		fi.isDevice = true
		fi.mode |= os.ModeDevice
	case KindFIFO:
		fi.FileType = FileTypeFifo
		fi.isFifo = true
		fi.mode |= os.ModeNamedPipe
	case KindSocket:
		fi.FileType = FileTypeSocket
		fi.isSocket = true
		fi.mode |= os.ModeSocket
	case KindHardlink:
		fi.FileType = FileTypeHardlink
	default:
		fi.FileType = FileTypeFile
	}

	// ContentRange for regular files
	if e.IsRegularFile() && e.ContentOffset > 0 {
		fi.ContentRange = []uint64{e.ContentOffset, e.ContentOffset + e.FileSize}
	}

	// Xattrs
	fi.Xattrs = EntryToXAttrs(e)

	return fi
}

// EntryToXAttrs extracts extended attributes from an entry into a map.
// Returns nil if the entry has no xattrs or fcaps.
func EntryToXAttrs(e *Entry) map[string][]byte {
	nx := len(e.Metadata.XAttrs)
	if nx == 0 && e.Metadata.FCaps == nil {
		return nil
	}
	xattrs := make(map[string][]byte, nx+1)
	for _, xa := range e.Metadata.XAttrs {
		xattrs[string(xa.Name())] = xa.Value()
	}
	if e.Metadata.FCaps != nil {
		xattrs["security.capability"] = e.Metadata.FCaps
	}
	return xattrs
}
