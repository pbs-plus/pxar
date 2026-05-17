package pxar

import (
	"os"
	"sync"
	"time"
)

// FileInfo implements os.FileInfo for a pxar archive entry.
// It carries all POSIX stat fields needed for restore, FUSE, and display.
type FileInfo struct {
	name      string
	size      int64
	mode      os.FileMode
	modTime   time.Time
	uid       uint32
	gid       uint32
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
// The type flags (isDir, isSymlink, etc.) are derived from mode bits
// if they haven't been set via the typed methods.
func NewFileInfo(name string, size int64, mode os.FileMode, modTime time.Time, uid, gid uint32) *FileInfo {
	fi := fileInfoPool.Get().(*FileInfo)
	fi.name = name
	fi.size = size
	fi.mode = mode
	fi.modTime = modTime
	fi.uid = uid
	fi.gid = gid
	fi.isDir = mode&os.ModeDir != 0
	fi.isSymlink = mode&os.ModeSymlink != 0
	fi.isDevice = mode&os.ModeDevice != 0
	fi.isFifo = mode&os.ModeNamedPipe != 0
	fi.isSocket = mode&os.ModeSocket != 0
	return fi
}

func (fi *FileInfo) Name() string       { return fi.name }
func (fi *FileInfo) Size() int64        { return fi.size }
func (fi *FileInfo) Mode() os.FileMode  { return fi.mode }
func (fi *FileInfo) ModTime() time.Time { return fi.modTime }
func (fi *FileInfo) IsDir() bool        { return fi.isDir }
func (fi *FileInfo) Sys() any           { return fi }
func (fi *FileInfo) UID() uint32        { return fi.uid }
func (fi *FileInfo) GID() uint32        { return fi.gid }
func (fi *FileInfo) IsSymlink() bool    { return fi.isSymlink }
func (fi *FileInfo) IsDevice() bool     { return fi.isDevice }
func (fi *FileInfo) IsFifo() bool       { return fi.isFifo }
func (fi *FileInfo) IsSocket() bool     { return fi.isSocket }

// EntryToFileInfo converts a low-level Entry to a FileInfo.
// This is the canonical conversion — consumers should use this instead
// of reimplementing it.
func EntryToFileInfo(e *Entry) *FileInfo {
	mode := os.FileMode(e.Metadata.Stat.Mode & 0o7777)

	fi := fileInfoPool.Get().(*FileInfo)
	fi.name = e.FileName()
	fi.size = int64(e.FileSize)
	fi.mode = mode
	fi.modTime = time.Unix(e.Metadata.Stat.Mtime.Secs, int64(e.Metadata.Stat.Mtime.Nanos))
	fi.uid = e.Metadata.Stat.UID
	fi.gid = e.Metadata.Stat.GID
	fi.isDir = false
	fi.isSymlink = false
	fi.isDevice = false
	fi.isFifo = false
	fi.isSocket = false

	switch e.Kind {
	case KindDirectory:
		fi.isDir = true
		fi.mode |= os.ModeDir
	case KindSymlink:
		fi.isSymlink = true
		fi.mode |= os.ModeSymlink
	case KindDevice:
		fi.isDevice = true
		fi.mode |= os.ModeDevice
	case KindFIFO:
		fi.isFifo = true
		fi.mode |= os.ModeNamedPipe
	case KindSocket:
		fi.isSocket = true
		fi.mode |= os.ModeSocket
	}

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
