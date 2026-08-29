package transfer

import (
	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
)

// CopyOption configures a file transfer operation.
type CopyOption struct {
	SourceCryptConfig *datastore.CryptConfig
	TargetCryptConfig *datastore.CryptConfig
	SourceCryptMode   datastore.CryptMode
	TargetCryptMode   datastore.CryptMode
	OnProgress        func(path string, bytes uint64)
	TargetFormat      format.FormatVersion
	Overwrite         bool
}

// PathMapping maps a source path to a destination path inside the archives.
type PathMapping struct {
	Src string // path in the source archive
	Dst string // path in the target archive
}

type WalkFilter uint

const (
	WalkFiles     WalkFilter = 1 << iota // regular files
	WalkDirs                             // directories
	WalkSymlinks                         // symbolic links
	WalkHardlinks                        // hard links
	WalkDevices                          // device nodes
	WalkFifos                            // named pipes (FIFOs)
	WalkSockets                          // unix sockets

	WalkAll WalkFilter = WalkFiles | WalkDirs | WalkSymlinks |
		WalkHardlinks | WalkDevices | WalkFifos | WalkSockets
)

// matches reports whether an entry with the given kind passes the filter.
func (f WalkFilter) matches(kind pxar.EntryKind) bool {
	switch kind {
	case pxar.KindFile:
		return f&WalkFiles != 0
	case pxar.KindDirectory:
		return f&WalkDirs != 0
	case pxar.KindSymlink:
		return f&WalkSymlinks != 0
	case pxar.KindHardlink:
		return f&WalkHardlinks != 0
	case pxar.KindDevice:
		return f&WalkDevices != 0
	case pxar.KindFIFO:
		return f&WalkFifos != 0
	case pxar.KindSocket:
		return f&WalkSockets != 0
	default:
		return false
	}
}

// CatalogEntry is a stripped-down entry for index-building. It contains
// only the fields needed for cataloging: path, kind, size, and parent.
type CatalogEntry struct {
	Path       string
	ParentPath string
	Kind       pxar.EntryKind
	FileSize   uint64
}
