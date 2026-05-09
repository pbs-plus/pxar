package transfer

import (
	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
)

// TransferOption configures a file transfer operation.
type TransferOption struct {
	SourceCryptConfig *datastore.CryptConfig
	TargetCryptConfig *datastore.CryptConfig
	OnProgress        func(path string, bytes uint64)
	TargetFormat      format.FormatVersion
	Overwrite         bool
}

// PathMapping maps a source path to a destination path inside the archives.
type PathMapping struct {
	Src string // path in the source archive
	Dst string // path in the target archive
}

// WalkFunc is called for each entry encountered during WalkTree.
// entry is the archive entry. content is the file data (nil for non-files).
// Return nil to continue, or an error to stop.
type WalkFunc func(entry *pxar.Entry, content []byte) error

// MetaWalkFunc is called for each entry during a metadata-only walk.
// Unlike WalkFunc, no content parameter is provided since content is never read.
type MetaWalkFunc func(entry *pxar.Entry) error

// WalkFilter is a bitmask that controls which entry types are visited during
// a walk. Entries whose type is not in the mask are skipped entirely — the
// callback is never invoked for them, and directories are not descended into.
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
	case pxar.KindFifo:
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

// WalkOption configures walk behavior. The zero value walks all entry types
// and reads file content (equivalent to the original WalkTree behavior).
type WalkOption struct {
	// MetaOnly skips reading file content. When true, content is never read
	// from the archive and the content parameter passed to WalkFunc is always nil.
	MetaOnly bool

	// Filter is a bitmask of entry types to include. Entries not matching
	// the filter are skipped without invoking the callback. Directories that
	// are filtered out are not descended into. Zero means accept all types.
	Filter WalkFilter

	// SkipCount fast-forwards past the first N entries without invoking the
	// callback. Entries are still decoded but the walk callback is skipped.
	// Useful for resuming a previous walk.
	SkipCount int
}

// WalkMetaOnly is a convenience WalkOption for metadata-only walks.
var WalkMetaOnly = WalkOption{MetaOnly: true}
