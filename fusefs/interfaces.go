// Package fusefs provides a read-only filesystem interface over pxar archives,
// compatible with hanwen/go-fuse's InodeEmbedder and Inode interfaces.
//
// This package does NOT import go-fuse — it defines compatible interfaces
// so callers can wrap them into a go-fuse FileSystem without the library
// needing the dependency.
package fusefs

import (
	"io"
	"syscall"
)

// Attr represents file metadata, compatible with go-fuse's fuse.Attr.
type Attr struct {
	Ino       uint64
	Size      uint64
	Blocks    uint64
	Atime     uint64
	Mtime     uint64
	Ctime     uint64
	Atimensec uint32
	Mtimensec uint32
	Ctimensec uint32
	Mode      uint32
	Nlink     uint32
	Uid       uint32
	Gid       uint32
	Rdev      uint32
	Blksize   uint32
}

// DirEntry is a directory entry, compatible with go-fuse's fuse.DirEntry.
type DirEntry struct {
	Ino uint64
	Mode uint32
	Name string
}

// DirEntryIndex adds an offset to DirEntry for readdir pagination.
type DirEntryIndex struct {
	DirEntry
	Offset uint64
}

// XAttr represents an extended attribute.
type XAttr struct {
	Name  string
	Value []byte
}

// FileSystem is the interface that maps pxar archive entries to FUSE operations.
// It is compatible with hanwen/go-fuse's InodeEmbedder and RawFileSystem patterns.
type FileSystem interface {
	// Lookup finds a child entry by name in the given parent directory inode.
	Lookup(parentInode uint64, name string) (inode uint64, attr Attr, err error)

	// Getattr returns the attributes for the given inode.
	Getattr(inode uint64) (Attr, error)

	// Readdir returns directory entries starting at the given offset.
	// Returns entries and the next offset. An empty slice signals end-of-directory.
	Readdir(inode uint64, offset uint64) ([]DirEntryIndex, error)

	// Open informs the kernel that the file is being opened.
	Open(inode uint64, flags uint32) error

	// Read reads data from a file inode at the given offset.
	// FUSE requires no short reads (except at EOF).
	Read(inode uint64, dest []byte, offset int64) (int, error)

	// Readlink returns the symlink target for the given inode.
	Readlink(inode uint64) (string, error)

	// ListXAttr returns all extended attribute names for the given inode.
	ListXAttr(inode uint64) ([]string, error)

	// GetXAttr returns the value of a specific extended attribute.
	GetXAttr(inode uint64, attr string) ([]byte, error)

	// Forget decrements the reference count for an inode.
	Forget(inode uint64, count uint64)

	// Statfs returns filesystem statistics.
	Statfs() (syscall.Statfs_t, error)

	// Access checks file accessibility.
	Access(inode uint64, mask uint32) error

	// Release is called when a file is closed.
	Release(inode uint64)
}

// EntryRangeInfo describes where an entry lives in the archive.
type EntryRangeInfo struct {
	Start uint64
	End   uint64
}

// ContentRange describes where file content lives in the archive.
type ContentRange struct {
	Offset uint64
	Size   uint64
}

// ReaderAt is a minimal read interface (io.ReaderAt subset).
type ReaderAt interface {
	ReadAt(p []byte, off int64) (n int, err error)
}

// Ensure standard library interfaces are satisfied.
var _ io.ReaderAt = (ReaderAt)(nil)
var _ FileSystem = (*Session)(nil)
