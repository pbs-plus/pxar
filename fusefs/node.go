package fusefs

import (
	"sync"
	"syscall"

	"github.com/sonroyaalmerol/pxar/format"
)

const (
	// RootInode is the inode number for the root directory.
	RootInode = 1

	// NonDirBit is set in the high bit of inodes for non-directory entries.
	NonDirBit uint64 = 1 << 63
)

// IsDirInode returns true if the inode represents a directory.
func IsDirInode(inode uint64) bool {
	return inode&NonDirBit == 0
}

// Node holds cached metadata for a filesystem entry.
type Node struct {
	mu           sync.Mutex
	inode        uint64
	parent       uint64
	refs         uint64
	entryStart   uint64
	entryEnd     uint64
	contentOff   uint64
	contentSize  uint64
	hasContent   bool
	cachedStat   format.Stat
	cachedSize   uint64
	hasCache     bool
}

// newNode creates a node with an initial reference count of 1.
func newNode(inode, parent, entryStart, entryEnd uint64) *Node {
	return &Node{
		inode:      inode,
		parent:     parent,
		refs:       1,
		entryStart: entryStart,
		entryEnd:   entryEnd,
	}
}

// Inode returns the node's inode number.
func (n *Node) Inode() uint64 { return n.inode }

// Parent returns the parent inode.
func (n *Node) Parent() uint64 { return n.parent }

// Ref increments the reference count. Returns false if the node is already dead.
func (n *Node) Ref() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.refs == 0 {
		return false
	}
	n.refs++
	return true
}

// Unref decrements the reference count. Returns true if the node should be removed.
func (n *Node) Unref(count uint64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if count >= n.refs {
		n.refs = 0
		return true
	}
	n.refs -= count
	return false
}

// SetContent sets the file content location in the archive.
func (n *Node) SetContent(offset, size uint64) {
	n.contentOff = offset
	n.contentSize = size
	n.hasContent = true
}

// Content returns the content offset and size.
func (n *Node) Content() (offset, size uint64, ok bool) {
	return n.contentOff, n.contentSize, n.hasContent
}

// SetCache stores cached stat metadata.
func (n *Node) SetCache(stat format.Stat, fileSize uint64) {
	n.cachedStat = stat
	n.cachedSize = fileSize
	n.hasCache = true
}

// Cache returns cached stat metadata.
func (n *Node) Cache() (format.Stat, uint64, bool) {
	return n.cachedStat, n.cachedSize, n.hasCache
}

// EntryRange returns the entry's byte range in the archive.
func (n *Node) EntryRange() (start, end uint64) {
	return n.entryStart, n.entryEnd
}

// StatToAttr converts pxar metadata to FUSE Attr.
func StatToAttr(inode uint64, stat format.Stat, fileSize uint64) Attr {
	nlink := uint32(1)
	if IsDirInode(inode) {
		nlink = 2
	}

	return Attr{
		Ino:       inode,
		Size:      fileSize,
		Blocks:    (fileSize + 511) / 512,
		Atime:     uint64(stat.Mtime.Secs),
		Mtime:     uint64(stat.Mtime.Secs),
		Ctime:     uint64(stat.Mtime.Secs),
		Atimensec: stat.Mtime.Nanos,
		Mtimensec: stat.Mtime.Nanos,
		Ctimensec: stat.Mtime.Nanos,
		Mode:      pxarModeToSyscall(stat.Mode),
		Nlink:     nlink,
		Uid:       stat.UID,
		Gid:       stat.GID,
		Blksize:   4096,
	}
}

// pxarModeToSyscall converts a pxar mode (format.ModeIFREG | perms) to a syscall mode.
func pxarModeToSyscall(mode uint64) uint32 {
	var ft uint32
	switch mode & format.ModeIFMT {
	case format.ModeIFDIR:
		ft = syscall.S_IFDIR
	case format.ModeIFREG:
		ft = syscall.S_IFREG
	case format.ModeIFLNK:
		ft = syscall.S_IFLNK
	case format.ModeIFBLK:
		ft = syscall.S_IFBLK
	case format.ModeIFCHR:
		ft = syscall.S_IFCHR
	case format.ModeIFIFO:
		ft = syscall.S_IFIFO
	case format.ModeIFSOCK:
		ft = syscall.S_IFSOCK
	}
	perms := uint32(mode & 0o7777)
	return ft | perms
}
