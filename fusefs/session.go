package fusefs

import (
	"fmt"
	"io"
	"sync"
	"syscall"

	"github.com/sonroyaalmerol/pxar/accessor"
	pxar "github.com/sonroyaalmerol/pxar"
)

// Session implements FileSystem over a pxar archive, providing FUSE-compatible
// operations without importing go-fuse directly.
type Session struct {
	mu     sync.Mutex
	acc    *accessor.Accessor
	reader io.ReadSeeker
	size   int64
	nodes  map[uint64]*Node
}

// NewSession creates a new FUSE filesystem session over a pxar archive.
func NewSession(r io.ReadSeeker, size int64) (*Session, error) {
	acc := accessor.NewAccessor(r)

	s := &Session{
		acc:    acc,
		reader: r,
		size:   size,
		nodes:  make(map[uint64]*Node),
	}

	// Read root entry to initialize
	s.mu.Lock()
	defer s.mu.Unlock()

	root, err := acc.ReadRoot()
	if err != nil {
		return nil, fmt.Errorf("read root: %w", err)
	}

	// Root inode: entry_end = archive size (no NonDirBit)
	rootEnd := uint64(size)
	s.nodes[RootInode] = newNode(RootInode, RootInode, 0, rootEnd)
	s.nodes[RootInode].SetContent(root.ContentOffset, 0)
	s.nodes[RootInode].SetCache(root.Metadata.Stat, 0)

	return s, nil
}

// Close releases resources held by the session.
func (s *Session) Close() error {
	s.mu.Lock()
	s.nodes = make(map[uint64]*Node)
	s.mu.Unlock()
	return nil
}

// Lookup finds a child entry by name in the given parent directory.
func (s *Session) Lookup(parentInode uint64, name string) (uint64, Attr, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dirOffset, err := s.dirContentOffset(parentInode)
	if err != nil {
		return 0, Attr{}, err
	}

	entries, err := s.acc.ListDirectory(int64(dirOffset))
	if err != nil {
		return 0, Attr{}, err
	}

	for i := range entries {
		if entries[i].FileName() != name {
			continue
		}
		entry := &entries[i]
		inode := s.toInode(entry)
		s.ensureNode(inode, parentInode, entry)

		attr := StatToAttr(inode, entry.Metadata.Stat, entry.FileSize)
		return inode, attr, nil
	}

	return 0, Attr{}, syscall.ENOENT
}

// Getattr returns attributes for the given inode.
func (s *Session) Getattr(inode uint64) (Attr, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	node, ok := s.nodes[inode]
	if !ok {
		return Attr{}, syscall.ENOENT
	}

	// Use cached stat if available (e.g., root inode)
	if stat, sz, hasCache := node.Cache(); hasCache {
		return StatToAttr(inode, stat, sz), nil
	}

	entry, err := s.acc.ReadEntryAt(int64(node.entryStart))
	if err != nil {
		return Attr{}, err
	}

	return StatToAttr(inode, entry.Metadata.Stat, entry.FileSize), nil
}

// Readdir returns directory entries starting at the given offset.
func (s *Session) Readdir(inode uint64, offset uint64) ([]DirEntryIndex, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dirOffset, err := s.dirContentOffset(inode)
	if err != nil {
		return nil, err
	}

	entries, err := s.acc.ListDirectory(int64(dirOffset))
	if err != nil {
		return nil, err
	}

	// Offsets 0..len(entries)-1 = child entries
	// Offset len(entries) = "."
	// Offset len(entries)+1 = ".."

	if int(offset) < len(entries) {
		var result []DirEntryIndex
		childEntries := entries[offset:]
		for i, e := range childEntries {
			childInode := s.toInode(&e)
			s.ensureNode(childInode, inode, &e)
			attr := StatToAttr(childInode, e.Metadata.Stat, e.FileSize)

			result = append(result, DirEntryIndex{
				DirEntry: DirEntry{
					Ino:  childInode,
					Mode: attr.Mode,
					Name: e.FileName(),
				},
				Offset: offset + uint64(i) + 1,
			})
		}
		return result, nil
	}

	if int(offset) == len(entries) {
		// "." entry
		attr := Attr{Ino: inode, Mode: syscall.S_IFDIR | 0o555, Nlink: 2}
		if node, ok := s.nodes[inode]; ok {
			if entry, err := s.acc.ReadEntryAt(int64(node.entryStart)); err == nil {
				attr = StatToAttr(inode, entry.Metadata.Stat, entry.FileSize)
			}
		}
		return []DirEntryIndex{{
			DirEntry: DirEntry{Ino: inode, Name: ".", Mode: attr.Mode},
			Offset:   offset + 1,
		}}, nil
	}

	if int(offset) == len(entries)+1 {
		// ".." entry
		parentInode := uint64(RootInode)
		if node, ok := s.nodes[inode]; ok && node.parent != 0 {
			parentInode = node.parent
		}
		attr := Attr{Ino: parentInode, Mode: syscall.S_IFDIR | 0o555, Nlink: 2}
		if pnode, ok := s.nodes[parentInode]; ok {
			if entry, err := s.acc.ReadEntryAt(int64(pnode.entryStart)); err == nil {
				attr = StatToAttr(parentInode, entry.Metadata.Stat, entry.FileSize)
			}
		}
		return []DirEntryIndex{{
			DirEntry: DirEntry{Ino: parentInode, Name: "..", Mode: attr.Mode},
			Offset:   offset + 1,
		}}, nil
	}

	return nil, nil
}

// Open is a no-op for read-only archives.
func (s *Session) Open(inode uint64, flags uint32) error {
	return nil
}

// Read reads data from a file. FUSE requires no short reads except at EOF.
func (s *Session) Read(inode uint64, dest []byte, offset int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	node, ok := s.nodes[inode]
	if !ok {
		return 0, syscall.ENOENT
	}

	if IsDirInode(inode) {
		return 0, syscall.EISDIR
	}

	contentOff, contentSz, hasContent := node.Content()
	if !hasContent {
		return 0, syscall.EBADF
	}

	if offset >= int64(contentSz) {
		return 0, nil
	}

	remaining := int64(contentSz) - offset
	toRead := int64(len(dest))
	if toRead > remaining {
		toRead = remaining
	}

	if _, err := s.reader.Seek(int64(contentOff)+offset, io.SeekStart); err != nil {
		return 0, err
	}

	n, err := io.ReadFull(s.reader, dest[:toRead])
	if err != nil && err != io.ErrUnexpectedEOF {
		return n, err
	}
	return n, nil
}

// Readlink returns the symlink target.
func (s *Session) Readlink(inode uint64) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	node, ok := s.nodes[inode]
	if !ok {
		return "", syscall.ENOENT
	}

	if inode == RootInode {
		return "", syscall.EINVAL
	}

	entry, err := s.acc.ReadEntryAt(int64(node.entryStart))
	if err != nil {
		return "", err
	}

	if entry.LinkTarget == "" {
		return "", syscall.EINVAL
	}
	return entry.LinkTarget, nil
}

// ListXAttr returns extended attribute names.
func (s *Session) ListXAttr(inode uint64) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	node, ok := s.nodes[inode]
	if !ok {
		return nil, syscall.ENOENT
	}

	// Root inode has no FILENAME header, so ReadEntryAt won't work.
	// Root xattrs come from the cached stat read in NewSession.
	if inode == RootInode {
		return nil, nil
	}

	entry, err := s.acc.ReadEntryAt(int64(node.entryStart))
	if err != nil {
		return nil, err
	}

	var names []string
	for _, xa := range entry.Metadata.XAttrs {
		names = append(names, string(xa.Name()))
	}
	if entry.Metadata.FCaps != nil {
		names = append(names, "security.capability")
	}
	return names, nil
}

// GetXAttr returns the value of a specific extended attribute.
func (s *Session) GetXAttr(inode uint64, attr string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	node, ok := s.nodes[inode]
	if !ok {
		return nil, syscall.ENOENT
	}

	if inode == RootInode {
		return nil, syscall.ENODATA
	}

	entry, err := s.acc.ReadEntryAt(int64(node.entryStart))
	if err != nil {
		return nil, err
	}

	for _, xa := range entry.Metadata.XAttrs {
		if string(xa.Name()) == attr {
			return xa.Value(), nil
		}
	}
	if attr == "security.capability" && entry.Metadata.FCaps != nil {
		return entry.Metadata.FCaps, nil
	}
	return nil, syscall.ENODATA
}

// Forget decrements the reference count for an inode.
func (s *Session) Forget(inode uint64, count uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if node, ok := s.nodes[inode]; ok {
		if node.Unref(count) {
			delete(s.nodes, inode)
		}
	}
}

// Statfs returns filesystem statistics.
func (s *Session) Statfs() (syscall.Statfs_t, error) {
	var stat syscall.Statfs_t
	stat.Blocks = uint64(s.size / 512)
	stat.Bsize = 4096
	stat.Namelen = 255
	stat.Frsize = 512
	return stat, nil
}

// Access checks file accessibility (always allowed for read-only).
func (s *Session) Access(inode uint64, mask uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodes[inode]; !ok {
		return syscall.ENOENT
	}
	return nil
}

// Release is a no-op.
func (s *Session) Release(inode uint64) {}

// dirContentOffset returns the content area offset for a directory inode.
func (s *Session) dirContentOffset(inode uint64) (uint64, error) {
	if inode == RootInode {
		// Root content offset was stored during NewSession
		if node, ok := s.nodes[RootInode]; ok {
			off, _, has := node.Content()
			if has {
				return off, nil
			}
		}
		// Fallback: read root entry
		root, err := s.acc.ReadRoot()
		if err != nil {
			return 0, err
		}
		return root.ContentOffset, nil
	}
	if !IsDirInode(inode) {
		return 0, syscall.ENOTDIR
	}
	node, ok := s.nodes[inode]
	if !ok {
		return 0, syscall.ENOENT
	}
	off, _, has := node.Content()
	if !has {
		// Read the entry to get content offset
		entry, err := s.acc.ReadEntryAt(int64(node.entryStart))
		if err != nil {
			return 0, err
		}
		node.SetContent(entry.ContentOffset, 0)
		return entry.ContentOffset, nil
	}
	return off, nil
}

// toInode computes the inode number for a pxar entry.
// Directories use entry_end (high bit clear), files use entry_start | NonDirBit.
func (s *Session) toInode(entry *pxar.Entry) uint64 {
	if entry.IsDir() {
		return entry.FileOffset + uint64(entry.FileSize)
	}
	return entry.FileOffset | NonDirBit
}

// ensureNode creates a node in the cache if it doesn't already exist.
func (s *Session) ensureNode(inode, parent uint64, entry *pxar.Entry) {
	if _, ok := s.nodes[inode]; ok {
		return
	}

	node := newNode(inode, parent, entry.FileOffset, entry.FileOffset+uint64(entry.FileSize))

	if entry.IsRegularFile() && entry.ContentOffset != 0 {
		node.SetContent(entry.ContentOffset, entry.FileSize)
	} else if entry.IsDir() && entry.ContentOffset != 0 {
		node.SetContent(entry.ContentOffset, 0)
	}

	s.nodes[inode] = node
}

// Ensure Session satisfies FileSystem.
var _ FileSystem = (*Session)(nil)
