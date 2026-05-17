// Package vfs provides a high-level, path-based virtual filesystem interface
// for pxar archives. It wraps the low-level offset-based accessor API with
// simple operations like Stat, ReadDir, Open, and ReadFile.
//
// Use NewLocalFS to create a filesystem backed by a transfer.ArchiveReader.
// Use NewRemoteFS to wrap a client-side transport.
package vfs

import (
	"fmt"
	"io"
	"sync"

	"github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/accessor"
	"github.com/pbs-plus/pxar/transfer"
)

// DirEntry is a lightweight directory entry returned by ReadDir.
type DirEntry struct {
	Name string
	Type pxar.EntryKind
	Info *pxar.FileInfo
}

// FileSystem provides path-based access to a pxar archive.
// It hides all offset/content tracking, goodbye tables, and format details.
// Implementations must be safe for concurrent use.
type FileSystem interface {
	// Stat returns file info for the entry at path.
	Stat(path string) (*pxar.FileInfo, error)

	// ReadDir lists entries in a directory.
	ReadDir(path string) ([]DirEntry, error)

	// Open returns a FileHandle for reading file content.
	// Returns an error for non-regular-file entries.
	Open(path string) (FileHandle, error)

	// Readlink returns the target of a symbolic link.
	Readlink(path string) (string, error)

	// ReadFile reads the entire content of a file.
	// Convenience wrapper around Open + io.ReadAll.
	ReadFile(path string) ([]byte, error)

	// ListXAttrs returns extended attributes for the entry at path.
	ListXAttrs(path string) (map[string][]byte, error)

	// Close releases all resources.
	Close() error
}

// FileHandle provides read access to a file within a pxar archive.
// It implements io.Reader, io.ReaderAt, io.Seeker, and io.Closer.
type FileHandle interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer

	// Size returns the total file size.
	Size() int64
}

// RemoteFS extends FileSystem with client-side transport operations.
// Both the local accessor and a remote pipe client implement this,
// giving consumers a single type regardless of proximity to the archive.
type RemoteFS interface {
	FileSystem

	// SendError reports an error to the remote side.
	SendError(err error) error

	// Done signals that the client has finished using the filesystem.
	Done() error
}

// --- LocalFileSystem ---

// LocalFileSystem provides path-based access to a pxar archive backed by an
// ArchiveReader. It implements FileSystem, hiding all offset tracking, goodbye
// table lookups, and format details.
//
// Thread safety: all methods are safe for concurrent use.
type LocalFileSystem struct {
	reader transfer.ArchiveReader

	mu    sync.Mutex
	cache map[string]*entryAndInfo
}

type entryAndInfo struct {
	entry *pxar.Entry
	info  *pxar.FileInfo
}

// NewLocalFS creates a FileSystem backed by an ArchiveReader.
func NewLocalFS(reader transfer.ArchiveReader) *LocalFileSystem {
	return &LocalFileSystem{
		reader: reader,
		cache:  make(map[string]*entryAndInfo, 256),
	}
}

// Reader returns the underlying ArchiveReader for advanced operations
// (e.g., ReadEntryAt for full metadata, PayloadReaderAt for FUSE).
func (fs *LocalFileSystem) Reader() transfer.ArchiveReader {
	return fs.reader
}

// Stat returns file info for the entry at path.
func (fs *LocalFileSystem) Stat(path string) (*pxar.FileInfo, error) {
	p := cleanPath(path)
	if cached := fs.getCached(p); cached != nil {
		return cached.info, nil
	}
	entry, err := fs.lookup(p)
	if err != nil {
		return nil, err
	}
	return fs.put(p, entry).info, nil
}

// ReadDir lists entries in a directory.
func (fs *LocalFileSystem) ReadDir(path string) ([]DirEntry, error) {
	p := cleanPath(path)

	dirEntry, err := fs.lookup(p)
	if err != nil {
		return nil, err
	}
	if !dirEntry.IsDir() {
		return nil, fmt.Errorf("pxar: %q is not a directory", p)
	}
	fs.put(p, dirEntry)

	var entries []DirEntry
	err = fs.reader.ListDirectory(int64(dirEntry.ContentOffset), accessor.ListOption{Minimal: true}, func(e *pxar.Entry) error {
		childPath := joinPath(p, e.FileName())
		info := pxar.EntryToFileInfo(e)
		fs.put(childPath, e)
		entries = append(entries, DirEntry{
			Name: e.FileName(),
			Type: e.Kind,
			Info: info,
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("pxar: readdir %q: %w", p, err)
	}
	return entries, nil
}

// Open returns a FileHandle for reading file content.
func (fs *LocalFileSystem) Open(path string) (FileHandle, error) {
	p := cleanPath(path)

	entry, err := fs.lookup(p)
	if err != nil {
		return nil, err
	}
	if !entry.IsRegularFile() {
		return nil, fmt.Errorf("pxar: %q is not a regular file", p)
	}
	fs.put(p, entry)

	rc, err := fs.reader.ReadFileContentReader(entry)
	if err != nil {
		return nil, fmt.Errorf("pxar: open %q: %w", p, err)
	}
	return &archiveFileHandle{
		reader: rc,
		size:   int64(entry.FileSize),
	}, nil
}

// Readlink returns the target of a symbolic link.
func (fs *LocalFileSystem) Readlink(path string) (string, error) {
	p := cleanPath(path)

	entry, err := fs.lookup(p)
	if err != nil {
		return "", err
	}
	if !entry.IsSymlink() {
		return "", fmt.Errorf("pxar: %q is not a symlink", p)
	}
	fs.put(p, entry)
	return entry.LinkTarget, nil
}

// ReadFile reads the entire content of a file.
func (fs *LocalFileSystem) ReadFile(path string) ([]byte, error) {
	fh, err := fs.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = fh.Close() }()
	return io.ReadAll(fh)
}

// ListXAttrs returns extended attributes for the entry at path.
func (fs *LocalFileSystem) ListXAttrs(path string) (map[string][]byte, error) {
	p := cleanPath(path)

	entry, err := fs.lookup(p)
	if err != nil {
		return nil, err
	}

	if len(entry.Metadata.XAttrs) == 0 && entry.Metadata.FCaps == nil {
		fs.mu.Lock()
		full, rerr := fs.reader.ReadEntryAt(int64(entry.FileOffset))
		fs.mu.Unlock()
		if rerr != nil {
			return nil, nil
		}
		entry = full
		fs.put(p, entry)
	}

	return pxar.EntryToXAttrs(entry), nil
}

// Close releases resources.
func (fs *LocalFileSystem) Close() error {
	if fs.reader != nil {
		return fs.reader.Close()
	}
	return nil
}

// LookupEntry returns the raw pxar.Entry for a path. Useful for consumers
// that need low-level fields (FileOffset, PayloadOffset, etc).
func (fs *LocalFileSystem) LookupEntry(path string) (*pxar.Entry, error) {
	p := cleanPath(path)
	return fs.lookup(p)
}

func (fs *LocalFileSystem) lookup(path string) (*pxar.Entry, error) {
	if cached := fs.getCached(path); cached != nil {
		return cached.entry, nil
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	if cached, ok := fs.cache[path]; ok {
		return cached.entry, nil
	}

	entry, err := fs.reader.Lookup(path)
	if err != nil {
		return nil, fmt.Errorf("pxar: lookup %q: %w", path, err)
	}
	return entry, nil
}

func (fs *LocalFileSystem) getCached(path string) *entryAndInfo {
	fs.mu.Lock()
	cached := fs.cache[path]
	fs.mu.Unlock()
	return cached
}

func (fs *LocalFileSystem) put(path string, e *pxar.Entry) *entryAndInfo {
	combined := &entryAndInfo{entry: e, info: pxar.EntryToFileInfo(e)}
	fs.mu.Lock()
	fs.cache[path] = combined
	fs.mu.Unlock()
	return combined
}

// archiveFileHandle wraps an io.ReadCloser as a FileHandle.
type archiveFileHandle struct {
	reader io.ReadCloser
	size   int64
	offset int64
}

func (h *archiveFileHandle) Read(p []byte) (int, error) {
	n, err := h.reader.Read(p)
	h.offset += int64(n)
	return n, err
}

func (h *archiveFileHandle) ReadAt(p []byte, off int64) (int, error) {
	if ra, ok := h.reader.(io.ReaderAt); ok {
		return ra.ReadAt(p, off)
	}
	if off != h.offset {
		return 0, fmt.Errorf("pxar: ReadAt requires sequential access (requested %d, current %d)", off, h.offset)
	}
	return h.Read(p)
}

func (h *archiveFileHandle) Seek(offset int64, whence int) (int64, error) {
	if seeker, ok := h.reader.(io.Seeker); ok {
		n, err := seeker.Seek(offset, whence)
		h.offset = n
		return n, err
	}
	switch whence {
	case io.SeekCurrent:
		return h.offset, nil
	case io.SeekStart:
		if offset < h.offset {
			return h.offset, fmt.Errorf("pxar: cannot seek backward on sequential reader")
		}
		if _, err := io.CopyN(io.Discard, h.reader, offset-h.offset); err != nil {
			return h.offset, err
		}
		h.offset = offset
		return offset, nil
	case io.SeekEnd:
		return h.size, nil
	default:
		return h.offset, fmt.Errorf("pxar: invalid whence %d", whence)
	}
}

func (h *archiveFileHandle) Close() error { return h.reader.Close() }
func (h *archiveFileHandle) Size() int64  { return h.size }

// --- Helpers ---

func cleanPath(p string) string {
	if p == "" || p == "/" {
		return "/"
	}
	if p[0] == '/' {
		return p[1:]
	}
	return p
}

func joinPath(parent, child string) string {
	if parent == "" || parent == "/" {
		return child
	}
	return parent + "/" + child
}
