package vfs

import (
	"context"
	"fmt"
	"io"
	"sync"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/accessor"
	"github.com/pbs-plus/pxar/transfer"
)

// OffsetFileSystem provides offset-based access to a pxar archive.
// Unlike the path-based FileSystem, operations use byte offsets from
// the archive structure (entry offsets, content offsets). This mirrors
// the wire protocol used by PBS agents for restore operations.
//
// Thread safety: all methods are safe for concurrent use.
type OffsetFileSystem interface {
	// Root returns the root directory entry.
	Root() (*pxar.FileInfo, error)

	// Lookup finds an entry by archive-internal path.
	Lookup(path string) (*pxar.FileInfo, error)

	// ReadDir lists entries in a directory identified by content offset
	// or entry range end. The offset is resolved to content offset
	// internally via the cache.
	ReadDir(offset uint64) ([]pxar.FileInfo, error)

	// GetAttr returns attributes for an entry identified by its
	// file offset (pxar.Entry.FileOffset).
	GetAttr(entryStart uint64) (*pxar.FileInfo, error)

	// Read reads raw file content from a content range.
	// contentStart is the ContentOffset, offset is the byte offset
	// within the content, size is the number of bytes to read.
	Read(contentStart, contentEnd, offset uint64, size uint) ([]byte, error)

	// ReadContentReader returns a streaming reader for an entire file,
	// looked up by content start offset. The caller must close the reader.
	ReadContentReader(contentStart, contentEnd uint64) (io.ReadCloser, error)

	// ReadLink returns the target of a symlink identified by entry offset.
	ReadLink(entryStart uint64) ([]byte, error)

	// ListXAttrs returns extended attributes for an entry identified
	// by entry offset. Re-reads full metadata if the cached entry was
	// decoded with minimal mode.
	ListXAttrs(entryStart uint64) (map[string][]byte, error)

	// Close releases all resources.
	Close() error
}

// OffsetStats holds read progress statistics.
type OffsetStats struct {
	FilesAccessed   int64
	FoldersAccessed int64
	TotalBytes      int64
}

// StatsProvider is an optional interface that offset filesystems
// can implement to expose read progress statistics.
type StatsProvider interface {
	Stats() OffsetStats
}

// --- LocalOffsetFS ---

// LocalOffsetFS implements OffsetFileSystem backed by a transfer.ArchiveReader.
// It maintains offset-based caches for entry lookup, content lookup, and
// directory range resolution — matching the PBS wire protocol patterns.
type LocalOffsetFS struct {
	reader transfer.ArchiveReader

	// metaMu serializes metadata stream access (Seek + Read is not thread-safe).
	metaMu sync.Mutex

	// Offset caches
	cacheMu       sync.RWMutex
	entryCache    map[uint64]*pxar.Entry // FileOffset -> Entry
	contentCache  map[uint64]*pxar.Entry // ContentOffset -> Entry
	rangeToOffset map[uint64]uint64      // EntryRangeEnd -> ContentOffset

	// Stats (no lock needed for atomic field updates)
	files   int64
	folders int64
	bytes   int64
}

// NewLocalOffsetFS creates an offset-based filesystem backed by an ArchiveReader.
func NewLocalOffsetFS(reader transfer.ArchiveReader) *LocalOffsetFS {
	return &LocalOffsetFS{
		reader:        reader,
		entryCache:    make(map[uint64]*pxar.Entry, 256),
		contentCache:  make(map[uint64]*pxar.Entry, 64),
		rangeToOffset: make(map[uint64]uint64, 64),
	}
}

// Stats returns current read progress statistics.
func (fs *LocalOffsetFS) Stats() OffsetStats {
	return OffsetStats{
		FilesAccessed:   fs.files,
		FoldersAccessed: fs.folders,
		TotalBytes:      fs.bytes,
	}
}

// Root returns the root directory entry.
func (fs *LocalOffsetFS) Root() (*pxar.FileInfo, error) {
	fs.metaMu.Lock()
	entry, err := fs.reader.ReadRoot()
	fs.metaMu.Unlock()
	if err != nil {
		return nil, err
	}
	fs.cacheEntry(entry)
	return pxar.EntryToFileInfo(entry), nil
}

// Lookup finds an entry by archive-internal path.
func (fs *LocalOffsetFS) Lookup(path string) (*pxar.FileInfo, error) {
	fs.metaMu.Lock()
	entry, err := fs.reader.Lookup(path)
	fs.metaMu.Unlock()
	if err != nil {
		return nil, err
	}
	fs.cacheEntry(entry)
	fs.incCount(entry)
	return pxar.EntryToFileInfo(entry), nil
}

// ReadDir lists entries in a directory identified by offset.
// The offset may be a ContentOffset or a legacy EntryRangeEnd —
// it is resolved to ContentOffset via the internal cache.
func (fs *LocalOffsetFS) ReadDir(offset uint64) ([]pxar.FileInfo, error) {
	offset = fs.resolveContentOffset(offset)

	fs.metaMu.Lock()
	entries := make([]pxar.FileInfo, 0, 64)
	err := fs.reader.ListDirectory(int64(offset), accessor.ListOption{Minimal: true}, func(e *pxar.Entry) error {
		info := pxar.EntryToFileInfo(e)
		entries = append(entries, *info)
		pxar.ReleaseFileInfo(info)
		fs.cacheEntry(e)
		fs.incCount(e)
		return nil
	})
	fs.metaMu.Unlock()
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// GetAttr returns attributes for an entry by file offset.
func (fs *LocalOffsetFS) GetAttr(entryStart uint64) (*pxar.FileInfo, error) {
	if e := fs.getCachedEntry(entryStart); e != nil {
		fs.incCount(e)
		return pxar.EntryToFileInfo(e), nil
	}

	fs.metaMu.Lock()
	entry, err := fs.reader.ReadEntryAt(int64(entryStart))
	fs.metaMu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("entry at offset %d: %w", entryStart, err)
	}
	fs.cacheEntry(entry)
	fs.incCount(entry)
	return pxar.EntryToFileInfo(entry), nil
}

// Read reads raw file content from a content range.
func (fs *LocalOffsetFS) Read(contentStart, contentEnd, offset uint64, size uint) ([]byte, error) {
	entry := fs.getCachedContentEntry(contentStart)
	if entry == nil {
		return nil, fmt.Errorf("entry with content offset %d not found in cache", contentStart)
	}

	rc, err := fs.reader.ReadFileContentReader(entry)
	if err != nil {
		return nil, fmt.Errorf("open content reader: %w", err)
	}
	defer rc.Close()

	if offset > 0 {
		if _, err := io.CopyN(io.Discard, rc, int64(offset)); err != nil {
			return nil, fmt.Errorf("seek to offset: %w", err)
		}
	}

	buf := make([]byte, size)
	n, err := io.ReadFull(rc, buf)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return nil, fmt.Errorf("read content: %w", err)
	}

	fs.bytes += int64(n)
	return buf[:n], nil
}

// ReadContentReader returns a streaming reader for file content by offset.
func (fs *LocalOffsetFS) ReadContentReader(contentStart, contentEnd uint64) (io.ReadCloser, error) {
	entry := fs.getCachedContentEntry(contentStart)
	if entry == nil {
		return nil, fmt.Errorf("entry with content offset %d not found in cache", contentStart)
	}
	fs.bytes += int64(entry.FileSize)
	return fs.reader.ReadFileContentReader(entry)
}

// ReadLink returns the symlink target by entry offset.
func (fs *LocalOffsetFS) ReadLink(entryStart uint64) ([]byte, error) {
	if e := fs.getCachedEntry(entryStart); e != nil {
		if e.LinkTarget != "" {
			return []byte(e.LinkTarget), nil
		}
	}

	fs.metaMu.Lock()
	entry, err := fs.reader.ReadEntryAt(int64(entryStart))
	fs.metaMu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("symlink entry at offset %d: %w", entryStart, err)
	}
	fs.cacheEntry(entry)
	if entry.LinkTarget != "" {
		return []byte(entry.LinkTarget), nil
	}
	return nil, fmt.Errorf("symlink entry at offset %d has no target", entryStart)
}

// ListXAttrs returns extended attributes for an entry by offset.
func (fs *LocalOffsetFS) ListXAttrs(entryStart uint64) (map[string][]byte, error) {
	e := fs.getCachedEntry(entryStart)
	if e == nil {
		return nil, fmt.Errorf("entry at offset %d not found", entryStart)
	}

	// If the cached entry has no xattrs but might have them
	// (loaded with Minimal), re-read the full entry.
	if len(e.Metadata.XAttrs) == 0 && e.Metadata.FCaps == nil {
		fs.metaMu.Lock()
		full, err := fs.reader.ReadEntryAt(int64(entryStart))
		fs.metaMu.Unlock()
		if err != nil {
			return nil, nil
		}
		e = full
		fs.cacheEntry(e)
	}

	return pxar.EntryToXAttrs(e), nil
}

// Close releases all resources.
func (fs *LocalOffsetFS) Close() error {
	if fs.reader != nil {
		return fs.reader.Close()
	}
	return nil
}

// Reader returns the underlying ArchiveReader for advanced operations.
func (fs *LocalOffsetFS) Reader() transfer.ArchiveReader {
	return fs.reader
}

// --- internal helpers ---

func (fs *LocalOffsetFS) cacheEntry(e *pxar.Entry) {
	fs.cacheMu.Lock()
	fs.entryCache[e.FileOffset] = e
	if e.IsRegularFile() && e.ContentOffset > 0 {
		fs.contentCache[e.ContentOffset] = e
	}
	if e.IsDir() && e.ContentOffset > 0 {
		fs.rangeToOffset[e.FileOffset+e.FileSize] = e.ContentOffset
	}
	fs.cacheMu.Unlock()
}

func (fs *LocalOffsetFS) getCachedEntry(offset uint64) *pxar.Entry {
	fs.cacheMu.RLock()
	e := fs.entryCache[offset]
	fs.cacheMu.RUnlock()
	return e
}

func (fs *LocalOffsetFS) getCachedContentEntry(contentOffset uint64) *pxar.Entry {
	fs.cacheMu.RLock()
	e := fs.contentCache[contentOffset]
	fs.cacheMu.RUnlock()
	return e
}

func (fs *LocalOffsetFS) resolveContentOffset(offset uint64) uint64 {
	fs.cacheMu.RLock()
	co, ok := fs.rangeToOffset[offset]
	fs.cacheMu.RUnlock()
	if ok {
		return co
	}
	return offset
}

func (fs *LocalOffsetFS) incCount(e *pxar.Entry) {
	if e.IsDir() {
		fs.folders++
	} else {
		fs.files++
	}
}

// --- Offset RPC protocol ---

// OffsetRPCTransport is the transport interface for offset-based remote FS.
// Implement this to bridge to any wire format or transport (arpc, gRPC, etc.).
type OffsetRPCTransport interface {
	// Call invokes a remote method with a typed request/response.
	Call(ctx context.Context, method string, req, resp any) error

	// CallBinary invokes a remote method that returns raw bytes.
	// Copies into dst and returns the number of bytes written.
	CallBinary(ctx context.Context, method string, req any, dst []byte) (int, error)

	// CallStream invokes a remote method that returns a stream.
	CallStream(ctx context.Context, method string, req any) (io.ReadCloser, error)

	// Close releases transport resources.
	Close() error
}

// Offset method constants for RPC routing.
const (
	OffsetMethodRoot       = "pxar.Root"
	OffsetMethodLookup     = "pxar.Lookup"
	OffsetMethodReadDir    = "pxar.ReadDir"
	OffsetMethodGetAttr    = "pxar.GetAttr"
	OffsetMethodRead       = "pxar.Read"
	OffsetMethodReadStream = "pxar.ReadStream"
	OffsetMethodReadLink   = "pxar.ReadLink"
	OffsetMethodListXAttrs = "pxar.ListXAttrs"
	OffsetMethodError      = "pxar.Error"
	OffsetMethodDone       = "pxar.Done"
)

// --- OffsetRemoteFS (client) ---

// OffsetRemoteFS implements OffsetFileSystem over an OffsetRPCTransport.
type OffsetRemoteFS struct {
	transport OffsetRPCTransport
}

// NewOffsetRemoteFS creates an OffsetFileSystem backed by an OffsetRPCTransport.
func NewOffsetRemoteFS(transport OffsetRPCTransport) *OffsetRemoteFS {
	return &OffsetRemoteFS{transport: transport}
}

func (fs *OffsetRemoteFS) Root() (*pxar.FileInfo, error) {
	var fi pxar.FileInfo
	if err := fs.transport.Call(context.Background(), OffsetMethodRoot, nil, &fi); err != nil {
		return nil, err
	}
	return &fi, nil
}

func (fs *OffsetRemoteFS) Lookup(path string) (*pxar.FileInfo, error) {
	var fi pxar.FileInfo
	if err := fs.transport.Call(context.Background(), OffsetMethodLookup, map[string]string{"path": path}, &fi); err != nil {
		return nil, err
	}
	return &fi, nil
}

func (fs *OffsetRemoteFS) ReadDir(offset uint64) ([]pxar.FileInfo, error) {
	var entries []pxar.FileInfo
	if err := fs.transport.Call(context.Background(), OffsetMethodReadDir, map[string]uint64{"offset": offset}, &entries); err != nil {
		return nil, err
	}
	return entries, nil
}

func (fs *OffsetRemoteFS) GetAttr(entryStart uint64) (*pxar.FileInfo, error) {
	var fi pxar.FileInfo
	if err := fs.transport.Call(context.Background(), OffsetMethodGetAttr, map[string]uint64{"entry_start": entryStart}, &fi); err != nil {
		return nil, err
	}
	return &fi, nil
}

func (fs *OffsetRemoteFS) Read(contentStart, contentEnd, offset uint64, size uint) ([]byte, error) {
	req := map[string]uint64{
		"content_start": contentStart,
		"content_end":   contentEnd,
		"offset":        offset,
		"size":          uint64(size),
	}
	buf := make([]byte, size)
	n, err := fs.transport.CallBinary(context.Background(), OffsetMethodRead, req, buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func (fs *OffsetRemoteFS) ReadContentReader(contentStart, contentEnd uint64) (io.ReadCloser, error) {
	req := map[string]uint64{
		"content_start": contentStart,
		"content_end":   contentEnd,
	}
	return fs.transport.CallStream(context.Background(), OffsetMethodReadStream, req)
}

func (fs *OffsetRemoteFS) ReadLink(entryStart uint64) ([]byte, error) {
	var target []byte
	if err := fs.transport.Call(context.Background(), OffsetMethodReadLink, map[string]uint64{"entry_start": entryStart}, &target); err != nil {
		return nil, err
	}
	return target, nil
}

func (fs *OffsetRemoteFS) ListXAttrs(entryStart uint64) (map[string][]byte, error) {
	var xattrs map[string][]byte
	if err := fs.transport.Call(context.Background(), OffsetMethodListXAttrs, map[string]uint64{"entry_start": entryStart}, &xattrs); err != nil {
		return nil, err
	}
	return xattrs, nil
}

func (fs *OffsetRemoteFS) Close() error {
	_ = fs.transport.Call(context.Background(), OffsetMethodDone, nil, nil)
	return fs.transport.Close()
}

// --- OffsetRemoteServer (server-side handler) ---

// OffsetRemoteServer serves OffsetFileSystem operations as typed handler methods.
// Register each handler with your RPC framework using the OffsetMethod* constants.
//
// Example with arpc:
//
//	srv := vfs.NewOffsetRemoteServer(offsetFS)
//	router.Handle("pxar.Root", func(req *arpc.Request) (arpc.Response, error) {
//	    fi, err := srv.HandleRoot()
//	    data, _ := cbor.Marshal(fi)
//	    return arpc.Response{Status: 200, Data: data}, err
//	})
type OffsetRemoteServer struct {
	fs OffsetFileSystem
}

// NewOffsetRemoteServer creates a server that dispatches to the given OffsetFileSystem.
func NewOffsetRemoteServer(fs OffsetFileSystem) *OffsetRemoteServer {
	return &OffsetRemoteServer{fs: fs}
}

// HandleRoot returns the root entry.
func (s *OffsetRemoteServer) HandleRoot() (*pxar.FileInfo, error) {
	return s.fs.Root()
}

// HandleLookup finds an entry by path.
func (s *OffsetRemoteServer) HandleLookup(path string) (*pxar.FileInfo, error) {
	return s.fs.Lookup(path)
}

// HandleReadDir lists directory entries by offset.
func (s *OffsetRemoteServer) HandleReadDir(offset uint64) ([]pxar.FileInfo, error) {
	return s.fs.ReadDir(offset)
}

// HandleGetAttr returns entry attributes by file offset.
func (s *OffsetRemoteServer) HandleGetAttr(entryStart uint64) (*pxar.FileInfo, error) {
	return s.fs.GetAttr(entryStart)
}

// HandleRead reads raw file content.
func (s *OffsetRemoteServer) HandleRead(contentStart, contentEnd, offset uint64, size uint) ([]byte, error) {
	return s.fs.Read(contentStart, contentEnd, offset, size)
}

// HandleReadStream returns a streaming reader for file content.
func (s *OffsetRemoteServer) HandleReadStream(contentStart, contentEnd uint64) (io.ReadCloser, error) {
	return s.fs.ReadContentReader(contentStart, contentEnd)
}

// HandleReadLink returns symlink target.
func (s *OffsetRemoteServer) HandleReadLink(entryStart uint64) ([]byte, error) {
	return s.fs.ReadLink(entryStart)
}

// HandleListXAttrs returns extended attributes.
func (s *OffsetRemoteServer) HandleListXAttrs(entryStart uint64) (map[string][]byte, error) {
	return s.fs.ListXAttrs(entryStart)
}

// HandleError receives a client-reported error.
func (s *OffsetRemoteServer) HandleError(errMsg string) error {
	return nil
}

// HandleDone signals session completion.
func (s *OffsetRemoteServer) HandleDone() error {
	return nil
}
