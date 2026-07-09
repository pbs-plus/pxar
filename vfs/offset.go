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

// FileSystem provides offset-based access to a pxar archive.
// Operations use byte offsets from the archive structure
// (entry offsets, content offsets). This mirrors the wire protocol
// used by PBS agents for restore operations.
//
// Thread safety: all methods are safe for concurrent use.
type FileSystem interface {
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

// Stats holds read progress statistics.
type Stats struct {
	FilesAccessed   int64
	FoldersAccessed int64
	TotalBytes      int64
}

// StatsProvider is an optional interface that offset filesystems
// can implement to expose read progress statistics.
type StatsProvider interface {
	Stats() Stats
}

// --- LocalFS ---

const defaultMaxCacheEntries = 4096

type LocalFS struct {
	reader transfer.ArchiveReader

	metaMu sync.Mutex

	cacheMu       sync.RWMutex
	entryCache    map[uint64]*pxar.Entry
	contentCache  map[uint64]*pxar.Entry
	rangeToOffset map[uint64]uint64
	entryOrder    []uint64
	contentOrder  []uint64
	rangeOrder    []uint64
	maxCache      int

	rootEntry *pxar.Entry

	files   int64
	folders int64
	bytes   int64
}

func NewLocalFS(reader transfer.ArchiveReader) *LocalFS {
	return &LocalFS{
		reader:        reader,
		entryCache:    make(map[uint64]*pxar.Entry, 256),
		contentCache:  make(map[uint64]*pxar.Entry, 64),
		rangeToOffset: make(map[uint64]uint64, 64),
		entryOrder:    make([]uint64, 0, 256),
		contentOrder:  make([]uint64, 0, 64),
		rangeOrder:    make([]uint64, 0, 64),
		maxCache:      defaultMaxCacheEntries,
	}
}

func (fs *LocalFS) SetMaxCache(n int) *LocalFS {
	fs.cacheMu.Lock()
	defer fs.cacheMu.Unlock()
	fs.maxCache = n
	if n > 0 {
		fs.evictToLimit()
	}
	return fs
}

// Stats returns current read progress statistics.
func (fs *LocalFS) Stats() Stats {
	return Stats{
		FilesAccessed:   fs.files,
		FoldersAccessed: fs.folders,
		TotalBytes:      fs.bytes,
	}
}

// Root returns the root directory entry.
func (fs *LocalFS) Root() (*pxar.FileInfo, error) {
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
func (fs *LocalFS) Lookup(path string) (*pxar.FileInfo, error) {
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
func (fs *LocalFS) ReadDir(offset uint64) ([]pxar.FileInfo, error) {
	offset = fs.resolveContentOffset(offset)

	fs.metaMu.Lock()
	entries := make([]pxar.FileInfo, 0, 64)
	err := fs.reader.ListDirectory(int64(offset), accessor.ListOption{Minimal: true}, func(e *pxar.Entry) error {
		info := pxar.EntryToFileInfo(e)
		entries = append(entries, *info)
		pxar.PutFileInfo(info)
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
func (fs *LocalFS) GetAttr(entryStart uint64) (*pxar.FileInfo, error) {
	if entryStart == 0 {
		if e := fs.rootEntry; e != nil {
			fs.incCount(e)
			return pxar.EntryToFileInfo(e), nil
		}
		fs.metaMu.Lock()
		root, err := fs.reader.ReadRoot()
		fs.metaMu.Unlock()
		if err != nil {
			return nil, fmt.Errorf("root entry: %w", err)
		}
		fs.cacheEntry(root)
		fs.incCount(root)
		return pxar.EntryToFileInfo(root), nil
	}

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
func (fs *LocalFS) Read(contentStart, contentEnd, offset uint64, size uint) ([]byte, error) {
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
func (fs *LocalFS) ReadContentReader(contentStart, contentEnd uint64) (io.ReadCloser, error) {
	entry := fs.getCachedContentEntry(contentStart)
	if entry == nil {
		return nil, fmt.Errorf("entry with content offset %d not found in cache", contentStart)
	}
	fs.bytes += int64(entry.FileSize)
	return fs.reader.ReadFileContentReader(entry)
}

// ReadLink returns the symlink target by entry offset.
func (fs *LocalFS) ReadLink(entryStart uint64) ([]byte, error) {
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

func (fs *LocalFS) ListXAttrs(entryStart uint64) (map[string][]byte, error) {
	e := fs.getCachedEntry(entryStart)

	if entryStart == 0 {
		if e != nil && len(e.Metadata.XAttrs) > 0 {
			return pxar.EntryXAttrs(e), nil
		}
		fs.metaMu.Lock()
		root, err := fs.reader.ReadRootFull()
		fs.metaMu.Unlock()
		if err != nil {
			return nil, nil
		}
		fs.cacheEntry(root)
		return pxar.EntryXAttrs(root), nil
	}

	if e == nil {
		fs.metaMu.Lock()
		var err error
		e, err = fs.reader.ReadEntryAt(int64(entryStart))
		fs.metaMu.Unlock()
		if err != nil {
			return nil, fmt.Errorf("entry at offset %d: %w", entryStart, err)
		}
		fs.cacheEntry(e)
	}

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

	return pxar.EntryXAttrs(e), nil
}

func (fs *LocalFS) Close() error {
	fs.cacheMu.Lock()
	fs.entryCache = nil
	fs.contentCache = nil
	fs.rangeToOffset = nil
	fs.entryOrder = nil
	fs.contentOrder = nil
	fs.rangeOrder = nil
	fs.cacheMu.Unlock()
	if fs.reader != nil {
		return fs.reader.Close()
	}
	return nil
}

// Reader returns the underlying ArchiveReader for advanced operations.
func (fs *LocalFS) Reader() transfer.ArchiveReader {
	return fs.reader
}

// --- internal helpers ---

func (fs *LocalFS) cacheEntry(e *pxar.Entry) {
	fs.cacheMu.Lock()
	if e.FileOffset == 0 {
		fs.rootEntry = e
	}
	fs.entryCache[e.FileOffset] = e
	fs.entryOrder = append(fs.entryOrder, e.FileOffset)
	if e.IsRegularFile() && e.ContentOffset > 0 {
		fs.contentCache[e.ContentOffset] = e
		fs.contentOrder = append(fs.contentOrder, e.ContentOffset)
	}
	if e.IsDir() && e.ContentOffset > 0 {
		key := e.FileOffset + e.FileSize
		fs.rangeToOffset[key] = e.ContentOffset
		fs.rangeOrder = append(fs.rangeOrder, key)
	}
	if fs.maxCache > 0 {
		fs.evictToLimit()
	}
	fs.cacheMu.Unlock()
}

func (fs *LocalFS) evictToLimit() {
	for len(fs.entryCache) > fs.maxCache && len(fs.entryOrder) > 0 {
		key := fs.entryOrder[0]
		fs.entryOrder = fs.entryOrder[1:]
		delete(fs.entryCache, key)
	}
}

func (fs *LocalFS) getCachedEntry(offset uint64) *pxar.Entry {
	fs.cacheMu.RLock()
	e := fs.entryCache[offset]
	if e == nil && offset == 0 {
		e = fs.rootEntry
	}
	fs.cacheMu.RUnlock()
	return e
}

func (fs *LocalFS) getCachedContentEntry(contentOffset uint64) *pxar.Entry {
	fs.cacheMu.RLock()
	e := fs.contentCache[contentOffset]
	fs.cacheMu.RUnlock()
	return e
}

func (fs *LocalFS) resolveContentOffset(offset uint64) uint64 {
	fs.cacheMu.RLock()
	co, ok := fs.rangeToOffset[offset]
	fs.cacheMu.RUnlock()
	if ok {
		return co
	}
	return offset
}

func (fs *LocalFS) incCount(e *pxar.Entry) {
	if e.IsDir() {
		fs.folders++
	} else {
		fs.files++
	}
}

// --- Offset RPC protocol ---

// RPCTransport is the transport interface for offset-based remote FS.
// Implement this to bridge to any wire format or transport (arpc, gRPC, etc.).
type RPCTransport interface {
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
	MethodRoot       = "pxar.Root"
	MethodLookup     = "pxar.Lookup"
	MethodReadDir    = "pxar.ReadDir"
	MethodGetAttr    = "pxar.GetAttr"
	MethodRead       = "pxar.Read"
	MethodReadStream = "pxar.ReadStream"
	MethodReadLink   = "pxar.ReadLink"
	MethodListXAttrs = "pxar.ListXAttrs"
	MethodError      = "pxar.Error"
	MethodDone       = "pxar.Done"
)

// --- RemoteFS (client) ---

// RemoteFS implements FileSystem over an RPCTransport.
type RemoteFS struct {
	transport RPCTransport
}

// NewRemoteFS creates an FileSystem backed by an RPCTransport.
func NewRemoteFS(transport RPCTransport) *RemoteFS {
	return &RemoteFS{transport: transport}
}

func (fs *RemoteFS) Root() (*pxar.FileInfo, error) {
	var fi pxar.FileInfo
	if err := fs.transport.Call(context.Background(), MethodRoot, nil, &fi); err != nil {
		return nil, err
	}
	return &fi, nil
}

func (fs *RemoteFS) Lookup(path string) (*pxar.FileInfo, error) {
	var fi pxar.FileInfo
	if err := fs.transport.Call(context.Background(), MethodLookup, map[string]string{"path": path}, &fi); err != nil {
		return nil, err
	}
	return &fi, nil
}

func (fs *RemoteFS) ReadDir(offset uint64) ([]pxar.FileInfo, error) {
	var entries []pxar.FileInfo
	if err := fs.transport.Call(context.Background(), MethodReadDir, map[string]uint64{"offset": offset}, &entries); err != nil {
		return nil, err
	}
	return entries, nil
}

func (fs *RemoteFS) GetAttr(entryStart uint64) (*pxar.FileInfo, error) {
	var fi pxar.FileInfo
	if err := fs.transport.Call(context.Background(), MethodGetAttr, map[string]uint64{"entry_start": entryStart}, &fi); err != nil {
		return nil, err
	}
	return &fi, nil
}

func (fs *RemoteFS) Read(contentStart, contentEnd, offset uint64, size uint) ([]byte, error) {
	req := map[string]uint64{
		"content_start": contentStart,
		"content_end":   contentEnd,
		"offset":        offset,
		"size":          uint64(size),
	}
	buf := make([]byte, size)
	n, err := fs.transport.CallBinary(context.Background(), MethodRead, req, buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func (fs *RemoteFS) ReadContentReader(contentStart, contentEnd uint64) (io.ReadCloser, error) {
	req := map[string]uint64{
		"content_start": contentStart,
		"content_end":   contentEnd,
	}
	return fs.transport.CallStream(context.Background(), MethodReadStream, req)
}

func (fs *RemoteFS) ReadLink(entryStart uint64) ([]byte, error) {
	var target []byte
	if err := fs.transport.Call(context.Background(), MethodReadLink, map[string]uint64{"entry_start": entryStart}, &target); err != nil {
		return nil, err
	}
	return target, nil
}

func (fs *RemoteFS) ListXAttrs(entryStart uint64) (map[string][]byte, error) {
	var xattrs map[string][]byte
	if err := fs.transport.Call(context.Background(), MethodListXAttrs, map[string]uint64{"entry_start": entryStart}, &xattrs); err != nil {
		return nil, err
	}
	return xattrs, nil
}

func (fs *RemoteFS) Close() error {
	_ = fs.transport.Call(context.Background(), MethodDone, nil, nil)
	return fs.transport.Close()
}

// --- RemoteServer (server-side handler) ---

// RemoteServer serves FileSystem operations as typed handler methods.
// Register each handler with your RPC framework using the Method* constants.
//
// Example with arpc:
//
//	srv := vfs.NewRemoteServer(offsetFS)
//	router.Handle("pxar.Root", func(req *arpc.Request) (arpc.Response, error) {
//	    fi, err := srv.HandleRoot()
//	    data, _ := cbor.Marshal(fi)
//	    return arpc.Response{Status: 200, Data: data}, err
//	})
type RemoteServer struct {
	fs FileSystem
}

// NewRemoteServer creates a server that dispatches to the given FileSystem.
func NewRemoteServer(fs FileSystem) *RemoteServer {
	return &RemoteServer{fs: fs}
}

// HandleRoot returns the root entry.
func (s *RemoteServer) HandleRoot() (*pxar.FileInfo, error) {
	return s.fs.Root()
}

// HandleLookup finds an entry by path.
func (s *RemoteServer) HandleLookup(path string) (*pxar.FileInfo, error) {
	return s.fs.Lookup(path)
}

// HandleReadDir lists directory entries by offset.
func (s *RemoteServer) HandleReadDir(offset uint64) ([]pxar.FileInfo, error) {
	return s.fs.ReadDir(offset)
}

// HandleGetAttr returns entry attributes by file offset.
func (s *RemoteServer) HandleGetAttr(entryStart uint64) (*pxar.FileInfo, error) {
	return s.fs.GetAttr(entryStart)
}

// HandleRead reads raw file content.
func (s *RemoteServer) HandleRead(contentStart, contentEnd, offset uint64, size uint) ([]byte, error) {
	return s.fs.Read(contentStart, contentEnd, offset, size)
}

// HandleReadStream returns a streaming reader for file content.
func (s *RemoteServer) HandleReadStream(contentStart, contentEnd uint64) (io.ReadCloser, error) {
	return s.fs.ReadContentReader(contentStart, contentEnd)
}

// HandleReadLink returns symlink target.
func (s *RemoteServer) HandleReadLink(entryStart uint64) ([]byte, error) {
	return s.fs.ReadLink(entryStart)
}

// HandleListXAttrs returns extended attributes.
func (s *RemoteServer) HandleListXAttrs(entryStart uint64) (map[string][]byte, error) {
	return s.fs.ListXAttrs(entryStart)
}

// HandleError receives a client-reported error.
func (s *RemoteServer) HandleError(errMsg string) error {
	return nil
}

// HandleDone signals session completion.
func (s *RemoteServer) HandleDone() error {
	return nil
}
