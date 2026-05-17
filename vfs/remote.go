package vfs

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	pxar "github.com/pbs-plus/pxar"
)

// RPCTransport abstracts the client-side RPC mechanism.
// Implement this to bridge to any wire format or transport (arpc, gRPC,
// HTTP, raw TCP, etc.). The library never prescribes serialization.
//
// Example adapter for arpc:
//
//	type arpcTransport struct{ pipe *arpc.StreamPipe }
//	func (t *arpcTransport) Call(ctx context.Context, method string, req, resp any) error {
//	    return t.pipe.Call(ctx, method, req, resp)
//	}
//	func (t *arpcTransport) CallBinary(ctx context.Context, method string, req any, dst []byte) (int, error) {
//	    return t.pipe.CallBinary(ctx, method, req, dst)
//	}
//	func (t *arpcTransport) Close() error { t.pipe.Close(); return nil }
type RPCTransport interface {
	// Call invokes a remote method with a typed request/response.
	Call(ctx context.Context, method string, req any, resp any) error

	// CallBinary invokes a remote method that returns raw bytes.
	// Copies into dst and returns the number of bytes written.
	CallBinary(ctx context.Context, method string, req any, dst []byte) (int, error)

	// Close releases transport resources.
	Close() error
}

// --- Method names ---
// Consumers register these with their RPC router.
const (
	MethodStat       = "pxar.Stat"
	MethodReadDir    = "pxar.ReadDir"
	MethodReadFile   = "pxar.ReadFile"
	MethodRead       = "pxar.Read"
	MethodReadlink   = "pxar.Readlink"
	MethodListXAttrs = "pxar.ListXAttrs"
	MethodError      = "pxar.Error"
	MethodDone       = "pxar.Done"
)

// --- Protocol types ---
// These structs define the RPC contract between client and server.
// Serialize them however you like (JSON, CBOR, protobuf, msgpack, etc.).

type StatRequest struct {
	Path string
}

type StatResponse struct {
	Name    string
	Kind    pxar.EntryKind
	Size    uint64
	Mode    uint32
	UID     uint32
	GID     uint32
	MtimeS  int64
	MtimeNS uint32
	IsDir   bool
	IsLink  bool
	IsDev   bool
	IsFifo  bool
	IsSock  bool
}

type ReadDirRequest struct {
	Path string
}

type ReadDirResponseEntry struct {
	Name    string
	Kind    pxar.EntryKind
	Size    uint64
	Mode    uint32
	UID     uint32
	GID     uint32
	MtimeS  int64
	MtimeNS uint32
	IsDir   bool
	IsLink  bool
	IsDev   bool
	IsFifo  bool
	IsSock  bool
}

type ReadDirResponse struct {
	Entries []ReadDirResponseEntry
}

type ReadFileRequest struct {
	Path string
}

type ReadRequest struct {
	Path   string
	Offset uint64
	Size   uint
}

type ReadlinkRequest struct {
	Path string
}

type ReadlinkResponse struct {
	Target string
}

type ListXAttrsRequest struct {
	Path string
}

type ErrorRequest struct {
	Error string
}

// --- RemoteFileSystem (client) ---

// RemoteFileSystem implements RemoteFS over an RPCTransport.
// It provides the same path-based API as LocalFileSystem, but all
// operations are forwarded to a RemoteServer instance.
//
// Thread safety: all methods are safe for concurrent use.
type RemoteFileSystem struct {
	transport RPCTransport

	mu    sync.Mutex
	cache map[string]*entryAndInfo
}

// NewRemoteFS creates a RemoteFS backed by an RPCTransport.
func NewRemoteFS(transport RPCTransport) *RemoteFileSystem {
	return &RemoteFileSystem{
		transport: transport,
		cache:     make(map[string]*entryAndInfo, 256),
	}
}

// Stat returns file info for the entry at path.
func (fs *RemoteFileSystem) Stat(path string) (*pxar.FileInfo, error) {
	p := cleanPath(path)
	if cached := fs.getCached(p); cached != nil {
		return cached.info, nil
	}

	resp, err := fs.stat(p)
	if err != nil {
		return nil, err
	}

	fi := respToFileInfo(resp)
	fs.put(p, &pxar.Entry{Path: p, Kind: resp.Kind, FileSize: resp.Size}, fi)
	return fi, nil
}

// ReadDir lists entries in a directory.
func (fs *RemoteFileSystem) ReadDir(path string) ([]DirEntry, error) {
	p := cleanPath(path)

	req := &ReadDirRequest{Path: p}
	var resp ReadDirResponse
	if err := fs.transport.Call(context.Background(), MethodReadDir, req, &resp); err != nil {
		return nil, fmt.Errorf("pxar: readdir %q: %w", p, err)
	}

	entries := make([]DirEntry, len(resp.Entries))
	for i, re := range resp.Entries {
		fi := dirEntryRespToFileInfo(&re)
		childPath := joinPath(p, re.Name)
		fs.putInfo(childPath, re.Kind, re.Size, fi)
		entries[i] = DirEntry{
			Name: re.Name,
			Type: re.Kind,
			Info: fi,
		}
	}
	return entries, nil
}

// Open returns a FileHandle for reading file content.
func (fs *RemoteFileSystem) Open(path string) (FileHandle, error) {
	p := cleanPath(path)

	resp, err := fs.stat(p)
	if err != nil {
		return nil, err
	}
	if resp.IsDir || resp.IsLink {
		return nil, fmt.Errorf("pxar: %q is not a regular file", p)
	}

	return &remoteFileHandle{fs: fs, path: p, size: int64(resp.Size)}, nil
}

// ReadFile reads the entire content of a file.
func (fs *RemoteFileSystem) ReadFile(path string) ([]byte, error) {
	p := cleanPath(path)

	// Try cache first to avoid extra RPC
	if cached := fs.getCached(p); cached != nil {
		if cached.info.IsDir() || cached.info.IsSymlink() {
			return nil, fmt.Errorf("pxar: %q is not a regular file", p)
		}
		if cached.info.Size() == 0 {
			return nil, nil
		}
		req := &ReadRequest{Path: p, Size: uint(cached.info.Size())}
		dst := make([]byte, cached.info.Size())
		n, err := fs.transport.CallBinary(context.Background(), MethodRead, req, dst)
		if err != nil {
			return nil, fmt.Errorf("pxar: readfile %q: %w", p, err)
		}
		return dst[:n], nil
	}

	resp, err := fs.stat(p)
	if err != nil {
		return nil, err
	}
	if resp.IsDir || resp.IsLink {
		return nil, fmt.Errorf("pxar: %q is not a regular file", p)
	}
	if resp.Size == 0 {
		return nil, nil
	}

	req := &ReadRequest{Path: p, Size: uint(resp.Size)}
	dst := make([]byte, resp.Size)
	n, err := fs.transport.CallBinary(context.Background(), MethodRead, req, dst)
	if err != nil {
		return nil, fmt.Errorf("pxar: readfile %q: %w", p, err)
	}
	return dst[:n], nil
}

// Readlink returns the target of a symbolic link.
func (fs *RemoteFileSystem) Readlink(path string) (string, error) {
	p := cleanPath(path)

	req := &ReadlinkRequest{Path: p}
	var resp ReadlinkResponse
	if err := fs.transport.Call(context.Background(), MethodReadlink, req, &resp); err != nil {
		return "", fmt.Errorf("pxar: readlink %q: %w", p, err)
	}
	return resp.Target, nil
}

// ListXAttrs returns extended attributes for the entry at path.
func (fs *RemoteFileSystem) ListXAttrs(path string) (map[string][]byte, error) {
	p := cleanPath(path)

	req := &ListXAttrsRequest{Path: p}
	var resp map[string][]byte
	if err := fs.transport.Call(context.Background(), MethodListXAttrs, req, &resp); err != nil {
		return nil, fmt.Errorf("pxar: listxattrs %q: %w", p, err)
	}
	return resp, nil
}

// Close releases resources.
func (fs *RemoteFileSystem) Close() error {
	if fs.transport != nil {
		return fs.transport.Close()
	}
	return nil
}

// SendError reports an error to the remote side.
func (fs *RemoteFileSystem) SendError(err error) error {
	req := &ErrorRequest{Error: err.Error()}
	return fs.transport.Call(context.Background(), MethodError, req, nil)
}

// Done signals that the client has finished using the filesystem.
func (fs *RemoteFileSystem) Done() error {
	return fs.transport.Call(context.Background(), MethodDone, nil, nil)
}

func (fs *RemoteFileSystem) stat(path string) (*StatResponse, error) {
	req := &StatRequest{Path: path}
	var resp StatResponse
	if err := fs.transport.Call(context.Background(), MethodStat, req, &resp); err != nil {
		return nil, fmt.Errorf("pxar: stat %q: %w", path, err)
	}
	return &resp, nil
}

func (fs *RemoteFileSystem) getCached(path string) *entryAndInfo {
	fs.mu.Lock()
	cached := fs.cache[path]
	fs.mu.Unlock()
	return cached
}

func (fs *RemoteFileSystem) put(path string, e *pxar.Entry, fi *pxar.FileInfo) *entryAndInfo {
	combined := newEntryAndInfo(e, fi)
	fs.mu.Lock()
	if old, ok := fs.cache[path]; ok {
		releaseEntryAndInfo(old)
	}
	fs.cache[path] = combined
	fs.mu.Unlock()
	return combined
}

func (fs *RemoteFileSystem) putInfo(path string, kind pxar.EntryKind, size uint64, fi *pxar.FileInfo) {
	combined := newEntryAndInfo(nil, fi)
	combined._kind = kind
	combined._size = size
	combined._path = path
	fs.mu.Lock()
	if old, ok := fs.cache[path]; ok {
		releaseEntryAndInfo(old)
	}
	fs.cache[path] = combined
	fs.mu.Unlock()
}

// --- remoteFileHandle ---

type remoteFileHandle struct {
	fs     *RemoteFileSystem
	path   string
	size   int64
	offset int64
}

func (h *remoteFileHandle) Read(p []byte) (int, error) {
	if h.offset >= h.size {
		return 0, io.EOF
	}

	remaining := h.size - h.offset
	toRead := uint64(len(p))
	if uint64(toRead) > uint64(remaining) {
		toRead = uint64(remaining)
	}
	if toRead == 0 {
		return 0, io.EOF
	}

	req := &ReadRequest{
		Path:   h.path,
		Offset: uint64(h.offset),
		Size:   uint(toRead),
	}

	n, err := h.fs.transport.CallBinary(context.Background(), MethodRead, req, p[:toRead])
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, io.EOF
	}

	h.offset += int64(n)
	return n, nil
}

func (h *remoteFileHandle) ReadAt(p []byte, off int64) (int, error) {
	if off >= h.size {
		return 0, io.EOF
	}

	remaining := h.size - off
	toRead := uint64(len(p))
	if uint64(toRead) > uint64(remaining) {
		toRead = uint64(remaining)
	}
	if toRead == 0 {
		return 0, io.EOF
	}

	req := &ReadRequest{
		Path:   h.path,
		Offset: uint64(off),
		Size:   uint(toRead),
	}

	return h.fs.transport.CallBinary(context.Background(), MethodRead, req, p[:toRead])
}

func (h *remoteFileHandle) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		h.offset = offset
	case io.SeekCurrent:
		h.offset += offset
	case io.SeekEnd:
		h.offset = h.size + offset
	default:
		return h.offset, fmt.Errorf("pxar: invalid whence %d", whence)
	}
	return h.offset, nil
}

func (h *remoteFileHandle) Close() error { return nil }
func (h *remoteFileHandle) Size() int64  { return h.size }

// --- RemoteServer (server-side handler) ---

// RemoteServer serves FileSystem operations as typed handler methods.
// Register each handler with your RPC framework using the Method* constants.
//
// Example with a hypothetical router:
//
//	srv := vfs.NewRemoteServer(fs)
//	router.Handle(vfs.MethodStat, func(req Request) (Response, error) {
//	    var r vfs.StatRequest
//	    decode(req.Body, &r)
//	    resp, err := srv.HandleStat(&r)
//	    return encode(resp), err
//	})
type RemoteServer struct {
	fs FileSystem
}

// NewRemoteServer creates a server that dispatches to the given FileSystem.
func NewRemoteServer(fs FileSystem) *RemoteServer {
	return &RemoteServer{fs: fs}
}

// HandleStat returns file info for a path.
func (s *RemoteServer) HandleStat(req *StatRequest) (*StatResponse, error) {
	fi, err := s.fs.Stat(req.Path)
	if err != nil {
		return nil, err
	}
	resp := fileInfoToStatResp(fi)
	return &resp, nil
}

// HandleReadDir lists entries in a directory.
func (s *RemoteServer) HandleReadDir(req *ReadDirRequest) (*ReadDirResponse, error) {
	entries, err := s.fs.ReadDir(req.Path)
	if err != nil {
		return nil, err
	}

	resp := &ReadDirResponse{Entries: make([]ReadDirResponseEntry, len(entries))}
	for i, e := range entries {
		if e.Info != nil {
			resp.Entries[i] = dirEntryToResp(e.Name, e.Info)
			resp.Entries[i].Kind = e.Type
		} else {
			resp.Entries[i] = ReadDirResponseEntry{Name: e.Name, Kind: e.Type}
		}
	}
	return resp, nil
}

// HandleReadFile returns the entire content of a file.
func (s *RemoteServer) HandleReadFile(req *ReadFileRequest) ([]byte, error) {
	return s.fs.ReadFile(req.Path)
}

// HandleRead returns a byte range from a file.
// The returned byte slice is the response body for CallBinary.
func (s *RemoteServer) HandleRead(req *ReadRequest) ([]byte, error) {
	fh, err := s.fs.Open(req.Path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = fh.Close() }()

	if req.Offset > 0 {
		if _, err := fh.Seek(int64(req.Offset), io.SeekStart); err != nil {
			return nil, err
		}
	}

	size := req.Size
	if size == 0 {
		size = uint(fh.Size())
	}

	bufp := readBufPool.Get().(*[]byte)
	if cap(*bufp) < int(size) {
		readBufPool.Put(bufp) // return small buffer
		b := make([]byte, size)
		bufp = &b
	} else {
		*bufp = (*bufp)[:size]
	}
	buf := *bufp

	n, err := io.ReadFull(fh, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		*bufp = buf[:cap(buf)]
		readBufPool.Put(bufp)
		return nil, err
	}

	result := make([]byte, n)
	copy(result, buf[:n])
	*bufp = buf[:cap(buf)]
	readBufPool.Put(bufp)
	return result, nil
}

// HandleReadlink returns the symlink target.
func (s *RemoteServer) HandleReadlink(req *ReadlinkRequest) (*ReadlinkResponse, error) {
	target, err := s.fs.Readlink(req.Path)
	if err != nil {
		return nil, err
	}
	return &ReadlinkResponse{Target: target}, nil
}

// HandleListXAttrs returns extended attributes for a path.
func (s *RemoteServer) HandleListXAttrs(req *ListXAttrsRequest) (map[string][]byte, error) {
	return s.fs.ListXAttrs(req.Path)
}

// HandleError receives a client-reported error. Override or wrap
// RemoteServer to add logging or error channel behavior.
func (s *RemoteServer) HandleError(req *ErrorRequest) error {
	return nil
}

// HandleDone signals session completion.
func (s *RemoteServer) HandleDone() error {
	return nil
}

var readBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 4096)
		return &b
	},
}

// --- Conversion helpers ---

func respToFileInfo(r *StatResponse) *pxar.FileInfo {
	return pxar.NewFileInfo(
		r.Name,
		int64(r.Size),
		buildMode(r.Mode, r.IsDir, r.IsLink, r.IsDev, r.IsFifo, r.IsSock),
		time.Unix(r.MtimeS, int64(r.MtimeNS)),
		r.UID,
		r.GID,
	)
}

func dirEntryRespToFileInfo(r *ReadDirResponseEntry) *pxar.FileInfo {
	return pxar.NewFileInfo(
		r.Name,
		int64(r.Size),
		buildMode(r.Mode, r.IsDir, r.IsLink, r.IsDev, r.IsFifo, r.IsSock),
		time.Unix(r.MtimeS, int64(r.MtimeNS)),
		r.UID,
		r.GID,
	)
}

func fileInfoToStatResp(fi *pxar.FileInfo) StatResponse {
	mt := fi.ModTime()
	return StatResponse{
		Name:    fi.Name(),
		Kind:    kindFromFileInfo(fi),
		Size:    uint64(fi.Size()),
		Mode:    uint32(fi.Mode() & 0o7777),
		UID:     fi.UID(),
		GID:     fi.GID(),
		MtimeS:  mt.Unix(),
		MtimeNS: uint32(mt.Nanosecond()),
		IsDir:   fi.IsDir(),
		IsLink:  fi.IsSymlink(),
		IsDev:   fi.IsDevice(),
		IsFifo:  fi.IsFifo(),
		IsSock:  fi.IsSocket(),
	}
}

func dirEntryToResp(name string, fi *pxar.FileInfo) ReadDirResponseEntry {
	mt := fi.ModTime()
	return ReadDirResponseEntry{
		Name:    name,
		Kind:    kindFromFileInfo(fi),
		Size:    uint64(fi.Size()),
		Mode:    uint32(fi.Mode() & 0o7777),
		UID:     fi.UID(),
		GID:     fi.GID(),
		MtimeS:  mt.Unix(),
		MtimeNS: uint32(mt.Nanosecond()),
		IsDir:   fi.IsDir(),
		IsLink:  fi.IsSymlink(),
		IsDev:   fi.IsDevice(),
		IsFifo:  fi.IsFifo(),
		IsSock:  fi.IsSocket(),
	}
}

func kindFromFileInfo(fi *pxar.FileInfo) pxar.EntryKind {
	switch {
	case fi.IsDir():
		return pxar.KindDirectory
	case fi.IsSymlink():
		return pxar.KindSymlink
	case fi.IsDevice():
		return pxar.KindDevice
	case fi.IsFifo():
		return pxar.KindFIFO
	case fi.IsSocket():
		return pxar.KindSocket
	default:
		return pxar.KindFile
	}
}

func buildMode(perm uint32, isDir, isLink, isDev, isFifo, isSock bool) os.FileMode {
	mode := os.FileMode(perm & 0o7777)
	if isDir {
		mode |= os.ModeDir
	}
	if isLink {
		mode |= os.ModeSymlink
	}
	if isDev {
		mode |= os.ModeDevice
	}
	if isFifo {
		mode |= os.ModeNamedPipe
	}
	if isSock {
		mode |= os.ModeSocket
	}
	return mode
}

// Compile-time checks.
var (
	_ RemoteFS = (*RemoteFileSystem)(nil)
)
