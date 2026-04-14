package backupproxy

import (
	"context"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/format"
)

// FileSystemAccessor is the interface the client uses to access the local
// filesystem. Users provide their own implementation. This indirection allows
// testing without a real filesystem and running in constrained environments.
type FileSystemAccessor interface {
	Stat(path string) (format.Stat, error)
	ReadDir(path string) ([]DirEntry, error)
	ReadFile(path string, offset, length int64) ([]byte, error)
	ReadLink(path string) (string, error)

	// Extended metadata methods. Implementations that don't support these
	// should return nil/empty values. The default implementations below
	// return zero values, allowing partial implementations to embed
	// NoExtendedAttrs for convenience.

	// GetXAttrs returns the extended attributes for the given path.
	GetXAttrs(path string) ([]format.XAttr, error)

	// GetACL returns the POSIX ACL entries for the given path.
	GetACL(path string) (pxar.ACL, error)

	// GetFCaps returns the file capabilities for the given path.
	GetFCaps(path string) ([]byte, error)
}

// NoExtendedAttrs provides no-op implementations of the extended metadata
// methods. Embed this in FileSystemAccessor implementations that don't
// support xattrs, ACLs, or file capabilities.
type NoExtendedAttrs struct{}

func (NoExtendedAttrs) GetXAttrs(string) ([]format.XAttr, error) { return nil, nil }
func (NoExtendedAttrs) GetACL(string) (pxar.ACL, error)          { return pxar.ACL{}, nil }
func (NoExtendedAttrs) GetFCaps(string) ([]byte, error)          { return nil, nil }

// ClientProvider is the interface the server uses to access client data.
// Transport implementations bridge this to the actual network.
// Each method call corresponds to one request-response round trip.
type ClientProvider interface {
	Stat(ctx context.Context, path string) (format.Stat, error)
	ReadDir(ctx context.Context, path string) ([]DirEntry, error)
	ReadFile(ctx context.Context, path string, offset, length int64) ([]byte, error)
	ReadLink(ctx context.Context, path string) (string, error)

	// Extended metadata methods for full archive fidelity.
	GetXAttrs(ctx context.Context, path string) ([]format.XAttr, error)
	GetACL(ctx context.Context, path string) (pxar.ACL, error)
	GetFCaps(ctx context.Context, path string) ([]byte, error)
}

// LocalClient implements ClientProvider by delegating to a FileSystemAccessor.
// This is the client-side component: runs on the machine being backed up.
type LocalClient struct {
	fs FileSystemAccessor
}

// NewLocalClient creates a client backed by the given FileSystemAccessor.
func NewLocalClient(fs FileSystemAccessor) *LocalClient {
	return &LocalClient{fs: fs}
}

// Stat returns file metadata for the given path by delegating to the
// underlying FileSystemAccessor.
func (lc *LocalClient) Stat(_ context.Context, path string) (format.Stat, error) {
	return lc.fs.Stat(path)
}

// ReadDir returns directory entries for the given path by delegating to the
// underlying FileSystemAccessor.
func (lc *LocalClient) ReadDir(_ context.Context, path string) ([]DirEntry, error) {
	return lc.fs.ReadDir(path)
}

// ReadFile returns file content at the given path and offset by delegating to
// the underlying FileSystemAccessor.
func (lc *LocalClient) ReadFile(_ context.Context, path string, offset, length int64) ([]byte, error) {
	return lc.fs.ReadFile(path, offset, length)
}

// ReadLink returns the symlink target for the given path by delegating to the
// underlying FileSystemAccessor.
func (lc *LocalClient) ReadLink(_ context.Context, path string) (string, error) {
	return lc.fs.ReadLink(path)
}

// GetXAttrs returns the extended attributes for the given path.
func (lc *LocalClient) GetXAttrs(_ context.Context, path string) ([]format.XAttr, error) {
	return lc.fs.GetXAttrs(path)
}

// GetACL returns the POSIX ACL entries for the given path.
func (lc *LocalClient) GetACL(_ context.Context, path string) (pxar.ACL, error) {
	return lc.fs.GetACL(path)
}

// GetFCaps returns the file capabilities for the given path.
func (lc *LocalClient) GetFCaps(_ context.Context, path string) ([]byte, error) {
	return lc.fs.GetFCaps(path)
}
