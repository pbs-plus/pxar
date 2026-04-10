package backupproxy

import (
	"context"

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
}

// ClientProvider is the interface the server uses to access client data.
// Transport implementations bridge this to the actual network.
// Each method call corresponds to one request-response round trip.
type ClientProvider interface {
	Stat(ctx context.Context, path string) (format.Stat, error)
	ReadDir(ctx context.Context, path string) ([]DirEntry, error)
	ReadFile(ctx context.Context, path string, offset, length int64) ([]byte, error)
	ReadLink(ctx context.Context, path string) (string, error)
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

func (lc *LocalClient) Stat(_ context.Context, path string) (format.Stat, error) {
	return lc.fs.Stat(path)
}

func (lc *LocalClient) ReadDir(_ context.Context, path string) ([]DirEntry, error) {
	return lc.fs.ReadDir(path)
}

func (lc *LocalClient) ReadFile(_ context.Context, path string, offset, length int64) ([]byte, error) {
	return lc.fs.ReadFile(path, offset, length)
}

func (lc *LocalClient) ReadLink(_ context.Context, path string) (string, error) {
	return lc.fs.ReadLink(path)
}
