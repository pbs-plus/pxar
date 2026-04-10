package backupproxy

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
	pxar "github.com/pbs-plus/pxar"
)

// Server orchestrates pull backups: walks the client filesystem, encodes a pxar
// archive, chunks it with buzhash, and uploads to a RemoteStore.
type Server struct {
	client ClientProvider
	store  RemoteStore
}

// NewServer creates a backup server with the given client provider and store.
func NewServer(client ClientProvider, store RemoteStore) *Server {
	return &Server{client: client, store: store}
}

// BackupResult describes the outcome of a backup operation.
type BackupResult struct {
	Manifest   *datastore.Manifest
	TotalBytes int64
	FileCount  int
	DirCount   int
	Duration   time.Duration
}

// RunBackup executes a full pull backup of the given root path from the client.
func (s *Server) RunBackup(ctx context.Context, root string, config BackupConfig) (*BackupResult, error) {
	start := time.Now()

	sess, err := s.store.StartSession(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("start session: %w", err)
	}

	rootStat, err := s.client.Stat(ctx, root)
	if err != nil {
		return nil, fmt.Errorf("stat root: %w", err)
	}

	// Streaming pipeline: encoder → pipe → upload
	pipeReader, pipeWriter := io.Pipe()

	uploadErrCh := make(chan uploadResult, 1)
	go func() {
		result, err := sess.UploadArchive(ctx, "root.pxar.didx", pipeReader)
		uploadErrCh <- uploadResult{result: result, err: err}
	}()

	rootMeta := &pxar.Metadata{Stat: rootStat}
	enc := encoder.NewEncoder(pipeWriter, nil, rootMeta, nil)

	result := &BackupResult{}
	if err := s.walkDir(ctx, root, enc, result); err != nil {
		pipeWriter.CloseWithError(err)
		<-uploadErrCh
		return nil, err
	}

	if err := enc.Close(); err != nil {
		pipeWriter.CloseWithError(err)
		<-uploadErrCh
		return nil, fmt.Errorf("close encoder: %w", err)
	}
	pipeWriter.Close()

	upload := <-uploadErrCh
	if upload.err != nil {
		return nil, fmt.Errorf("upload archive: %w", upload.err)
	}
	result.TotalBytes = int64(upload.result.Size)

	manifest, err := sess.Finish(ctx)
	if err != nil {
		return nil, fmt.Errorf("finish session: %w", err)
	}
	result.Manifest = manifest
	result.Duration = time.Since(start)

	return result, nil
}

type uploadResult struct {
	result *UploadResult
	err    error
}

func (s *Server) walkDir(ctx context.Context, dirPath string, enc *encoder.Encoder, result *BackupResult) error {
	entries, err := s.client.ReadDir(ctx, dirPath)
	if err != nil {
		return fmt.Errorf("readdir %q: %w", dirPath, err)
	}

	for _, entry := range entries {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		meta := &pxar.Metadata{Stat: entry.Stat}
		fullPath := dirPath + "/" + entry.Name

		switch {
		case entry.Stat.IsDir():
			result.DirCount++
			if err := enc.CreateDirectory(entry.Name, meta); err != nil {
				return fmt.Errorf("create dir %q: %w", entry.Name, err)
			}
			if err := s.walkDir(ctx, fullPath, enc, result); err != nil {
				return err
			}
			if err := enc.Finish(); err != nil {
				return fmt.Errorf("finish dir %q: %w", entry.Name, err)
			}

		case entry.Stat.IsRegularFile():
			result.FileCount++
			if err := s.encodeFile(ctx, enc, entry.Name, fullPath, meta); err != nil {
				return fmt.Errorf("file %q: %w", entry.Name, err)
			}

		case entry.Stat.IsSymlink():
			target, err := s.client.ReadLink(ctx, fullPath)
			if err != nil {
				return fmt.Errorf("readlink %q: %w", fullPath, err)
			}
			if err := enc.AddSymlink(meta, entry.Name, target); err != nil {
				return fmt.Errorf("symlink %q: %w", entry.Name, err)
			}

		case entry.Stat.IsDevice():
			if err := enc.AddDevice(meta, entry.Name, format.Device{}); err != nil {
				return fmt.Errorf("device %q: %w", entry.Name, err)
			}

		case entry.Stat.IsFIFO():
			if err := enc.AddFIFO(meta, entry.Name); err != nil {
				return fmt.Errorf("fifo %q: %w", entry.Name, err)
			}

		case entry.Stat.IsSocket():
			if err := enc.AddSocket(meta, entry.Name); err != nil {
				return fmt.Errorf("socket %q: %w", entry.Name, err)
			}
		}
	}

	return nil
}

func (s *Server) encodeFile(ctx context.Context, enc *encoder.Encoder, name, fullPath string, meta *pxar.Metadata) error {
	// Read entire file from client. format.Stat doesn't carry file size,
	// so we must read to determine it. For large files, a streaming approach
	// (stat first, then stream) would require a size field in the protocol.
	data, err := s.client.ReadFile(ctx, fullPath, 0, -1)
	if err != nil {
		return err
	}
	_, err = enc.AddFile(meta, name, data)
	return err
}
