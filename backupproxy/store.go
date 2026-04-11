package backupproxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
)

// encodeChunkBlob encodes a chunk as a PBS blob, optionally compressing with zstd.
func encodeChunkBlob(chunk []byte, compress bool) ([]byte, error) {
	if compress {
		blob, err := datastore.EncodeCompressedBlob(chunk)
		if err != nil {
			return nil, fmt.Errorf("compress chunk: %w", err)
		}
		return blob.Bytes(), nil
	}
	blob, err := datastore.EncodeBlob(chunk)
	if err != nil {
		return nil, fmt.Errorf("encode chunk: %w", err)
	}
	return blob.Bytes(), nil
}

// addFileInfo appends a file entry to the manifest file list.
func addFileInfo(files *[]datastore.FileInfo, name string, size uint64, digest [32]byte) {
	*files = append(*files, datastore.FileInfo{
		Filename: name,
		Size:     size,
		CSum:     hex.EncodeToString(digest[:]),
	})
}

// RemoteStore abstracts the backup storage backend.
type RemoteStore interface {
	StartSession(ctx context.Context, config BackupConfig) (BackupSession, error)
}

// BackupSession represents an active backup upload session.
type BackupSession interface {
	UploadArchive(ctx context.Context, name string, data io.Reader) (*UploadResult, error)
	UploadBlob(ctx context.Context, name string, data []byte) error
	Finish(ctx context.Context) (*datastore.Manifest, error)
}

// LocalStore implements RemoteStore using a local filesystem directory.
// It uses datastore.ChunkStore for chunk storage and writes index/blob files
// to disk. Intended for testing and offline backups.
type LocalStore struct {
	baseDir  string
	compress bool
	config   buzhash.Config
}

// NewLocalStore creates a LocalStore backed by the given directory.
func NewLocalStore(baseDir string, config buzhash.Config, compress bool) (*LocalStore, error) {
	chunkDir := filepath.Join(baseDir, ".chunks")
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		return nil, fmt.Errorf("create chunk dir: %w", err)
	}
	return &LocalStore{
		baseDir:  baseDir,
		compress: compress,
		config:   config,
	}, nil
}

// StartSession creates a new local backup session.
func (ls *LocalStore) StartSession(_ context.Context, config BackupConfig) (BackupSession, error) {
	chunkStore, err := datastore.NewChunkStore(ls.baseDir)
	if err != nil {
		return nil, fmt.Errorf("create chunk store: %w", err)
	}

	return &localSession{
		store:       chunkStore,
		config:      config,
		chunkConfig: ls.config,
		compress:    ls.compress,
		baseDir:     ls.baseDir,
		files:       make([]datastore.FileInfo, 0),
	}, nil
}

// localSession implements BackupSession for local filesystem storage.
type localSession struct {
	store       *datastore.ChunkStore
	config      BackupConfig
	chunkConfig buzhash.Config
	compress    bool
	baseDir     string
	files       []datastore.FileInfo
}

func (s *localSession) UploadArchive(_ context.Context, name string, data io.Reader) (*UploadResult, error) {
	chunker := buzhash.NewChunker(data, s.chunkConfig)
	idx := datastore.NewDynamicIndexWriter(time.Now().Unix())

	var totalOffset uint64

	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("chunk: %w", err)
		}

		digest := sha256.Sum256(chunk)

		storeData, err := encodeChunkBlob(chunk, s.compress)
		if err != nil {
			return nil, err
		}

		if _, _, err := s.store.InsertChunk(digest, storeData); err != nil {
			return nil, fmt.Errorf("store chunk: %w", err)
		}

		totalOffset += uint64(len(chunk))
		idx.Add(totalOffset, digest)
	}

	raw, err := idx.Finish()
	if err != nil {
		return nil, fmt.Errorf("finish index: %w", err)
	}

	indexPath := filepath.Join(s.baseDir, name)
	if err := os.WriteFile(indexPath, raw, 0o644); err != nil {
		return nil, fmt.Errorf("write index: %w", err)
	}

	indexDigest := sha256.Sum256(raw)

	result := &UploadResult{
		Filename: name,
		Size:     uint64(len(raw)),
		Digest:   indexDigest,
	}

	addFileInfo(&s.files, name, uint64(len(raw)), indexDigest)

	return result, nil
}

func (s *localSession) UploadBlob(_ context.Context, name string, data []byte) error {
	blobPath := filepath.Join(s.baseDir, name)
	if err := os.WriteFile(blobPath, data, 0o644); err != nil {
		return fmt.Errorf("write blob: %w", err)
	}

	digest := sha256.Sum256(data)
	addFileInfo(&s.files, name, uint64(len(data)), digest)

	return nil
}

func (s *localSession) Finish(_ context.Context) (*datastore.Manifest, error) {
	manifest := &datastore.Manifest{
		BackupType: s.config.BackupType.String(),
		BackupID:   s.config.BackupID,
		BackupTime: s.config.BackupTime,
		Files:      s.files,
	}

	data, err := manifest.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal manifest: %w", err)
	}

	manifestPath := filepath.Join(s.baseDir, "index.json")
	if err := os.WriteFile(manifestPath, data, 0o644); err != nil {
		return nil, fmt.Errorf("write manifest: %w", err)
	}

	return manifest, nil
}
