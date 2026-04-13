package backupproxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
)

var blobBufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 4<<20) // 4MB default
		return &buf
	},
}

func getBlobBuf() *[]byte {
	return blobBufPool.Get().(*[]byte)
}

func putBlobBuf(bp *[]byte) {
	*bp = (*bp)[:0]
	blobBufPool.Put(bp)
}

// encodeChunkBlob encodes a chunk as a PBS blob, optionally compressing with zstd.
// Returns the encoded bytes. Callers who need zero-alloc should use encodeChunkBlobTo.
func encodeChunkBlob(chunk []byte, compress bool) ([]byte, error) {
	bp := getBlobBuf()
	dst := *bp
	encoded, err := encodeChunkBlobTo(dst, chunk, compress)
	if err != nil {
		putBlobBuf(bp)
		return nil, err
	}
	// Copy out of pooled buffer before returning to pool
	result := make([]byte, len(encoded))
	copy(result, encoded)
	putBlobBuf(bp)
	return result, nil
}

// encodeChunkBlobTo encodes a chunk as a PBS blob into dst, without allocating.
// The returned slice is a sub-slice of the provided buffer.
func encodeChunkBlobTo(dst []byte, chunk []byte, compress bool) ([]byte, error) {
	if compress {
		return datastore.EncodeCompressedBlobTo(dst, chunk)
	}
	return datastore.EncodeBlobTo(dst, chunk)
}

// addFileInfo appends a file entry to the manifest file list.
func addFileInfo(files *[]datastore.FileInfo, name string, size uint64, digest [32]byte) {
	var hexBuf [64]byte
	hex.Encode(hexBuf[:], digest[:])
	*files = append(*files, datastore.FileInfo{
		Filename: name,
		Size:     size,
		CSum:     string(hexBuf[:]),
	})
}

// SplitArchiveResult contains the results of uploading a split archive.
// The metadata and payload are uploaded as separate .didx files.
type SplitArchiveResult struct {
	MetadataResult *UploadResult
	PayloadResult  *UploadResult
}

// RemoteStore abstracts the backup storage backend.
type RemoteStore interface {
	StartSession(ctx context.Context, config BackupConfig) (BackupSession, error)
}

// BackupSession represents an active backup upload session.
type BackupSession interface {
	UploadArchive(ctx context.Context, name string, data io.Reader) (*UploadResult, error)
	UploadSplitArchive(ctx context.Context, metadataName string, metadataData io.Reader, payloadName string, payloadData io.Reader) (*SplitArchiveResult, error)
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

	indexDigest := idx.Csum()

	result := &UploadResult{
		Filename: name,
		Size:     totalOffset,
		Digest:   indexDigest,
	}

	addFileInfo(&s.files, name, totalOffset, indexDigest)

	return result, nil
}

func (s *localSession) UploadSplitArchive(_ context.Context, metadataName string, metadataData io.Reader, payloadName string, payloadData io.Reader) (*SplitArchiveResult, error) {
	metaResult, err := s.UploadArchive(nil, metadataName, metadataData)
	if err != nil {
		return nil, fmt.Errorf("metadata archive: %w", err)
	}

	payloadResult, err := s.UploadArchive(nil, payloadName, payloadData)
	if err != nil {
		return nil, fmt.Errorf("payload archive: %w", err)
	}

	return &SplitArchiveResult{
		MetadataResult: metaResult,
		PayloadResult:  payloadResult,
	}, nil
}

func (s *localSession) UploadBlob(_ context.Context, name string, data []byte) error {
	blob, err := datastore.EncodeBlob(data)
	if err != nil {
		return fmt.Errorf("encode blob: %w", err)
	}
	blobData := blob.Bytes()

	blobPath := filepath.Join(s.baseDir, name)
	if err := os.WriteFile(blobPath, blobData, 0o644); err != nil {
		return fmt.Errorf("write blob: %w", err)
	}

	digest := sha256.Sum256(blobData)
	addFileInfo(&s.files, name, uint64(len(blobData)), digest)

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
