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
// When cc is non-nil, the chunk is encrypted with AES-256-GCM.
// Returns the encoded bytes. Callers who need zero-alloc should use encodeChunkBlobTo.
func encodeChunkBlob(chunk []byte, compress bool, cc *datastore.CryptConfig) ([]byte, error) {
	if cc != nil {
		bp := datastore.BlobBufPool.Get().(*[]byte)
		dst := (*bp)[:0]
		encoded, err := datastore.EncodeEncryptedBlobTo(dst, chunk, cc, compress)
		if err != nil {
			datastore.PutBlobBuf(bp)
			return nil, err
		}
		result := make([]byte, len(encoded))
		copy(result, encoded)
		datastore.PutBlobBuf(bp)
		return result, nil
	}
	bp := datastore.BlobBufPool.Get().(*[]byte)
	dst := (*bp)[:0]
	encoded, err := encodeChunkBlobTo(dst, chunk, compress)
	if err != nil {
		datastore.PutBlobBuf(bp)
		return nil, err
	}
	result := make([]byte, len(encoded))
	copy(result, encoded)
	datastore.PutBlobBuf(bp)
	return result, nil
}

// borrowChunkBlob encodes a chunk as a PBS blob into a pooled buffer and returns
// the encoded slice together with the pool handle. The encoded slice aliases
// the pooled buffer — callers MUST consume it synchronously and then call
// datastore.PutBlobBuf(bp). This eliminates the per-chunk copy that
// encodeChunkBlob performs, which is the dominant allocation in the upload
// hot path (the blob is always consumed synchronously: HTTP/2 framer write for
// remote uploads, os.WriteFile for local chunk stores).
func borrowChunkBlob(chunk []byte, compress bool, cc *datastore.CryptConfig) (encoded []byte, bp *[]byte, err error) {
	bp = datastore.BlobBufPool.Get().(*[]byte)
	dst := (*bp)[:0]
	if cc != nil {
		encoded, err = datastore.EncodeEncryptedBlobTo(dst, chunk, cc, compress)
	} else {
		encoded, err = encodeChunkBlobTo(dst, chunk, compress)
	}
	if err != nil {
		datastore.PutBlobBuf(bp)
		return nil, nil, err
	}
	return encoded, bp, nil
}

// encodeChunkBlobTo encodes a chunk as a PBS blob into dst, without allocating.
// The returned slice is a sub-slice of the provided buffer.
func encodeChunkBlobTo(dst []byte, chunk []byte, compress bool) ([]byte, error) {
	if compress {
		return datastore.EncodeCompressedBlobTo(dst, chunk)
	}
	return datastore.EncodeBlobTo(dst, chunk)
}

// chunkDigest computes the SHA-256 digest of a chunk, using the CryptConfig's
// id_key for encrypted mode (SHA-256(data || id_key)) or plain SHA-256 otherwise.
func chunkDigest(chunk []byte, cc *datastore.CryptConfig) [32]byte {
	if cc != nil {
		return cc.ComputeDigest(chunk)
	}
	return sha256.Sum256(chunk)
}

// addFileInfo appends a file entry to the manifest file list.
func addFileInfo(files *[]datastore.BackupFileInfo, name string, size uint64, digest [32]byte, cryptMode string) {
	var hexBuf [64]byte
	hex.Encode(hexBuf[:], digest[:])
	*files = append(*files, datastore.BackupFileInfo{
		Filename:  name,
		Size:      size,
		CSum:      string(hexBuf[:]),
		CryptMode: cryptMode,
	})
}

// SplitArchiveResult contains the results of uploading a split archive.
// The metadata and payload are uploaded as separate .didx files.
type SplitArchiveResult struct {
	MetadataResult *UploadResult
	PayloadResult  *UploadResult
}

// PreviousSnapshotSource provides read access to a previous backup snapshot
// for metadata change detection. It can read archive files and download chunks.
type PreviousSnapshotSource interface {
	ReadArchive(filename string) ([]byte, error)
	ChunkSource() datastore.ChunkSource
	Close() error
}

// RemoteStore abstracts the backup storage backend.
type RemoteStore interface {
	RemoteStoreBase
	SnapshotReader
}

// RemoteStoreBase contains the session creation method.
type RemoteStoreBase interface {
	StartSession(ctx context.Context, config BackupConfig) (BackupSession, error)
}

// SnapshotReader can read files from previous snapshots.
type SnapshotReader interface {
	ReadPreviousArchive(ctx context.Context, backupType datastore.BackupType, backupID string, backupTime int64, namespace, filename string) ([]byte, error)
	NewPreviousSnapshotSource(ctx context.Context, backupType datastore.BackupType, backupID string, backupTime int64, namespace string) (PreviousSnapshotSource, error)
}

// BackupSession represents an active backup upload session.
// KnownChunkRef references a chunk already stored in the datastore.
type KnownChunkRef struct {
	Digest [32]byte
	Size   uint64 // chunk size in bytes
}

type BackupSession interface {
	UploadArchive(ctx context.Context, name string, data io.Reader) (*UploadResult, error)
	UploadSplitArchive(ctx context.Context, metadataName string, metadataData io.Reader, payloadName string, payloadData io.Reader) (*SplitArchiveResult, error)
	UploadBlob(ctx context.Context, name string, data []byte) error
	UploadPayloadInterleaved(ctx context.Context, name string, newData io.Reader, injections <-chan InjectChunks) (*UploadResult, error)
	Finish(ctx context.Context) (*datastore.Manifest, error)
}

// InjectChunks describes a batch of reused chunks to inject into the payload
// stream at a specific offset.
//
// Boundary is the absolute payload-stream offset (encoder.PayloadPosition)
// at which the injection occurs, before the encoder is advanced by Size.
// It mirrors the `boundary` field of pbs-client's InjectChunks and is what
// the payload chunker uses to interleave injected chunks with new data in
// the correct offset order. Without it the new-data and injected-chunk
// offsets drift apart, producing server errors like "strange chunk offset".
type InjectChunks struct {
	Chunks   []KnownChunkRef
	Size     uint64
	Boundary uint64
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
		files:       make([]datastore.BackupFileInfo, 0),
	}, nil
}

// localSession implements BackupSession for local filesystem storage.
type localSession struct {
	store       *datastore.ChunkStore
	baseDir     string
	files       []datastore.BackupFileInfo
	config      BackupConfig
	chunkConfig buzhash.Config
	compress    bool
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

		digest := chunkDigest(chunk, s.config.CryptConfig)

		storeData, bp, err := borrowChunkBlob(chunk, s.compress, s.config.CryptConfig)
		if err != nil {
			return nil, err
		}
		_, _, insErr := s.store.InsertChunk(digest, storeData)
		datastore.PutBlobBuf(bp)
		if insErr != nil {
			return nil, fmt.Errorf("store chunk: %w", insErr)
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

	addFileInfo(&s.files, name, totalOffset, indexDigest, string(s.config.CryptMode))

	return result, nil
}

func (s *localSession) UploadSplitArchive(ctx context.Context, metadataName string, metadataData io.Reader, payloadName string, payloadData io.Reader) (*SplitArchiveResult, error) {
	metaResult, err := s.UploadArchive(ctx, metadataName, metadataData)
	if err != nil {
		return nil, fmt.Errorf("metadata archive: %w", err)
	}

	payloadResult, err := s.UploadArchive(ctx, payloadName, payloadData)
	if err != nil {
		return nil, fmt.Errorf("payload archive: %w", err)
	}

	return &SplitArchiveResult{
		MetadataResult: metaResult,
		PayloadResult:  payloadResult,
	}, nil
}

func (s *localSession) UploadBlob(_ context.Context, name string, data []byte) error {
	var blobData []byte
	if s.config.CryptConfig != nil && s.config.CryptMode == datastore.CryptModeEncrypt {
		enc, err := datastore.EncodeEncryptedBlob(data, s.config.CryptConfig, false)
		if err != nil {
			return fmt.Errorf("encode encrypted blob: %w", err)
		}
		blobData = enc.Bytes()
	} else {
		blob, err := datastore.EncodeBlob(data)
		if err != nil {
			return fmt.Errorf("encode blob: %w", err)
		}
		blobData = blob.Bytes()
	}

	blobPath := filepath.Join(s.baseDir, name)
	if err := os.WriteFile(blobPath, blobData, 0o644); err != nil {
		return fmt.Errorf("write blob: %w", err)
	}

	digest := sha256.Sum256(blobData)
	addFileInfo(&s.files, name, uint64(len(blobData)), digest, string(s.config.CryptMode))

	return nil
}

func (s *localSession) UploadPayloadInterleaved(_ context.Context, name string, newData io.Reader, injections <-chan InjectChunks) (*UploadResult, error) {
	sink := &localPayloadSink{
		session:    s,
		idx:        datastore.NewDynamicIndexWriter(time.Now().Unix()),
		localKnown: make(map[[32]byte]bool),
	}
	totalSize, err := interleavePayload(s.chunkConfig, newData, injections, sink)
	if err != nil {
		return nil, err
	}

	raw, err := sink.idx.Finish()
	if err != nil {
		return nil, fmt.Errorf("finish index: %w", err)
	}
	indexPath := filepath.Join(s.baseDir, name)
	if err := os.WriteFile(indexPath, raw, 0o644); err != nil {
		return nil, fmt.Errorf("write index: %w", err)
	}

	indexDigest := sink.idx.Csum()
	result := &UploadResult{
		Filename: name,
		Size:     totalSize,
		Digest:   indexDigest,
	}
	addFileInfo(&s.files, name, totalSize, indexDigest, string(s.config.CryptMode))
	return result, nil
}

// localPayloadSink implements payloadSink for a local chunk store. New chunks
// are stored via the ChunkStore and every chunk is recorded in one shared
// DynamicIndexWriter, matching the PBS path's offset accounting.
type localPayloadSink struct {
	session    *localSession
	idx        *datastore.DynamicIndexWriter
	localKnown map[[32]byte]bool
}

func (s *localPayloadSink) putRaw(offset uint64, raw []byte) error {
	if len(raw) == 0 {
		return nil
	}
	digest := chunkDigest(raw, s.session.config.CryptConfig)
	if !s.localKnown[digest] {
		storeData, bp, err := borrowChunkBlob(raw, s.session.compress, s.session.config.CryptConfig)
		if err != nil {
			return err
		}
		_, _, insErr := s.session.store.InsertChunk(digest, storeData)
		datastore.PutBlobBuf(bp)
		if insErr != nil {
			return fmt.Errorf("store chunk: %w", insErr)
		}
		s.localKnown[digest] = true
	}
	endOffset := offset + uint64(len(raw))
	s.idx.Add(endOffset, digest)
	return nil
}

func (s *localPayloadSink) putInjection(offset uint64, inj InjectChunks) error {
	cur := offset
	for _, c := range inj.Chunks {
		s.idx.Add(cur+c.Size, c.Digest)
		cur += c.Size
	}
	return nil
}

func (s *localSession) Finish(_ context.Context) (*datastore.Manifest, error) {
	manifest := &datastore.Manifest{
		BackupType: s.config.BackupType.String(),
		BackupID:   s.config.BackupID,
		BackupTime: s.config.BackupTime,
		Files:      s.files,
		CryptMode:  string(s.config.CryptMode),
	}

	if s.config.CryptConfig != nil && s.config.CryptMode != datastore.CryptModeNone {
		if err := datastore.SignManifest(manifest, s.config.CryptConfig); err != nil {
			return nil, fmt.Errorf("sign manifest: %w", err)
		}
	}

	data, err := manifest.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal manifest: %w", err)
	}

	// Manifest is never encrypted — even in encrypt mode, the manifest
	// must remain readable so that file listings and metadata are accessible.

	manifestPath := filepath.Join(s.baseDir, "index.json")
	if err := os.WriteFile(manifestPath, data, 0o644); err != nil {
		return nil, fmt.Errorf("write manifest: %w", err)
	}

	return manifest, nil
}

// ReadPreviousArchive reads an archive file from a previous local backup snapshot.
func (ls *LocalStore) ReadPreviousArchive(_ context.Context, _ datastore.BackupType, _ string, _ int64, _, filename string) ([]byte, error) {
	return nil, fmt.Errorf("local store: use Dir field in PreviousBackupRef for file lookup")
}

// localSnapshotSource implements PreviousSnapshotSource for local filesystem storage.
type localSnapshotSource struct {
	chunkSrc *datastore.ChunkStoreSource
	dir      string
}

func (ls *localSnapshotSource) ReadArchive(filename string) ([]byte, error) {
	return os.ReadFile(filepath.Join(ls.dir, filename))
}

func (ls *localSnapshotSource) ChunkSource() datastore.ChunkSource {
	return ls.chunkSrc
}

func (ls *localSnapshotSource) Close() error { return nil }

// NewPreviousSnapshotSource creates a PreviousSnapshotSource for a local backup snapshot.
func (ls *LocalStore) NewPreviousSnapshotSource(_ context.Context, _ datastore.BackupType, _ string, _ int64, _ string) (PreviousSnapshotSource, error) {
	return nil, fmt.Errorf("use Dir field in PreviousBackupRef for local store")
}

// NewPreviousSnapshotSourceFromDir creates a PreviousSnapshotSource from a local directory.
func NewPreviousSnapshotSourceFromDir(dir string) (PreviousSnapshotSource, error) {
	cs, err := datastore.NewChunkStore(dir)
	if err != nil {
		return nil, fmt.Errorf("create chunk store: %w", err)
	}
	return &localSnapshotSource{
		dir:      dir,
		chunkSrc: datastore.NewChunkStoreSource(cs),
	}, nil
}
