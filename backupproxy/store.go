package backupproxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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
		encoded, err := datastore.EncodeEncryptedBlob(dst, chunk, cc, compress)
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
		encoded, err = datastore.EncodeEncryptedBlob(dst, chunk, cc, compress)
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
		return datastore.EncodeCompressedBlob(dst, chunk)
	}
	return datastore.EncodeBlob(dst, chunk)
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

// KnownChunkRef references a chunk and can carry its exact encoded blob when
// the target session may not know it yet.
type KnownChunkRef struct {
	Digest          [32]byte
	Size            uint64 // decoded chunk size in bytes
	LoadEncodedBlob func() ([]byte, error)
}

// DynamicIndexPublisher provides an index-preserving fast path for immutable local snapshots.
type DynamicIndexPublisher interface {
	PublishDynamicIndex(ctx context.Context, name, sourcePath string) (*UploadResult, error)
}

type BackupSession interface {
	UploadArchive(ctx context.Context, name string, data io.Reader) (*UploadResult, error)
	UploadSplitArchive(ctx context.Context, metadataName string, metadataData io.Reader, payloadName string, payloadData io.Reader) (*SplitArchiveResult, error)
	UploadBlob(ctx context.Context, name string, data []byte) error
	UploadPayloadInterleaved(ctx context.Context, name string, newData io.Reader, injections <-chan InjectChunks) (*UploadResult, error)
	Finish(ctx context.Context) (*datastore.Manifest, error)
	Close() error
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
	Chunks    []KnownChunkRef
	Size      uint64
	Boundary  uint64
	Processed chan error
}

// LocalStore implements RemoteStore using a local filesystem directory.
// It uses datastore.ChunkStore for chunk storage and writes index/blob files
// to disk. Intended for testing and offline backups.
type LocalStore struct {
	baseDir       string
	chunkBase     string
	compress      bool
	config        buzhash.Config
	manifestBlob  bool
	reuseExisting bool
	uid           int
	gid           int
	syncWrites    bool
}

// NewLocalStore creates a LocalStore backed by the given directory.
func NewLocalStore(baseDir string, config buzhash.Config, compress bool) (*LocalStore, error) {
	chunkDir := filepath.Join(baseDir, ".chunks")
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		return nil, fmt.Errorf("create chunk dir: %w", err)
	}
	return &LocalStore{
		baseDir:   baseDir,
		chunkBase: baseDir,
		compress:  compress,
		config:    config,
		uid:       -1,
		gid:       -1,
	}, nil
}

// DatastoreStoreOptions configures direct publication into an existing PBS datastore.
type DatastoreStoreOptions struct {
	Compress   bool
	UID        int
	GID        int
	SyncWrites bool
}

// NewDatastoreStore creates a local store that writes snapshot files separately from the shared chunk store.
func NewDatastoreStore(datastoreDir, snapshotDir string, config buzhash.Config, opts DatastoreStoreOptions) (*LocalStore, error) {
	datastoreDir, err := filepath.Abs(datastoreDir)
	if err != nil {
		return nil, fmt.Errorf("resolve datastore path: %w", err)
	}
	snapshotDir, err = filepath.Abs(snapshotDir)
	if err != nil {
		return nil, fmt.Errorf("resolve snapshot path: %w", err)
	}
	rel, err := filepath.Rel(datastoreDir, snapshotDir)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("snapshot path %q is outside datastore %q", snapshotDir, datastoreDir)
	}
	if info, err := os.Stat(filepath.Join(datastoreDir, ".chunks")); err != nil || !info.IsDir() {
		return nil, fmt.Errorf("datastore chunk directory is unavailable")
	}
	if info, err := os.Stat(snapshotDir); err != nil || !info.IsDir() {
		return nil, fmt.Errorf("snapshot directory is unavailable")
	}
	return &LocalStore{
		baseDir:       snapshotDir,
		chunkBase:     datastoreDir,
		compress:      opts.Compress,
		config:        config,
		manifestBlob:  true,
		reuseExisting: true,
		uid:           opts.UID,
		gid:           opts.GID,
		syncWrites:    opts.SyncWrites,
	}, nil
}

// StartSession creates a new local backup session.
func (ls *LocalStore) StartSession(_ context.Context, config BackupConfig) (BackupSession, error) {
	chunkStore, err := datastore.NewOwnedChunkStore(ls.chunkBase, ls.uid, ls.gid, ls.syncWrites)
	if err != nil {
		return nil, fmt.Errorf("create chunk store: %w", err)
	}

	return &localSession{
		store:         chunkStore,
		config:        config,
		chunkConfig:   ls.config,
		compress:      ls.compress,
		baseDir:       ls.baseDir,
		manifestBlob:  ls.manifestBlob,
		reuseExisting: ls.reuseExisting,
		uid:           ls.uid,
		gid:           ls.gid,
		syncWrites:    ls.syncWrites,
		files:         make([]datastore.BackupFileInfo, 0),
	}, nil
}

// localSession implements BackupSession for local filesystem storage.
type localSession struct {
	store         *datastore.ChunkStore
	baseDir       string
	files         []datastore.BackupFileInfo
	config        BackupConfig
	chunkConfig   buzhash.Config
	compress      bool
	manifestBlob  bool
	reuseExisting bool
	uid           int
	gid           int
	syncWrites    bool
}

func (s *localSession) createSnapshotTemp(name string) (*os.File, string, error) {
	if filepath.Base(name) != name || name == "." || name == "" {
		return nil, "", fmt.Errorf("invalid snapshot filename %q", name)
	}
	tmp, err := os.CreateTemp(s.baseDir, "."+name+".tmp-*")
	if err != nil {
		return nil, "", err
	}
	if err := tmp.Chmod(0o644); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return nil, "", err
	}
	if s.uid >= 0 || s.gid >= 0 {
		if err := tmp.Chown(s.uid, s.gid); err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
			return nil, "", err
		}
	}
	return tmp, tmp.Name(), nil
}

func (s *localSession) publishSnapshotTemp(tmp *os.File, tmpName, name string) error {
	if s.syncWrites {
		if err := tmp.Sync(); err != nil {
			_ = tmp.Close()
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, filepath.Join(s.baseDir, name))
}

func (s *localSession) writeSnapshotFile(name string, data []byte) error {
	tmp, tmpName, err := s.createSnapshotTemp(name)
	if err != nil {
		return err
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}()
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	return s.publishSnapshotTemp(tmp, tmpName, name)
}

func (s *localSession) UploadArchive(_ context.Context, name string, data io.Reader) (*UploadResult, error) {
	tmp, tmpName, err := s.createSnapshotTemp(name)
	if err != nil {
		return nil, fmt.Errorf("create index: %w", err)
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}()
	idx, err := datastore.NewDynamicIndexStreamWriter(tmp, time.Now().Unix())
	if err != nil {
		return nil, err
	}
	chunker := buzhash.NewChunker(data, s.chunkConfig)
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
		_, _, insertErr := s.store.InsertChunk(digest, storeData)
		datastore.PutBlobBuf(bp)
		if insertErr != nil {
			return nil, fmt.Errorf("store chunk: %w", insertErr)
		}
		totalOffset += uint64(len(chunk))
		if err := idx.Add(totalOffset, digest); err != nil {
			return nil, err
		}
	}
	indexDigest, indexSize, err := idx.Finish()
	if err != nil {
		return nil, fmt.Errorf("finish index: %w", err)
	}
	if indexSize != totalOffset {
		return nil, fmt.Errorf("index size %d does not match archive size %d", indexSize, totalOffset)
	}
	if err := s.publishSnapshotTemp(tmp, tmpName, name); err != nil {
		return nil, fmt.Errorf("write index: %w", err)
	}
	result := &UploadResult{Filename: name, Size: totalOffset, Digest: indexDigest}
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
		enc, err := datastore.EncodeEncryptedBlob(nil, data, s.config.CryptConfig, false)
		if err != nil {
			return fmt.Errorf("encode encrypted blob: %w", err)
		}
		blobData = enc
	} else {
		blob, err := datastore.EncodeBlob(nil, data)
		if err != nil {
			return fmt.Errorf("encode blob: %w", err)
		}
		blobData = blob
	}

	if err := s.writeSnapshotFile(name, blobData); err != nil {
		return fmt.Errorf("write blob: %w", err)
	}

	digest := sha256.Sum256(blobData)
	addFileInfo(&s.files, name, uint64(len(blobData)), digest, string(s.config.CryptMode))

	return nil
}

func (s *localSession) PublishDynamicIndex(ctx context.Context, name, sourcePath string) (*UploadResult, error) {
	if !s.reuseExisting {
		return nil, fmt.Errorf("dynamic index publication requires a shared datastore chunk store")
	}
	index, err := datastore.OpenDynamicIndex(sourcePath)
	if err != nil {
		return nil, fmt.Errorf("open source index: %w", err)
	}
	defer func() { _ = index.Close() }()
	indexDigest, totalSize := index.ComputeCsum()
	if indexDigest != index.IndexCsum() {
		return nil, fmt.Errorf("source index checksum mismatch")
	}
	validator := localPayloadSink{
		session:    s,
		localKnown: newDigestCache(localChunkCacheCapacity),
		ctx:        ctx,
	}
	const validationBatchSize = 1024
	for start := 0; start < index.Count(); start += validationBatchSize {
		end := min(start+validationBatchSize, index.Count())
		chunks := make([]KnownChunkRef, 0, end-start)
		for position := start; position < end; position++ {
			info, ok := index.ChunkInfo(position)
			if !ok {
				return nil, fmt.Errorf("source index entry %d is unavailable", position)
			}
			chunks = append(chunks, KnownChunkRef{Digest: info.Digest, Size: info.End - info.Start})
		}
		if err := validator.validateReused(chunks); err != nil {
			return nil, err
		}
		for _, chunk := range chunks {
			validator.reportProgress(chunk.Size, 0)
		}
	}

	tmp, tmpName, err := s.createSnapshotTemp(name)
	if err != nil {
		return nil, fmt.Errorf("create target index: %w", err)
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}()
	source, err := os.Open(sourcePath)
	if err != nil {
		return nil, fmt.Errorf("open source index data: %w", err)
	}
	if err := cloneFile(tmp, source); err != nil {
		_ = source.Close()
		return nil, fmt.Errorf("clone source index: %w", err)
	}
	if err := source.Close(); err != nil {
		return nil, err
	}
	if err := s.publishSnapshotTemp(tmp, tmpName, name); err != nil {
		return nil, fmt.Errorf("publish target index: %w", err)
	}
	result := &UploadResult{Filename: name, Size: totalSize, Digest: indexDigest}
	addFileInfo(&s.files, name, totalSize, indexDigest, string(s.config.CryptMode))
	return result, nil
}

func (s *localSession) UploadPayloadInterleaved(ctx context.Context, name string, newData io.Reader, injections <-chan InjectChunks) (*UploadResult, error) {
	tmp, tmpName, err := s.createSnapshotTemp(name)
	if err != nil {
		return nil, fmt.Errorf("create index: %w", err)
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}()
	idx, err := datastore.NewDynamicIndexStreamWriter(tmp, time.Now().Unix())
	if err != nil {
		return nil, err
	}
	sink := &localPayloadSink{
		session:    s,
		idx:        idx,
		localKnown: newDigestCache(localChunkCacheCapacity),
		ctx:        ctx,
	}
	totalSize, err := interleavePayload(s.chunkConfig, newData, injections, sink)
	if err != nil {
		return nil, err
	}
	indexDigest, indexSize, err := sink.idx.Finish()
	if err != nil {
		return nil, fmt.Errorf("finish index: %w", err)
	}
	if indexSize != totalSize {
		return nil, fmt.Errorf("index size %d does not match payload size %d", indexSize, totalSize)
	}
	if err := s.publishSnapshotTemp(tmp, tmpName, name); err != nil {
		return nil, fmt.Errorf("write index: %w", err)
	}
	result := &UploadResult{Filename: name, Size: totalSize, Digest: indexDigest}
	addFileInfo(&s.files, name, totalSize, indexDigest, string(s.config.CryptMode))
	return result, nil
}

// localPayloadSink implements payloadSink for a local chunk store. New chunks
// are stored via the ChunkStore and every chunk is recorded in one shared
// DynamicIndexWriter, matching the PBS path's offset accounting.
const (
	localChunkCacheCapacity     = 64 << 10
	localChunkValidationWorkers = 20
)

type digestCache struct {
	items    map[[32]byte]struct{}
	ring     [][32]byte
	capacity int
	next     int
}

func newDigestCache(capacity int) *digestCache {
	capacity = max(capacity, 1)
	initial := min(capacity, 256)
	return &digestCache{
		items:    make(map[[32]byte]struct{}, initial),
		ring:     make([][32]byte, 0, initial),
		capacity: capacity,
	}
}

func (c *digestCache) contains(digest [32]byte) bool {
	_, ok := c.items[digest]
	return ok
}

func (c *digestCache) add(digest [32]byte) {
	if c.contains(digest) {
		return
	}
	if len(c.ring) < c.capacity {
		c.ring = append(c.ring, digest)
	} else {
		delete(c.items, c.ring[c.next])
		c.ring[c.next] = digest
		c.next = (c.next + 1) % len(c.ring)
	}
	c.items[digest] = struct{}{}
}

type localPayloadSink struct {
	session    *localSession
	idx        *datastore.DynamicIndexStreamWriter
	localKnown *digestCache
	ctx        context.Context
	progress   UploadProgress
}

func (s *localPayloadSink) putRaw(offset uint64, raw []byte) error {
	if len(raw) == 0 {
		return nil
	}
	digest := chunkDigest(raw, s.session.config.CryptConfig)
	uploadedSize := uint64(0)
	if !s.localKnown.contains(digest) {
		storeData, bp, err := borrowChunkBlob(raw, s.session.compress, s.session.config.CryptConfig)
		if err != nil {
			return err
		}
		uploadedSize = uint64(len(storeData))
		_, _, insErr := s.session.store.InsertChunk(digest, storeData)
		datastore.PutBlobBuf(bp)
		if insErr != nil {
			return fmt.Errorf("store chunk: %w", insErr)
		}
		s.localKnown.add(digest)
	}
	endOffset := offset + uint64(len(raw))
	if err := s.idx.Add(endOffset, digest); err != nil {
		return err
	}
	s.reportProgress(uint64(len(raw)), uploadedSize)
	return nil
}

func (s *localPayloadSink) putInjection(offset uint64, inj InjectChunks) error {
	if s.session.reuseExisting {
		if err := s.validateReused(inj.Chunks); err != nil {
			return err
		}
	}
	cur := offset
	for _, c := range inj.Chunks {
		uploadedSize := uint64(0)
		if !s.localKnown.contains(c.Digest) && c.LoadEncodedBlob != nil {
			blob, err := c.LoadEncodedBlob()
			if err != nil {
				return fmt.Errorf("load replayed chunk: %w", err)
			}
			uploadedSize = uint64(len(blob))
			if _, _, err := s.session.store.InsertChunk(c.Digest, blob); err != nil {
				return fmt.Errorf("store replayed chunk: %w", err)
			}
		}
		s.localKnown.add(c.Digest)
		if err := s.idx.Add(cur+c.Size, c.Digest); err != nil {
			return err
		}
		cur += c.Size
		s.reportProgress(c.Size, uploadedSize)
	}
	return nil
}

func (s *localPayloadSink) validateReused(chunks []KnownChunkRef) error {
	unique := make([][32]byte, 0, len(chunks))
	seen := make(map[[32]byte]struct{}, len(chunks))
	for _, chunk := range chunks {
		if s.localKnown.contains(chunk.Digest) {
			continue
		}
		if _, ok := seen[chunk.Digest]; ok {
			continue
		}
		seen[chunk.Digest] = struct{}{}
		unique = append(unique, chunk.Digest)
	}
	if len(unique) == 0 {
		return nil
	}
	workers := min(localChunkValidationWorkers, len(unique))
	var next atomic.Uint64
	var firstErr error
	var errOnce sync.Once
	var wait sync.WaitGroup
	for range workers {
		wait.Go(func() {
			for {
				if err := s.ctx.Err(); err != nil {
					errOnce.Do(func() { firstErr = err })
					return
				}
				index := int(next.Add(1) - 1)
				if index >= len(unique) {
					return
				}
				digest := unique[index]
				if _, err := s.session.store.StatChunk(digest); err != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("verify reused chunk %x: %w", digest[:8], err)
					})
					return
				}
			}
		})
	}
	wait.Wait()
	if firstErr != nil {
		return firstErr
	}
	for _, digest := range unique {
		s.localKnown.add(digest)
	}
	return nil
}

func (s *localPayloadSink) reportProgress(processed, uploaded uint64) {
	s.progress.ProcessedChunks++
	s.progress.ProcessedBytes += processed
	if uploaded > 0 {
		s.progress.UploadedChunks++
		s.progress.UploadedBytes += uploaded
	}
	if onProgress := s.session.config.OnUploadProgress; onProgress != nil {
		onProgress(s.progress)
	}
}

func (s *localSession) Finish(_ context.Context) (*datastore.Manifest, error) {
	manifest := &datastore.Manifest{
		BackupType: s.config.BackupType.String(),
		BackupID:   s.config.BackupID,
		BackupTime: s.config.BackupTime,
		Files:      s.files,
	}
	if !s.manifestBlob {
		manifest.CryptMode = string(s.config.CryptMode)
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

	manifestName := "index.json"
	if s.manifestBlob {
		blob, err := datastore.EncodeBlob(nil, data)
		if err != nil {
			return nil, fmt.Errorf("encode manifest blob: %w", err)
		}
		data = blob
		manifestName = "index.json.blob"
	}
	if err := s.writeSnapshotFile(manifestName, data); err != nil {
		return nil, fmt.Errorf("write manifest: %w", err)
	}

	return manifest, nil
}

func (s *localSession) Close() error { return nil }

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
