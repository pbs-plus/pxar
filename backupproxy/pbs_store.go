package backupproxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"

	"time"

	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
)

// pbsBackupProtocol abstracts PBS backup protocol operations for testability.
type pbsBackupProtocol interface {
	dynamicIndexCreate(archiveName string) (uint64, error)
	dynamicChunkUpload(wid uint64, digest string, size, encodedSize int, data []byte) error
	pipelineChunkUploads(wid uint64, chunks []chunkUploadReq) error
	dynamicIndexAppend(wid uint64, digests []string, offsets []uint64) error
	dynamicIndexClose(wid uint64, chunkCount int, size uint64, csum string) error
	blobUpload(fileName string, encodedSize int, data []byte) error
	downloadPrevious(archiveName string) ([]byte, error)
	finish() error
	close()
}

// h2Protocol implements pbsBackupProtocol using an H2 connection.
type h2Protocol struct {
	conn *pbsH2Conn
}

func (p *h2Protocol) dynamicIndexCreate(archiveName string) (uint64, error) {
	params := url.Values{}
	params.Set("archive-name", archiveName)
	data, err := p.conn.do("POST", "dynamic_index", params, nil, "")
	if err != nil {
		return 0, fmt.Errorf("create dynamic index: %w", err)
	}
	var wid uint64
	if err := json.Unmarshal(data, &wid); err != nil {
		return 0, fmt.Errorf("parse wid: %w (body: %s)", err, data)
	}
	return wid, nil
}

// downloadPrevious calls the backup session's "previous" endpoint for the given
// archive name. This triggers server-side chunk registration: the PBS server
// reads the previous backup's index and registers all its chunk digests in the
// session's known_chunks map, allowing dynamic_index_append to reference them.
// Returns the raw DIDX index data.
func (p *h2Protocol) downloadPrevious(archiveName string) ([]byte, error) {
	params := url.Values{}
	params.Set("archive-name", archiveName)
	data, err := p.conn.doRaw("GET", "previous", params)
	if err != nil {
		return nil, fmt.Errorf("download previous %s: %w", archiveName, err)
	}
	return data, nil
}

func (p *h2Protocol) dynamicChunkUpload(wid uint64, digest string, size, encodedSize int, data []byte) error {
	return p.pipelineChunkUploads(wid, []chunkUploadReq{{
		digest:      digest,
		size:        size,
		encodedSize: encodedSize,
		data:        data,
	}})
}

// pipelineChunkUploads sends multiple chunk upload requests concurrently over H2.
// All requests are sent before any responses are read (pipelining), matching
// the Rust PBS client's behavior where h2.send_request is called without awaiting.
// Returns the first error encountered, or nil.
func (p *h2Protocol) pipelineChunkUploads(wid uint64, chunks []chunkUploadReq) error {
	if len(chunks) == 0 {
		return nil
	}

	entries := make([]pipelineEntry, len(chunks))
	for i, c := range chunks {
		params := url.Values{}
		params.Set("wid", strconv.FormatUint(wid, 10))
		params.Set("digest", c.digest)
		params.Set("size", strconv.Itoa(c.size))
		params.Set("encoded-size", strconv.Itoa(c.encodedSize))
		entries[i] = pipelineEntry{
			method:      "POST",
			path:        "dynamic_chunk",
			params:      params,
			body:        c.data,
			contentType: "application/octet-stream",
		}
	}

	results := p.conn.pipeline(entries)
	for i, r := range results {
		if r.err != nil {
			return fmt.Errorf("upload chunk %s: %w", chunks[i].digest, r.err)
		}
	}
	return nil
}

type chunkUploadReq struct {
	digest      string
	size        int
	encodedSize int
	data        []byte
}

func (p *h2Protocol) dynamicIndexAppend(wid uint64, digests []string, offsets []uint64) error {
	body := map[string]any{
		"wid":         wid,
		"digest-list": digests,
		"offset-list": offsets,
	}
	bodyData, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal append body: %w", err)
	}
	_, err = p.conn.do("PUT", "dynamic_index", nil, bodyData, "application/json")
	if err != nil {
		return fmt.Errorf("append index: %w", err)
	}
	return nil
}

func (p *h2Protocol) dynamicIndexClose(wid uint64, chunkCount int, size uint64, csum string) error {
	params := url.Values{}
	params.Set("wid", strconv.FormatUint(wid, 10))
	params.Set("chunk-count", strconv.Itoa(chunkCount))
	params.Set("size", strconv.FormatUint(size, 10))
	params.Set("csum", csum)
	_, err := p.conn.do("POST", "dynamic_close", params, nil, "")
	if err != nil {
		return fmt.Errorf("close index: %w", err)
	}
	return nil
}

func (p *h2Protocol) blobUpload(fileName string, encodedSize int, data []byte) error {
	params := url.Values{}
	params.Set("file-name", fileName)
	params.Set("encoded-size", strconv.Itoa(encodedSize))
	_, err := p.conn.do("POST", "blob", params, data, "application/octet-stream")
	if err != nil {
		return fmt.Errorf("upload blob: %w", err)
	}
	return nil
}

func (p *h2Protocol) finish() error {
	_, err := p.conn.do("POST", "finish", nil, nil, "")
	if err != nil {
		return fmt.Errorf("finish: %w", err)
	}
	return nil
}

func (p *h2Protocol) close() {
	p.conn.close()
}

// PBSConfig holds configuration for connecting to a Proxmox Backup Server.
type PBSConfig struct {
	BaseURL       string
	Datastore     string
	AuthToken     string
	Namespace     string
	SkipTLSVerify bool
}

// PBSStore implements RemoteStore via the PBS H2 backup protocol.
type PBSStore struct {
	config   PBSConfig
	chunkCfg buzhash.Config
	compress bool
}

// NewPBSStore creates a PBS remote store with the given configuration.
func NewPBSStore(config PBSConfig, chunkCfg buzhash.Config, compress bool) *PBSStore {
	return &PBSStore{
		config:   config,
		chunkCfg: chunkCfg,
		compress: compress,
	}
}

// StartSession dials PBS via H2 upgrade and returns a backup session.
func (ps *PBSStore) StartSession(ctx context.Context, config BackupConfig) (BackupSession, error) {
	h2Conn, err := dialPBSH2(ctx, ps.config.BaseURL, ps.config.Datastore, ps.config.AuthToken, config, ps.config.SkipTLSVerify)
	if err != nil {
		return nil, fmt.Errorf("PBS H2 connect: %w", err)
	}

	return &pbsSession{
		store:    ps,
		proto:    &h2Protocol{conn: h2Conn},
		config:   config,
		compress: ps.compress,
		chunkCfg: ps.chunkCfg,
		files:    make([]datastore.BackupFileInfo, 0),
	}, nil
}

// pbsSession implements BackupSession for PBS.
type pbsSession struct {
	proto       pbsBackupProtocol
	store       *PBSStore
	knownChunks map[[32]byte]bool
	files       []datastore.BackupFileInfo
	config      BackupConfig
	chunkCfg    buzhash.Config
	compress    bool
}

func (s *pbsSession) UploadArchive(ctx context.Context, name string, data io.Reader) (*UploadResult, error) {
	// Register previous chunks on the server so dynamicIndexAppend can reference them.
	// downloadPrevious both downloads the index AND registers all chunk digests in
	// the server's session registry. Without this, skipped (cached) chunks would be
	// rejected by dynamic_append as unknown.
	if s.knownChunks == nil {
		s.knownChunks = make(map[[32]byte]bool)

		if s.config.PreviousBackup != nil {
			prev := s.config.PreviousBackup

			// Register previous chunks server-side via the backup protocol.
			if prevData, err := s.proto.downloadPrevious(name); err == nil {
				if idx, err := datastore.ParseDynamicIndex(prevData); err == nil {
					for i := 0; i < idx.Count(); i++ {
						if info, ok := idx.ChunkInfo(i); ok {
							s.knownChunks[info.Digest] = true
						}
					}
				}
			} else {
				// Fallback: use reader protocol if backup protocol's previous
				// endpoint is unavailable (e.g., first backup in new namespace).
				// In this case all chunks will be uploaded fresh.
				data, err := s.store.ReadPreviousArchive(ctx, prev.BackupType, prev.BackupID, prev.BackupTime, prev.Namespace, name)
				if err == nil {
					if idx, err := datastore.ParseDynamicIndex(data); err == nil {
						for i := 0; i < idx.Count(); i++ {
							if info, ok := idx.ChunkInfo(i); ok {
								s.knownChunks[info.Digest] = true
							}
						}
					}
				}
			}
		}
	}

	wid, err := s.proto.dynamicIndexCreate(name)
	if err != nil {
		return nil, err
	}

	chunker := buzhash.NewChunker(data, s.chunkCfg)
	idx := datastore.NewDynamicIndexWriter(time.Now().Unix())

	digests := make([]string, 0, 64)
	offsets := make([]uint64, 0, 64)

	// Batch size for dynamicIndexAppend to avoid "Request body too large" errors.
	// PBS has a limit on the append body size; 1024 entries per batch is safe.
	const appendBatchSize = 1024

	var hexBuf [64]byte
	var (
		totalSize  uint64
		chunkCount int
	)

	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("chunk: %w", err)
		}

		digest := chunkDigest(chunk, s.config.CryptConfig)

		chunkOffset := totalSize
		totalSize += uint64(len(chunk))
		chunkCount++

		idx.Add(totalSize, digest)

		hex.Encode(hexBuf[:], digest[:])
		hexDigest := string(hexBuf[:])

		// Deduplication: only upload if chunk is not in our local cache
		exists := s.knownChunks != nil && s.knownChunks[digest]

		if !exists {
			blobData, bp, err := borrowChunkBlob(chunk, s.compress, s.config.CryptConfig)
			if err != nil {
				return nil, err
			}

			err = s.proto.dynamicChunkUpload(wid, hexDigest, len(chunk), len(blobData), blobData)
			datastore.PutBlobBuf(bp)
			if err != nil {
				return nil, err
			}

			// Add to cache so we don't upload the same chunk twice in this session
			if s.knownChunks != nil {
				s.knownChunks[digest] = true
			}
		}

		digests = append(digests, hexDigest)
		offsets = append(offsets, chunkOffset)

		// Flush batch if we've reached the batch size limit
		if len(digests) >= appendBatchSize {
			if err := s.proto.dynamicIndexAppend(wid, digests, offsets); err != nil {
				return nil, fmt.Errorf("append index batch: %w", err)
			}
			digests = digests[:0]
			offsets = offsets[:0]
		}
	}

	if _, err := idx.Finish(); err != nil {
		return nil, fmt.Errorf("finish index: %w", err)
	}

	// Append chunk references to PBS dynamic index
	if chunkCount > 0 {
		if err := s.proto.dynamicIndexAppend(wid, digests, offsets); err != nil {
			return nil, err
		}
	}

	// Use the index writer's compute_csum which matches PBS's expected format:
	// SHA256(end_offset_LE || chunk_digest || ...) over all entries.
	indexDigest := idx.Csum()
	pbsChecksum := hex.EncodeToString(indexDigest[:])
	if err := s.proto.dynamicIndexClose(wid, chunkCount, totalSize, pbsChecksum); err != nil {
		return nil, err
	}

	result := &UploadResult{
		Filename: name,
		Size:     totalSize,
		Digest:   indexDigest,
	}

	addFileInfo(&s.files, name, totalSize, indexDigest, string(s.config.CryptMode))

	return result, nil
}

func (s *pbsSession) UploadPayloadInterleaved(ctx context.Context, name string, newData io.Reader, injections <-chan InjectChunks) (*UploadResult, error) {
	if s.knownChunks == nil {
		s.knownChunks = make(map[[32]byte]bool, 16)
	}

	if _, err := s.proto.downloadPrevious(name); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: download previous for chunk registration: %v\n", err)
	}

	wid, err := s.proto.dynamicIndexCreate(name)
	if err != nil {
		return nil, err
	}

	sink := &pbsPayloadSink{
		session:      s,
		proto:        s.proto,
		wid:          wid,
		idx:          datastore.NewDynamicIndexWriter(time.Now().Unix()),
		batchDigests: make([]string, 0, pbsAppendBatchSize),
		batchOffsets: make([]uint64, 0, pbsAppendBatchSize),
	}
	totalSize, err := interleavePayload(s.chunkCfg, newData, injections, sink)
	if err != nil {
		return nil, err
	}
	if err := sink.flushBatch(); err != nil {
		return nil, fmt.Errorf("append final batch: %w", err)
	}

	if _, err := sink.idx.Finish(); err != nil {
		return nil, fmt.Errorf("finish index: %w", err)
	}
	indexDigest := sink.idx.Csum()
	pbsChecksum := hex.EncodeToString(indexDigest[:])
	if err := s.proto.dynamicIndexClose(wid, sink.chunkCount, totalSize, pbsChecksum); err != nil {
		return nil, err
	}

	result := &UploadResult{
		Filename: name,
		Size:     totalSize,
		Digest:   indexDigest,
	}
	addFileInfo(&s.files, name, totalSize, indexDigest, string(s.config.CryptMode))
	return result, nil
}

// pbsPayloadSink implements payloadSink for the PBS backup protocol. It uploads
// unknown chunks, batches dynamic-index appends, and records every chunk (new,
// dedup-known, and injected) in one shared DynamicIndexWriter so the index and
// its checksum match proxmox-backup's compute_csum (end_offset || digest).
type pbsPayloadSink struct {
	session *pbsSession
	proto   pbsBackupProtocol
	wid     uint64
	idx     *datastore.DynamicIndexWriter

	batchDigests []string
	batchOffsets []uint64
	chunkCount   int

	localKnown map[[32]byte]bool
	hexBuf     [64]byte
}

const pbsAppendBatchSize = 1024

func (s *pbsPayloadSink) flushBatch() error {
	if len(s.batchDigests) == 0 {
		return nil
	}
	if err := s.proto.dynamicIndexAppend(s.wid, s.batchDigests, s.batchOffsets); err != nil {
		return err
	}
	s.batchDigests = s.batchDigests[:0]
	s.batchOffsets = s.batchOffsets[:0]
	return nil
}

// appendIndex records one chunk at [offset, offset+size) in the shared index.
// The local index stores end offsets (matching PBS add_chunk); the server
// appends the start offset. This keeps the index checksum identical to PBS.
func (s *pbsPayloadSink) appendIndex(offset uint64, digest [32]byte, size uint64) error {
	endOffset := offset + size
	s.idx.Add(endOffset, digest)
	hex.Encode(s.hexBuf[:], digest[:])
	s.batchDigests = append(s.batchDigests, string(s.hexBuf[:]))
	s.batchOffsets = append(s.batchOffsets, offset)
	s.chunkCount++
	if len(s.batchDigests) >= pbsAppendBatchSize {
		return s.flushBatch()
	}
	return nil
}

func (s *pbsPayloadSink) putRaw(offset uint64, raw []byte) error {
	if len(raw) == 0 {
		return nil
	}
	if s.localKnown == nil {
		s.localKnown = make(map[[32]byte]bool, len(s.session.knownChunks))
		for d := range s.session.knownChunks {
			s.localKnown[d] = true
		}
	}
	digest := chunkDigest(raw, s.session.config.CryptConfig)
	if s.localKnown[digest] || s.session.knownChunks[digest] {
		return s.appendIndex(offset, digest, uint64(len(raw)))
	}
	blob, bp, err := borrowChunkBlob(raw, s.session.compress, s.session.config.CryptConfig)
	if err != nil {
		return err
	}
	hex.Encode(s.hexBuf[:], digest[:])
	err = s.proto.dynamicChunkUpload(s.wid, string(s.hexBuf[:]), len(raw), len(blob), blob)
	datastore.PutBlobBuf(bp)
	if err != nil {
		return err
	}
	s.localKnown[digest] = true
	s.session.knownChunks[digest] = true
	return s.appendIndex(offset, digest, uint64(len(raw)))
}

func (s *pbsPayloadSink) putInjection(offset uint64, inj InjectChunks) error {
	cur := offset
	for _, c := range inj.Chunks {
		s.session.knownChunks[c.Digest] = true
		if err := s.appendIndex(cur, c.Digest, c.Size); err != nil {
			return err
		}
		cur += c.Size
	}
	return s.flushBatch()
}
func (s *pbsSession) UploadSplitArchive(ctx context.Context, metadataName string, metadataData io.Reader, payloadName string, payloadData io.Reader) (*SplitArchiveResult, error) {
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

func (s *pbsSession) UploadBlob(_ context.Context, name string, data []byte) error {
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

	if err := s.proto.blobUpload(name, len(blobData), blobData); err != nil {
		return err
	}

	digest := sha256.Sum256(blobData)
	addFileInfo(&s.files, name, uint64(len(blobData)), digest, string(s.config.CryptMode))

	return nil
}

// ReadPreviousArchive reads an archive file from a previous PBS backup snapshot.
func (ps *PBSStore) ReadPreviousArchive(ctx context.Context, backupType datastore.BackupType, backupID string, backupTime int64, namespace, filename string) ([]byte, error) {
	cfg := PBSConfig{
		BaseURL:       ps.config.BaseURL,
		Datastore:     ps.config.Datastore,
		AuthToken:     ps.config.AuthToken,
		SkipTLSVerify: ps.config.SkipTLSVerify,
		Namespace:     namespace,
	}
	reader := NewPBSReader(cfg, backupType.String(), backupID, backupTime)
	if err := reader.Connect(ctx); err != nil {
		return nil, fmt.Errorf("connect reader: %w", err)
	}
	defer reader.Close()
	return reader.DownloadFile(filename)
}

// pbsSnapshotSource implements PreviousSnapshotSource for PBS.
type pbsSnapshotSource struct {
	reader *PBSReader
}

func (ps *pbsSnapshotSource) ReadArchive(filename string) ([]byte, error) {
	return ps.reader.DownloadFile(filename)
}

func (ps *pbsSnapshotSource) ChunkSource() datastore.ChunkSource {
	return ps.reader.AsChunkSource()
}

func (ps *pbsSnapshotSource) Close() error {
	return ps.reader.Close()
}

// NewPreviousSnapshotSource creates a PreviousSnapshotSource connected to a PBS snapshot.
func (ps *PBSStore) NewPreviousSnapshotSource(ctx context.Context, backupType datastore.BackupType, backupID string, backupTime int64, namespace string) (PreviousSnapshotSource, error) {
	cfg := PBSConfig{
		BaseURL:       ps.config.BaseURL,
		Datastore:     ps.config.Datastore,
		AuthToken:     ps.config.AuthToken,
		SkipTLSVerify: ps.config.SkipTLSVerify,
		Namespace:     namespace,
	}
	reader := NewPBSReader(cfg, backupType.String(), backupID, backupTime)
	if err := reader.Connect(ctx); err != nil {
		return nil, fmt.Errorf("connect reader: %w", err)
	}
	return &pbsSnapshotSource{reader: reader}, nil
}

func (s *pbsSession) Finish(_ context.Context) (*datastore.Manifest, error) {
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

	manifestData, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("marshal manifest: %w", err)
	}

	// Manifest is never encrypted — PBS must be able to read it.
	// In encrypt mode, data chunks are encrypted but the manifest is
	// only signed (HMAC-SHA256) and compressed.
	blob, err := datastore.EncodeCompressedBlob(manifestData)
	if err != nil {
		return nil, fmt.Errorf("encode manifest blob: %w", err)
	}
	manifestBlobBytes := blob.Bytes()

	if err := s.proto.blobUpload("index.json.blob", len(manifestBlobBytes), manifestBlobBytes); err != nil {
		return nil, fmt.Errorf("upload manifest: %w", err)
	}

	if err := s.proto.finish(); err != nil {
		return nil, err
	}

	s.proto.close()
	return manifest, nil
}
