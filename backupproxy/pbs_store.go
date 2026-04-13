package backupproxy

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"time"

	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
)

// pbsBackupProtocol abstracts PBS backup protocol operations for testability.
type pbsBackupProtocol interface {
	dynamicIndexCreate(archiveName string) (uint64, error)
	dynamicChunkUpload(wid uint64, digest string, size, encodedSize int, data []byte) error
	dynamicIndexAppend(wid uint64, digests []string, offsets []uint64) error
	dynamicIndexClose(wid uint64, chunkCount int, size uint64, csum string) error
	blobUpload(fileName string, encodedSize int, data []byte) error
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

func (p *h2Protocol) dynamicChunkUpload(wid uint64, digest string, size, encodedSize int, data []byte) error {
	params := url.Values{}
	params.Set("wid", strconv.FormatUint(wid, 10))
	params.Set("digest", digest)
	params.Set("size", strconv.Itoa(size))
	params.Set("encoded-size", strconv.Itoa(encodedSize))
	_, err := p.conn.do("POST", "dynamic_chunk", params, data, "application/octet-stream")
	if err != nil {
		return fmt.Errorf("upload chunk: %w", err)
	}
	return nil
}

func (p *h2Protocol) dynamicIndexAppend(wid uint64, digests []string, offsets []uint64) error {
	body := map[string]interface{}{
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
	BaseURL       string // PBS API base URL (e.g. "https://pbs:8007/api2/json")
	Datastore     string // target datastore name
	AuthToken     string // PBS API token ("TOKENID:SECRET")
	SkipTLSVerify bool   // disable TLS certificate verification
	Namespace     string // optional namespace for the backup
}

// PBSRemoteStore implements RemoteStore via the PBS H2 backup protocol.
type PBSRemoteStore struct {
	config   PBSConfig
	chunkCfg buzhash.Config
	compress bool
}

// NewPBSRemoteStore creates a PBS remote store with the given configuration.
func NewPBSRemoteStore(config PBSConfig, chunkCfg buzhash.Config, compress bool) *PBSRemoteStore {
	return &PBSRemoteStore{
		config:   config,
		chunkCfg: chunkCfg,
		compress: compress,
	}
}

// StartSession dials PBS via H2 upgrade and returns a backup session.
func (ps *PBSRemoteStore) StartSession(ctx context.Context, config BackupConfig) (BackupSession, error) {
	h2Conn, err := dialPBSH2(ctx, ps.config.BaseURL, ps.config.Datastore, ps.config.AuthToken, config, ps.config.SkipTLSVerify)
	if err != nil {
		return nil, fmt.Errorf("PBS H2 connect: %w", err)
	}

	return &pbsSession{
		proto:    &h2Protocol{conn: h2Conn},
		config:   config,
		compress: ps.compress,
		chunkCfg: ps.chunkCfg,
		files:    make([]datastore.FileInfo, 0),
	}, nil
}

// pbsSession implements BackupSession for PBS.
type pbsSession struct {
	proto    pbsBackupProtocol
	config   BackupConfig
	compress bool
	chunkCfg buzhash.Config
	files    []datastore.FileInfo
}

func (s *pbsSession) UploadArchive(_ context.Context, name string, data io.Reader) (*UploadResult, error) {
	wid, err := s.proto.dynamicIndexCreate(name)
	if err != nil {
		return nil, err
	}

	chunker := buzhash.NewChunker(data, s.chunkCfg)
	idx := datastore.NewDynamicIndexWriter(time.Now().Unix())
	pbsHash := sha256.New()

	estChunks := 32
	digests := make([]string, 0, estChunks)
	offsets := make([]uint64, 0, estChunks)

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

		digest := sha256.Sum256(chunk)

		blobData, err := encodeChunkBlob(chunk, s.compress)
		if err != nil {
			return nil, err
		}

		chunkOffset := totalSize
		totalSize += uint64(len(chunk))
		chunkCount++

		// Local index for digest computation
		idx.Add(totalSize, digest)

		// PBS running checksum: end_offset (LE) || digest
		var offsetBuf [8]byte
		binary.LittleEndian.PutUint64(offsetBuf[:], totalSize)
		pbsHash.Write(offsetBuf[:])
		pbsHash.Write(digest[:])

		// Upload chunk to PBS
		digestHex := hex.EncodeToString(digest[:])
		if err := s.proto.dynamicChunkUpload(wid, digestHex, len(chunk), len(blobData), blobData); err != nil {
			return nil, err
		}

		digests = append(digests, digestHex)
		offsets = append(offsets, chunkOffset)
	}

	// Finish local index for digest
	raw, err := idx.Finish()
	if err != nil {
		return nil, fmt.Errorf("finish index: %w", err)
	}
	indexDigest := sha256.Sum256(raw)

	// Append chunk references to PBS dynamic index
	if chunkCount > 0 {
		if err := s.proto.dynamicIndexAppend(wid, digests, offsets); err != nil {
			return nil, err
		}
	}

	// Close PBS index
	pbsChecksum := hex.EncodeToString(pbsHash.Sum(nil))
	if err := s.proto.dynamicIndexClose(wid, chunkCount, totalSize, pbsChecksum); err != nil {
		return nil, err
	}

	result := &UploadResult{
		Filename: name,
		Size:     uint64(len(raw)),
		Digest:   indexDigest,
	}

	addFileInfo(&s.files, name, uint64(len(raw)), indexDigest)

	return result, nil
}

func (s *pbsSession) UploadBlob(_ context.Context, name string, data []byte) error {
	blob, err := datastore.EncodeBlob(data)
	if err != nil {
		return fmt.Errorf("encode blob: %w", err)
	}
	blobData := blob.Bytes()

	if err := s.proto.blobUpload(name, len(blobData), blobData); err != nil {
		return err
	}

	digest := sha256.Sum256(blobData)
	addFileInfo(&s.files, name, uint64(len(blobData)), digest)

	return nil
}

func (s *pbsSession) Finish(_ context.Context) (*datastore.Manifest, error) {
	manifest := &datastore.Manifest{
		BackupType: s.config.BackupType.String(),
		BackupID:   s.config.BackupID,
		BackupTime: s.config.BackupTime,
		Files:      s.files,
	}

	// Upload manifest blob before finishing
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("marshal manifest: %w", err)
	}
	manifestBlob, err := datastore.EncodeBlob(manifestData)
	if err != nil {
		return nil, fmt.Errorf("encode manifest blob: %w", err)
	}
	manifestBlobBytes := manifestBlob.Bytes()
	if err := s.proto.blobUpload("index.json.blob", len(manifestBlobBytes), manifestBlobBytes); err != nil {
		return nil, fmt.Errorf("upload manifest: %w", err)
	}

	if err := s.proto.finish(); err != nil {
		return nil, err
	}

	s.proto.close()
	return manifest, nil
}
