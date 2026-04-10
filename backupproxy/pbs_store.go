package backupproxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
)

// PBSConfig holds configuration for connecting to a Proxmox Backup Server.
type PBSConfig struct {
	BaseURL       string // PBS API base URL (e.g. "https://pbs:8007/api2/json")
	Datastore     string // target datastore name
	AuthToken     string // PBS API token ("TOKENID:SECRET")
	SkipTLSVerify bool   // disable TLS certificate verification
}

// PBSRemoteStore implements RemoteStore via the PBS HTTP API.
type PBSRemoteStore struct {
	client   *http.Client
	config   PBSConfig
	chunkCfg buzhash.Config
	compress bool
}

// NewPBSRemoteStore creates a PBS remote store with the given configuration.
func NewPBSRemoteStore(config PBSConfig, chunkCfg buzhash.Config, compress bool) *PBSRemoteStore {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if config.SkipTLSVerify {
		transport.TLSClientConfig.InsecureSkipVerify = true
	}
	return &PBSRemoteStore{
		client:   &http.Client{Transport: transport},
		config:   config,
		chunkCfg: chunkCfg,
		compress: compress,
	}
}

func (ps *PBSRemoteStore) doRequest(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, ps.config.BaseURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "PBSAPIToken "+ps.config.AuthToken)
	if body != nil {
		req.Header.Set("Content-Type", "application/octet-stream")
	}
	return ps.client.Do(req)
}

// StartSession starts a backup session on PBS.
func (ps *PBSRemoteStore) StartSession(ctx context.Context, config BackupConfig) (BackupSession, error) {
	path := fmt.Sprintf("/admin/datastore/%s/backup?backup-type=%s&backup-id=%s&backup-time=%d",
		ps.config.Datastore, config.BackupType.String(), config.BackupID, config.BackupTime)

	resp, err := ps.doRequest(ctx, http.MethodPost, path, nil)
	if err != nil {
		return nil, fmt.Errorf("start backup: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("start backup: HTTP %d: %s", resp.StatusCode, body)
	}

	return &pbsSession{
		store:    ps,
		config:   config,
		compress: ps.compress,
		files:    make([]datastore.FileInfo, 0),
	}, nil
}

// pbsSession implements BackupSession for PBS.
type pbsSession struct {
	store    *PBSRemoteStore
	config   BackupConfig
	compress bool
	files    []datastore.FileInfo
}

func (s *pbsSession) UploadArchive(ctx context.Context, name string, data io.Reader) (*UploadResult, error) {
	chunker := buzhash.NewChunker(data, s.store.chunkCfg)
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

		exists, err := s.chunkExists(ctx, digest)
		if err != nil {
			return nil, fmt.Errorf("check chunk: %w", err)
		}

		if !exists {
			var blobData []byte
			if s.compress {
				blob, err := datastore.EncodeCompressedBlob(chunk)
				if err != nil {
					return nil, fmt.Errorf("compress chunk: %w", err)
				}
				blobData = blob.Bytes()
			} else {
				blob, err := datastore.EncodeBlob(chunk)
				if err != nil {
					return nil, fmt.Errorf("encode chunk: %w", err)
				}
				blobData = blob.Bytes()
			}

			if err := s.uploadChunk(ctx, digest, blobData); err != nil {
				return nil, fmt.Errorf("upload chunk: %w", err)
			}
		}

		totalOffset += uint64(len(chunk))
		idx.Add(totalOffset, digest)
	}

	raw, err := idx.Finish()
	if err != nil {
		return nil, fmt.Errorf("finish index: %w", err)
	}

	if err := s.uploadFile(ctx, name, raw); err != nil {
		return nil, fmt.Errorf("upload index: %w", err)
	}

	indexDigest := sha256.Sum256(raw)

	result := &UploadResult{
		Filename: name,
		Size:     uint64(len(raw)),
		Digest:   indexDigest,
	}

	s.files = append(s.files, datastore.FileInfo{
		Filename: name,
		Size:     uint64(len(raw)),
		CSum:     hex.EncodeToString(indexDigest[:]),
	})

	return result, nil
}

func (s *pbsSession) chunkExists(ctx context.Context, digest [32]byte) (bool, error) {
	path := fmt.Sprintf("/admin/datastore/%s/chunk?digest=%s",
		s.store.config.Datastore, hex.EncodeToString(digest[:]))

	resp, err := s.store.doRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK, nil
}

func (s *pbsSession) uploadChunk(ctx context.Context, digest [32]byte, data []byte) error {
	path := fmt.Sprintf("/admin/datastore/%s/chunk?digest=%s&encoded-size=%d",
		s.store.config.Datastore, hex.EncodeToString(digest[:]), len(data))

	resp, err := s.store.doRequest(ctx, http.MethodPut, path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
	}
	return nil
}

func (s *pbsSession) uploadFile(ctx context.Context, name string, data []byte) error {
	path := fmt.Sprintf("/admin/datastore/%s/blob?file=%s",
		s.store.config.Datastore, name)

	resp, err := s.store.doRequest(ctx, http.MethodPut, path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upload %q: HTTP %d: %s", name, resp.StatusCode, body)
	}
	return nil
}

func (s *pbsSession) UploadBlob(ctx context.Context, name string, data []byte) error {
	if err := s.uploadFile(ctx, name, data); err != nil {
		return fmt.Errorf("upload blob: %w", err)
	}

	digest := sha256.Sum256(data)
	s.files = append(s.files, datastore.FileInfo{
		Filename: name,
		Size:     uint64(len(data)),
		CSum:     hex.EncodeToString(digest[:]),
	})

	return nil
}

func (s *pbsSession) Finish(ctx context.Context) (*datastore.Manifest, error) {
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

	path := fmt.Sprintf("/admin/datastore/%s/finish", s.store.config.Datastore)
	resp, err := s.store.doRequest(ctx, http.MethodPost, path, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("finish backup: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("finish backup: HTTP %d: %s", resp.StatusCode, body)
	}

	return manifest, nil
}
