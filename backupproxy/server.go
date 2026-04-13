package backupproxy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
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

type uploadResult struct {
	result *UploadResult
	err    error
}

// RunBackupWithMode dispatches to the appropriate backup method based on
// config.DetectionMode. It uses RunBackup for legacy, RunSplitBackup for
// data, and RunMetadataBackup for metadata mode.
func (s *Server) RunBackupWithMode(ctx context.Context, root string, config BackupConfig) (*BackupResult, error) {
	switch config.DetectionMode {
	case DetectionMetadata:
		return s.RunMetadataBackup(ctx, root, config)
	case DetectionData:
		return s.RunSplitBackup(ctx, root, config)
	case DetectionLegacy:
		fallthrough
	default:
		return s.RunBackup(ctx, root, config)
	}
}

// RunBackup executes a full pull backup using the legacy v1 format (single archive).
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

	pipeReader, pipeWriter := io.Pipe()

	uploadErrCh := make(chan uploadResult, 1)
	go func() {
		result, err := sess.UploadArchive(ctx, "root.pxar.didx", pipeReader)
		uploadErrCh <- uploadResult{result: result, err: err}
	}()

	rootMeta := &pxar.Metadata{Stat: rootStat}
	enc := encoder.NewEncoder(pipeWriter, nil, rootMeta, nil)

	result := &BackupResult{}
	if err := s.walkDir(ctx, root, enc, nil, result); err != nil {
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

// RunSplitBackup executes a full pull backup using the split archive format (v2, data mode).
func (s *Server) RunSplitBackup(ctx context.Context, root string, config BackupConfig) (*BackupResult, error) {
	start := time.Now()

	sess, err := s.store.StartSession(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("start session: %w", err)
	}

	rootStat, err := s.client.Stat(ctx, root)
	if err != nil {
		return nil, fmt.Errorf("stat root: %w", err)
	}

	metaReader, metaWriter := io.Pipe()
	payloadReader, payloadWriter := io.Pipe()

	metaUploadErrCh := make(chan uploadResult, 1)
	go func() {
		result, err := sess.UploadArchive(ctx, "root.mpxar.didx", metaReader)
		metaUploadErrCh <- uploadResult{result: result, err: err}
	}()

	payloadUploadErrCh := make(chan uploadResult, 1)
	go func() {
		result, err := sess.UploadArchive(ctx, "root.ppxar.didx", payloadReader)
		payloadUploadErrCh <- uploadResult{result: result, err: err}
	}()

	rootMeta := &pxar.Metadata{Stat: rootStat}
	enc := encoder.NewEncoder(metaWriter, payloadWriter, rootMeta, nil)

	result := &BackupResult{}
	if err := s.walkDir(ctx, root, enc, nil, result); err != nil {
		metaWriter.CloseWithError(err)
		payloadWriter.CloseWithError(err)
		<-metaUploadErrCh
		<-payloadUploadErrCh
		return nil, err
	}

	if err := enc.Close(); err != nil {
		metaWriter.CloseWithError(err)
		payloadWriter.CloseWithError(err)
		<-metaUploadErrCh
		<-payloadUploadErrCh
		return nil, fmt.Errorf("close encoder: %w", err)
	}
	metaWriter.Close()
	payloadWriter.Close()

	metaUpload := <-metaUploadErrCh
	if metaUpload.err != nil {
		return nil, fmt.Errorf("upload metadata archive: %w", metaUpload.err)
	}
	payloadUpload := <-payloadUploadErrCh
	if payloadUpload.err != nil {
		return nil, fmt.Errorf("upload payload archive: %w", payloadUpload.err)
	}

	result.TotalBytes = int64(metaUpload.result.Size + payloadUpload.result.Size)

	manifest, err := sess.Finish(ctx)
	if err != nil {
		return nil, fmt.Errorf("finish session: %w", err)
	}
	result.Manifest = manifest
	result.Duration = time.Since(start)

	return result, nil
}

// RunMetadataBackup executes an incremental pull backup using metadata change detection.
// It downloads the previous backup's metadata and payload catalogs, compares current
// file metadata against them, and only reads content from the client for files that
// changed. Unchanged files reuse their payload data from the previous backup.
func (s *Server) RunMetadataBackup(ctx context.Context, root string, config BackupConfig) (*BackupResult, error) {
	if config.PreviousBackup == nil {
		return nil, fmt.Errorf("metadata mode requires PreviousBackup to be set")
	}

	prev := config.PreviousBackup

	// Download previous snapshot's metadata and payload indexes
	var metaIdxData, payloadIdxData []byte
	if prev.Dir != "" {
		// Local store: read from directory
		var err error
		metaIdxData, err = ReadPreviousArchiveDir(prev.Dir, "root.mpxar.didx")
		if err != nil {
			return nil, fmt.Errorf("read previous metadata index: %w", err)
		}
		payloadIdxData, err = ReadPreviousArchiveDir(prev.Dir, "root.ppxar.didx")
		if err != nil {
			return nil, fmt.Errorf("read previous payload index: %w", err)
		}
	} else {
		// Remote store (PBS): download via reader protocol
		var err error
		metaIdxData, err = s.store.ReadPreviousArchive(ctx, prev.BackupType, prev.BackupID, prev.BackupTime, prev.Namespace, "root.mpxar.didx")
		if err != nil {
			return nil, fmt.Errorf("download previous metadata index: %w", err)
		}
		payloadIdxData, err = s.store.ReadPreviousArchive(ctx, prev.BackupType, prev.BackupID, prev.BackupTime, prev.Namespace, "root.ppxar.didx")
		if err != nil {
			return nil, fmt.Errorf("download previous payload index: %w", err)
		}
	}

	metaIdx, err := datastore.ReadDynamicIndex(metaIdxData)
	if err != nil {
		return nil, fmt.Errorf("parse previous metadata index: %w", err)
	}
	payloadIdx, err := datastore.ReadDynamicIndex(payloadIdxData)
	if err != nil {
		return nil, fmt.Errorf("parse previous payload index: %w", err)
	}

	// Build a chunk source for restoring previous data
	chunkSource, err := s.buildChunkSource(config, prev)
	if err != nil {
		return nil, fmt.Errorf("build chunk source: %w", err)
	}

	// Build the metadata catalog from previous backup
	catalog, err := BuildCatalog(metaIdx, chunkSource)
	if err != nil {
		return nil, fmt.Errorf("build metadata catalog: %w", err)
	}

	// Build a restorer for the previous payload stream
	restorer := datastore.NewRestorer(chunkSource)

	// Start the backup session
	start := time.Now()
	sess, err := s.store.StartSession(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("start session: %w", err)
	}

	rootStat, err := s.client.Stat(ctx, root)
	if err != nil {
		return nil, fmt.Errorf("stat root: %w", err)
	}

	metaReader, metaWriter := io.Pipe()
	payloadReader, payloadWriter := io.Pipe()

	metaUploadErrCh := make(chan uploadResult, 1)
	go func() {
		result, err := sess.UploadArchive(ctx, "root.mpxar.didx", metaReader)
		metaUploadErrCh <- uploadResult{result: result, err: err}
	}()

	payloadUploadErrCh := make(chan uploadResult, 1)
	go func() {
		result, err := sess.UploadArchive(ctx, "root.ppxar.didx", payloadReader)
		payloadUploadErrCh <- uploadResult{result: result, err: err}
	}()

	rootMeta := &pxar.Metadata{Stat: rootStat}
	enc := encoder.NewEncoder(metaWriter, payloadWriter, rootMeta, nil)

	result := &BackupResult{}
	mw := &metadataWalker{
		server:     s,
		catalog:    catalog,
		payloadIdx: payloadIdx,
		restorer:   restorer,
		ctx:        ctx,
	}
	if err := s.walkDir(ctx, root, enc, mw, result); err != nil {
		metaWriter.CloseWithError(err)
		payloadWriter.CloseWithError(err)
		<-metaUploadErrCh
		<-payloadUploadErrCh
		return nil, err
	}

	if err := enc.Close(); err != nil {
		metaWriter.CloseWithError(err)
		payloadWriter.CloseWithError(err)
		<-metaUploadErrCh
		<-payloadUploadErrCh
		return nil, fmt.Errorf("close encoder: %w", err)
	}
	metaWriter.Close()
	payloadWriter.Close()

	metaUpload := <-metaUploadErrCh
	if metaUpload.err != nil {
		return nil, fmt.Errorf("upload metadata archive: %w", metaUpload.err)
	}
	payloadUpload := <-payloadUploadErrCh
	if payloadUpload.err != nil {
		return nil, fmt.Errorf("upload payload archive: %w", payloadUpload.err)
	}

	result.TotalBytes = int64(metaUpload.result.Size + payloadUpload.result.Size)

	manifest, err := sess.Finish(ctx)
	if err != nil {
		return nil, fmt.Errorf("finish session: %w", err)
	}
	result.Manifest = manifest
	result.Duration = time.Since(start)

	return result, nil
}

// metadataWalker decides for each file whether to reuse its payload
// from the previous backup or read fresh content from the client.
type metadataWalker struct {
	server     *Server
	catalog    Catalog
	payloadIdx *datastore.DynamicIndexReader
	restorer   *datastore.Restorer
	ctx        context.Context
}

// shouldReusePayload checks if a file's metadata matches the catalog entry.
// If so, it writes the previous payload data into the encoder and returns true.
func (mw *metadataWalker) maybeReusePayload(enc *encoder.Encoder, name, fullPath string, current DirEntry) (bool, error) {
	// Look up path in the catalog
	prev, ok := mw.catalog[fullPath]
	if !ok {
		return false, nil
	}

	// Check if metadata matches
	if !EntryMatches(current, prev) {
		return false, nil
	}

	// Only regular files can reuse payload
	if !prev.IsRegularFile {
		return false, nil
	}

	// Restore the file's payload from previous backup
	if prev.FileSize == 0 {
		_, err := enc.AddFile(&prev.Metadata, name, nil)
		return true, err
	}

	var dataBuf bytes.Buffer
	if err := mw.restorer.RestoreRange(mw.payloadIdx, prev.PayloadOffset, prev.FileSize, &dataBuf); err != nil {
		return false, fmt.Errorf("restore payload for %q: %w", fullPath, err)
	}

	_, err := enc.AddFile(&prev.Metadata, name, dataBuf.Bytes())
	return true, err
}

func (s *Server) buildChunkSource(config BackupConfig, prev *PreviousBackupRef) (datastore.ChunkSource, error) {
	if prev.Dir != "" {
		// Local store: use the chunk store from the previous backup's directory
		cs, err := datastore.NewChunkStore(prev.Dir)
		if err != nil {
			return nil, fmt.Errorf("create chunk store: %w", err)
		}
		return datastore.NewChunkStoreSource(cs), nil
	}
	return nil, fmt.Errorf("metadata mode with remote store requires Dir in PreviousBackupRef")
}

func (s *Server) walkDir(ctx context.Context, dirPath string, enc *encoder.Encoder, mw *metadataWalker, result *BackupResult) error {
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
			if err := s.walkDir(ctx, fullPath, enc, mw, result); err != nil {
				return err
			}
			if err := enc.Finish(); err != nil {
				return fmt.Errorf("finish dir %q: %w", entry.Name, err)
			}

		case entry.Stat.IsRegularFile():
			result.FileCount++
			if mw != nil {
				// In metadata mode: try to reuse payload from previous backup
				if reused, err := mw.maybeReusePayload(enc, entry.Name, fullPath, entry); reused || err != nil {
					if err != nil {
						return fmt.Errorf("reuse payload for %q: %w", entry.Name, err)
					}
					continue
				}
			}
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
	data, err := s.client.ReadFile(ctx, fullPath, 0, -1)
	if err != nil {
		return err
	}
	_, err = enc.AddFile(meta, name, data)
	return err
}
