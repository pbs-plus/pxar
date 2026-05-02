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

type catalogBuilder struct {
	w        *datastore.CatalogWriter
	dirStack []string
}

func newCatalogBuilder(buf *bytes.Buffer) *catalogBuilder {
	cb := &catalogBuilder{
		w: datastore.NewCatalogWriter(buf),
	}
	cb.w.StartDirectory("")
	return cb
}

func (cb *catalogBuilder) finish() error {
	return cb.w.Finish()
}

func (cb *catalogBuilder) startDir(name string) {
	cb.dirStack = append(cb.dirStack, name)
	cb.w.StartDirectory(name)
}

func (cb *catalogBuilder) endDir() {
	cb.dirStack = cb.dirStack[:len(cb.dirStack)-1]
	cb.w.EndDirectory()
}

func (cb *catalogBuilder) addFile(name string, size uint64, mtime int64) {
	cb.w.AddFile(name, size, mtime)
}

func (cb *catalogBuilder) addSymlink(name string) {
	cb.w.AddSymlink(name)
}

func (cb *catalogBuilder) addBlockDevice(name string) {
	cb.w.AddBlockDevice(name)
}

func (cb *catalogBuilder) addCharDevice(name string) {
	cb.w.AddCharDevice(name)
}

func (cb *catalogBuilder) addFifo(name string) {
	cb.w.AddFifo(name)
}

func (cb *catalogBuilder) addSocket(name string) {
	cb.w.AddSocket(name)
}

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
	Manifest        *datastore.Manifest
	TotalBytes      int64
	FileCount       int
	DirCount        int
	Duration        time.Duration
	CatalogUploaded bool
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
	var catBuf bytes.Buffer
	catBuilder := newCatalogBuilder(&catBuf)
	if err := s.walkDir(ctx, root, enc, nil, catBuilder, result); err != nil {
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

	if err := catBuilder.finish(); err != nil {
		return nil, fmt.Errorf("finish catalog: %w", err)
	}
	if catBuf.Len() > 0 {
		if _, err := sess.UploadArchive(ctx, "catalog.pcat1.didx", bytes.NewReader(catBuf.Bytes())); err != nil {
			return nil, fmt.Errorf("upload catalog: %w", err)
		}
		result.CatalogUploaded = true
	}

	manifest, err := sess.Finish(ctx)
	if err != nil {
		return nil, fmt.Errorf("finish session: %w", err)
	}
	result.Manifest = manifest
	result.Duration = time.Since(start)

	return result, nil
}

// RunSplitBackup executes a full pull backup using the split archive format (v2, data mode).
// The encoder writes metadata and payload to buffers first, then uploads them
// sequentially via UploadSplitArchive. This avoids the io.Pipe deadlock that
// occurs when UploadSplitArchive reads one stream at a time while the encoder
// writes both simultaneously.
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

	var metaBuf, payloadBuf bytes.Buffer
	rootMeta := &pxar.Metadata{Stat: rootStat}
	enc := encoder.NewEncoder(&metaBuf, &payloadBuf, rootMeta, nil)

	result := &BackupResult{}
	var catBuf bytes.Buffer
	catBuilder := newCatalogBuilder(&catBuf)
	if err := s.walkDir(ctx, root, enc, nil, catBuilder, result); err != nil {
		return nil, err
	}

	if err := enc.Close(); err != nil {
		return nil, fmt.Errorf("close encoder: %w", err)
	}

	splitResult, err := sess.UploadSplitArchive(ctx,
		"root.mpxar.didx", &metaBuf,
		"root.ppxar.didx", &payloadBuf,
	)
	if err != nil {
		return nil, fmt.Errorf("upload split archive: %w", err)
	}

	result.TotalBytes = int64(splitResult.MetadataResult.Size + splitResult.PayloadResult.Size)

	if err := catBuilder.finish(); err != nil {
		return nil, fmt.Errorf("finish catalog: %w", err)
	}
	if catBuf.Len() > 0 {
		if _, err := sess.UploadArchive(ctx, "catalog.pcat1.didx", bytes.NewReader(catBuf.Bytes())); err != nil {
			return nil, fmt.Errorf("upload catalog: %w", err)
		}
		result.CatalogUploaded = true
	}

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

	// Create a snapshot source for reading previous backup data
	var snapSrc PreviousSnapshotSource
	var err error
	if prev.Dir != "" {
		snapSrc, err = NewPreviousSnapshotSourceFromDir(prev.Dir)
	} else {
		snapSrc, err = s.store.NewPreviousSnapshotSource(ctx, prev.BackupType, prev.BackupID, prev.BackupTime, prev.Namespace)
	}
	if err != nil {
		return nil, fmt.Errorf("create snapshot source: %w", err)
	}
	defer snapSrc.Close()

	// Download previous snapshot's indexes
	metaIdxData, err := snapSrc.ReadArchive("root.mpxar.didx")
	if err != nil {
		return nil, fmt.Errorf("read previous metadata index: %w", err)
	}
	payloadIdxData, err := snapSrc.ReadArchive("root.ppxar.didx")
	if err != nil {
		return nil, fmt.Errorf("read previous payload index: %w", err)
	}

	metaIdx, err := datastore.ReadDynamicIndex(metaIdxData)
	if err != nil {
		return nil, fmt.Errorf("parse previous metadata index: %w", err)
	}
	payloadIdx, err := datastore.ReadDynamicIndex(payloadIdxData)
	if err != nil {
		return nil, fmt.Errorf("parse previous payload index: %w", err)
	}

	// Build the metadata catalog from previous backup
	catalog, err := BuildCatalog(metaIdx, snapSrc.ChunkSource())
	if err != nil {
		return nil, fmt.Errorf("build metadata catalog: %w", err)
	}

	// Build a restorer for the previous payload stream
	restorer := datastore.NewRestorer(snapSrc.ChunkSource())

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

	var metaBuf, payloadBuf bytes.Buffer
	rootMeta := &pxar.Metadata{Stat: rootStat}
	enc := encoder.NewEncoder(&metaBuf, &payloadBuf, rootMeta, nil)

	result := &BackupResult{}
	mw := &metadataWalker{
		server:     s,
		catalog:    catalog,
		payloadIdx: payloadIdx,
		restorer:   restorer,
		ctx:        ctx,
	}
	var catBuf bytes.Buffer
	catBuilder := newCatalogBuilder(&catBuf)
	if err := s.walkDir(ctx, root, enc, mw, catBuilder, result); err != nil {
		return nil, err
	}

	if err := enc.Close(); err != nil {
		return nil, fmt.Errorf("close encoder: %w", err)
	}

	splitResult, err := sess.UploadSplitArchive(ctx,
		"root.mpxar.didx", &metaBuf,
		"root.ppxar.didx", &payloadBuf,
	)
	if err != nil {
		return nil, fmt.Errorf("upload split archive: %w", err)
	}

	result.TotalBytes = int64(splitResult.MetadataResult.Size + splitResult.PayloadResult.Size)

	if err := catBuilder.finish(); err != nil {
		return nil, fmt.Errorf("finish catalog: %w", err)
	}
	if catBuf.Len() > 0 {
		if _, err := sess.UploadArchive(ctx, "catalog.pcat1.didx", bytes.NewReader(catBuf.Bytes())); err != nil {
			return nil, fmt.Errorf("upload catalog: %w", err)
		}
		result.CatalogUploaded = true
	}

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
func (mw *metadataWalker) maybeReusePayload(enc *encoder.Encoder, name, fullPath string, current DirEntry, currentMeta pxar.Metadata) (bool, error) {
	// Look up path in the catalog
	prev, ok := mw.catalog[fullPath]
	if !ok {
		return false, nil
	}

	// Check if metadata matches (stat + xattrs + ACLs + FCaps)
	if !EntryMatches(current, currentMeta, prev) {
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

func (s *Server) walkDir(ctx context.Context, dirPath string, enc *encoder.Encoder, mw *metadataWalker, catBuilder *catalogBuilder, result *BackupResult) error {
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

		// Collect extended metadata (xattrs, ACLs, FCaps)
		if xattrs, err := s.client.GetXAttrs(ctx, fullPath); err == nil {
			meta.XAttrs = xattrs
		}
		if acl, err := s.client.GetACL(ctx, fullPath); err == nil {
			meta.ACL = acl
		}
		if fcaps, err := s.client.GetFCaps(ctx, fullPath); err == nil {
			if len(fcaps) > 0 {
				meta.FCaps = fcaps
			}
		}

		// Propagate extended metadata to DirEntry for change detection
		entry.XAttrs = meta.XAttrs
		entry.ACL = meta.ACL
		entry.FCaps = meta.FCaps
		entry.QuotaProjectID = meta.QuotaProjectID

		switch {
		case entry.Stat.IsDir():
			result.DirCount++
			catBuilder.startDir(entry.Name)
			if err := enc.CreateDirectory(entry.Name, meta); err != nil {
				return fmt.Errorf("create dir %q: %w", entry.Name, err)
			}
			if err := s.walkDir(ctx, fullPath, enc, mw, catBuilder, result); err != nil {
				return err
			}
			if err := enc.Finish(); err != nil {
				return fmt.Errorf("finish dir %q: %w", entry.Name, err)
			}
			catBuilder.endDir()

		case entry.Stat.IsRegularFile():
			result.FileCount++
			catBuilder.addFile(entry.Name, entry.Size, entry.Stat.Mtime.Secs)
			if mw != nil {
				if reused, err := mw.maybeReusePayload(enc, entry.Name, fullPath, entry, *meta); reused || err != nil {
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
			catBuilder.addSymlink(entry.Name)
			if err := enc.AddSymlink(meta, entry.Name, target); err != nil {
				return fmt.Errorf("symlink %q: %w", entry.Name, err)
			}

		case entry.Stat.IsDevice():
			if entry.Stat.IsCharDev() {
				catBuilder.addCharDevice(entry.Name)
			} else {
				catBuilder.addBlockDevice(entry.Name)
			}
			if err := enc.AddDevice(meta, entry.Name, format.Device{}); err != nil {
				return fmt.Errorf("device %q: %w", entry.Name, err)
			}

		case entry.Stat.IsFIFO():
			catBuilder.addFifo(entry.Name)
			if err := enc.AddFIFO(meta, entry.Name); err != nil {
				return fmt.Errorf("fifo %q: %w", entry.Name, err)
			}

		case entry.Stat.IsSocket():
			catBuilder.addSocket(entry.Name)
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
