package transfer

import (
	"context"
	"fmt"
	"io"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
)

// PBSArchiveReader reads archives from a PBS remote store.
// It downloads the index file(s) and reconstructs the archive stream
// using chunks from the PBS reader protocol.
type PBSArchiveReader struct {
	inner ArchiveReader
	pbs   *backupproxy.PBSReader
}

// PBSArchiveConfig holds the configuration for opening a PBS archive.
type PBSArchiveConfig struct {
	// Config is the PBS connection configuration.
	Config backupproxy.PBSConfig
	// BackupType is the backup type (e.g., "host", "vm").
	BackupType string
	// BackupID is the backup ID.
	BackupID string
	// BackupTime is the backup timestamp.
	BackupTime int64
	// ArchiveName is the filename of the archive in the backup snapshot.
	// For v1: "root.pxar.didx"
	// For v2: use MetaName + PayloadName
	ArchiveName string
	// MetaName is the metadata stream filename for split archives (v2).
	MetaName string
	// PayloadName is the payload stream filename for split archives (v2).
	PayloadName string

	// MetaOnly skips downloading the payload stream entirely for v2 split
	// archives. When true, only the metadata index (.mpxar.didx) is
	// downloaded. This halves data transfer for browsing/indexing operations
	// that never touch file content. ReadFileContent and ReadFileContentReader
	// will return errors for files with PayloadOffset > 0.
	MetaOnly bool
}

// NewPBSArchiveReader creates a reader for a PBS remote archive.
// For v1 archives, set ArchiveName. For v2 split archives, set MetaName and PayloadName.
func NewPBSArchiveReader(ctx context.Context, cfg PBSArchiveConfig) (*PBSArchiveReader, error) {
	pbs := backupproxy.NewPBSReader(cfg.Config, cfg.BackupType, cfg.BackupID, cfg.BackupTime)
	if err := pbs.Connect(ctx); err != nil {
		return nil, fmt.Errorf("connect to PBS: %w", err)
	}

	var inner ArchiveReader

	if cfg.MetaName != "" && cfg.PayloadName != "" {
		// v2 split archive
		metaIdxData, err := pbs.DownloadFile(cfg.MetaName)
		if err != nil {
			pbs.Close()
			return nil, fmt.Errorf("download metadata index: %w", err)
		}

		if cfg.MetaOnly {
			// MetaOnly: skip payload download entirely. Use metadata-only reader.
			inner, err = NewSplitArchiveReaderMetaOnly(metaIdxData, pbs.AsChunkSource())
			if err != nil {
				pbs.Close()
				return nil, fmt.Errorf("create meta-only reader: %w", err)
			}
		} else {
			payloadIdxData, err := pbs.DownloadFile(cfg.PayloadName)
			if err != nil {
				pbs.Close()
				return nil, fmt.Errorf("download payload index: %w", err)
			}

			inner, err = NewSplitArchiveReader(metaIdxData, payloadIdxData, pbs.AsChunkSource())
			if err != nil {
				pbs.Close()
				return nil, fmt.Errorf("create split reader: %w", err)
			}
		}
	} else if cfg.ArchiveName != "" {
		// v1 archive
		idxData, err := pbs.DownloadFile(cfg.ArchiveName)
		if err != nil {
			pbs.Close()
			return nil, fmt.Errorf("download index: %w", err)
		}

		inner, err = NewChunkedArchiveReader(idxData, pbs.AsChunkSource())
		if err != nil {
			pbs.Close()
			return nil, fmt.Errorf("create chunked reader: %w", err)
		}
	} else {
		pbs.Close()
		return nil, fmt.Errorf("must specify ArchiveName (v1) or MetaName+PayloadName (v2)")
	}

	return &PBSArchiveReader{
		inner: inner,
		pbs:   pbs,
	}, nil
}

func (r *PBSArchiveReader) ReadRoot() (*pxar.Entry, error) {
	return r.inner.ReadRoot()
}

func (r *PBSArchiveReader) Lookup(path string) (*pxar.Entry, error) {
	return r.inner.Lookup(path)
}

func (r *PBSArchiveReader) LookupBatch(paths []string) ([]*pxar.Entry, error) {
	return r.inner.LookupBatch(paths)
}

func (r *PBSArchiveReader) ListDirectory(dirOffset int64) ([]pxar.Entry, error) {
	return r.inner.ListDirectory(dirOffset)
}

func (r *PBSArchiveReader) ListDirectoryWithOptions(dirOffset int64, opts ListOption) ([]pxar.Entry, error) {
	return r.inner.ListDirectoryWithOptions(dirOffset, opts)
}

func (r *PBSArchiveReader) ReadFileContent(entry *pxar.Entry) ([]byte, error) {
	return r.inner.ReadFileContent(entry)
}

func (r *PBSArchiveReader) ReadFileContentReader(entry *pxar.Entry) (io.ReadCloser, error) {
	return r.inner.ReadFileContentReader(entry)
}

func (r *PBSArchiveReader) ReadCatalog() ([]CatalogEntry, error) {
	return r.inner.ReadCatalog()
}

func (r *PBSArchiveReader) Close() error {
	var err error
	if closeErr := r.inner.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if closeErr := r.pbs.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	return err
}

// ensure context is used
var _ context.Context
