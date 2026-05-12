package transfer

import (
	"context"
	"fmt"
	"io"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/accessor"
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
	BackupType  string
	BackupID    string
	ArchiveName string
	MetaName    string
	PayloadName string
	Config      backupproxy.PBSConfig
	BackupTime  int64
	MetaOnly    bool
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

func (r *PBSArchiveReader) ListDirectory(dirOffset int64, opts accessor.ListOption, fn func(*pxar.Entry) error) error {
	return r.inner.ListDirectory(dirOffset, opts, fn)
}

func (r *PBSArchiveReader) ReadFileContent(entry *pxar.Entry) ([]byte, error) {
	return r.inner.ReadFileContent(entry)
}

func (r *PBSArchiveReader) ReadFileContentReader(entry *pxar.Entry) (io.ReadCloser, error) {
	return r.inner.ReadFileContentReader(entry)
}

func (r *PBSArchiveReader) ReadCatalog(fn func(CatalogEntry) error) error {
	return readCatalog(r.inner, fn)
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
