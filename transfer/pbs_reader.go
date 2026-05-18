package transfer

import (
	"context"
	"fmt"
	"io"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/accessor"
	"github.com/pbs-plus/pxar/backupproxy"
)

// PBSReader reads archives from a PBS remote store.
// It downloads the index file(s) and reconstructs the archive stream
// using chunks from the PBS reader protocol.
type PBSReader struct {
	inner ArchiveReader
	pbs   *backupproxy.PBSReader
}

// PBSReaderConfig holds the configuration for opening a PBS archive.
type PBSReaderConfig struct {
	BackupType  string
	BackupID    string
	ArchiveName string
	MetaName    string
	PayloadName string
	Config      backupproxy.PBSConfig
	BackupTime  int64
	MetaOnly    bool
}

// NewPBSReader creates a reader for a PBS remote archive.
// For v1 archives, set ArchiveName. For v2 split archives, set MetaName and PayloadName.
func NewPBSReader(ctx context.Context, cfg PBSReaderConfig) (*PBSReader, error) {
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
			inner, err = NewSplitReaderMetaOnly(metaIdxData, pbs.AsChunkSource())
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

			inner, err = NewSplitReader(metaIdxData, payloadIdxData, pbs.AsChunkSource())
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

		inner, err = NewChunkedReader(idxData, pbs.AsChunkSource())
		if err != nil {
			pbs.Close()
			return nil, fmt.Errorf("create chunked reader: %w", err)
		}
	} else {
		pbs.Close()
		return nil, fmt.Errorf("must specify ArchiveName (v1) or MetaName+PayloadName (v2)")
	}

	return &PBSReader{
		inner: inner,
		pbs:   pbs,
	}, nil
}

func (r *PBSReader) ReadRoot() (*pxar.Entry, error) {
	return r.inner.ReadRoot()
}

func (r *PBSReader) Lookup(path string) (*pxar.Entry, error) {
	return r.inner.Lookup(path)
}

func (r *PBSReader) ListDirectory(dirOffset int64, opts accessor.ListOption, fn func(*pxar.Entry) error) error {
	return r.inner.ListDirectory(dirOffset, opts, fn)
}

func (r *PBSReader) ReadFileContentReader(entry *pxar.Entry) (io.ReadCloser, error) {
	return r.inner.ReadFileContentReader(entry)
}

func (r *PBSReader) ReadEntryAt(offset int64) (*pxar.Entry, error) {
	return r.inner.ReadEntryAt(offset)
}

func (r *PBSReader) ReadCatalog(fn func(CatalogEntry) error) error {
	return readCatalog(r.inner, fn)
}

func (r *PBSReader) Close() error {
	var err error
	if closeErr := r.inner.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if closeErr := r.pbs.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	return err
}
