package backupproxy

import (
	"bytes"
	"fmt"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/accessor"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
)

// CatalogEntry holds metadata from a previous backup's .mpxar archive,
// used for metadata change detection.
type CatalogEntry struct {
	Path          string
	Stat          format.Stat
	Metadata      pxar.Metadata
	FileSize      uint64
	PayloadOffset uint64
	IsRegularFile bool
}

// Catalog is a map from normalized file path to CatalogEntry.
type Catalog map[string]*CatalogEntry

// BuildCatalog constructs a metadata catalog from a previous backup's
// .mpxar.didx and .ppxar.didx indexes. It restores the metadata stream
// from chunks and walks it with the accessor to extract file entries.
func BuildCatalog(metaIdx *datastore.DynamicIndexReader, chunkSource datastore.ChunkSource) (Catalog, error) {
	var metaBuf bytes.Buffer
	restorer := datastore.NewRestorer(chunkSource)
	if err := restorer.RestoreFile(metaIdx, &metaBuf); err != nil {
		return nil, fmt.Errorf("restore metadata stream: %w", err)
	}

	acc := accessor.NewAccessor(bytes.NewReader(metaBuf.Bytes()))
	root, err := acc.ReadRoot()
	if err != nil {
		return nil, fmt.Errorf("read root entry: %w", err)
	}

	catalog := make(Catalog)
	catalog["/"] = &CatalogEntry{
		Path:          "/",
		Stat:          root.Metadata.Stat,
		Metadata:      root.Metadata,
		IsRegularFile: false,
	}

	contentOffset := int64(root.ContentOffset)
	if err := walkCatalogDir(acc, "/", contentOffset, catalog); err != nil {
		return nil, err
	}

	return catalog, nil
}

func walkCatalogDir(acc *accessor.Accessor, dirPath string, dirOffset int64, catalog Catalog) error {
	entries, err := acc.ListDirectory(dirOffset)
	if err != nil {
		return fmt.Errorf("list directory %q: %w", dirPath, err)
	}

	for _, entry := range entries {
		// Build normalized path
		var entryPath string
		if dirPath != "/" {
			entryPath = dirPath + "/" + entry.Path
		} else {
			entryPath = "/" + entry.Path
		}

		catEntry := &CatalogEntry{
			Path:          entryPath,
			Stat:          entry.Metadata.Stat,
			Metadata:      entry.Metadata,
			PayloadOffset: entry.PayloadOffset,
			FileSize:      entry.FileSize,
			IsRegularFile: entry.IsRegularFile(),
		}
		catalog[entryPath] = catEntry

		if entry.IsDir() {
			// entry.ContentOffset points to where children start
			if err := walkCatalogDir(acc, entryPath, int64(entry.ContentOffset), catalog); err != nil {
				return err
			}
		}
	}

	return nil
}

// EntryMatches checks if a current directory entry matches a catalog entry
// for metadata change detection. Returns true if the file metadata hasn't changed.
// Compares stat fields, file type, size, xattrs, ACLs, FCaps, and QuotaProjectID.
func EntryMatches(current DirEntry, catalogMetadata pxar.Metadata, prev *CatalogEntry) bool {
	if prev == nil {
		return false
	}

	if current.Stat.FileType() != prev.Stat.FileType() {
		return false
	}

	if current.Stat.IsRegularFile() && current.Size != prev.FileSize {
		return false
	}

	if !current.Stat.MetadataEqual(prev.Stat) {
		return false
	}

	if !catalogMetadata.MetadataEqual(prev.Metadata) {
		return false
	}

	return true
}
