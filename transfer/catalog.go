package transfer

import (
	"fmt"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/accessor"
)

// readCatalog extracts the full directory tree as a flat list of CatalogEntry
// values using minimal decoding. It walks the tree via ListDirectory
// with Minimal: true, avoiding payload reads entirely.
func readCatalog(r ArchiveReader) ([]CatalogEntry, error) {
	root, err := r.ReadRoot()
	if err != nil {
		return nil, fmt.Errorf("read root: %w", err)
	}

	var entries []CatalogEntry
	err = catalogDir(r, root, "/", &entries)
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func catalogDir(r ArchiveReader, dir *pxar.Entry, parentPath string, out *[]CatalogEntry) error {
	*out = append(*out, CatalogEntry{
		Path:       dir.Path,
		Kind:       dir.Kind,
		FileSize:   dir.FileSize,
		ParentPath: parentPath,
	})

	if !dir.IsDir() {
		return nil
	}

	dirPath := dir.Path
	if dirPath != "/" {
		dirPath = dirPath + "/"
	}

	err := r.ListDirectory(int64(dir.ContentOffset), accessor.ListOption{Minimal: true}, func(child *pxar.Entry) error {
		childPath := dirPath + child.Path

		if child.IsDir() {
			// Copy the entry since we modify its path for recursion
			childCopy := *child
			childCopy.Path = childPath
			if err := catalogDir(r, &childCopy, dirPath, out); err != nil {
				return err
			}
		} else {
			*out = append(*out, CatalogEntry{
				Path:       childPath,
				Kind:       child.Kind,
				FileSize:   child.FileSize,
				ParentPath: dirPath,
			})
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("list directory %q: %w", dir.Path, err)
	}

	return nil
}

// ReadCatalogOn is a convenience function that extracts a catalog from any
// ArchiveReader. It is equivalent to calling r.ReadCatalog().
func ReadCatalogOn(r ArchiveReader) ([]CatalogEntry, error) {
	return r.ReadCatalog()
}

// Compile-time check: pxar.Entry kind helpers are available for catalogDir.
var _ pxar.EntryKind
