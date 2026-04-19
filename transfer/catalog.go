package transfer

import (
	"fmt"

	pxar "github.com/pbs-plus/pxar"
)

// readCatalog extracts the full directory tree as a flat list of CatalogEntry
// values using minimal decoding. It walks the tree via ListDirectoryWithOptions
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

	children, err := r.ListDirectoryWithOptions(int64(dir.ContentOffset), ListOption{Minimal: true})
	if err != nil {
		return fmt.Errorf("list directory %q: %w", dir.Path, err)
	}

	dirPath := dir.Path
	if dirPath != "/" {
		dirPath = dirPath + "/"
	}

	for i := range children {
		child := &children[i]
		childPath := dirPath + child.Path

		if child.IsDir() {
			savedPath := child.Path
			child.Path = childPath
			if err := catalogDir(r, child, dirPath, out); err != nil {
				return err
			}
			child.Path = savedPath
		} else {
			*out = append(*out, CatalogEntry{
				Path:       childPath,
				Kind:       child.Kind,
				FileSize:   child.FileSize,
				ParentPath: dirPath,
			})
		}
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
