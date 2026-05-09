package transfer

import (
	"fmt"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/accessor"
)

// readCatalog streams the full directory tree via a callback with minimal
// decoding. For each entry, fn is called; if fn returns a non-nil error,
// iteration stops and the error is returned.
func readCatalog(r ArchiveReader, fn func(CatalogEntry) error) error {
	root, err := r.ReadRoot()
	if err != nil {
		return fmt.Errorf("read root: %w", err)
	}
	return catalogDir(r, root, "/", fn)
}

func catalogDir(r ArchiveReader, dir *pxar.Entry, parentPath string, fn func(CatalogEntry) error) error {
	if err := fn(CatalogEntry{
		Path:       dir.Path,
		Kind:       dir.Kind,
		FileSize:   dir.FileSize,
		ParentPath: parentPath,
	}); err != nil {
		return err
	}

	if !dir.IsDir() {
		return nil
	}

	dirPath := dir.Path
	if dirPath != "/" {
		dirPath = dirPath + "/"
	}

	return r.ListDirectory(int64(dir.ContentOffset), accessor.ListOption{Minimal: true}, func(child *pxar.Entry) error {
		childPath := dirPath + child.Path
		if child.IsDir() {
			childCopy := *child
			childCopy.Path = childPath
			return catalogDir(r, &childCopy, dirPath, fn)
		}
		return fn(CatalogEntry{
			Path:       childPath,
			Kind:       child.Kind,
			FileSize:   child.FileSize,
			ParentPath: dirPath,
		})
	})
}

// Compile-time check: pxar.Entry kind helpers are available for catalogDir.
var _ pxar.EntryKind
