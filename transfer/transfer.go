package transfer

import (
	"fmt"
	"path/filepath"
	"strings"

	pxar "github.com/pbs-plus/pxar"
)

// Copy copies specific files from the source archive to the target writer.
// Each path in paths is looked up in the source and written to the target.
// Paths should be relative to the archive root (e.g., "etc/hosts", "var/log/syslog").
func Copy(src ArchiveReader, dst ArchiveWriter, paths []string, opts TransferOption) error {
	for _, p := range paths {
		entry, err := src.Lookup(p)
		if err != nil {
			return fmt.Errorf("lookup %q in source: %w", p, err)
		}

		if entry.IsDir() {
			// Copy entire directory tree
			if err := copyDir(src, dst, entry, opts); err != nil {
				return err
			}
		} else {
			if err := copyEntry(src, dst, entry, opts); err != nil {
				return err
			}
		}

		if opts.ProgressCallback != nil {
			opts.ProgressCallback(p, 0)
		}
	}
	return nil
}

// CopyTree copies a directory tree from srcPath in the source archive to
// dstPath in the target. If dstPath is empty, entries are copied to the same
// path in the target.
func CopyTree(src ArchiveReader, dst ArchiveWriter, srcPath, dstPath string, opts TransferOption) error {
	root, err := src.Lookup(srcPath)
	if err != nil {
		return fmt.Errorf("lookup %q in source: %w", srcPath, err)
	}

	if !root.IsDir() {
		return fmt.Errorf("source path %q is not a directory", srcPath)
	}

	return walkAndCopy(src, dst, root, srcPath, dstPath, opts)
}

// Merge merges the entire source archive tree into the target.
// All entries from the source are written to the target.
func Merge(src ArchiveReader, dst ArchiveWriter, opts TransferOption) error {
	root, err := src.ReadRoot()
	if err != nil {
		return fmt.Errorf("read source root: %w", err)
	}

	entries, err := src.ListDirectory(int64(root.ContentOffset))
	if err != nil {
		return fmt.Errorf("list source root: %w", err)
	}

	for i := range entries {
		child := &entries[i]
		if child.IsDir() {
			if err := copyDir(src, dst, child, opts); err != nil {
				return err
			}
		} else {
			if err := copyEntry(src, dst, child, opts); err != nil {
				return err
			}
		}
	}
	return nil
}

// copyEntry copies a single non-directory entry to the target.
func copyEntry(src ArchiveReader, dst ArchiveWriter, entry *pxar.Entry, opts TransferOption) error {
	var content []byte
	if entry.IsRegularFile() {
		var err error
		content, err = src.ReadFileContent(entry)
		if err != nil {
			return fmt.Errorf("read file content for %q: %w", entry.Path, err)
		}
	}
	return dst.WriteEntry(entry, content)
}

// copyDir copies a directory and all its contents to the target.
func copyDir(src ArchiveReader, dst ArchiveWriter, dir *pxar.Entry, opts TransferOption) error {
	if err := dst.BeginDirectory(dir.FileName(), &dir.Metadata); err != nil {
		return fmt.Errorf("begin directory %q: %w", dir.Path, err)
	}

	entries, err := src.ListDirectory(int64(dir.ContentOffset))
	if err != nil {
		return fmt.Errorf("list directory %q: %w", dir.Path, err)
	}

	for i := range entries {
		child := &entries[i]
		if child.IsDir() {
			if err := copyDir(src, dst, child, opts); err != nil {
				return err
			}
		} else {
			if err := copyEntry(src, dst, child, opts); err != nil {
				return err
			}
		}
	}

	return dst.EndDirectory()
}

// walkAndCopy walks a source directory tree and copies entries to dst,
// remapping paths from srcPath to dstPath.
func walkAndCopy(src ArchiveReader, dst ArchiveWriter, root *pxar.Entry, srcPath, dstPath string, opts TransferOption) error {
	dirName := root.FileName()
	if dstPath != "" {
		// Use the destination path's basename as the directory name
		dirName = filepath.Base(dstPath)
	}

	if err := dst.BeginDirectory(dirName, &root.Metadata); err != nil {
		return fmt.Errorf("begin directory %q: %w", dirName, err)
	}

	entries, err := src.ListDirectory(int64(root.ContentOffset))
	if err != nil {
		return fmt.Errorf("list directory %q: %w", root.Path, err)
	}

	for i := range entries {
		child := &entries[i]
		// Remap path for the target
		if dstPath != "" {
			childPath := child.Path
			if strings.HasPrefix(childPath, srcPath) {
				child.Path = dstPath + strings.TrimPrefix(childPath, srcPath)
			}
		}

		if child.IsDir() {
			if err := copyDir(src, dst, child, opts); err != nil {
				return err
			}
		} else {
			if err := copyEntry(src, dst, child, opts); err != nil {
				return err
			}
		}
	}

	return dst.EndDirectory()
}