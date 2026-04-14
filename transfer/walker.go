package transfer

import (
	"fmt"

	pxar "github.com/pbs-plus/pxar"
)

// ErrSkipDir can be returned by a WalkFunc to skip a directory's children.
var ErrSkipDir = fmt.Errorf("skip directory")

// WalkTree walks a directory tree from an ArchiveReader, calling fn for each
// entry in encoder-compatible order. For directories, fn is called before
// children are walked. Content is populated for regular files; for other
// entry types, content is nil.
//
// If fn returns ErrSkipDir for a directory entry, the directory's children
// are skipped. If fn returns any other error, walking stops.
func WalkTree(reader ArchiveReader, rootPath string, fn WalkFunc) error {
	var root *pxar.Entry
	var err error

	if rootPath == "" || rootPath == "/" {
		root, err = reader.ReadRoot()
	} else {
		root, err = reader.Lookup(rootPath)
	}
	if err != nil {
		return fmt.Errorf("lookup root path %q: %w", rootPath, err)
	}

	if !root.IsDir() {
		// Single file entry
		var content []byte
		if root.IsRegularFile() {
			content, err = reader.ReadFileContent(root)
			if err != nil {
				return fmt.Errorf("read file content: %w", err)
			}
		}
		return fn(root, content)
	}

	return walkDir(reader, root, fn)
}

func walkDir(reader ArchiveReader, dir *pxar.Entry, fn WalkFunc) error {
	// Call fn for the directory itself
	if err := fn(dir, nil); err != nil {
		if err == ErrSkipDir {
			return nil
		}
		return err
	}

	// List children
	entries, err := reader.ListDirectory(int64(dir.ContentOffset))
	if err != nil {
		return fmt.Errorf("list directory: %w", err)
	}

	for i := range entries {
		child := &entries[i]

		if child.IsDir() {
			if err := walkDir(reader, child, fn); err != nil {
				return err
			}
		} else {
			var content []byte
			if child.IsRegularFile() {
				content, err = reader.ReadFileContent(child)
				if err != nil {
					return fmt.Errorf("read file %q: %w", child.Path, err)
				}
			}
			if err := fn(child, content); err != nil {
				return err
			}
		}
	}

	return nil
}