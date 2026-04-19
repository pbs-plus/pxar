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
	return walkTreeInternal(reader, rootPath, WalkOption{}, fn)
}

// WalkTreeWith walks a directory tree with the given options.
// When opts.MetaOnly is true, file content is never read and content is always nil.
// When opts.Filter is non-zero, entries not matching the filter are skipped.
func WalkTreeWith(reader ArchiveReader, rootPath string, opts WalkOption, fn WalkFunc) error {
	return walkTreeInternal(reader, rootPath, opts, fn)
}

// WalkTreeMeta walks a directory tree in metadata-only mode with a simplified
// callback. Content is never read, and the filter mask controls which entry
// types are visited. Use WalkAll to visit all types.
func WalkTreeMeta(reader ArchiveReader, rootPath string, filter WalkFilter, fn MetaWalkFunc) error {
	opts := WalkOption{MetaOnly: true, Filter: filter}
	return walkTreeInternal(reader, rootPath, opts, func(entry *pxar.Entry, _ []byte) error {
		return fn(entry)
	})
}

func walkTreeInternal(reader ArchiveReader, rootPath string, opts WalkOption, fn WalkFunc) error {
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

	// Wrap the callback to handle SkipCount
	remaining := opts.SkipCount
	walkFn := fn
	if remaining > 0 {
		walkFn = func(entry *pxar.Entry, content []byte) error {
			if remaining > 0 {
				remaining--
				return nil
			}
			return fn(entry, content)
		}
	}

	// For non-directory roots, filter applies directly.
	if !root.IsDir() {
		if opts.Filter != 0 && !opts.Filter.matches(root.Kind) {
			return nil
		}
		var content []byte
		if !opts.MetaOnly && root.IsRegularFile() {
			content, err = reader.ReadFileContent(root)
			if err != nil {
				return fmt.Errorf("read file content: %w", err)
			}
		}
		return walkFn(root, content)
	}

	// Root is a directory — check if it should be yielded to the callback.
	if opts.Filter == 0 || opts.Filter.matches(root.Kind) {
		return walkDir(reader, root, opts, walkFn)
	}
	// Filtered out of callback but still descend for children.
	return walkDirDescend(reader, root, opts, walkFn)
}

func walkDir(reader ArchiveReader, dir *pxar.Entry, opts WalkOption, fn WalkFunc) error {
	// Call fn for the directory itself
	if err := fn(dir, nil); err != nil {
		if err == ErrSkipDir {
			return nil
		}
		return err
	}

	return walkDirChildren(reader, dir, opts, fn)
}

// walkDirDescend descends into a directory's children without invoking the
// callback for the directory itself. Used when the directory is filtered out
// but its children may still be relevant.
func walkDirDescend(reader ArchiveReader, dir *pxar.Entry, opts WalkOption, fn WalkFunc) error {
	return walkDirChildren(reader, dir, opts, fn)
}

func walkDirChildren(reader ArchiveReader, dir *pxar.Entry, opts WalkOption, fn WalkFunc) error {

	// List children
	entries, err := reader.ListDirectory(int64(dir.ContentOffset))
	if err != nil {
		return fmt.Errorf("list directory: %w", err)
	}

	// Build full paths for children
	for i := range entries {
		if dir.Path == "/" {
			entries[i].Path = "/" + entries[i].Path
		} else {
			entries[i].Path = dir.Path + "/" + entries[i].Path
		}
	}

	for i := range entries {
		child := &entries[i]

		if child.IsDir() {
			// Always descend into directories. Only invoke the callback if
			// the directory's kind passes the filter.
			if opts.Filter == 0 || opts.Filter.matches(child.Kind) {
				if err := walkDir(reader, child, opts, fn); err != nil {
					return err
				}
			} else {
				// Filtered out of callback but still descend for children.
				if err := walkDirDescend(reader, child, opts, fn); err != nil {
					return err
				}
			}
		} else {
			// Skip non-directory entries that don't match the filter.
			if opts.Filter != 0 && !opts.Filter.matches(child.Kind) {
				continue
			}
			var content []byte
			if !opts.MetaOnly && child.IsRegularFile() {
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
