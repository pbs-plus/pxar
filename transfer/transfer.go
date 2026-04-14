package transfer

import (
	"fmt"
	"strings"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/format"
)

// Copy copies files from the source archive to the target writer.
// Each PathMapping specifies a source path (inside the source archive) and
// a destination path (inside the target archive). If the source entry is a
// directory, the entire subtree is copied with paths remapped from Src to Dst
// prefix. If the source entry is a file, the file is written with its path
// remapped.
func Copy(src ArchiveReader, dst ArchiveWriter, mappings []PathMapping, opts TransferOption) error {
	for _, m := range mappings {
		entry, err := src.Lookup(m.Src)
		if err != nil {
			return fmt.Errorf("lookup %q in source: %w", m.Src, err)
		}

		if entry.IsDir() {
			if err := CopyTree(src, dst, m.Src, m.Dst, opts); err != nil {
				return err
			}
		} else {
			// Remap entry path from Src to Dst prefix
			if m.Dst != "" && m.Dst != m.Src {
				entry.Path = m.Dst + strings.TrimPrefix(entry.Path, m.Src)
			}
			if err := copyEntry(src, dst, entry, opts); err != nil {
				return err
			}
		}

		if opts.ProgressCallback != nil {
			opts.ProgressCallback(m.Src, 0)
		}
	}
	return nil
}

// CopyTree copies a directory tree from srcPath in the source archive to
// dstPath in the target. All entries under the source directory have their
// paths remapped from the srcPath prefix to the dstPath prefix.
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

// copyDir copies a directory and all its contents to the target,
// remapping paths from srcPath prefix to dstPath prefix.
func copyDir(src ArchiveReader, dst ArchiveWriter, dir *pxar.Entry, srcPath, dstPath string, opts TransferOption) error {
	if err := dst.BeginDirectory(dir.FileName(), &dir.Metadata); err != nil {
		return fmt.Errorf("begin directory %q: %w", dir.Path, err)
	}

	entries, err := src.ListDirectory(int64(dir.ContentOffset))
	if err != nil {
		return fmt.Errorf("list directory %q: %w", dir.Path, err)
	}

	for i := range entries {
		child := &entries[i]
		// Remap path for the target
		if dstPath != "" && dstPath != srcPath {
			childPath := child.Path
			if strings.HasPrefix(childPath, srcPath) {
				child.Path = dstPath + strings.TrimPrefix(childPath, srcPath)
			}
		}

		if child.IsDir() {
			if err := copyDir(src, dst, child, srcPath, dstPath, opts); err != nil {
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
	entries, err := src.ListDirectory(int64(root.ContentOffset))
	if err != nil {
		return fmt.Errorf("list directory %q: %w", root.Path, err)
	}

	// When srcPath is "/", we're copying from the archive root — don't create
	// a new directory since the target writer already has one from Begin.
	if srcPath == "/" {
		for i := range entries {
			child := &entries[i]
			if dstPath != "" && dstPath != "/" {
				childPath := child.Path
				if strings.HasPrefix(childPath, srcPath) {
					child.Path = dstPath + strings.TrimPrefix(childPath, srcPath)
				}
			}

			if child.IsDir() {
				if err := copyDir(src, dst, child, srcPath, dstPath, opts); err != nil {
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

	// Create intermediate directories for dstPath components that don't exist
	// in srcPath. For example, CopyTree("/a", "/backup/a") needs to create
	// the "backup" directory before creating "a" inside it.
	//
	// src="/a"      → srcParts = ["a"]
	// dst="/backup/a" → dstParts = ["backup", "a"]
	// Extra ancestors = dstParts[0:len(dstParts)-len(srcParts)] = ["backup"]
	srcParts := splitPath(srcPath)
	dstParts := splitPath(dstPath)
	extraAncestors := len(dstParts) - len(srcParts)
	if extraAncestors < 0 {
		extraAncestors = 0
	}

	// Create ancestor directories that exist in dstPath but not in srcPath
	for i := 0; i < extraAncestors; i++ {
		if err := dst.BeginDirectory(dstParts[i], &pxar.Metadata{
			Stat: format.Stat{Mode: format.ModeIFDIR | 0o755},
		}); err != nil {
			return fmt.Errorf("begin directory %q: %w", dstParts[i], err)
		}
	}

	// Create the leaf directory (the source directory, possibly renamed)
	dirName := root.FileName()
	if dstPath != srcPath && len(dstParts) > 0 {
		dirName = dstParts[len(dstParts)-1]
	}
	if err := dst.BeginDirectory(dirName, &root.Metadata); err != nil {
		return fmt.Errorf("begin directory %q: %w", dirName, err)
	}

	for i := range entries {
		child := &entries[i]
		if dstPath != "" && dstPath != srcPath {
			childPath := child.Path
			if strings.HasPrefix(childPath, srcPath) {
				child.Path = dstPath + strings.TrimPrefix(childPath, srcPath)
			}
		}

		if child.IsDir() {
			if err := copyDir(src, dst, child, srcPath, dstPath, opts); err != nil {
				return err
			}
		} else {
			if err := copyEntry(src, dst, child, opts); err != nil {
				return err
			}
		}
	}

	if err := dst.EndDirectory(); err != nil {
		return fmt.Errorf("end directory: %w", err)
	}

	// Close ancestor directories
	for i := 0; i < extraAncestors; i++ {
		if err := dst.EndDirectory(); err != nil {
			return fmt.Errorf("end directory: %w", err)
		}
	}

	return nil
}

// splitPath splits a path like "/backup/etc" into ["backup", "etc"].
func splitPath(p string) []string {
	p = strings.TrimPrefix(p, "/")
	p = strings.TrimSuffix(p, "/")
	if p == "" {
		return nil
	}
	return strings.Split(p, "/")
}