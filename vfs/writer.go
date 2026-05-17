package vfs

import (
	"fmt"
	"io"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
)

// VisitFunc is called to get children of a directory.
// Return the list of child entries. For child directories, set the
// Children field to a non-nil VisitFunc to continue recursion.
// Return nil to stop (leaf directory or file).
type VisitFunc func(dirPath string) ([]ChildEntry, error)

// ChildEntry describes one entry to write to the archive.
type ChildEntry struct {
	// Name is the filename (single component, no slashes).
	Name string

	// Kind is the entry type.
	Kind pxar.EntryKind

	// Meta is the entry metadata.
	Meta *pxar.Metadata

	// LinkTarget for symlinks.
	LinkTarget string

	// DeviceInfo for device nodes.
	DeviceInfo format.Device

	// Content for regular files (in-memory). Mutually exclusive with
	// Reader and PayloadOffset.
	Content []byte

	// Reader for streaming file data. Must set Size too.
	// Mutually exclusive with Content and PayloadOffset.
	Reader io.Reader
	Size   uint64

	// PayloadOffset for dedup references. Mutually exclusive with
	// Content and Reader.
	PayloadOffset uint64

	// Children is the VisitFunc for directory entries.
	// If nil, the directory is written empty.
	Children VisitFunc
}

// WriteTree writes a complete pxar archive from a recursive visitor.
// The visitor callback is called for the root's children, then recursively
// for each directory's children. This replaces the manual
// BeginDirectory/EndDirectory push-pop pattern.
//
// Example:
//
//	err := vfs.WriteTree(w, rootMeta, opts, func(dir string) ([]vfs.ChildEntry, error) {
//	    switch dir {
//	    case "/":
//	        return []vfs.ChildEntry{
//	            {Name: "file.txt", Kind: pxar.KindFile, Meta: meta, Content: data},
//	            {Name: "subdir", Kind: pxar.KindDirectory, Meta: dirMeta, Children: subdirVisit},
//	        }, nil
//	    case "subdir":
//	        return []vfs.ChildEntry{
//	            {Name: "nested.txt", Kind: pxar.KindFile, Meta: meta, Reader: r, Size: 42},
//	        }, nil
//	    }
//	    return nil, nil
//	})
func WriteTree(w transfer.ArchiveWriter, rootMeta *pxar.Metadata, opts transfer.WriterOptions, visit VisitFunc) error {
	if err := w.Begin(rootMeta, opts); err != nil {
		return fmt.Errorf("pxar: begin archive: %w", err)
	}
	if err := writeChildren(w, "/", visit); err != nil {
		return err
	}
	return w.Finish()
}

// WalkTree walks a source FileSystem and writes to an ArchiveWriter.
// The visitor callback can modify entries, skip entries, or provide
// alternate data sources (payload refs, new content).
//
// This is the primary way to copy/transform one archive into another.
func WalkTree(src FileSystem, w transfer.ArchiveWriter, rootMeta *pxar.Metadata, opts transfer.WriterOptions, visitor SourceWalkFunc) error {
	if err := w.Begin(rootMeta, opts); err != nil {
		return fmt.Errorf("pxar: begin archive: %w", err)
	}
	if err := walkSourceDir(src, "/", w, visitor); err != nil {
		return err
	}
	return w.Finish()
}

// SourceWalkFunc is called for each entry during a source tree walk.
// The visitor populates the ChildEntry to control what gets written.
// For directories, set Children to control recursion.
// Return io.EOF to stop the entire walk.
type SourceWalkFunc func(srcPath string, info *pxar.FileInfo, entry *ChildEntry) error

func walkSourceDir(src FileSystem, dirPath string, w transfer.ArchiveWriter, visitor SourceWalkFunc) error {
	entries, err := src.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("pxar: readdir %q: %w", dirPath, err)
	}

	for _, de := range entries {
		childPath := joinPath(dirPath, de.Name)
		fi := de.Info
		if fi == nil {
			fi, err = src.Stat(childPath)
			if err != nil {
				return fmt.Errorf("pxar: stat %q: %w", childPath, err)
			}
		}

		ce := ChildEntry{
			Name: de.Name,
			Kind: de.Type,
			Meta: fileInfoToMeta(fi),
		}

		if err := visitor(childPath, fi, &ce); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		if err := writeChildEntry(w, src, childPath, fi, &ce, visitor); err != nil {
			return err
		}
	}
	return nil
}

func writeChildEntry(w transfer.ArchiveWriter, src FileSystem, childPath string, fi *pxar.FileInfo, ce *ChildEntry, visitor SourceWalkFunc) error {
	entry := &pxar.Entry{
		Path:       ce.Name,
		Kind:       ce.Kind,
		Metadata:   derefMeta(ce.Meta),
		LinkTarget: ce.LinkTarget,
		DeviceInfo: ce.DeviceInfo,
		FileSize:   ce.Size,
	}

	switch ce.Kind {
	case pxar.KindDirectory:
		if err := w.BeginDirectory(ce.Name, ce.Meta); err != nil {
			return fmt.Errorf("pxar: begin dir %q: %w", ce.Name, err)
		}
		if ce.Children != nil {
			// Custom visitor for this directory
			if err := writeChildren(w, ce.Name, ce.Children); err != nil {
				return err
			}
		} else {
			// Continue walking source
			if err := walkSourceDir(src, childPath, w, visitor); err != nil {
				return err
			}
		}
		if err := w.EndDirectory(); err != nil {
			return fmt.Errorf("pxar: end dir %q: %w", ce.Name, err)
		}

	case pxar.KindFile:
		if err := writeFileEntry(w, src, childPath, fi, ce, entry); err != nil {
			return err
		}

	case pxar.KindSymlink:
		target, err := src.Readlink(childPath)
		if err != nil {
			return fmt.Errorf("pxar: readlink %q: %w", childPath, err)
		}
		entry.LinkTarget = target
		if err := w.WriteEntry(entry, nil); err != nil {
			return fmt.Errorf("pxar: write symlink %q: %w", ce.Name, err)
		}

	default:
		if err := w.WriteEntry(entry, nil); err != nil {
			return fmt.Errorf("pxar: write entry %q: %w", ce.Name, err)
		}
	}
	return nil
}

func writeFileEntry(w transfer.ArchiveWriter, src FileSystem, childPath string, fi *pxar.FileInfo, ce *ChildEntry, entry *pxar.Entry) error {
	switch {
	case ce.PayloadOffset > 0:
		if err := w.WriteEntryRef(entry, ce.PayloadOffset); err != nil {
			return fmt.Errorf("pxar: write ref %q: %w", ce.Name, err)
		}
	case ce.Reader != nil:
		if err := w.WriteEntryReader(entry, ce.Reader, ce.Size); err != nil {
			return fmt.Errorf("pxar: write file %q: %w", ce.Name, err)
		}
	case ce.Content != nil:
		if err := w.WriteEntry(entry, ce.Content); err != nil {
			return fmt.Errorf("pxar: write file %q: %w", ce.Name, err)
		}
	default:
		fh, err := src.Open(childPath)
		if err != nil {
			return fmt.Errorf("pxar: open %q: %w", childPath, err)
		}
		entry.FileSize = uint64(fi.Size())
		if err := w.WriteEntryReader(entry, fh, uint64(fi.Size())); err != nil {
			_ = fh.Close()
			return fmt.Errorf("pxar: write file %q: %w", ce.Name, err)
		}
		_ = fh.Close()
	}
	return nil
}

func writeChildren(w transfer.ArchiveWriter, dirPath string, visit VisitFunc) error {
	children, err := visit(dirPath)
	if err != nil {
		return fmt.Errorf("pxar: visit %q: %w", dirPath, err)
	}
	for i := range children {
		c := &children[i]
		entry := &pxar.Entry{
			Path:       c.Name,
			Kind:       c.Kind,
			Metadata:   derefMeta(c.Meta),
			LinkTarget: c.LinkTarget,
			DeviceInfo: c.DeviceInfo,
			FileSize:   c.Size,
		}

		switch c.Kind {
		case pxar.KindDirectory:
			if err := w.BeginDirectory(c.Name, c.Meta); err != nil {
				return fmt.Errorf("pxar: begin dir %q: %w", c.Name, err)
			}
			visitFn := c.Children
			if visitFn == nil {
				visitFn = func(string) ([]ChildEntry, error) { return nil, nil }
			}
			if err := writeChildren(w, c.Name, visitFn); err != nil {
				return err
			}
			if err := w.EndDirectory(); err != nil {
				return fmt.Errorf("pxar: end dir %q: %w", c.Name, err)
			}

		case pxar.KindFile:
			switch {
			case c.PayloadOffset > 0:
				if err := w.WriteEntryRef(entry, c.PayloadOffset); err != nil {
					return fmt.Errorf("pxar: write ref %q: %w", c.Name, err)
				}
			case c.Reader != nil:
				if err := w.WriteEntryReader(entry, c.Reader, c.Size); err != nil {
					return fmt.Errorf("pxar: write file %q: %w", c.Name, err)
				}
			case c.Content != nil:
				if err := w.WriteEntry(entry, c.Content); err != nil {
					return fmt.Errorf("pxar: write file %q: %w", c.Name, err)
				}
			default:
				if err := w.WriteEntry(entry, nil); err != nil {
					return fmt.Errorf("pxar: write empty file %q: %w", c.Name, err)
				}
			}

		case pxar.KindSymlink, pxar.KindDevice, pxar.KindFIFO, pxar.KindSocket:
			if err := w.WriteEntry(entry, nil); err != nil {
				return fmt.Errorf("pxar: write entry %q: %w", c.Name, err)
			}
		}
	}
	return nil
}

// StreamTreeWriter is a convenience wrapper around encoder.Encoder that
// implements the VisitFunc-based tree writing pattern without requiring
// a transfer.ArchiveWriter.
type StreamTreeWriter struct {
	enc    *encoder.Encoder
	closed bool
}

// NewStreamTreeWriter creates a tree writer that outputs to the given writers.
// For v2 (split) format, provide both output and payloadOut.
func NewStreamTreeWriter(output, payloadOut io.Writer, rootMeta *pxar.Metadata, prelude []byte) *StreamTreeWriter {
	enc := encoder.NewEncoder(output, payloadOut, rootMeta, prelude)
	return &StreamTreeWriter{enc: enc}
}

// WriteTree writes the archive using a VisitFunc.
func (w *StreamTreeWriter) WriteTree(visit VisitFunc) error {
	if err := w.streamChildren(visit); err != nil {
		return err
	}
	return w.Close()
}

func (w *StreamTreeWriter) streamChildren(visit VisitFunc) error {
	children, err := visit("/")
	if err != nil {
		return fmt.Errorf("pxar: visit root: %w", err)
	}
	return w.streamChildList(children, visit)
}

func (w *StreamTreeWriter) streamChildList(children []ChildEntry, visit VisitFunc) error {
	for i := range children {
		c := &children[i]

		switch c.Kind {
		case pxar.KindDirectory:
			if err := w.enc.CreateDirectory(c.Name, c.Meta); err != nil {
				return fmt.Errorf("pxar: create dir %q: %w", c.Name, err)
			}
			visitFn := c.Children
			if visitFn == nil {
				visitFn = func(string) ([]ChildEntry, error) { return nil, nil }
			}
			subChildren, err := visitFn(c.Name)
			if err != nil {
				return fmt.Errorf("pxar: visit %q: %w", c.Name, err)
			}
			if err := w.streamChildList(subChildren, visit); err != nil {
				return err
			}
			if err := w.enc.Finish(); err != nil {
				return fmt.Errorf("pxar: finish dir %q: %w", c.Name, err)
			}

		case pxar.KindFile:
			switch {
			case c.PayloadOffset > 0:
				if _, err := w.enc.AddPayloadRef(c.Meta, c.Name, c.Size, c.PayloadOffset); err != nil {
					return fmt.Errorf("pxar: add payload ref %q: %w", c.Name, err)
				}
			case c.Reader != nil:
				fw, err := w.enc.CreateFile(c.Meta, c.Name, c.Size)
				if err != nil {
					return fmt.Errorf("pxar: create file %q: %w", c.Name, err)
				}
				if _, err := io.Copy(fw, c.Reader); err != nil {
					_ = fw.Close()
					return fmt.Errorf("pxar: write file %q: %w", c.Name, err)
				}
				if err := fw.Close(); err != nil {
					return fmt.Errorf("pxar: close file %q: %w", c.Name, err)
				}
			case c.Content != nil:
				if _, err := w.enc.AddFile(c.Meta, c.Name, c.Content); err != nil {
					return fmt.Errorf("pxar: add file %q: %w", c.Name, err)
				}
			default:
				if _, err := w.enc.AddFile(c.Meta, c.Name, nil); err != nil {
					return fmt.Errorf("pxar: add empty file %q: %w", c.Name, err)
				}
			}

		case pxar.KindSymlink:
			if err := w.enc.AddSymlink(c.Meta, c.Name, c.LinkTarget); err != nil {
				return fmt.Errorf("pxar: add symlink %q: %w", c.Name, err)
			}
		case pxar.KindDevice:
			if err := w.enc.AddDevice(c.Meta, c.Name, c.DeviceInfo); err != nil {
				return fmt.Errorf("pxar: add device %q: %w", c.Name, err)
			}
		case pxar.KindFIFO:
			if err := w.enc.AddFIFO(c.Meta, c.Name); err != nil {
				return fmt.Errorf("pxar: add fifo %q: %w", c.Name, err)
			}
		case pxar.KindSocket:
			if err := w.enc.AddSocket(c.Meta, c.Name); err != nil {
				return fmt.Errorf("pxar: add socket %q: %w", c.Name, err)
			}
		}
	}
	return nil
}

// Encoder returns the underlying encoder for advanced operations
// (e.g., getting file offsets for hardlink tracking).
func (w *StreamTreeWriter) Encoder() *encoder.Encoder {
	return w.enc
}

// Close finalizes the archive (writes root goodbye table).
func (w *StreamTreeWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	return w.enc.Close()
}

// --- Helpers ---

func derefMeta(m *pxar.Metadata) pxar.Metadata {
	if m == nil {
		return pxar.Metadata{}
	}
	return *m
}

func fileInfoToMeta(fi *pxar.FileInfo) *pxar.Metadata {
	perm := uint64(fi.Mode() & 0o7777)
	var mode uint64
	if fi.IsDir() {
		mode = format.ModeIFDIR | perm
	} else if fi.IsSymlink() {
		mode = format.ModeIFLNK | perm
	} else if fi.IsDevice() {
		mode = format.ModeIFCHR | perm
	} else if fi.IsFifo() {
		mode = format.ModeIFIFO | perm
	} else if fi.IsSocket() {
		mode = format.ModeIFSOCK | perm
	} else {
		mode = format.ModeIFREG | perm
	}

	mt := fi.ModTime()
	return &pxar.Metadata{
		Stat: format.Stat{
			Mode:  mode,
			UID:   fi.UID(),
			GID:   fi.GID(),
			Mtime: format.NewStatxTimestamp(mt.Unix(), uint32(mt.Nanosecond())),
		},
	}
}
