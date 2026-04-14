package transfer

import (
	"fmt"
	"io"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
)

// WriterOptions configures how an ArchiveWriter creates archives.
type WriterOptions struct {
	// Format is the output format version (v1 or v2).
	Format format.FormatVersion
	// Prelude is optional prelude data for v2 archives.
	Prelude []byte
}

// ArchiveWriter provides unified write access to any pxar archive format.
type ArchiveWriter interface {
	// Begin starts writing to a new archive with the given root metadata.
	Begin(rootMeta *pxar.Metadata, opts WriterOptions) error

	// WriteEntry writes an entry (file, symlink, device, etc.) to the archive.
	// For regular files, content is the file data. For other types, content may be nil.
	WriteEntry(entry *pxar.Entry, content []byte) error

	// BeginDirectory pushes a directory context.
	BeginDirectory(name string, meta *pxar.Metadata) error

	// EndDirectory pops a directory context.
	EndDirectory() error

	// Finish finalizes the archive.
	Finish() error

	// Close releases resources.
	Close() error
}

// StreamArchiveWriter writes a pxar archive to one or two io.Writer streams.
// For v1 format, only output is used. For v2 format, both output and payloadOut
// are used.
type StreamArchiveWriter struct {
	output     io.Writer
	payloadOut io.Writer
	enc        *encoder.Encoder
	dirDepth   int
	opts       WriterOptions
	closers    []io.Closer
}

// NewStreamArchiveWriter creates a writer for v1 (unified) format.
func NewStreamArchiveWriter(output io.Writer) *StreamArchiveWriter {
	return &StreamArchiveWriter{
		output: output,
	}
}

// NewSplitStreamArchiveWriter creates a writer for v2 (split) format.
func NewSplitStreamArchiveWriter(output, payloadOut io.Writer) *StreamArchiveWriter {
	return &StreamArchiveWriter{
		output:     output,
		payloadOut: payloadOut,
	}
}

func (w *StreamArchiveWriter) Begin(rootMeta *pxar.Metadata, opts WriterOptions) error {
	w.opts = opts
	var prelude []byte
	if len(opts.Prelude) > 0 {
		prelude = opts.Prelude
	}

	w.enc = encoder.NewEncoder(w.output, w.payloadOut, rootMeta, prelude)
	w.dirDepth = 1 // root directory is implicitly open
	return nil
}

func (w *StreamArchiveWriter) WriteEntry(entry *pxar.Entry, content []byte) error {
	if w.enc == nil {
		return fmt.Errorf("writer not initialized, call Begin first")
	}

	name := entry.FileName()

	switch entry.Kind {
	case pxar.KindFile:
		_, err := w.enc.AddFile(&entry.Metadata, name, content)
		return err

	case pxar.KindSymlink:
		return w.enc.AddSymlink(&entry.Metadata, name, entry.LinkTarget)

	case pxar.KindHardlink:
		// Hardlinks need a LinkOffset which is archive-specific.
		// The walker should track offset mappings and use WriteHardlink instead.
		return fmt.Errorf("hardlink write requires WriteHardlink with target offset")

	case pxar.KindDevice:
		return w.enc.AddDevice(&entry.Metadata, name, entry.DeviceInfo)

	case pxar.KindFifo:
		return w.enc.AddFIFO(&entry.Metadata, name)

	case pxar.KindSocket:
		return w.enc.AddSocket(&entry.Metadata, name)

	default:
		return fmt.Errorf("unsupported entry kind: %v", entry.Kind)
	}
}

// WriteHardlink writes a hard link entry with an explicit target offset.
func (w *StreamArchiveWriter) WriteHardlink(name string, target string, targetOffset encoder.LinkOffset) error {
	if w.enc == nil {
		return fmt.Errorf("writer not initialized, call Begin first")
	}
	return w.enc.AddHardlink(name, target, targetOffset)
}

// Encoder returns the underlying encoder for advanced operations.
// This is useful for getting file offsets for hardlink tracking.
func (w *StreamArchiveWriter) Encoder() *encoder.Encoder {
	return w.enc
}

func (w *StreamArchiveWriter) BeginDirectory(name string, meta *pxar.Metadata) error {
	if w.enc == nil {
		return fmt.Errorf("writer not initialized, call Begin first")
	}
	w.dirDepth++
	return w.enc.CreateDirectory(name, meta)
}

func (w *StreamArchiveWriter) EndDirectory() error {
	if w.enc == nil {
		return fmt.Errorf("writer not initialized, call Begin first")
	}
	if w.dirDepth <= 1 {
		return fmt.Errorf("no directory to finish")
	}
	w.dirDepth--
	return w.enc.Finish()
}

func (w *StreamArchiveWriter) Finish() error {
	if w.enc == nil {
		return fmt.Errorf("writer not initialized, call Begin first")
	}
	// Close remaining directory stack (except root)
	for w.dirDepth > 1 {
		if err := w.enc.Finish(); err != nil {
			return err
		}
		w.dirDepth--
	}
	return w.enc.Close()
}

func (w *StreamArchiveWriter) Close() error {
	var err error
	for _, c := range w.closers {
		if closeErr := c.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}