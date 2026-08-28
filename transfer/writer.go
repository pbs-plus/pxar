package transfer

import (
	"fmt"
	"io"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
)

// Options configures how an ArchiveWriter creates archives.
type Options struct {
	Prelude []byte
	Format  format.FormatVersion
}

// ArchiveWriter provides unified write access to any pxar archive format.
type ArchiveWriter interface {
	// Begin starts writing to a new archive with the given root metadata.
	Begin(rootMeta *pxar.Metadata, opts Options) error

	// WriteEntry writes an entry (file, symlink, device, etc.) to the archive.
	// For regular files, content is the file data. For other types, content may be nil.
	WriteEntry(entry *pxar.Entry, content []byte) error

	// WriteEntryRef writes an entry that references existing payload data
	// without writing the payload itself. The payloadOffset is the byte offset
	// in the original payload stream. Used for chunk-level deduplication.
	WriteEntryRef(entry *pxar.Entry, payloadOffset uint64) error

	// WriteEntryReader writes a file entry with content streamed from r.
	// size is the total byte count. For non-file entries (symlink, device,
	// fifo, socket), r and size are ignored and content is nil.
	// The caller must ensure r provides exactly size bytes.
	WriteEntryReader(entry *pxar.Entry, r io.Reader, size uint64) error

	// BeginDirectory pushes a directory context.
	BeginDirectory(name string, meta *pxar.Metadata) error

	// EndDirectory pops a directory context.
	EndDirectory() error

	// InjectChunks injects existing chunks at the current encoder position.
	// The encoder's payload write position is advanced by the total size.
	InjectChunks(chunks []backupproxy.KnownChunkRef) error

	// Encoder returns the underlying encoder for payload position queries.
	Encoder() *encoder.Encoder

	// Finish finalizes the archive.
	Finish() error

	// Close releases resources.
	Close() error
}

// StreamWriter writes a pxar archive to one or two io.Writer streams.
// For v1 format, only output is used. For v2 format, both output and payloadOut
// are used.
type StreamWriter struct {
	output     io.Writer
	payloadOut io.Writer
	enc        *encoder.Encoder
	closers    []io.Closer
	opts       Options
	dirDepth   int
	lastOffset encoder.LinkOffset
	hasOffset  bool
}

// NewStreamWriter creates a writer for v1 (unified) format.
func NewStreamWriter(output io.Writer) *StreamWriter {
	return &StreamWriter{
		output: output,
	}
}

// NewSplitStreamWriter creates a writer for v2 (split) format.
func NewSplitStreamWriter(output, payloadOut io.Writer) *StreamWriter {
	return &StreamWriter{
		output:     output,
		payloadOut: payloadOut,
	}
}

func (w *StreamWriter) Begin(rootMeta *pxar.Metadata, opts Options) error {
	w.opts = opts
	var prelude []byte
	if len(opts.Prelude) > 0 {
		prelude = opts.Prelude
	}

	w.enc = encoder.NewEncoder(w.output, w.payloadOut, rootMeta, prelude)
	w.dirDepth = 1
	w.lastOffset, w.hasOffset = 0, false
	return nil
}

func (w *StreamWriter) WriteEntry(entry *pxar.Entry, content []byte) error {
	if w.enc == nil {
		return fmt.Errorf("writer not initialized, call Begin first")
	}

	name := entry.FileName()

	switch entry.Kind {
	case pxar.KindFile:
		offset, err := w.enc.AddFile(&entry.Metadata, name, content)
		if err == nil {
			w.lastOffset, w.hasOffset = offset, true
		}
		return err

	case pxar.KindSymlink:
		return w.enc.AddSymlink(&entry.Metadata, name, entry.LinkTarget)

	case pxar.KindHardlink:
		// Hardlinks need a LinkOffset which is archive-specific.
		// The walker should track offset mappings and use WriteHardlink instead.
		return fmt.Errorf("hardlink write requires WriteHardlink with target offset")

	case pxar.KindDevice:
		return w.enc.AddDevice(&entry.Metadata, name, entry.DeviceInfo)

	case pxar.KindFIFO:
		return w.enc.AddFIFO(&entry.Metadata, name)

	case pxar.KindSocket:
		return w.enc.AddSocket(&entry.Metadata, name)

	default:
		return fmt.Errorf("unsupported entry kind: %v", entry.Kind)
	}
}

func (w *StreamWriter) WriteEntryReader(entry *pxar.Entry, r io.Reader, size uint64) error {
	if w.enc == nil {
		return fmt.Errorf("writer not initialized, call Begin first")
	}
	name := entry.FileName()
	switch entry.Kind {
	case pxar.KindFile:
		fw, err := w.enc.CreateFile(&entry.Metadata, name, size)
		if err != nil {
			return err
		}
		w.lastOffset, w.hasOffset = fw.FileOffset(), true
		if _, err := io.Copy(fw, r); err != nil {
			fw.Close()
			return err
		}
		return fw.Close()
	case pxar.KindSymlink:
		return w.enc.AddSymlink(&entry.Metadata, name, entry.LinkTarget)
	case pxar.KindDevice:
		return w.enc.AddDevice(&entry.Metadata, name, entry.DeviceInfo)
	case pxar.KindFIFO:
		return w.enc.AddFIFO(&entry.Metadata, name)
	case pxar.KindSocket:
		return w.enc.AddSocket(&entry.Metadata, name)
	default:
		return fmt.Errorf("unsupported entry kind: %v", entry.Kind)
	}
}

func (w *StreamWriter) WriteEntryRef(entry *pxar.Entry, payloadOffset uint64) error {
	if w.enc == nil {
		return fmt.Errorf("writer not initialized, call Begin first")
	}
	name := entry.FileName()
	switch entry.Kind {
	case pxar.KindFile:
		offset, err := w.enc.AddPayloadRef(&entry.Metadata, name, entry.FileSize, payloadOffset)
		if err == nil {
			w.lastOffset, w.hasOffset = offset, true
		}
		return err
	case pxar.KindSymlink:
		return w.enc.AddSymlink(&entry.Metadata, name, entry.LinkTarget)
	case pxar.KindDevice:
		return w.enc.AddDevice(&entry.Metadata, name, entry.DeviceInfo)
	case pxar.KindFIFO:
		return w.enc.AddFIFO(&entry.Metadata, name)
	case pxar.KindSocket:
		return w.enc.AddSocket(&entry.Metadata, name)
	default:
		return fmt.Errorf("unsupported entry kind: %v", entry.Kind)
	}
}

// WriteHardlink writes a hard link entry with an explicit target offset.
func (w *StreamWriter) WriteHardlink(name string, target string, targetOffset encoder.LinkOffset) error {
	if w.enc == nil {
		return fmt.Errorf("writer not initialized, call Begin first")
	}
	return w.enc.AddHardlink(name, target, targetOffset)
}

func (w *StreamWriter) LastEntryOffset() (encoder.LinkOffset, bool) {
	return w.lastOffset, w.hasOffset
}

// Encoder returns the underlying encoder for advanced operations.
// This is useful for getting file offsets for hardlink tracking.
func (w *StreamWriter) Encoder() *encoder.Encoder {
	return w.enc
}

func (w *StreamWriter) BeginDirectory(name string, meta *pxar.Metadata) error {
	if w.enc == nil {
		return fmt.Errorf("writer not initialized, call Begin first")
	}
	w.dirDepth++
	return w.enc.CreateDirectory(name, meta)
}

func (w *StreamWriter) EndDirectory() error {
	if w.enc == nil {
		return fmt.Errorf("writer not initialized, call Begin first")
	}
	if w.dirDepth <= 1 {
		return fmt.Errorf("no directory to finish")
	}
	w.dirDepth--
	return w.enc.Finish()
}

func (w *StreamWriter) Finish() error {
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

func (w *StreamWriter) InjectChunks(chunks []backupproxy.KnownChunkRef) error {
	return fmt.Errorf("InjectChunks not supported by StreamWriter")
}

func (w *StreamWriter) Close() error {
	var err error
	for _, c := range w.closers {
		if closeErr := c.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}
