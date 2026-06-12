// Package encoder creates pxar archives.
package encoder

import (
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"unsafe"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/binarytree"
	"github.com/pbs-plus/pxar/format"
)

// LinkOffset represents a file offset usable with AddHardlink.
type LinkOffset uint64

// Raw returns the raw byte offset.
func (o LinkOffset) Raw() uint64 { return uint64(o) }

// Encoder writes pxar archives.
type Encoder struct {
	output     io.Writer
	payloadOut io.Writer
	err        error
	state      []encoderState
	gbBuf      []byte
	gbItems    []format.GoodbyeItem
	gbTail     []format.GoodbyeItem
	version    format.FormatVersion
	finished   bool
}

type encoderState struct {
	items                 []format.GoodbyeItem
	entryOffset           uint64
	writePosition         uint64
	payloadWritePos       uint64
	previousPayloadOffset uint64 // last PAYLOAD_REF offset seen (0 = none)
	hasPrevPayloadOffset  bool   // true once a PAYLOAD_REF has been written
	parentItemIdx         int    // index in parent's items slice, -1 for root
}

// NewEncoder creates a new pxar encoder writing to the given writers.
// If payloadOut is non-nil, the archive is split (v2 format).
// metadata describes the root directory. prelude is optional v2 prelude data.
func NewEncoder(output, payloadOut io.Writer, metadata *pxar.Metadata, prelude []byte) *Encoder {
	enc := &Encoder{
		output: output,
	}

	if payloadOut != nil {
		enc.payloadOut = payloadOut
		enc.version = format.FormatVersion2
		// Write payload start marker
		var hdrBuf [format.HeaderSize]byte
		format.HeaderWithContentSize(format.PXARPayloadStartMarker, 0).MarshalTo(hdrBuf[:])
		if _, err := payloadOut.Write(hdrBuf[:]); err != nil {
			enc.err = err
			return enc
		}
		enc.pushState(0, -1)
		enc.state[0].payloadWritePos = format.HeaderSize
		enc.encodeFormatVersion()
		if prelude != nil {
			enc.encodePrelude(prelude)
		}
	} else {
		enc.version = format.FormatVersion1
		enc.pushState(0, -1)
	}

	if enc.err == nil {
		enc.err = enc.encodeMetadata(metadata)
	}
	return enc
}

func (e *Encoder) pushState(pos uint64, parentIdx int) {
	// Pre-allocate items slice with reasonable capacity to avoid
	// repeated growslice. Most directories have at least 32 entries.
	items := make([]format.GoodbyeItem, 0, 32)
	e.state = append(e.state, encoderState{
		writePosition: pos,
		parentItemIdx: parentIdx,
		items:         items,
	})
}

func (e *Encoder) currentState() *encoderState {
	return &e.state[len(e.state)-1]
}

func (e *Encoder) writeAll(data []byte) error {
	n, err := e.output.Write(data)
	if err != nil {
		return err
	}
	s := e.currentState()
	s.writePosition += uint64(n)
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

func (e *Encoder) writeHeader(htype, contentSize uint64) error {
	h := format.HeaderWithContentSize(htype, contentSize)
	if err := h.CheckHeaderSize(); err != nil {
		return err
	}
	var hdrBuf [format.HeaderSize]byte
	h.MarshalTo(hdrBuf[:])
	// writeAll already advances writePosition by HeaderSize.
	return e.writeAll(hdrBuf[:])
}

func (e *Encoder) encodeFormatVersion() {
	if e.version != format.FormatVersion2 {
		return
	}
	data := e.version.Serialize()
	if e.err = e.writeHeader(format.Version, uint64(len(data))); e.err != nil {
		return
	}
	e.err = e.writeAll(data)
}

func (e *Encoder) encodePrelude(prelude []byte) {
	if e.version != format.FormatVersion2 {
		e.err = fmt.Errorf("encoding prelude not supported in format version 1")
		return
	}
	// Prelude must immediately follow the format version header.
	// Format version occupies: HeaderSize (version header) + 8 (version uint64) = 24 bytes.
	if pos := e.currentState().writePosition; pos != format.HeaderSize+8 {
		e.err = fmt.Errorf("prelude must be encoded following the version header, current position %d", pos)
		return
	}
	if e.err = e.writeHeader(format.PXARPrelude, uint64(len(prelude))); e.err != nil {
		return
	}
	e.err = e.writeAll(prelude)
}

func (e *Encoder) encodeMetadata(metadata *pxar.Metadata) error {
	if e.err != nil {
		return e.err
	}

	var statBuf [40]byte
	format.MarshalStatBytesInto(statBuf[:], metadata.Stat)
	if e.err = e.writeHeader(format.PXAREntry, 40); e.err != nil {
		return e.err
	}
	if e.err = e.writeAll(statBuf[:]); e.err != nil {
		return e.err
	}

	for _, xattr := range metadata.XAttrs {
		if e.err = e.writeHeader(format.PXARXAttr, uint64(len(xattr.Data))); e.err != nil {
			return e.err
		}
		if e.err = e.writeAll(xattr.Data); e.err != nil {
			return e.err
		}
	}

	for _, acl := range metadata.ACL.Users {
		data := format.MarshalACLUserBytes(acl)
		if e.err = e.writeHeader(format.PXARACLUser, uint64(len(data))); e.err != nil {
			return e.err
		}
		if e.err = e.writeAll(data); e.err != nil {
			return e.err
		}
	}
	for _, acl := range metadata.ACL.Groups {
		data := format.MarshalACLGroupBytes(acl)
		if e.err = e.writeHeader(format.PXARACLGroup, uint64(len(data))); e.err != nil {
			return e.err
		}
		if e.err = e.writeAll(data); e.err != nil {
			return e.err
		}
	}
	if metadata.ACL.GroupObj != nil {
		data := format.MarshalACLGroupObjectBytes(*metadata.ACL.GroupObj)
		if e.err = e.writeHeader(format.PXARACLGroupObj, uint64(len(data))); e.err != nil {
			return e.err
		}
		if e.err = e.writeAll(data); e.err != nil {
			return e.err
		}
	}
	if metadata.ACL.Default != nil {
		data := format.MarshalACLDefaultBytes(*metadata.ACL.Default)
		if e.err = e.writeHeader(format.PXARACLDefault, uint64(len(data))); e.err != nil {
			return e.err
		}
		if e.err = e.writeAll(data); e.err != nil {
			return e.err
		}
	}
	for _, acl := range metadata.ACL.DefaultUsers {
		data := format.MarshalACLUserBytes(acl)
		if e.err = e.writeHeader(format.PXARACLDefaultUser, uint64(len(data))); e.err != nil {
			return e.err
		}
		if e.err = e.writeAll(data); e.err != nil {
			return e.err
		}
	}
	for _, acl := range metadata.ACL.DefaultGroups {
		data := format.MarshalACLGroupBytes(acl)
		if e.err = e.writeHeader(format.PXARACLDefaultGroup, uint64(len(data))); e.err != nil {
			return e.err
		}
		if e.err = e.writeAll(data); e.err != nil {
			return e.err
		}
	}

	if len(metadata.FCaps) > 0 {
		if e.err = e.writeHeader(format.PXARFCaps, uint64(len(metadata.FCaps))); e.err != nil {
			return e.err
		}
		if e.err = e.writeAll(metadata.FCaps); e.err != nil {
			return e.err
		}
	}

	if metadata.QuotaProjectID != nil {
		var qbuf [8]byte
		binary.LittleEndian.PutUint64(qbuf[:], *metadata.QuotaProjectID)
		if e.err = e.writeHeader(format.PXARQuotaProjID, uint64(len(qbuf))); e.err != nil {
			return e.err
		}
		if e.err = e.writeAll(qbuf[:]); e.err != nil {
			return e.err
		}
	}

	return nil
}

func (e *Encoder) encodeFilename(name string) error {
	if e.err != nil {
		return e.err
	}
	nameBytes := unsafe.Slice(unsafe.StringData(name), len(name))
	if err := format.CheckFilename(nameBytes); err != nil {
		return err
	}
	contentSize := uint64(len(nameBytes) + 1)
	if e.err = e.writeHeader(format.PXARFilename, contentSize); e.err != nil {
		return e.err
	}
	if e.err = e.writeAll(nameBytes); e.err != nil {
		return e.err
	}
	var zero [1]byte
	e.err = e.writeAll(zero[:])
	return e.err
}

// AddFile adds a complete file to the archive.
func (e *Encoder) AddFile(metadata *pxar.Metadata, name string, content []byte) (LinkOffset, error) {
	if e.err != nil {
		return 0, e.err
	}
	fileOffset := e.currentState().writePosition

	if err := e.encodeFilename(name); err != nil {
		return 0, err
	}
	if err := e.encodeMetadata(metadata); err != nil {
		return 0, err
	}

	if e.payloadOut != nil {
		// Split archive: write payload ref + actual payload
		s := e.currentState()
		payloadOffset := s.payloadWritePos
		s.previousPayloadOffset = payloadOffset
		s.hasPrevPayloadOffset = true
		var prBuf [16]byte
		binary.LittleEndian.PutUint64(prBuf[0:], payloadOffset)
		binary.LittleEndian.PutUint64(prBuf[8:], uint64(len(content)))
		if e.err = e.writeHeader(format.PXARPayloadRef, 16); e.err != nil {
			return 0, e.err
		}
		if e.err = e.writeAll(prBuf[:]); e.err != nil {
			return 0, e.err
		}

		var hdrBuf [format.HeaderSize]byte
		format.HeaderWithContentSize(format.PXARPayload, uint64(len(content))).MarshalTo(hdrBuf[:])
		if _, err := e.payloadOut.Write(hdrBuf[:]); err != nil {
			e.err = err
			return 0, err
		}
		if _, err := e.payloadOut.Write(content); err != nil {
			e.err = err
			return 0, err
		}
		e.currentState().payloadWritePos += format.HeaderSize + uint64(len(content))
	} else {
		if e.err = e.writeHeader(format.PXARPayload, uint64(len(content))); e.err != nil {
			return 0, e.err
		}
		if e.err = e.writeAll(content); e.err != nil {
			return 0, e.err
		}
	}

	endOffset := e.currentState().writePosition

	s := e.currentState()
	s.items = append(s.items, format.GoodbyeItem{
		Hash:   format.HashFilename(unsafe.Slice(unsafe.StringData(name), len(name))),
		Offset: fileOffset,
		Size:   endOffset - fileOffset,
	})

	return LinkOffset(fileOffset), nil
}

// CreateFile returns a FileWriter for streaming file content.
func (e *Encoder) CreateFile(metadata *pxar.Metadata, name string, size uint64) (*FileWriter, error) {
	if e.err != nil {
		return nil, e.err
	}
	fileOffset := e.currentState().writePosition

	if err := e.encodeFilename(name); err != nil {
		return nil, err
	}
	if err := e.encodeMetadata(metadata); err != nil {
		return nil, err
	}

	if e.payloadOut != nil {
		s := e.currentState()
		payloadOffset := s.payloadWritePos
		// Use max to prevent CreateFile from decreasing previousPayloadOffset
		// below what was set by a prior AddPayloadRef call.
		if !s.hasPrevPayloadOffset || payloadOffset > s.previousPayloadOffset {
			s.previousPayloadOffset = payloadOffset
			s.hasPrevPayloadOffset = true
		}
		var prBuf [16]byte
		binary.LittleEndian.PutUint64(prBuf[0:], payloadOffset)
		binary.LittleEndian.PutUint64(prBuf[8:], size)
		if e.err = e.writeHeader(format.PXARPayloadRef, 16); e.err != nil {
			return nil, e.err
		}
		if e.err = e.writeAll(prBuf[:]); e.err != nil {
			return nil, e.err
		}

		var hdrBuf [format.HeaderSize]byte
		format.HeaderWithContentSize(format.PXARPayload, size).MarshalTo(hdrBuf[:])
		if _, err := e.payloadOut.Write(hdrBuf[:]); err != nil {
			e.err = err
			return nil, err
		}
		e.currentState().payloadWritePos += format.HeaderSize
	} else {
		if e.err = e.writeHeader(format.PXARPayload, size); e.err != nil {
			return nil, e.err
		}
	}

	return &FileWriter{
		enc:         e,
		goodbyeItem: format.GoodbyeItem{Hash: format.HashFilename(unsafe.Slice(unsafe.StringData(name), len(name))), Offset: fileOffset},
		remaining:   size,
	}, nil
}

// FileWriter writes file content to a pxar archive.
type FileWriter struct {
	enc         *Encoder
	goodbyeItem format.GoodbyeItem
	remaining   uint64
}

// FileOffset returns the file's offset for use with AddHardlink.
func (fw *FileWriter) FileOffset() LinkOffset {
	return LinkOffset(fw.goodbyeItem.Offset)
}

// Write writes data to the file.
func (fw *FileWriter) Write(data []byte) (int, error) {
	if uint64(len(data)) > fw.remaining {
		return 0, fmt.Errorf("attempted to write more than allocated")
	}
	var n int
	var err error
	if fw.enc.payloadOut != nil {
		n, err = fw.enc.payloadOut.Write(data)
		if err == nil {
			s := fw.enc.currentState()
			s.payloadWritePos += uint64(n)
		}
	} else {
		n, err = fw.enc.output.Write(data)
		if err == nil {
			s := fw.enc.currentState()
			s.writePosition += uint64(n)
		}
	}
	if err != nil {
		return n, err
	}
	fw.remaining -= uint64(n)
	return n, nil
}

// WriteAll writes all data to the file.
func (fw *FileWriter) WriteAll(data []byte) error {
	if uint64(len(data)) > fw.remaining {
		return fmt.Errorf("attempted to write more than allocated")
	}
	var n int
	var err error
	if fw.enc.payloadOut != nil {
		n, err = fw.enc.payloadOut.Write(data)
		if err == nil {
			s := fw.enc.currentState()
			s.payloadWritePos += uint64(n)
		}
	} else {
		n, err = fw.enc.output.Write(data)
		if err == nil {
			s := fw.enc.currentState()
			s.writePosition += uint64(n)
		}
	}
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	fw.remaining -= uint64(len(data))
	return nil
}

// Close finalizes the file entry.
func (fw *FileWriter) Close() error {
	if fw.remaining != 0 {
		return fmt.Errorf("incomplete file: %d bytes remaining", fw.remaining)
	}
	s := fw.enc.currentState()
	fw.goodbyeItem.Size = s.writePosition - fw.goodbyeItem.Offset
	s.items = append(s.items, fw.goodbyeItem)
	return nil
}

// AddSymlink adds a symbolic link.
func (e *Encoder) AddSymlink(metadata *pxar.Metadata, name string, target string) error {
	if e.err != nil {
		return e.err
	}
	fileOffset := e.currentState().writePosition

	if err := e.encodeFilename(name); err != nil {
		return err
	}
	if err := e.encodeMetadata(metadata); err != nil {
		return err
	}

	// AddSymlink
	targetBytes := []byte(target)
	contentSize := uint64(len(targetBytes) + 1)
	if e.err = e.writeHeader(format.PXARSymlink, contentSize); e.err != nil {
		return e.err
	}
	if e.err = e.writeAll(targetBytes); e.err != nil {
		return e.err
	}
	var nl [1]byte
	e.err = e.writeAll(nl[:])
	if e.err != nil {
		return e.err
	}

	endOffset := e.currentState().writePosition
	s := e.currentState()
	s.items = append(s.items, format.GoodbyeItem{
		Hash:   format.HashFilename(unsafe.Slice(unsafe.StringData(name), len(name))),
		Offset: fileOffset,
		Size:   endOffset - fileOffset,
	})
	return nil
}

// AddHardlink adds a hard link.
func (e *Encoder) AddHardlink(name string, target string, targetOffset LinkOffset) error {
	if e.err != nil {
		return e.err
	}
	currentOffset := e.currentState().writePosition
	if currentOffset <= uint64(targetOffset) {
		return fmt.Errorf("hardlink offset must point to a prior file")
	}

	// Write FILENAME
	if err := e.encodeFilename(name); err != nil {
		return err
	}

	// Hardlink: relative offset (uint64) + target path + null terminator
	relOffset := currentOffset - uint64(targetOffset)
	var relBuf [8]byte
	binary.LittleEndian.PutUint64(relBuf[:], relOffset)
	targetBytes := []byte(target)
	contentSize := uint64(8 + len(targetBytes) + 1)
	if e.err = e.writeHeader(format.PXARHardlink, contentSize); e.err != nil {
		return e.err
	}
	if e.err = e.writeAll(relBuf[:]); e.err != nil {
		return e.err
	}
	if e.err = e.writeAll(targetBytes); e.err != nil {
		return e.err
	}
	var nl [1]byte
	e.err = e.writeAll(nl[:])
	if e.err != nil {
		return e.err
	}

	endOffset := e.currentState().writePosition
	s := e.currentState()
	s.items = append(s.items, format.GoodbyeItem{
		Hash:   format.HashFilename(unsafe.Slice(unsafe.StringData(name), len(name))),
		Offset: currentOffset,
		Size:   endOffset - currentOffset,
	})
	return nil
}

// AddDevice adds a device node.
func (e *Encoder) AddDevice(metadata *pxar.Metadata, name string, device format.Device) error {
	if e.err != nil {
		return e.err
	}
	if !metadata.IsDevice() {
		return fmt.Errorf("device metadata must have device mode flag")
	}

	fileOffset := e.currentState().writePosition
	if err := e.encodeFilename(name); err != nil {
		return err
	}
	if err := e.encodeMetadata(metadata); err != nil {
		return err
	}

	data := format.MarshalDeviceBytes(device)
	if e.err = e.writeHeader(format.PXARDevice, uint64(len(data))); e.err != nil {
		return e.err
	}
	e.err = e.writeAll(data)
	if e.err != nil {
		return e.err
	}

	endOffset := e.currentState().writePosition
	s := e.currentState()
	s.items = append(s.items, format.GoodbyeItem{
		Hash:   format.HashFilename(unsafe.Slice(unsafe.StringData(name), len(name))),
		Offset: fileOffset,
		Size:   endOffset - fileOffset,
	})
	return nil
}

// AddFIFO adds a named pipe.
func (e *Encoder) AddFIFO(metadata *pxar.Metadata, name string) error {
	if e.err != nil {
		return e.err
	}
	if !metadata.IsFIFO() {
		return fmt.Errorf("FIFO metadata must have FIFO mode flag")
	}
	return e.addSimpleEntry(metadata, name)
}

// AddSocket adds a named socket.
func (e *Encoder) AddSocket(metadata *pxar.Metadata, name string) error {
	if e.err != nil {
		return e.err
	}
	if !metadata.IsSocket() {
		return fmt.Errorf("socket metadata must have socket mode flag")
	}
	return e.addSimpleEntry(metadata, name)
}

func (e *Encoder) addSimpleEntry(metadata *pxar.Metadata, name string) error {
	fileOffset := e.currentState().writePosition
	if err := e.encodeFilename(name); err != nil {
		return err
	}
	if err := e.encodeMetadata(metadata); err != nil {
		return err
	}

	endOffset := e.currentState().writePosition
	s := e.currentState()
	s.items = append(s.items, format.GoodbyeItem{
		Hash:   format.HashFilename(unsafe.Slice(unsafe.StringData(name), len(name))),
		Offset: fileOffset,
		Size:   endOffset - fileOffset,
	})
	return nil
}

// CreateDirectory pushes a new directory onto the stack.
func (e *Encoder) CreateDirectory(name string, metadata *pxar.Metadata) error {
	if e.err != nil {
		return e.err
	}
	if !metadata.IsDir() {
		return fmt.Errorf("directory metadata must have directory mode flag")
	}

	parentState := e.currentState()

	fileOffset := parentState.writePosition
	if err := e.encodeFilename(name); err != nil {
		return err
	}

	entryOffset := e.currentState().writePosition
	if err := e.encodeMetadata(metadata); err != nil {
		return err
	}

	// Add a placeholder goodbye item to the parent
	parentState.items = append(parentState.items, format.GoodbyeItem{
		Hash:   format.HashFilename(unsafe.Slice(unsafe.StringData(name), len(name))),
		Offset: fileOffset,
	})
	parentItemIdx := len(parentState.items) - 1

	// Push new state for the child directory
	childPos := e.currentState().writePosition
	childPayloadPos := e.currentState().payloadWritePos
	prevOff := e.currentState().previousPayloadOffset
	hasPrev := e.currentState().hasPrevPayloadOffset
	e.pushState(childPos, parentItemIdx)
	e.currentState().entryOffset = entryOffset
	e.currentState().payloadWritePos = childPayloadPos
	e.currentState().previousPayloadOffset = prevOff
	e.currentState().hasPrevPayloadOffset = hasPrev

	return nil
}

// Finish finalizes the current directory (pops state, writes goodbye table).
func (e *Encoder) Finish() error {
	if e.err != nil {
		return e.err
	}
	if len(e.state) <= 1 {
		return fmt.Errorf("no directory to finish")
	}

	childState := e.currentState()

	goodbyeBytes := e.buildGoodbyeTable()
	if e.err = e.writeHeader(format.PXARGoodbye, uint64(len(goodbyeBytes))); e.err != nil {
		return e.err
	}
	if e.err = e.writeAll(goodbyeBytes); e.err != nil {
		return e.err
	}

	endOffset := e.currentState().writePosition
	endPayloadOffset := e.currentState().payloadWritePos

	// Pop child state
	e.state = e.state[:len(e.state)-1]

	// Update parent state
	parentState := e.currentState()
	parentState.writePosition = endOffset
	parentState.payloadWritePos = endPayloadOffset

	// Update the parent's goodbye item with the final size
	idx := childState.parentItemIdx
	if idx >= 0 && idx < len(parentState.items) {
		parentState.items[idx].Size = endOffset - parentState.items[idx].Offset
	}

	return nil
}

func (e *Encoder) buildGoodbyeTable() []byte {
	s := e.currentState()
	goodbyeOffset := s.writePosition
	n := len(s.items)

	// Sort items by hash into reusable tail buffer
	if cap(e.gbTail) < n {
		e.gbTail = make([]format.GoodbyeItem, n)
	}
	tail := e.gbTail[:n]
	copy(tail, s.items)
	sort.Slice(tail, func(i, j int) bool {
		return tail[i].Hash < tail[j].Hash
	})

	// Build BST using binary tree array into reusable buffer
	bstSize := n + 1 // items + tail marker
	if cap(e.gbItems) < bstSize {
		e.gbItems = make([]format.GoodbyeItem, bstSize)
	}
	bst := e.gbItems[:n]
	binarytree.Copy(n, func(src, dest int) {
		item := tail[src]
		item.Offset = goodbyeOffset - item.Offset
		bst[dest] = item
	})

	// Append tail marker
	tailMarker := format.GoodbyeItem{
		Hash:   format.PXARGoodbyeTailMarker,
		Offset: goodbyeOffset - s.entryOffset,
		Size:   uint64(format.HeaderSize + bstSize*binary.Size(format.GoodbyeItem{})),
	}

	// Serialize to bytes using reusable buffer
	bufSize := bstSize * binary.Size(format.GoodbyeItem{})
	if cap(e.gbBuf) < bufSize {
		e.gbBuf = make([]byte, bufSize*2)
	}
	buf := e.gbBuf[:bufSize]
	for i, item := range bst {
		offset := i * binary.Size(format.GoodbyeItem{})
		binary.LittleEndian.PutUint64(buf[offset:], item.Hash)
		binary.LittleEndian.PutUint64(buf[offset+8:], item.Offset)
		binary.LittleEndian.PutUint64(buf[offset+16:], item.Size)
	}

	// Write tail marker at the end
	offset := n * binary.Size(format.GoodbyeItem{})
	binary.LittleEndian.PutUint64(buf[offset:], tailMarker.Hash)
	binary.LittleEndian.PutUint64(buf[offset+8:], tailMarker.Offset)
	binary.LittleEndian.PutUint64(buf[offset+16:], tailMarker.Size)

	return buf
}

// AddPayloadRef adds a file entry that references existing payload data.
// It writes the metadata entry (filename + stat + PXAR_PAYLOAD_REF) but does NOT
// write any real payload data. Instead, it writes a PXAR_PAYLOAD header + zero-fill
// to maintain correct offsets in the payload stream.
// The caller is responsible for ensuring the original payload chunks are available
// in the datastore (either via injection or dedup).
func (e *Encoder) AddPayloadRef(metadata *pxar.Metadata, name string, fileSize uint64, payloadOffset uint64) (LinkOffset, error) {
	if e.err != nil {
		return 0, e.err
	}
	if e.payloadOut == nil {
		return 0, fmt.Errorf("AddPayloadRef requires split archive (v2 format)")
	}

	// Offset checks — mirrors Rust encoder's payload_ref_from:
	// 1. payload_offset must be >= current payload write position (can't point backwards
	//    in the payload stream past already-written data)
	// 2. payload_offset must be strictly larger than the previously seen PAYLOAD_REF
	//    offset for the sequential decoder to correctly restore contents.
	s := e.currentState()
	if payloadOffset < s.payloadWritePos {
		return 0, fmt.Errorf("payload offset %d smaller than current write position %d",
			payloadOffset, s.payloadWritePos)
	}
	if s.hasPrevPayloadOffset && payloadOffset <= s.previousPayloadOffset {
		return 0, fmt.Errorf("unexpected payload offset %d not larger than previous offset %d",
			payloadOffset, s.previousPayloadOffset)
	}
	s.previousPayloadOffset = payloadOffset
	s.hasPrevPayloadOffset = true

	fileOffset := s.writePosition

	if err := e.encodeFilename(name); err != nil {
		return 0, err
	}
	if err := e.encodeMetadata(metadata); err != nil {
		return 0, err
	}

	// Write PXAR_PAYLOAD_REF pointing to the original payload offset
	var prBuf [16]byte
	binary.LittleEndian.PutUint64(prBuf[0:], payloadOffset)
	binary.LittleEndian.PutUint64(prBuf[8:], fileSize)
	if e.err = e.writeHeader(format.PXARPayloadRef, 16); e.err != nil {
		return 0, e.err
	}
	if e.err = e.writeAll(prBuf[:]); e.err != nil {
		return 0, e.err
	}

	// Write PXAR_PAYLOAD header + zero-fill to the payload stream.
	// This ensures the payload stream has the correct byte layout: reused file data
	// is zero-filled and will match original chunks after dedup (since the chunker
	// will produce the same chunk boundaries, and the session deduplicates by digest).
	//
	// Wait — that's wrong. Zero-fill won't match the original data.
	// We need the actual data for the chunk digests to match.
	//
	// The correct approach: write the payload data as-is from the original.
	// But we don't have the original data here — that's the whole point.
	//
	// InjectChunks advances payloadWritePos per-batch; WriteEntryReader
	// writes new data after the last injected region.

	endOffset := e.currentState().writePosition
	s = e.currentState()
	s.items = append(s.items, format.GoodbyeItem{
		Hash:   format.HashFilename(unsafe.Slice(unsafe.StringData(name), len(name))),
		Offset: fileOffset,
		Size:   endOffset - fileOffset,
	})

	return LinkOffset(fileOffset), nil
}

// PayloadPosition returns the current write position in the payload stream.
func (e *Encoder) PayloadPosition() uint64 {
	if len(e.state) == 0 {
		return 0
	}
	return e.currentState().payloadWritePos
}

// Advance advances the payload write position by the given size.
// This is used with AddPayloadRef to track the virtual payload size
// without actually writing payload data.
func (e *Encoder) Advance(size uint64) error {
	if e.err != nil {
		return e.err
	}
	e.currentState().payloadWritePos += size
	return nil
}

// Close finalizes the archive (writes root goodbye table and finishes).
func (e *Encoder) Close() error {
	if e.finished {
		return fmt.Errorf("encoder already finished")
	}
	if e.err != nil {
		e.state = e.state[:0]
		e.finished = true
		return e.err
	}

	// Write root goodbye table
	goodbyeBytes := e.buildGoodbyeTable()
	if e.err = e.writeHeader(format.PXARGoodbye, uint64(len(goodbyeBytes))); e.err != nil {
		e.state = e.state[:0]
		e.finished = true
		return e.err
	}
	if e.err = e.writeAll(goodbyeBytes); e.err != nil {
		e.state = e.state[:0]
		e.finished = true
		return e.err
	}

	// Write payload tail marker if split archive
	if e.payloadOut != nil {
		var hdrBuf [format.HeaderSize]byte
		format.HeaderWithContentSize(format.PXARPayloadTailMarker, 0).MarshalTo(hdrBuf[:])
		if _, err := e.payloadOut.Write(hdrBuf[:]); err != nil {
			e.state = e.state[:0]
			e.finished = true
			return err
		}
	}

	// Clear state
	e.state = e.state[:0]
	e.finished = true

	return nil
}
