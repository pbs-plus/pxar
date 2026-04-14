// Package encoder creates pxar archives.
package encoder

import (
	"encoding/binary"
	"fmt"
	"io"
	"sort"

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
	state      []encoderState
	finished   bool
	version    format.FormatVersion
	copyBuf    []byte
	err        error
}

type encoderState struct {
	items           []format.GoodbyeItem
	entryOffset     uint64
	writePosition   uint64
	payloadWritePos uint64
	finished        bool
	// For tracking parent's goodbye item when this is a subdirectory
	parentItemIdx int // index in parent's items slice, -1 for root
}

// NewEncoder creates a new pxar encoder writing to the given writers.
// If payloadOut is non-nil, the archive is split (v2 format).
// metadata describes the root directory. prelude is optional v2 prelude data.
func NewEncoder(output, payloadOut io.Writer, metadata *pxar.Metadata, prelude []byte) *Encoder {
	enc := &Encoder{
		output:  output,
		copyBuf: make([]byte, 1024*1024),
	}

	if payloadOut != nil {
		enc.payloadOut = payloadOut
		enc.version = format.FormatVersion2
		// Write payload start marker
		h := format.HeaderWithContentSize(format.PXARPayloadStartMarker, 0)
		if err := binary.Write(payloadOut, binary.LittleEndian, &h); err != nil {
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
	e.state = append(e.state, encoderState{
		writePosition: pos,
		parentItemIdx: parentIdx,
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
	if err := binary.Write(e.output, binary.LittleEndian, &h); err != nil {
		return err
	}
	s := e.currentState()
	s.writePosition += format.HeaderSize
	return nil
}

func (e *Encoder) encodeFormatVersion() {
	if e.version != format.FormatVersion2 {
		return
	}
	data := e.version.Serialize()
	if e.err = e.writeHeader(format.PXARFormatVersion, uint64(len(data))); e.err != nil {
		return
	}
	e.err = e.writeAll(data)
}

func (e *Encoder) encodePrelude(prelude []byte) {
	if e.err = e.writeHeader(format.PXARPrelude, uint64(len(prelude))); e.err != nil {
		return
	}
	e.err = e.writeAll(prelude)
}

func (e *Encoder) encodeMetadata(metadata *pxar.Metadata) error {
	if e.err != nil {
		return e.err
	}

	statBytes := marshalStat(metadata.Stat)
	if e.err = e.writeHeader(format.PXAREntry, uint64(len(statBytes))); e.err != nil {
		return e.err
	}
	if e.err = e.writeAll(statBytes); e.err != nil {
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
		data := marshalACLUser(acl)
		if e.err = e.writeHeader(format.PXARACLUser, uint64(len(data))); e.err != nil {
			return e.err
		}
		if e.err = e.writeAll(data); e.err != nil {
			return e.err
		}
	}
	for _, acl := range metadata.ACL.Groups {
		data := marshalACLGroup(acl)
		if e.err = e.writeHeader(format.PXARACLGroup, uint64(len(data))); e.err != nil {
			return e.err
		}
		if e.err = e.writeAll(data); e.err != nil {
			return e.err
		}
	}
	if metadata.ACL.GroupObj != nil {
		data := marshalACLGroupObject(*metadata.ACL.GroupObj)
		if e.err = e.writeHeader(format.PXARACLGroupObj, uint64(len(data))); e.err != nil {
			return e.err
		}
		if e.err = e.writeAll(data); e.err != nil {
			return e.err
		}
	}
	if metadata.ACL.Default != nil {
		data := marshalACLDefault(*metadata.ACL.Default)
		if e.err = e.writeHeader(format.PXARACLDefault, uint64(len(data))); e.err != nil {
			return e.err
		}
		if e.err = e.writeAll(data); e.err != nil {
			return e.err
		}
	}
	for _, acl := range metadata.ACL.DefaultUsers {
		data := marshalACLUser(acl)
		if e.err = e.writeHeader(format.PXARACLDefaultUser, uint64(len(data))); e.err != nil {
			return e.err
		}
		if e.err = e.writeAll(data); e.err != nil {
			return e.err
		}
	}
	for _, acl := range metadata.ACL.DefaultGroups {
		data := marshalACLGroup(acl)
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
		data := marshalQuotaProjectID(*metadata.QuotaProjectID)
		if e.err = e.writeHeader(format.PXARQuotaProjID, uint64(len(data))); e.err != nil {
			return e.err
		}
		if e.err = e.writeAll(data); e.err != nil {
			return e.err
		}
	}

	return nil
}

func (e *Encoder) encodeFilename(name []byte) error {
	if e.err != nil {
		return e.err
	}
	if err := format.CheckFilename(name); err != nil {
		return err
	}
	contentSize := uint64(len(name) + 1)
	if e.err = e.writeHeader(format.PXARFilename, contentSize); e.err != nil {
		return e.err
	}
	if e.err = e.writeAll(name); e.err != nil {
		return e.err
	}
	e.err = e.writeAll([]byte{0})
	return e.err
}

// AddFile adds a complete file to the archive.
func (e *Encoder) AddFile(metadata *pxar.Metadata, name string, content []byte) (LinkOffset, error) {
	if e.err != nil {
		return 0, e.err
	}
	fileOffset := e.currentState().writePosition

	if err := e.encodeFilename([]byte(name)); err != nil {
		return 0, err
	}
	if err := e.encodeMetadata(metadata); err != nil {
		return 0, err
	}

	if e.payloadOut != nil {
		// Split archive: write payload ref + actual payload
		payloadOffset := e.currentState().payloadWritePos
		payloadRef := format.PayloadRef{Offset: payloadOffset, Size: uint64(len(content))}
		prData := payloadRef.Bytes()
		if e.err = e.writeHeader(format.PXARPayloadRef, uint64(len(prData))); e.err != nil {
			return 0, e.err
		}
		if e.err = e.writeAll(prData); e.err != nil {
			return 0, e.err
		}

		h := format.HeaderWithContentSize(format.PXARPayload, uint64(len(content)))
		if err := binary.Write(e.payloadOut, binary.LittleEndian, &h); err != nil {
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
		Hash:   format.HashFilename([]byte(name)),
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

	if err := e.encodeFilename([]byte(name)); err != nil {
		return nil, err
	}
	if err := e.encodeMetadata(metadata); err != nil {
		return nil, err
	}

	if e.payloadOut != nil {
		payloadOffset := e.currentState().payloadWritePos
		payloadRef := format.PayloadRef{Offset: payloadOffset, Size: size}
		prData := payloadRef.Bytes()
		if e.err = e.writeHeader(format.PXARPayloadRef, uint64(len(prData))); e.err != nil {
			return nil, e.err
		}
		if e.err = e.writeAll(prData); e.err != nil {
			return nil, e.err
		}

		h := format.HeaderWithContentSize(format.PXARPayload, size)
		if err := binary.Write(e.payloadOut, binary.LittleEndian, &h); err != nil {
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
		goodbyeItem: format.GoodbyeItem{Hash: format.HashFilename([]byte(name)), Offset: fileOffset},
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
	n, err := fw.enc.output.Write(data)
	if err != nil {
		return n, err
	}
	fw.remaining -= uint64(n)
	s := fw.enc.currentState()
	s.writePosition += uint64(n)
	return n, nil
}

// WriteAll writes all data to the file.
func (fw *FileWriter) WriteAll(data []byte) error {
	if uint64(len(data)) > fw.remaining {
		return fmt.Errorf("attempted to write more than allocated")
	}
	n, err := fw.enc.output.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	fw.remaining -= uint64(len(data))
	s := fw.enc.currentState()
	s.writePosition += uint64(n)
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

	if err := e.encodeFilename([]byte(name)); err != nil {
		return err
	}
	if err := e.encodeMetadata(metadata); err != nil {
		return err
	}

	targetBytes := []byte(target)
	contentSize := uint64(len(targetBytes) + 1)
	if e.err = e.writeHeader(format.PXARSymlink, contentSize); e.err != nil {
		return e.err
	}
	if e.err = e.writeAll(targetBytes); e.err != nil {
		return e.err
	}
	e.err = e.writeAll([]byte{0})
	if e.err != nil {
		return e.err
	}

	endOffset := e.currentState().writePosition
	s := e.currentState()
	s.items = append(s.items, format.GoodbyeItem{
		Hash:   format.HashFilename([]byte(name)),
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
	if err := e.encodeFilename([]byte(name)); err != nil {
		return err
	}

	// Hardlink: relative offset (uint64) + target path + null terminator
	relOffset := currentOffset - uint64(targetOffset)
	relBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(relBytes, relOffset)
	targetBytes := []byte(target)
	contentSize := uint64(8 + len(targetBytes) + 1)
	if e.err = e.writeHeader(format.PXARHardlink, contentSize); e.err != nil {
		return e.err
	}
	if e.err = e.writeAll(relBytes); e.err != nil {
		return e.err
	}
	if e.err = e.writeAll(targetBytes); e.err != nil {
		return e.err
	}
	e.err = e.writeAll([]byte{0})
	if e.err != nil {
		return e.err
	}

	endOffset := e.currentState().writePosition
	s := e.currentState()
	s.items = append(s.items, format.GoodbyeItem{
		Hash:   format.HashFilename([]byte(name)),
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
	if err := e.encodeFilename([]byte(name)); err != nil {
		return err
	}
	if err := e.encodeMetadata(metadata); err != nil {
		return err
	}

	data := marshalDevice(device)
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
		Hash:   format.HashFilename([]byte(name)),
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
	if err := e.encodeFilename([]byte(name)); err != nil {
		return err
	}
	if err := e.encodeMetadata(metadata); err != nil {
		return err
	}

	endOffset := e.currentState().writePosition
	s := e.currentState()
	s.items = append(s.items, format.GoodbyeItem{
		Hash:   format.HashFilename([]byte(name)),
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
	if err := e.encodeFilename([]byte(name)); err != nil {
		return err
	}

	entryOffset := e.currentState().writePosition
	if err := e.encodeMetadata(metadata); err != nil {
		return err
	}

	// Add a placeholder goodbye item to the parent
	parentState.items = append(parentState.items, format.GoodbyeItem{
		Hash:   format.HashFilename([]byte(name)),
		Offset: fileOffset,
	})
	parentItemIdx := len(parentState.items) - 1

	// Push new state for the child directory
	childPos := e.currentState().writePosition
	childPayloadPos := e.currentState().payloadWritePos
	e.pushState(childPos, parentItemIdx)
	e.currentState().entryOffset = entryOffset
	e.currentState().payloadWritePos = childPayloadPos

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

	// Sort items by hash
	tail := make([]format.GoodbyeItem, len(s.items))
	copy(tail, s.items)
	sort.Slice(tail, func(i, j int) bool {
		return tail[i].Hash < tail[j].Hash
	})

	// Build BST using binary tree array
	bst := make([]format.GoodbyeItem, len(tail))
	binarytree.Copy(len(tail), func(src, dest int) {
		item := tail[src]
		item.Offset = goodbyeOffset - item.Offset // relative offset
		bst[dest] = item
	})

	// Append tail marker
	bst = append(bst, format.GoodbyeItem{
		Hash:   format.PXARGoodbyeTailMarker,
		Offset: goodbyeOffset - s.entryOffset,
		Size:   uint64(format.HeaderSize + (len(tail)+1)*binary.Size(format.GoodbyeItem{})),
	})

	// Serialize to bytes
	buf := make([]byte, len(bst)*binary.Size(format.GoodbyeItem{}))
	for i, item := range bst {
		offset := i * binary.Size(format.GoodbyeItem{})
		binary.LittleEndian.PutUint64(buf[offset:], item.Hash)
		binary.LittleEndian.PutUint64(buf[offset+8:], item.Offset)
		binary.LittleEndian.PutUint64(buf[offset+16:], item.Size)
	}

	return buf
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
		h := format.HeaderWithContentSize(format.PXARPayloadTailMarker, 0)
		if err := binary.Write(e.payloadOut, binary.LittleEndian, &h); err != nil {
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

// Marshal helpers

func marshalStat(s format.Stat) []byte         { return format.MarshalStatBytes(s) }
func marshalDevice(d format.Device) []byte     { return format.MarshalDeviceBytes(d) }
func marshalACLUser(u format.ACLUser) []byte   { return format.MarshalACLUserBytes(u) }
func marshalACLGroup(g format.ACLGroup) []byte { return format.MarshalACLGroupBytes(g) }
func marshalACLGroupObject(o format.ACLGroupObject) []byte {
	return format.MarshalACLGroupObjectBytes(o)
}
func marshalACLDefault(d format.ACLDefault) []byte { return format.MarshalACLDefaultBytes(d) }

func marshalQuotaProjectID(id uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf[0:], id)
	return buf
}
