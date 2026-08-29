package datastore

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
)

const dynamicIndexWriteBufferSize = 1 << 20

// DynamicIndexStreamWriter bounds memory while preserving PBS's header-last checksum format.
type DynamicIndexStreamWriter struct {
	dst      io.WriteSeeker
	buf      *bufio.Writer
	header   DynamicIndexHeader
	hash     hash.Hash
	lastEnd  uint64
	finished bool
}

// NewDynamicIndexStreamWriter reserves the fixed header so entries need only one sequential write.
func NewDynamicIndexStreamWriter(dst io.WriteSeeker, ctime int64) (*DynamicIndexStreamWriter, error) {
	if dst == nil {
		return nil, fmt.Errorf("dynamic index destination is nil")
	}
	header := DynamicIndexHeader{
		Magic: MagicDynamicChunkIndex,
		UUID:  generateUUID(),
		Ctime: ctime,
	}
	if _, err := dst.Write(make([]byte, IndexHeaderSize)); err != nil {
		return nil, fmt.Errorf("write dynamic index header placeholder: %w", err)
	}
	return &DynamicIndexStreamWriter{
		dst:    dst,
		buf:    bufio.NewWriterSize(dst, dynamicIndexWriteBufferSize),
		header: header,
		hash:   sha256.New(),
	}, nil
}

// Add rejects non-monotonic ends before they can publish an unreadable index.
func (w *DynamicIndexStreamWriter) Add(endOffset uint64, digest [32]byte) error {
	if w.finished {
		return fmt.Errorf("dynamic index is already finished")
	}
	if endOffset <= w.lastEnd {
		return fmt.Errorf("dynamic index end offset %d is not greater than %d", endOffset, w.lastEnd)
	}
	var raw [DynamicEntrySize]byte
	binary.LittleEndian.PutUint64(raw[:8], endOffset)
	copy(raw[8:], digest[:])
	if _, err := w.buf.Write(raw[:]); err != nil {
		return fmt.Errorf("write dynamic index entry: %w", err)
	}
	_, _ = w.hash.Write(raw[:])
	w.lastEnd = endOffset
	return nil
}

// Finish commits the checksum only after every entry reaches the underlying file.
func (w *DynamicIndexStreamWriter) Finish() ([32]byte, uint64, error) {
	if w.finished {
		return w.header.IndexCsum, w.lastEnd, fmt.Errorf("dynamic index is already finished")
	}
	if err := w.buf.Flush(); err != nil {
		return [32]byte{}, 0, fmt.Errorf("flush dynamic index entries: %w", err)
	}
	copy(w.header.IndexCsum[:], w.hash.Sum(nil))
	if _, err := w.dst.Seek(0, io.SeekStart); err != nil {
		return [32]byte{}, 0, fmt.Errorf("seek dynamic index header: %w", err)
	}
	var raw [IndexHeaderSize]byte
	w.header.MarshalTo(raw[:])
	if _, err := w.dst.Write(raw[:]); err != nil {
		return [32]byte{}, 0, fmt.Errorf("write dynamic index header: %w", err)
	}
	if _, err := w.dst.Seek(0, io.SeekEnd); err != nil {
		return [32]byte{}, 0, fmt.Errorf("seek dynamic index end: %w", err)
	}
	w.finished = true
	return w.header.IndexCsum, w.lastEnd, nil
}
