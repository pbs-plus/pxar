package datastore

import (
	"encoding/binary"
	"fmt"
)

// Magic numbers from Proxmox Backup Server (file_formats.rs).
var (
	MagicUncompressedBlob = [8]byte{66, 171, 56, 7, 190, 131, 112, 161}
	MagicCompressedBlob   = [8]byte{49, 185, 88, 66, 111, 182, 163, 127}
	MagicEncryptedBlob    = [8]byte{123, 103, 133, 190, 34, 45, 76, 240}
	MagicEncrComprBlob    = [8]byte{230, 89, 27, 191, 11, 191, 216, 11}
	MagicFixedChunkIndex  = [8]byte{47, 127, 65, 237, 145, 253, 15, 205}
	MagicDynamicChunkIndex = [8]byte{28, 145, 78, 165, 25, 186, 179, 205}
	MagicCatalogFile      = [8]byte{145, 253, 96, 249, 196, 103, 88, 213}
)

const (
	BlobHeaderSize         = 12 // magic(8) + crc32(4)
	EncryptedBlobHeaderSize = 48 // magic(8) + crc32(4) + iv(16) + tag(16)
	IndexHeaderSize        = 4096
	DynamicEntrySize       = 40 // end_offset(8) + digest(32)
	FixedDigestSize        = 32
	MaxBlobSize            = 128 * 1024 * 1024 // 128MB
)

// BlobHeader is the 12-byte header for uncompressed and compressed blobs.
type BlobHeader struct {
	Magic [8]byte
	CRC   uint32
}

// MarshalTo writes the header to buf (must be at least BlobHeaderSize bytes).
func (h *BlobHeader) MarshalTo(buf []byte) {
	copy(buf[0:8], h.Magic[:])
	binary.LittleEndian.PutUint32(buf[8:12], h.CRC)
}

// UnmarshalBlobHeader parses a BlobHeader from raw bytes.
func UnmarshalBlobHeader(data []byte) (BlobHeader, error) {
	if len(data) < BlobHeaderSize {
		return BlobHeader{}, fmt.Errorf("blob header: need %d bytes, got %d", BlobHeaderSize, len(data))
	}
	var h BlobHeader
	copy(h.Magic[:], data[0:8])
	h.CRC = binary.LittleEndian.Uint32(data[8:12])
	return h, nil
}

// EncryptedBlobHeader is the 48-byte header for encrypted blobs.
type EncryptedBlobHeader struct {
	Magic [8]byte
	CRC   uint32
	IV    [16]byte
	Tag   [16]byte
}

// MarshalTo writes the header to buf (must be at least EncryptedBlobHeaderSize bytes).
func (h *EncryptedBlobHeader) MarshalTo(buf []byte) {
	copy(buf[0:8], h.Magic[:])
	binary.LittleEndian.PutUint32(buf[8:12], h.CRC)
	copy(buf[12:28], h.IV[:])
	copy(buf[28:44], h.Tag[:])
}

// UnmarshalEncryptedBlobHeader parses an EncryptedBlobHeader from raw bytes.
func UnmarshalEncryptedBlobHeader(data []byte) (EncryptedBlobHeader, error) {
	if len(data) < EncryptedBlobHeaderSize {
		return EncryptedBlobHeader{}, fmt.Errorf("encrypted blob header: need %d bytes, got %d", EncryptedBlobHeaderSize, len(data))
	}
	var h EncryptedBlobHeader
	copy(h.Magic[:], data[0:8])
	h.CRC = binary.LittleEndian.Uint32(data[8:12])
	copy(h.IV[:], data[12:28])
	copy(h.Tag[:], data[28:44])
	return h, nil
}

// DynamicIndexHeader is the 4096-byte header for dynamic chunk index files.
type DynamicIndexHeader struct {
	Magic     [8]byte
	UUID      [16]byte
	Ctime     int64
	IndexCsum [32]byte
}

// MarshalTo writes the header to buf (must be at least IndexHeaderSize bytes).
func (h *DynamicIndexHeader) MarshalTo(buf []byte) {
	copy(buf[0:8], h.Magic[:])
	copy(buf[8:24], h.UUID[:])
	binary.LittleEndian.PutUint64(buf[24:32], uint64(h.Ctime))
	copy(buf[32:64], h.IndexCsum[:])
	// rest is zero-padded
}

// UnmarshalDynamicIndexHeader parses a DynamicIndexHeader from raw bytes.
func UnmarshalDynamicIndexHeader(data []byte) (DynamicIndexHeader, error) {
	if len(data) < IndexHeaderSize {
		return DynamicIndexHeader{}, fmt.Errorf("dynamic index header: need %d bytes, got %d", IndexHeaderSize, len(data))
	}
	var h DynamicIndexHeader
	copy(h.Magic[:], data[0:8])
	copy(h.UUID[:], data[8:24])
	h.Ctime = int64(binary.LittleEndian.Uint64(data[24:32]))
	copy(h.IndexCsum[:], data[32:64])
	return h, nil
}

// FixedIndexHeader is the 4096-byte header for fixed chunk index files.
type FixedIndexHeader struct {
	Magic     [8]byte
	UUID      [16]byte
	Ctime     int64
	IndexCsum [32]byte
	Size      uint64
	ChunkSize uint64
}

// MarshalTo writes the header to buf (must be at least IndexHeaderSize bytes).
func (h *FixedIndexHeader) MarshalTo(buf []byte) {
	copy(buf[0:8], h.Magic[:])
	copy(buf[8:24], h.UUID[:])
	binary.LittleEndian.PutUint64(buf[24:32], uint64(h.Ctime))
	copy(buf[32:64], h.IndexCsum[:])
	binary.LittleEndian.PutUint64(buf[64:72], h.Size)
	binary.LittleEndian.PutUint64(buf[72:80], h.ChunkSize)
}

// UnmarshalFixedIndexHeader parses a FixedIndexHeader from raw bytes.
func UnmarshalFixedIndexHeader(data []byte) (FixedIndexHeader, error) {
	if len(data) < IndexHeaderSize {
		return FixedIndexHeader{}, fmt.Errorf("fixed index header: need %d bytes, got %d", IndexHeaderSize, len(data))
	}
	var h FixedIndexHeader
	copy(h.Magic[:], data[0:8])
	copy(h.UUID[:], data[8:24])
	h.Ctime = int64(binary.LittleEndian.Uint64(data[24:32]))
	copy(h.IndexCsum[:], data[32:64])
	h.Size = binary.LittleEndian.Uint64(data[64:72])
	h.ChunkSize = binary.LittleEndian.Uint64(data[72:80])
	return h, nil
}

// BlobHeaderSizeFor returns the header size for the given blob magic.
// Panics for unknown magic values.
func BlobHeaderSizeFor(magic [8]byte) int {
	switch magic {
	case MagicUncompressedBlob, MagicCompressedBlob:
		return BlobHeaderSize
	case MagicEncryptedBlob, MagicEncrComprBlob:
		return EncryptedBlobHeaderSize
	default:
		panic(fmt.Sprintf("unknown blob magic: %x", magic))
	}
}

// IsEncryptedMagic returns true for encrypted blob types.
func IsEncryptedMagic(magic [8]byte) bool {
	return magic == MagicEncryptedBlob || magic == MagicEncrComprBlob
}

// IsCompressedMagic returns true for compressed blob types.
func IsCompressedMagic(magic [8]byte) bool {
	return magic == MagicCompressedBlob || magic == MagicEncrComprBlob
}
