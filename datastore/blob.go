package datastore

import (
	"fmt"
	"hash/crc32"
	"sync"

	"github.com/klauspost/compress/zstd"
)

var zstdEncoderPool = sync.Pool{
	New: func() interface{} {
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
		if err != nil {
			panic(fmt.Sprintf("zstd encoder init: %v", err))
		}
		return enc
	},
}

var zstdDecoderPool = sync.Pool{
	New: func() interface{} {
		dec, err := zstd.NewReader(nil)
		if err != nil {
			panic(fmt.Sprintf("zstd decoder init: %v", err))
		}
		return dec
	},
}

// DataBlob represents a stored data blob with optional compression.
// The raw data contains the magic, CRC, and payload.
type DataBlob struct {
	raw []byte
}

// EncodeBlob creates an uncompressed blob from data.
func EncodeBlob(data []byte) (*DataBlob, error) {
	if len(data) > MaxBlobSize {
		return nil, fmt.Errorf("blob data too large: %d > %d", len(data), MaxBlobSize)
	}

	raw := make([]byte, BlobHeaderSize+len(data))
	copy(raw[0:8], MagicUncompressedBlob[:])
	binaryPutUint32(raw[8:12], crc32.ChecksumIEEE(data))
	copy(raw[BlobHeaderSize:], data)

	return &DataBlob{raw: raw}, nil
}

// EncodeBlobTo encodes data as an uncompressed blob into dst, which must have
// capacity of at least BlobHeaderSize+len(data). Returns the slice of dst
// containing the encoded blob. This avoids the DataBlob wrapper allocation.
func EncodeBlobTo(dst []byte, data []byte) ([]byte, error) {
	if len(data) > MaxBlobSize {
		return nil, fmt.Errorf("blob data too large: %d > %d", len(data), MaxBlobSize)
	}

	n := BlobHeaderSize + len(data)
	if cap(dst) < n {
		dst = make([]byte, n)
	} else {
		dst = dst[:n]
	}
	copy(dst[0:8], MagicUncompressedBlob[:])
	binaryPutUint32(dst[8:12], crc32.ChecksumIEEE(data))
	copy(dst[BlobHeaderSize:], data)

	return dst, nil
}

// EncodeCompressedBlob creates a compressed blob. Falls back to uncompressed
// if compression doesn't reduce size.
func EncodeCompressedBlob(data []byte) (*DataBlob, error) {
	if len(data) > MaxBlobSize {
		return nil, fmt.Errorf("blob data too large: %d > %d", len(data), MaxBlobSize)
	}

	if len(data) < 32 {
		return EncodeBlob(data)
	}

	compressed, err := zstdCompress(data)
	if err != nil {
		return nil, fmt.Errorf("zstd compress: %w", err)
	}

	if len(compressed) >= len(data) {
		return EncodeBlob(data)
	}

	raw := make([]byte, BlobHeaderSize+len(compressed))
	copy(raw[0:8], MagicCompressedBlob[:])
	binaryPutUint32(raw[8:12], crc32.ChecksumIEEE(compressed))
	copy(raw[BlobHeaderSize:], compressed)

	return &DataBlob{raw: raw}, nil
}

// EncodeCompressedBlobTo encodes data as a compressed blob into dst.
// If compression doesn't reduce size, falls back to uncompressed format.
// Returns the slice of dst containing the encoded blob.
func EncodeCompressedBlobTo(dst []byte, data []byte) ([]byte, error) {
	if len(data) > MaxBlobSize {
		return nil, fmt.Errorf("blob data too large: %d > %d", len(data), MaxBlobSize)
	}

	if len(data) < 32 {
		return EncodeBlobTo(dst, data)
	}

	compressed, err := zstdCompress(data)
	if err != nil {
		return nil, fmt.Errorf("zstd compress: %w", err)
	}

	if len(compressed) >= len(data) {
		return EncodeBlobTo(dst, data)
	}

	n := BlobHeaderSize + len(compressed)
	if cap(dst) < n {
		dst = make([]byte, n)
	} else {
		dst = dst[:n]
	}
	copy(dst[0:8], MagicCompressedBlob[:])
	binaryPutUint32(dst[8:12], crc32.ChecksumIEEE(compressed))
	copy(dst[BlobHeaderSize:], compressed)

	return dst, nil
}

// DecodeBlob decodes a raw blob, verifies CRC, and returns the payload data.
func DecodeBlob(raw []byte) ([]byte, error) {
	if len(raw) < BlobHeaderSize {
		return nil, fmt.Errorf("blob too short: %d bytes", len(raw))
	}

	var magic [8]byte
	copy(magic[:], raw[0:8])

	if err := validateBlobMagic(magic); err != nil {
		return nil, err
	}

	hdrSize := BlobHeaderSizeFor(magic)
	if len(raw) < hdrSize {
		return nil, fmt.Errorf("blob too short for header: %d < %d", len(raw), hdrSize)
	}

	storedCRC := binaryUint32(raw[8:12])
	data := raw[hdrSize:]

	if crc32.ChecksumIEEE(data) != storedCRC {
		return nil, fmt.Errorf("blob CRC mismatch")
	}

	if IsCompressedMagic(magic) {
		decompressed, err := zstdDecompress(data)
		if err != nil {
			return nil, fmt.Errorf("zstd decompress: %w", err)
		}
		return decompressed, nil
	}

	return data, nil
}

// Bytes returns the raw blob bytes (header + payload).
func (b *DataBlob) Bytes() []byte { return b.raw }

// Magic returns the blob magic number.
func (b *DataBlob) Magic() [8]byte {
	var m [8]byte
	copy(m[:], b.raw[0:8])
	return m
}

// CRC returns the stored CRC32 value.
func (b *DataBlob) CRC() uint32 {
	return binaryUint32(b.raw[8:12])
}

// IsCompressed returns true if the blob uses compression.
func (b *DataBlob) IsCompressed() bool {
	return IsCompressedMagic(b.Magic())
}

// IsEncrypted returns true if the blob uses encryption.
func (b *DataBlob) IsEncrypted() bool {
	return IsEncryptedMagic(b.Magic())
}

func validateBlobMagic(magic [8]byte) error {
	switch magic {
	case MagicUncompressedBlob, MagicCompressedBlob,
		MagicEncryptedBlob, MagicEncrComprBlob:
		return nil
	default:
		return fmt.Errorf("unknown blob magic: %x", magic)
	}
}

func zstdCompress(data []byte) ([]byte, error) {
	enc := zstdEncoderPool.Get().(*zstd.Encoder)
	defer zstdEncoderPool.Put(enc)
	return enc.EncodeAll(data, nil), nil
}

func zstdDecompress(data []byte) ([]byte, error) {
	dec := zstdDecoderPool.Get().(*zstd.Decoder)
	defer zstdDecoderPool.Put(dec)
	return dec.DecodeAll(data, nil)
}

func binaryPutUint32(buf []byte, v uint32) {
	_ = buf[3]
	buf[0] = byte(v)
	buf[1] = byte(v >> 8)
	buf[2] = byte(v >> 16)
	buf[3] = byte(v >> 24)
}

func binaryUint32(buf []byte) uint32 {
	_ = buf[3]
	return uint32(buf[0]) | uint32(buf[1])<<8 | uint32(buf[2])<<16 | uint32(buf[3])<<24
}
