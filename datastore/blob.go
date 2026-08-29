package datastore

import (
	"fmt"
	"hash/crc32"
	"sync"

	"github.com/klauspost/compress/zstd"
)

var zstdEncoderPool = sync.Pool{
	New: func() any {
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
		if err != nil {
			panic(fmt.Sprintf("zstd encoder init: %v", err))
		}
		return enc
	},
}

var zstdDecoderPool = sync.Pool{
	New: func() any {
		dec, err := zstd.NewReader(nil)
		if err != nil {
			panic(fmt.Sprintf("zstd decoder init: %v", err))
		}
		return dec
	},
}

// EncodeBlob encodes data into dst as an uncompressed blob.
func EncodeBlob(dst []byte, data []byte) ([]byte, error) {
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

// EncodeCompressedBlob encodes data into dst, compressing it when smaller.
func EncodeCompressedBlob(dst []byte, data []byte) ([]byte, error) {
	if len(data) > MaxBlobSize {
		return nil, fmt.Errorf("blob data too large: %d > %d", len(data), MaxBlobSize)
	}

	if len(data) < 32 {
		return EncodeBlob(dst, data)
	}

	compressed, err := zstdCompress(data)
	if err != nil {
		return nil, fmt.Errorf("zstd compress: %w", err)
	}

	if len(compressed) >= len(data) {
		return EncodeBlob(dst, data)
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

// DecodeBlob verifies and decodes raw into dst.
// For uncompressed blobs, returns a slice into raw (zero allocation).
// For encrypted blobs, use DecodeEncryptedBlob with a CryptConfig.
func DecodeBlob(dst []byte, raw []byte) ([]byte, error) {
	if len(raw) < BlobHeaderSize {
		return nil, fmt.Errorf("blob too short: %d bytes", len(raw))
	}

	var magic [8]byte
	copy(magic[:], raw[0:8])

	if err := validateBlobMagic(magic); err != nil {
		return nil, err
	}

	if IsEncryptedMagic(magic) {
		return nil, fmt.Errorf("encrypted blob requires CryptConfig, use DecodeEncryptedBlob")
	}

	hdrSize, err := BlobHeaderSizeFor(magic)
	if err != nil {
		return nil, err
	}
	if len(raw) < hdrSize {
		return nil, fmt.Errorf("blob too short for header: %d < %d", len(raw), hdrSize)
	}

	storedCRC := binaryUint32(raw[8:12])
	data := raw[hdrSize:]

	if crc32.ChecksumIEEE(data) != storedCRC {
		return nil, fmt.Errorf("blob CRC mismatch")
	}

	if IsCompressedMagic(magic) {
		dec := zstdDecoderPool.Get().(*zstd.Decoder)
		defer zstdDecoderPool.Put(dec)
		result, err := dec.DecodeAll(data, dst[:0])
		if err != nil {
			return nil, fmt.Errorf("zstd decompress: %w", err)
		}
		return result, nil
	}

	return data, nil
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

// encodeEncrypted is the shared logic for encrypting and encoding a blob.
// It returns the magic, ciphertext, iv (16 bytes, zero-padded nonce), and tag (16 bytes).
func encodeEncrypted(data []byte, cc *CryptConfig, compress bool) (magic [8]byte, ciphertext, iv, tag []byte, err error) {
	if len(data) > MaxBlobSize {
		err = fmt.Errorf("blob data too large: %d > %d", len(data), MaxBlobSize)
		return
	}

	var plaintext []byte
	if compress && len(data) >= 32 {
		compressed, compressErr := zstdCompress(data)
		if compressErr != nil {
			err = fmt.Errorf("zstd compress: %w", compressErr)
			return
		}
		if len(compressed) < len(data) {
			plaintext = compressed
			magic = MagicEncrComprBlob
		}
	}

	if plaintext == nil {
		plaintext = data
		magic = MagicEncryptedBlob
	}

	encrypted, encryptErr := cc.Encrypt(plaintext)
	if encryptErr != nil {
		err = fmt.Errorf("encrypt: %w", encryptErr)
		return
	}

	iv = encrypted[:16]
	ciphertext = encrypted[16 : len(encrypted)-16]
	tag = encrypted[len(encrypted)-16:]

	return
}

// EncodeEncryptedBlob encrypts data into dst, optionally compressing it first.
func EncodeEncryptedBlob(dst []byte, data []byte, cc *CryptConfig, compress bool) ([]byte, error) {
	if cc == nil {
		return nil, fmt.Errorf("CryptConfig required for encrypted blob")
	}

	magic, ciphertext, iv, tag, err := encodeEncrypted(data, cc, compress)
	if err != nil {
		return nil, err
	}

	n := EncryptedBlobHeaderSize + len(ciphertext)
	if cap(dst) < n {
		dst = make([]byte, n)
	} else {
		dst = dst[:n]
	}

	copy(dst[0:8], magic[:])
	binaryPutUint32(dst[8:12], crc32.ChecksumIEEE(ciphertext))
	copy(dst[12:28], iv)
	copy(dst[28:44], tag)
	copy(dst[EncryptedBlobHeaderSize:], ciphertext)

	return dst, nil
}

// DecodeEncryptedBlob decrypts a blob, reusing dst when possible.
func DecodeEncryptedBlob(dst []byte, raw []byte, cc *CryptConfig) ([]byte, error) {
	if cc == nil {
		return nil, fmt.Errorf("CryptConfig required for encrypted blob")
	}

	hdr, err := UnmarshalEncryptedBlobHeader(raw)
	if err != nil {
		return nil, err
	}

	data := raw[EncryptedBlobHeaderSize:]
	if crc32.ChecksumIEEE(data) != hdr.CRC {
		return nil, fmt.Errorf("encrypted blob CRC mismatch")
	}

	if hdr.Magic == MagicEncrComprBlob {
		gcmData := make([]byte, 0, len(data)+len(hdr.Tag))
		gcmData = append(gcmData, data...)
		gcmData = append(gcmData, hdr.Tag[:]...)
		decrypted, err := cc.cipher.Open(gcmData[:0], hdr.IV[:], gcmData, nil)
		if err != nil {
			return nil, fmt.Errorf("decrypt blob: %w", err)
		}

		dec := zstdDecoderPool.Get().(*zstd.Decoder)
		decompressed, err := dec.DecodeAll(decrypted, dst[:0])
		zstdDecoderPool.Put(dec)
		if err != nil {
			return nil, fmt.Errorf("zstd decompress: %w", err)
		}
		return decompressed, nil
	}

	gcmData := append(dst[:0], data...)
	gcmData = append(gcmData, hdr.Tag[:]...)
	decrypted, err := cc.cipher.Open(gcmData[:0], hdr.IV[:], gcmData, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt blob: %w", err)
	}
	return decrypted, nil
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
