package datastore

import (
	"bytes"
	"hash/crc32"
	"testing"
)

func TestBlobEncodeUncompressed(t *testing.T) {
	data := []byte("hello world")
	blob, err := EncodeBlob(data)
	if err != nil {
		t.Fatal(err)
	}

	if blob.Magic() != MagicUncompressedBlob {
		t.Errorf("magic = %x, want uncompressed", blob.Magic())
	}

	// CRC should be over the data
	expectedCRC := crc32.ChecksumIEEE(data)
	if blob.CRC() != expectedCRC {
		t.Errorf("crc = %x, want %x", blob.CRC(), expectedCRC)
	}
}

func TestBlobDecodeUncompressed(t *testing.T) {
	data := []byte("hello world")
	blob, err := EncodeBlob(data)
	if err != nil {
		t.Fatal(err)
	}

	raw := blob.Bytes()
	decoded, err := DecodeBlob(raw)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(decoded, data) {
		t.Errorf("decoded = %q, want %q", decoded, data)
	}
}

func TestBlobEncodeCompressed(t *testing.T) {
	// Use highly compressible data
	data := bytes.Repeat([]byte("aaaaaaaaaa"), 10000)
	blob, err := EncodeCompressedBlob(data)
	if err != nil {
		t.Fatal(err)
	}

	if blob.Magic() != MagicCompressedBlob {
		t.Errorf("magic = %x, want compressed", blob.Magic())
	}

	// Compressed should be smaller
	raw := blob.Bytes()
	if len(raw) >= len(data)+BlobHeaderSize {
		t.Errorf("compressed blob (%d bytes) not smaller than original (%d)", len(raw), len(data))
	}

	decoded, err := DecodeBlob(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, data) {
		t.Error("decompressed data doesn't match original")
	}
}

func TestBlobCompressFallbackToUncompressed(t *testing.T) {
	// Random data won't compress well
	data := make([]byte, 256)
	for i := range data {
		data[i] = byte(i)
	}

	blob, err := EncodeCompressedBlob(data)
	if err != nil {
		t.Fatal(err)
	}

	// Should fall back to uncompressed if compression doesn't help
	if blob.Magic() != MagicUncompressedBlob {
		t.Errorf("expected fallback to uncompressed for incompressible data")
	}
}

func TestBlobCRCTamperDetection(t *testing.T) {
	data := []byte("important data")
	blob, err := EncodeBlob(data)
	if err != nil {
		t.Fatal(err)
	}

	raw := blob.Bytes()
	// Tamper with data
	raw[13] ^= 0xFF

	_, err = DecodeBlob(raw)
	if err == nil {
		t.Error("expected CRC error for tampered data")
	}
}

func TestBlobMagicTamperDetection(t *testing.T) {
	data := []byte("test")
	blob, err := EncodeBlob(data)
	if err != nil {
		t.Fatal(err)
	}

	raw := blob.Bytes()
	// Tamper with magic
	raw[0] ^= 0xFF

	_, err = DecodeBlob(raw)
	if err == nil {
		t.Error("expected error for tampered magic")
	}
}

func TestBlobMaxSize(t *testing.T) {
	data := make([]byte, MaxBlobSize+1)
	_, err := EncodeBlob(data)
	if err == nil {
		t.Error("expected error for blob exceeding max size")
	}
}

func TestBlobEmpty(t *testing.T) {
	blob, err := EncodeBlob(nil)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := DecodeBlob(blob.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 0 {
		t.Errorf("decoded %d bytes, want 0", len(decoded))
	}
}

func TestBlobRoundTripVariousSizes(t *testing.T) {
	sizes := []int{0, 1, 100, 4096, 64 * 1024, 1024 * 1024}
	for _, size := range sizes {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i & 0xFF)
		}

		blob, err := EncodeBlob(data)
		if err != nil {
			t.Fatalf("size %d: encode: %v", size, err)
		}

		decoded, err := DecodeBlob(blob.Bytes())
		if err != nil {
			t.Fatalf("size %d: decode: %v", size, err)
		}
		if !bytes.Equal(decoded, data) {
			t.Errorf("size %d: mismatch", size)
		}
	}
}

func TestBlobCompressedRoundTrip(t *testing.T) {
	// Use compressible data at various sizes
	sizes := []int{100, 4096, 64 * 1024}
	for _, size := range sizes {
		data := bytes.Repeat([]byte("abcdefghij"), size/10+1)
		data = data[:size]

		blob, err := EncodeCompressedBlob(data)
		if err != nil {
			t.Fatalf("size %d: encode: %v", size, err)
		}

		decoded, err := DecodeBlob(blob.Bytes())
		if err != nil {
			t.Fatalf("size %d: decode: %v", size, err)
		}
		if !bytes.Equal(decoded, data) {
			t.Errorf("size %d: mismatch", size)
		}
	}
}

func TestBlobIsMethods(t *testing.T) {
	data := []byte("test")

	ub, _ := EncodeBlob(data)
	if ub.IsCompressed() || ub.IsEncrypted() {
		t.Error("uncompressed blob should not report compressed/encrypted")
	}

	cb, _ := EncodeCompressedBlob(bytes.Repeat(data, 1000))
	if !cb.IsCompressed() {
		t.Error("compressed blob should report compressed")
	}
	if cb.IsEncrypted() {
		t.Error("compressed blob should not report encrypted")
	}
}

func TestBlobRawData(t *testing.T) {
	data := []byte("test data")
	blob, err := EncodeBlob(data)
	if err != nil {
		t.Fatal(err)
	}

	raw := blob.Bytes()
	if len(raw) != BlobHeaderSize+len(data) {
		t.Errorf("raw size = %d, want %d", len(raw), BlobHeaderSize+len(data))
	}
}

func TestDecodeBlobTooShort(t *testing.T) {
	_, err := DecodeBlob([]byte{1, 2, 3})
	if err == nil {
		t.Error("expected error for too-short input")
	}
}

func TestBlobZeroAllocEncode(t *testing.T) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i)
	}

	allocs := testing.AllocsPerRun(100, func() {
		EncodeBlob(data)
	})
	// Two allocations: output buffer + DataBlob struct wrapper
	if allocs > 2 {
		t.Errorf("EncodeBlob allocated %.1f times, expected <= 2", allocs)
	}
}
