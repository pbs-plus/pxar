package datastore

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"
)

func blobMagic(raw []byte) (magic [8]byte) {
	copy(magic[:], raw)
	return magic
}

func TestBlobEncodeUncompressed(t *testing.T) {
	data := []byte("hello world")
	blob, err := EncodeBlob(nil, data)
	if err != nil {
		t.Fatal(err)
	}

	raw := blob
	var magic [8]byte
	copy(magic[:], raw[:8])
	if magic != MagicUncompressedBlob {
		t.Errorf("magic = %x, want uncompressed", magic)
	}

	// CRC should be over the data
	expectedCRC := crc32.ChecksumIEEE(data)
	storedCRC := binary.LittleEndian.Uint32(raw[8:12])
	if storedCRC != expectedCRC {
		t.Errorf("crc = %x, want %x", storedCRC, expectedCRC)
	}
}

func TestBlobDecodeUncompressed(t *testing.T) {
	data := []byte("hello world")
	blob, err := EncodeBlob(nil, data)
	if err != nil {
		t.Fatal(err)
	}

	raw := blob
	decoded, err := DecodeBlob(nil, raw)
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
	blob, err := EncodeCompressedBlob(nil, data)
	if err != nil {
		t.Fatal(err)
	}

	if blobMagic(blob) != MagicCompressedBlob {
		t.Errorf("magic = %x, want compressed", blobMagic(blob))
	}

	// Compressed should be smaller
	raw := blob
	if len(raw) >= len(data)+BlobHeaderSize {
		t.Errorf("compressed blob (%d bytes) not smaller than original (%d)", len(raw), len(data))
	}

	decoded, err := DecodeBlob(nil, raw)
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

	blob, err := EncodeCompressedBlob(nil, data)
	if err != nil {
		t.Fatal(err)
	}

	// Should fall back to uncompressed if compression doesn't help
	if blobMagic(blob) != MagicUncompressedBlob {
		t.Errorf("expected fallback to uncompressed for incompressible data")
	}
}

func TestBlobCRCTamperDetection(t *testing.T) {
	data := []byte("important data")
	blob, err := EncodeBlob(nil, data)
	if err != nil {
		t.Fatal(err)
	}

	raw := blob
	// Tamper with data
	raw[13] ^= 0xFF

	_, err = DecodeBlob(nil, raw)
	if err == nil {
		t.Error("expected CRC error for tampered data")
	}
}

func TestBlobMagicTamperDetection(t *testing.T) {
	data := []byte("test")
	blob, err := EncodeBlob(nil, data)
	if err != nil {
		t.Fatal(err)
	}

	raw := blob
	// Tamper with magic
	raw[0] ^= 0xFF

	_, err = DecodeBlob(nil, raw)
	if err == nil {
		t.Error("expected error for tampered magic")
	}
}

func TestBlobMaxSize(t *testing.T) {
	data := make([]byte, MaxBlobSize+1)
	_, err := EncodeBlob(nil, data)
	if err == nil {
		t.Error("expected error for blob exceeding max size")
	}
}

func TestBlobEmpty(t *testing.T) {
	blob, err := EncodeBlob(nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := DecodeBlob(nil, blob)
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

		blob, err := EncodeBlob(nil, data)
		if err != nil {
			t.Fatalf("size %d: encode: %v", size, err)
		}

		decoded, err := DecodeBlob(nil, blob)
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

		blob, err := EncodeCompressedBlob(nil, data)
		if err != nil {
			t.Fatalf("size %d: encode: %v", size, err)
		}

		decoded, err := DecodeBlob(nil, blob)
		if err != nil {
			t.Fatalf("size %d: decode: %v", size, err)
		}
		if !bytes.Equal(decoded, data) {
			t.Errorf("size %d: mismatch", size)
		}
	}
}

func TestBlobMagicKinds(t *testing.T) {
	data := []byte("test")

	ub, _ := EncodeBlob(nil, data)
	if IsCompressedMagic(blobMagic(ub)) || IsEncryptedMagic(blobMagic(ub)) {
		t.Error("uncompressed blob should not report compressed/encrypted")
	}

	cb, _ := EncodeCompressedBlob(nil, bytes.Repeat(data, 1000))
	if !IsCompressedMagic(blobMagic(cb)) {
		t.Error("compressed blob should report compressed")
	}
	if IsEncryptedMagic(blobMagic(cb)) {
		t.Error("compressed blob should not report encrypted")
	}
}

func TestBlobRawData(t *testing.T) {
	data := []byte("test data")
	blob, err := EncodeBlob(nil, data)
	if err != nil {
		t.Fatal(err)
	}

	raw := blob
	if len(raw) != BlobHeaderSize+len(data) {
		t.Errorf("raw size = %d, want %d", len(raw), BlobHeaderSize+len(data))
	}
}

func TestDecodeBlobTooShort(t *testing.T) {
	_, err := DecodeBlob(nil, []byte{1, 2, 3})
	if err == nil {
		t.Error("expected error for too-short input")
	}
}

func TestBlobEncodeReusesDestination(t *testing.T) {
	data := make([]byte, 4096)
	dst := make([]byte, 0, BlobHeaderSize+len(data))

	allocs := testing.AllocsPerRun(100, func() {
		_, _ = EncodeBlob(dst, data)
	})
	if allocs != 0 {
		t.Errorf("EncodeBlob allocated %.1f times, want 0", allocs)
	}
}

func TestEncryptedBlobRoundTrip(t *testing.T) {
	key, err := CreateRandomKey()
	if err != nil {
		t.Fatal(err)
	}
	cc, err := NewCryptConfig(key)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("secret backup data")
	blob, err := EncodeEncryptedBlob(nil, data, cc, false)
	if err != nil {
		t.Fatal(err)
	}

	if blobMagic(blob) != MagicEncryptedBlob {
		t.Errorf("magic = %x, want encrypted", blobMagic(blob))
	}

	if !IsEncryptedMagic(blobMagic(blob)) {
		t.Error("blob should report as encrypted")
	}

	decrypted, err := DecodeEncryptedBlob(nil, blob, cc)
	if err != nil {
		t.Fatal(err)
	}

	if string(decrypted) != string(data) {
		t.Errorf("decrypted = %q, want %q", decrypted, data)
	}
}

func TestEncryptedCompressedBlobRoundTrip(t *testing.T) {
	key, err := CreateRandomKey()
	if err != nil {
		t.Fatal(err)
	}
	cc, err := NewCryptConfig(key)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}

	blob, err := EncodeEncryptedBlob(nil, data, cc, true)
	if err != nil {
		t.Fatal(err)
	}

	if blobMagic(blob) != MagicEncrComprBlob && blobMagic(blob) != MagicEncryptedBlob {
		t.Errorf("magic = %x, want encrypted or encrypted+compressed", blobMagic(blob))
	}

	decrypted, err := DecodeEncryptedBlob(nil, blob, cc)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(decrypted, data) {
		t.Errorf("decrypted data mismatch (len %d vs %d)", len(decrypted), len(data))
	}
}

func TestEncryptedBlobWrongKey(t *testing.T) {
	key1, _ := CreateRandomKey()
	cc1, _ := NewCryptConfig(key1)

	key2, _ := CreateRandomKey()
	cc2, _ := NewCryptConfig(key2)

	data := []byte("secret data")
	blob, err := EncodeEncryptedBlob(nil, data, cc1, false)
	if err != nil {
		t.Fatal(err)
	}

	_, err = DecodeEncryptedBlob(nil, blob, cc2)
	if err == nil {
		t.Error("expected error decrypting with wrong key")
	}
}

func TestDecryptPlainBlobFails(t *testing.T) {
	key, _ := CreateRandomKey()
	cc, _ := NewCryptConfig(key)

	data := []byte("plain data")
	blob, err := EncodeBlob(nil, data)
	if err != nil {
		t.Fatal(err)
	}

	_, err = DecodeEncryptedBlob(nil, blob, cc)
	if err == nil {
		t.Error("expected error decrypting plain blob with DecodeEncryptedBlob")
	}
}

func TestDecryptEncryptedWithPlainFails(t *testing.T) {
	key, _ := CreateRandomKey()
	cc, _ := NewCryptConfig(key)

	data := []byte("secret data")
	blob, err := EncodeEncryptedBlob(nil, data, cc, false)
	if err != nil {
		t.Fatal(err)
	}

	_, err = DecodeBlob(nil, blob)
	if err == nil {
		t.Error("expected error calling DecodeBlob on encrypted blob")
	}
}
