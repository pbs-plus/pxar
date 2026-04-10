package datastore

import (
	"bytes"
	"testing"
)

func TestMagicNumbers(t *testing.T) {
	magics := []struct {
		name  string
		magic [8]byte
	}{
		{"UncompressedBlob", MagicUncompressedBlob},
		{"CompressedBlob", MagicCompressedBlob},
		{"EncryptedBlob", MagicEncryptedBlob},
		{"EncrComprBlob", MagicEncrComprBlob},
		{"FixedChunkIndex", MagicFixedChunkIndex},
		{"DynamicChunkIndex", MagicDynamicChunkIndex},
	}

	seen := make(map[[8]byte]string)
	for _, m := range magics {
		if prev, ok := seen[m.magic]; ok {
			t.Errorf("duplicate magic %x: %s and %s", m.magic, prev, m.name)
		}
		seen[m.magic] = m.name
		if m.magic == [8]byte{} {
			t.Errorf("%s magic is all zeros", m.name)
		}
	}
}

func TestBlobHeaderRoundTrip(t *testing.T) {
	orig := BlobHeader{
		Magic: MagicCompressedBlob,
		CRC:   0xDEADBEEF,
	}

	var buf [BlobHeaderSize]byte
	orig.MarshalTo(buf[:])

	got, err := UnmarshalBlobHeader(buf[:])
	if err != nil {
		t.Fatal(err)
	}
	if got.Magic != orig.Magic {
		t.Errorf("magic = %x, want %x", got.Magic, orig.Magic)
	}
	if got.CRC != orig.CRC {
		t.Errorf("crc = %x, want %x", got.CRC, orig.CRC)
	}
}

func TestEncryptedBlobHeaderRoundTrip(t *testing.T) {
	orig := EncryptedBlobHeader{
		Magic: MagicEncryptedBlob,
		CRC:   0xCAFEBABE,
	}
	copy(orig.IV[:], bytes.Repeat([]byte{0xAA}, 16))
	copy(orig.Tag[:], bytes.Repeat([]byte{0xBB}, 16))

	var buf [EncryptedBlobHeaderSize]byte
	orig.MarshalTo(buf[:])

	got, err := UnmarshalEncryptedBlobHeader(buf[:])
	if err != nil {
		t.Fatal(err)
	}
	if got.Magic != orig.Magic {
		t.Errorf("magic = %x, want %x", got.Magic, orig.Magic)
	}
	if got.CRC != orig.CRC {
		t.Errorf("crc = %x, want %x", got.CRC, orig.CRC)
	}
	if !bytes.Equal(got.IV[:], orig.IV[:]) {
		t.Errorf("iv mismatch")
	}
	if !bytes.Equal(got.Tag[:], orig.Tag[:]) {
		t.Errorf("tag mismatch")
	}
}

func TestHeaderSize(t *testing.T) {
	tests := []struct {
		magic [8]byte
		want  int
	}{
		{MagicUncompressedBlob, BlobHeaderSize},
		{MagicCompressedBlob, BlobHeaderSize},
		{MagicEncryptedBlob, EncryptedBlobHeaderSize},
		{MagicEncrComprBlob, EncryptedBlobHeaderSize},
	}
	for _, tt := range tests {
		got := BlobHeaderSizeFor(tt.magic)
		if got != tt.want {
			t.Errorf("BlobHeaderSizeFor(%x) = %d, want %d", tt.magic, got, tt.want)
		}
	}
}

func TestHeaderSizeUnknown(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for unknown magic")
		}
	}()
	BlobHeaderSizeFor([8]byte{1, 2, 3, 4, 5, 6, 7, 8})
}

func TestDynamicIndexHeaderRoundTrip(t *testing.T) {
	orig := DynamicIndexHeader{
		Magic: MagicDynamicChunkIndex,
		Ctime: 1700000000,
	}
	copy(orig.UUID[:], bytes.Repeat([]byte{0x11}, 16))
	copy(orig.IndexCsum[:], bytes.Repeat([]byte{0x22}, 32))

	var buf [IndexHeaderSize]byte
	orig.MarshalTo(buf[:])

	got, err := UnmarshalDynamicIndexHeader(buf[:])
	if err != nil {
		t.Fatal(err)
	}
	if got.Magic != orig.Magic {
		t.Errorf("magic mismatch")
	}
	if got.Ctime != orig.Ctime {
		t.Errorf("ctime = %d, want %d", got.Ctime, orig.Ctime)
	}
	if !bytes.Equal(got.UUID[:], orig.UUID[:]) {
		t.Errorf("uuid mismatch")
	}
	if !bytes.Equal(got.IndexCsum[:], orig.IndexCsum[:]) {
		t.Errorf("index_csum mismatch")
	}
}

func TestFixedIndexHeaderRoundTrip(t *testing.T) {
	orig := FixedIndexHeader{
		Magic:     MagicFixedChunkIndex,
		Ctime:     1700000000,
		Size:      10 << 30, // 10GB virtual
		ChunkSize: 4 << 20,  // 4MB chunks
	}
	copy(orig.UUID[:], bytes.Repeat([]byte{0x33}, 16))
	copy(orig.IndexCsum[:], bytes.Repeat([]byte{0x44}, 32))

	var buf [IndexHeaderSize]byte
	orig.MarshalTo(buf[:])

	got, err := UnmarshalFixedIndexHeader(buf[:])
	if err != nil {
		t.Fatal(err)
	}
	if got.Magic != orig.Magic {
		t.Errorf("magic mismatch")
	}
	if got.Ctime != orig.Ctime {
		t.Errorf("ctime = %d, want %d", got.Ctime, orig.Ctime)
	}
	if got.Size != orig.Size {
		t.Errorf("size = %d, want %d", got.Size, orig.Size)
	}
	if got.ChunkSize != orig.ChunkSize {
		t.Errorf("chunk_size = %d, want %d", got.ChunkSize, orig.ChunkSize)
	}
	if !bytes.Equal(got.UUID[:], orig.UUID[:]) {
		t.Errorf("uuid mismatch")
	}
	if !bytes.Equal(got.IndexCsum[:], orig.IndexCsum[:]) {
		t.Errorf("index_csum mismatch")
	}
}

func TestDynamicEntrySize(t *testing.T) {
	if DynamicEntrySize != 40 {
		t.Errorf("DynamicEntrySize = %d, want 40", DynamicEntrySize)
	}
}

func TestProxmoxMagicValues(t *testing.T) {
	// Verify exact magic values from Proxmox source
	tests := []struct {
		name  string
		magic [8]byte
		want  [8]byte
	}{
		{"UncompressedBlob", MagicUncompressedBlob, [8]byte{66, 171, 56, 7, 190, 131, 112, 161}},
		{"CompressedBlob", MagicCompressedBlob, [8]byte{49, 185, 88, 66, 111, 182, 163, 127}},
		{"EncryptedBlob", MagicEncryptedBlob, [8]byte{123, 103, 133, 190, 34, 45, 76, 240}},
		{"EncrComprBlob", MagicEncrComprBlob, [8]byte{230, 89, 27, 191, 11, 191, 216, 11}},
		{"FixedChunkIndex", MagicFixedChunkIndex, [8]byte{47, 127, 65, 237, 145, 253, 15, 205}},
		{"DynamicChunkIndex", MagicDynamicChunkIndex, [8]byte{28, 145, 78, 165, 25, 186, 179, 205}},
	}
	for _, tt := range tests {
		if tt.magic != tt.want {
			t.Errorf("%s magic = %v, want %v", tt.name, tt.magic, tt.want)
		}
	}
}

func TestBlobMagicDetection(t *testing.T) {
	tests := []struct {
		magic [8]byte
		enc   bool
		comp  bool
	}{
		{MagicUncompressedBlob, false, false},
		{MagicCompressedBlob, false, true},
		{MagicEncryptedBlob, true, false},
		{MagicEncrComprBlob, true, true},
	}
	for _, tt := range tests {
		if IsEncryptedMagic(tt.magic) != tt.enc {
			t.Errorf("IsEncryptedMagic(%x) = %v, want %v", tt.magic, !tt.enc, tt.enc)
		}
		if IsCompressedMagic(tt.magic) != tt.comp {
			t.Errorf("IsCompressedMagic(%x) = %v, want %v", tt.magic, !tt.comp, tt.comp)
		}
	}
}
