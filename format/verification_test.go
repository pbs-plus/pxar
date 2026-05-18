package format

import (
	"encoding/binary"
	"testing"
	"unsafe"
)

// TestSipHash24Vectors validates our SipHash-2-4 implementation against
// reference test vectors from the SipHash paper by Aumasson and Bernstein.
func TestSipHash24Vectors(t *testing.T) {
	tests := []struct {
		in  []byte
		out uint64
		key [16]byte
	}{
		// Reference vectors from SipHash-2-4 specification:
		// key = 00 01 02 ... 0f
		{
			key: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			in:  []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14},
			out: 0xa129ca6149be45e5,
		},
		// Empty message
		{
			key: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			in:  nil,
			out: 0x726fdb47dd0e0e31,
		},
	}

	for _, tt := range tests {
		k0 := binary.LittleEndian.Uint64(tt.key[0:8])
		k1 := binary.LittleEndian.Uint64(tt.key[8:16])
		got := siphash24(k0, k1, tt.in)
		if got != tt.out {
			t.Errorf("siphash24(key=%x, msg=%x) = %016x, want %016x", tt.key, tt.in, got, tt.out)
		}
	}
}

// TestSipHashDeterministic verifies that HashFilename with PBS keys produces
// deterministic, reproducible output.
func TestSipHashDeterministic(t *testing.T) {
	inputs := [][]byte{
		[]byte(""),
		[]byte("a"),
		[]byte("hello.txt"),
		[]byte("very.long.filename.with.many.dots.tar.gz"),
		[]byte("\x00\x01\x02\x03\x04\x05\x06\x07"),
	}

	// Run twice to verify deterministic output
	var results [][]uint64
	for range 2 {
		var run []uint64
		for _, in := range inputs {
			run = append(run, HashFilename(in))
		}
		results = append(results, run)
	}

	for i := range results[0] {
		if results[0][i] != results[1][i] {
			t.Errorf("HashFilename(%q) non-deterministic: run1=%016x run2=%016x",
				inputs[i], results[0][i], results[1][i])
		}
	}
}

// TestWireFormatStructSizes verifies that all wire-format structs have
// the exact byte sizes expected by the pxar binary format.
func TestWireFormatStructSizes(t *testing.T) {
	tests := []struct {
		name string
		got  uintptr
		want uintptr
	}{
		{"Header", unsafe.Sizeof(Header{}), 16},
		{"Stat", unsafe.Sizeof(Stat{}), 40},
		{"StatV1", unsafe.Sizeof(StatV1{}), 32},
		{"StatxTimestamp", unsafe.Sizeof(StatxTimestamp{}), 16},
		{"Device", unsafe.Sizeof(Device{}), 16},
		{"GoodbyeItem", unsafe.Sizeof(GoodbyeItem{}), 24},
		{"PayloadRef", unsafe.Sizeof(PayloadRef{}), 16},
		{"QuotaProjectID", unsafe.Sizeof(QuotaProjectID{}), 8},
	}

	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("%s: size=%d, want %d", tt.name, tt.got, tt.want)
		}
	}
}

// TestPXARMagicNumbers verifies that all pxar type constants are non-zero
// and distinct (no collisions).
func TestPXARMagicNumbers(t *testing.T) {
	types := map[string]uint64{
		"FORMAT_VERSION":       Version,
		"ENTRY":                PXAREntry,
		"ENTRY_V1":             PXAREntryV1,
		"PRELUDE":              PXARPrelude,
		"FILENAME":             PXARFilename,
		"SYMLINK":              PXARSymlink,
		"DEVICE":               PXARDevice,
		"XATTR":                PXARXAttr,
		"ACL_USER":             PXARACLUser,
		"ACL_GROUP":            PXARACLGroup,
		"ACL_GROUP_OBJ":        PXARACLGroupObj,
		"ACL_DEFAULT":          PXARACLDefault,
		"ACL_DEFAULT_USER":     PXARACLDefaultUser,
		"ACL_DEFAULT_GROUP":    PXARACLDefaultGroup,
		"FCAPS":                PXARFCaps,
		"QUOTA_PROJID":         PXARQuotaProjID,
		"HARDLINK":             PXARHardlink,
		"PAYLOAD":              PXARPayload,
		"PAYLOAD_REF":          PXARPayloadRef,
		"GOODBYE":              PXARGoodbye,
		"GOODBYE_TAIL_MARKER":  PXARGoodbyeTailMarker,
		"PAYLOAD_START_MARKER": PXARPayloadStartMarker,
		"PAYLOAD_TAIL_MARKER":  PXARPayloadTailMarker,
	}

	seen := make(map[uint64]string)
	for name, val := range types {
		if val == 0 {
			t.Errorf("%s has zero value", name)
		}
		if other, ok := seen[val]; ok {
			t.Errorf("%s and %s collide at %016x", name, other, val)
		}
		seen[val] = name
	}
}

// TestFileModeConstants verifies POSIX mode constants match expected values.
func TestFileModeConstants(t *testing.T) {
	tests := []struct {
		name  string
		value uint64
		octal uint64
	}{
		{"ModeIFMT", ModeIFMT, 0o170000},
		{"ModeIFSOCK", ModeIFSOCK, 0o140000},
		{"ModeIFLNK", ModeIFLNK, 0o120000},
		{"ModeIFREG", ModeIFREG, 0o100000},
		{"ModeIFBLK", ModeIFBLK, 0o060000},
		{"ModeIFDIR", ModeIFDIR, 0o040000},
		{"ModeIFCHR", ModeIFCHR, 0o020000},
		{"ModeIFIFO", ModeIFIFO, 0o010000},
	}

	for _, tt := range tests {
		if tt.value != tt.octal {
			t.Errorf("%s = %o, want %o", tt.name, tt.value, tt.octal)
		}
	}
}

// TestMaxSizeConstants verifies size limit constants are reasonable.
func TestMaxSizeConstants(t *testing.T) {
	if MaxFilenameLen < 1 || MaxFilenameLen > 256*1024 {
		t.Errorf("MaxFilenameLen=%d, expected reasonable value", MaxFilenameLen)
	}
	if MaxPathLen < 1 || MaxPathLen > 256*1024 {
		t.Errorf("MaxPathLen=%d, expected reasonable value", MaxPathLen)
	}
}

// TestHashKeys verifies PBS filename hash keys are non-zero and distinct.
func TestHashKeys(t *testing.T) {
	if HashKey1 == 0 {
		t.Error("HashKey1 is zero")
	}
	if HashKey2 == 0 {
		t.Error("HashKey2 is zero")
	}
	if HashKey1 == HashKey2 {
		t.Error("HashKey1 == HashKey2")
	}
}
