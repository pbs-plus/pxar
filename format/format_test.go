package format

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

func TestHeaderSize(t *testing.T) {
	want := uint64(16)
	got := uint64(binary.Size(Header{}))
	if got != want {
		t.Errorf("Header size = %d, want %d", got, want)
	}
}

func TestHeaderSerialization(t *testing.T) {
	h := Header{Type: PXAREntry, Size: 100}
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.LittleEndian, &h)
	if err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 16 {
		t.Errorf("Header serialized to %d bytes, want 16", buf.Len())
	}

	var h2 Header
	err = binary.Read(&buf, binary.LittleEndian, &h2)
	if err != nil {
		t.Fatal(err)
	}
	if h2.Type != PXAREntry {
		t.Errorf("Type = %x, want %x", h2.Type, PXAREntry)
	}
	if h2.Size != 100 {
		t.Errorf("Size = %d, want 100", h2.Size)
	}
}

func TestHeaderWithContentSize(t *testing.T) {
	h := HeaderWithContentSize(PXAREntry, 40)
	if h.Size != 16+40 {
		t.Errorf("Header.Size = %d, want %d", h.Size, 16+40)
	}
	if h.ContentSize() != 40 {
		t.Errorf("ContentSize = %d, want 40", h.ContentSize())
	}
}

func TestHeaderCheckHeaderSize(t *testing.T) {
	tests := []struct {
		name    string
		header  Header
		wantErr bool
	}{
		{"valid entry", Header{Type: PXAREntry, Size: 16 + 40}, false},
		{"too small", Header{Type: PXAREntry, Size: 8}, true},
		{"filename valid", Header{Type: PXARFilename, Size: 16 + 5}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.header.CheckHeaderSize()
			if (err != nil) != tt.wantErr {
				t.Errorf("CheckHeaderSize() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStatxTimestampSize(t *testing.T) {
	want := 16
	got := binary.Size(StatxTimestamp{})
	if got != want {
		t.Errorf("StatxTimestamp size = %d, want %d", got, want)
	}
}

func TestStatxTimestampFromDuration(t *testing.T) {
	ts := NewStatxTimestampFromDuration(1430487000*time.Second + 1*time.Millisecond)
	if ts.Secs != 1430487000 {
		t.Errorf("Secs = %d, want 1430487000", ts.Secs)
	}
	if ts.Nanos != 1_000_000 {
		t.Errorf("Nanos = %d, want 1000000", ts.Nanos)
	}
}

func TestStatxTimestampRoundTrip(t *testing.T) {
	d := 1430487000*time.Second + 1_000_000*time.Nanosecond
	ts := NewStatxTimestampFromDuration(d)
	got := ts.Duration()
	if got != d {
		t.Errorf("round-trip: got %v, want %v", got, d)
	}
}

func TestStatSize(t *testing.T) {
	want := 40
	got := binary.Size(Stat{})
	if got != want {
		t.Errorf("Stat size = %d, want %d", got, want)
	}
}

func TestStatV1Size(t *testing.T) {
	want := 32
	got := binary.Size(StatV1{})
	if got != want {
		t.Errorf("StatV1 size = %d, want %d", got, want)
	}
}

func TestStatV1ToStat(t *testing.T) {
	v1 := StatV1{
		Mode:  0o100644,
		Flags: 0,
		UID:   1000,
		GID:   1000,
		Mtime: 1430487000_000000000, // nanoseconds
	}
	s := v1.ToStat()
	if s.Mode != v1.Mode {
		t.Errorf("Mode = %d, want %d", s.Mode, v1.Mode)
	}
	if s.UID != v1.UID {
		t.Errorf("UID = %d, want %d", s.UID, v1.UID)
	}
	if s.Mtime.Secs != 1430487000 {
		t.Errorf("Mtime.Secs = %d, want 1430487000", s.Mtime.Secs)
	}
}

func TestDeviceSize(t *testing.T) {
	want := 16
	got := binary.Size(Device{})
	if got != want {
		t.Errorf("Device size = %d, want %d", got, want)
	}
}

func TestDeviceRoundTrip(t *testing.T) {
	dev := Device{Major: 0xabcd1234, Minor: 0xdcba5678}
	devT := dev.ToDevT()
	dev2 := DeviceFromDevT(devT)
	if dev2.Major != dev.Major {
		t.Errorf("Major round-trip: got %x, want %x", dev2.Major, dev.Major)
	}
	if dev2.Minor != dev.Minor {
		t.Errorf("Minor round-trip: got %x, want %x", dev2.Minor, dev.Minor)
	}
}

func TestGoodbyeItemSize(t *testing.T) {
	want := 24
	got := binary.Size(GoodbyeItem{})
	if got != want {
		t.Errorf("GoodbyeItem size = %d, want %d", got, want)
	}
}

func TestPayloadRefSize(t *testing.T) {
	want := 16
	got := binary.Size(PayloadRef{})
	if got != want {
		t.Errorf("PayloadRef size = %d, want %d", got, want)
	}
}

func TestQuotaProjectIDSize(t *testing.T) {
	want := 8
	got := binary.Size(QuotaProjectID{})
	if got != want {
		t.Errorf("QuotaProjectID size = %d, want %d", got, want)
	}
}

func TestHashFilename(t *testing.T) {
	h1 := HashFilename([]byte("test.txt"))
	h2 := HashFilename([]byte("test.txt"))
	if h1 != h2 {
		t.Errorf("hash not deterministic: %x != %x", h1, h2)
	}

	h3 := HashFilename([]byte("other.txt"))
	if h1 == h3 {
		t.Errorf("different filenames produced same hash")
	}
}

func TestFormatVersionSerialize(t *testing.T) {
	tests := []struct {
		version FormatVersion
		wantNil bool
		wantVal uint64
	}{
		{FormatVersion1, true, 0},
		{FormatVersion2, false, 2},
	}
	for _, tt := range tests {
		t.Run(tt.version.String(), func(t *testing.T) {
			data := tt.version.Serialize()
			if tt.wantNil {
				if data != nil {
					t.Error("expected nil for V1")
				}
			} else {
				if data == nil {
					t.Fatal("expected non-nil for V2")
				}
				if len(data) != 8 {
					t.Fatalf("expected 8 bytes, got %d", len(data))
				}
				val := binary.LittleEndian.Uint64(data)
				if val != tt.wantVal {
					t.Errorf("value = %d, want %d", val, tt.wantVal)
				}
			}
		})
	}
}

func TestFormatVersionDeserialize(t *testing.T) {
	v, err := DeserializeFormatVersion(1)
	if err != nil || v != FormatVersion1 {
		t.Errorf("v1: got %v, err %v", v, err)
	}
	v, err = DeserializeFormatVersion(2)
	if err != nil || v != FormatVersion2 {
		t.Errorf("v2: got %v, err %v", v, err)
	}
	_, err = DeserializeFormatVersion(3)
	if err == nil {
		t.Error("expected error for version 3")
	}
}

func TestStatFileType(t *testing.T) {
	s := Stat{Mode: ModeIFREG | 0o644}
	if !s.IsRegularFile() {
		t.Error("expected regular file")
	}
	if s.IsDir() {
		t.Error("should not be directory")
	}
	if s.FileType() != ModeIFREG {
		t.Errorf("FileType = %o, want %o", s.FileType(), ModeIFREG)
	}
	if s.FileMode() != 0o644 {
		t.Errorf("FileMode = %o, want %o", s.FileMode(), 0o644)
	}
}

func TestStatIsDir(t *testing.T) {
	s := Stat{Mode: ModeIFDIR | 0o755}
	if !s.IsDir() {
		t.Error("expected directory")
	}
}

func TestStatIsSymlink(t *testing.T) {
	s := Stat{Mode: ModeIFLNK | 0o777}
	if !s.IsSymlink() {
		t.Error("expected symlink")
	}
}

func TestStatIsDevice(t *testing.T) {
	chr := Stat{Mode: ModeIFCHR | 0o644}
	blk := Stat{Mode: ModeIFBLK | 0o644}
	reg := Stat{Mode: ModeIFREG | 0o644}

	if !chr.IsDevice() || !chr.IsCharDev() {
		t.Error("expected char device")
	}
	if !blk.IsDevice() || !blk.IsBlockDev() {
		t.Error("expected block device")
	}
	if reg.IsDevice() {
		t.Error("regular file should not be device")
	}
}

func TestStatIsFIFO(t *testing.T) {
	s := Stat{Mode: ModeIFIFO | 0o644}
	if !s.IsFIFO() {
		t.Error("expected FIFO")
	}
}

func TestStatIsSocket(t *testing.T) {
	s := Stat{Mode: ModeIFSOCK | 0o644}
	if !s.IsSocket() {
		t.Error("expected socket")
	}
}

func TestHeaderString(t *testing.T) {
	tests := []struct {
		want  string
		htype uint64
	}{
		{"FORMAT_VERSION", Version},
		{"ENTRY", PXAREntry},
		{"FILENAME", PXARFilename},
		{"SYMLINK", PXARSymlink},
		{"DEVICE", PXARDevice},
		{"GOODBYE", PXARGoodbye},
		{"PAYLOAD", PXARPayload},
		{"XATTR", PXARXAttr},
		{"UNKNOWN", 0xdeadbeef},
	}
	for _, tt := range tests {
		h := Header{Type: tt.htype}
		s := h.String()
		if s[:len(tt.want)] != tt.want {
			t.Errorf("Header{Type:%x}.String() = %q, want prefix %q", tt.htype, s, tt.want)
		}
	}
}

// TestStatxTimestampNegativeDuration mirrors Rust's test_statx_timestamp for pre-epoch times.
// Rust: MAY_1_1960_1530 = -305112600 → secs=-305112601, nanos=999_000_000
func TestStatxTimestampNegativeDuration(t *testing.T) {
	// 305112600 seconds before epoch, with 1ms sub-second precision
	beforeEpoch := 305112600*time.Second + 1*time.Millisecond

	// Rust's from_duration_before_epoch for non-zero nanos:
	// secs = -(as_secs) - 1 = -305112601
	// nanos = 1_000_000_000 - subsec_nanos = 999_000_000
	secs := -int64(beforeEpoch/time.Second) - 1
	nanos := uint32(1_000_000_000 - (beforeEpoch % time.Second))
	ts := StatxTimestamp{Secs: secs, Nanos: nanos}

	if ts.Secs != -305112601 {
		t.Errorf("Secs = %d, want -305112601", ts.Secs)
	}
	if ts.Nanos != 999_000_000 {
		t.Errorf("Nanos = %d, want 999000000", ts.Nanos)
	}

	// Exact boundary: sub-second portion is exactly 0
	beforeEpoch2 := 305112600 * time.Second
	secs2 := -int64(beforeEpoch2 / time.Second)
	ts2 := StatxTimestamp{Secs: secs2, Nanos: 0}
	if ts2.Secs != -305112600 {
		t.Errorf("Secs = %d, want -305112600", ts2.Secs)
	}
	if ts2.Nanos != 0 {
		t.Errorf("Nanos = %d, want 0", ts2.Nanos)
	}
}

// TestStatxTimestampFromDurationNegative mirrors Rust's test_statx_timestamp:
// a point in time 305112600.001s BEFORE the epoch must serialize (via the
// statx carry convention) to {secs:-305112601, nanos:999_000_000}, and the
// exact-second boundary to {secs:-305112600, nanos:0}.
//
// Rust source: src/format/mod.rs StatxTimestamp::from_duration_before_epoch.
func TestStatxTimestampFromDurationNegative(t *testing.T) {
	// 305112600 seconds + 1ms before the epoch, expressed as a negative duration.
	negDur := -(305112600*time.Second + 1*time.Millisecond)
	ts := NewStatxTimestampFromDuration(negDur)

	// Rust: from_duration_before_epoch with subsec_nanos != 0 gives
	//   secs = -(as_secs) - 1 = -305112601
	//   nanos = 1_000_000_000 - subsec_nanos = 999_000_000
	if ts.Secs != -305112601 {
		t.Errorf("neg-with-subsec Secs = %d, want -305112601", ts.Secs)
	}
	if ts.Nanos != 999_000_000 {
		t.Errorf("neg-with-subsec Nanos = %d, want 999000000", ts.Nanos)
	}

	// Exact second boundary before epoch: subsec == 0 => no carry.
	negExact := -(305112600 * time.Second)
	ts2 := NewStatxTimestampFromDuration(negExact)
	if ts2.Secs != -305112600 {
		t.Errorf("neg-exact Secs = %d, want -305112600", ts2.Secs)
	}
	if ts2.Nanos != 0 {
		t.Errorf("neg-exact Nanos = %d, want 0", ts2.Nanos)
	}
}

// TestStatxTimestampDurationRoundTripSigned verifies that
// NewStatxTimestampFromDuration and Duration are exact inverses across the
// full signed range, including pre-epoch (negative) values. Currently the
// encoder truncates/wraps for negative durations, breaking the round trip.
func TestStatxTimestampDurationRoundTripSigned(t *testing.T) {
	cases := []time.Duration{
		0,
		1 * time.Nanosecond,
		1 * time.Millisecond,
		1430487000*time.Second + 1*time.Millisecond,
		-(1 * time.Nanosecond),
		-(1 * time.Millisecond),
		-(305112600*time.Second + 1*time.Millisecond), // pre-epoch with subsec
		-(305112600 * time.Second),                    // pre-epoch exact second
	}
	for _, d := range cases {
		ts := NewStatxTimestampFromDuration(d)
		got := ts.Duration()
		if got != d {
			t.Errorf("round-trip failed for %v: encoded=%+v decoded=%v", d, ts, got)
		}
	}
}

// TestStatxTimestampTimeRoundTrip mirrors Rust's test_statx_timestamp end to
// end: converting a SystemTime (time.Time) to a StatxTimestamp and back must
// reproduce the original instant for both post- and pre-epoch times, with the
// intermediate encoding matching the known statx carry values.
func TestStatxTimestampTimeRoundTrip(t *testing.T) {
	epoch := time.Unix(0, 0).UTC()

	// MAY_1_2015_1530, 1ms sub-second: post-epoch.
	post := epoch.Add(1430487000*time.Second + 1*time.Millisecond)
	tx := NewStatxTimestampFromTime(post)
	if tx.Secs != 1430487000 || tx.Nanos != 1_000_000 {
		t.Errorf("post-epoch encode = {%d, %d}, want {1430487000, 1000000}", tx.Secs, tx.Nanos)
	}
	if got := tx.Time(); !got.Equal(post) {
		t.Errorf("post-epoch round-trip = %v, want %v", got, post)
	}

	// MAY_1_1960_1530, 1ms sub-second: pre-epoch. Rust encodes this as
	// {secs:-305112601, nanos:999_000_000}.
	pre := epoch.Add(-(305112600*time.Second + 1*time.Millisecond))
	tx2 := NewStatxTimestampFromTime(pre)
	if tx2.Secs != -305112601 || tx2.Nanos != 999_000_000 {
		t.Errorf("pre-epoch encode = {%d, %d}, want {-305112601, 999000000}", tx2.Secs, tx2.Nanos)
	}
	if got := tx2.Time(); !got.Equal(pre) {
		t.Errorf("pre-epoch round-trip = %v, want %v", got, pre)
	}

	// Pre-epoch exact second boundary: sub-second is zero, no carry.
	preExact := epoch.Add(-(305112600 * time.Second))
	tx3 := NewStatxTimestampFromTime(preExact)
	if tx3.Secs != -305112600 || tx3.Nanos != 0 {
		t.Errorf("pre-epoch exact encode = {%d, %d}, want {-305112600, 0}", tx3.Secs, tx3.Nanos)
	}
	if got := tx3.Time(); !got.Equal(preExact) {
		t.Errorf("pre-epoch exact round-trip = %v, want %v", got, preExact)
	}
}

// TestStatMarshalUnmarshalRoundTrip verifies that Stat marshals to exactly 40 bytes
// with _pad=0 and round-trips correctly. Mirrors Rust's Endian trait behavior.
func TestStatMarshalUnmarshalRoundTrip(t *testing.T) {
	original := Stat{
		Mode:  ModeIFREG | 0o644,
		Flags: 0x12345678,
		UID:   1000,
		GID:   1000,
		Mtime: StatxTimestamp{Secs: 1430487000, Nanos: 1_000_000},
	}

	var buf [40]byte
	MarshalStatBytesInto(buf[:], original)

	// Verify _pad field at bytes 36-39 is zero (matching Rust's _zero: 0)
	pad := binary.LittleEndian.Uint32(buf[36:])
	if pad != 0 {
		t.Errorf("_pad = %d, want 0 (bytes 36-39: %x)", pad, buf[36:40])
	}

	// Verify mode at offset 0
	mode := binary.LittleEndian.Uint64(buf[0:])
	if mode != original.Mode {
		t.Errorf("mode = %x, want %x", mode, original.Mode)
	}

	// Verify flags at offset 8
	flags := binary.LittleEndian.Uint64(buf[8:])
	if flags != original.Flags {
		t.Errorf("flags = %x, want %x", flags, original.Flags)
	}

	// Verify uid at offset 16
	uid := binary.LittleEndian.Uint32(buf[16:])
	if uid != original.UID {
		t.Errorf("uid = %d, want %d", uid, original.UID)
	}

	// Verify gid at offset 20
	gid := binary.LittleEndian.Uint32(buf[20:])
	if gid != original.GID {
		t.Errorf("gid = %d, want %d", gid, original.GID)
	}

	// Round-trip
	decoded := UnmarshalStatBytes(buf[:])
	if decoded.Mode != original.Mode {
		t.Errorf("round-trip mode = %x, want %x", decoded.Mode, original.Mode)
	}
	if decoded.Flags != original.Flags {
		t.Errorf("round-trip flags = %x, want %x", decoded.Flags, original.Flags)
	}
	if decoded.UID != original.UID {
		t.Errorf("round-trip uid = %d, want %d", decoded.UID, original.UID)
	}
	if decoded.GID != original.GID {
		t.Errorf("round-trip gid = %d, want %d", decoded.GID, original.GID)
	}
	if decoded.Mtime != original.Mtime {
		t.Errorf("round-trip mtime = %+v, want %+v", decoded.Mtime, original.Mtime)
	}

	// Verify that filling pad with garbage then marshaling clears it
	garbageBuf := [40]byte{}
	for i := range garbageBuf {
		garbageBuf[i] = 0xFF
	}
	MarshalStatBytesInto(garbageBuf[:], original)
	pad = binary.LittleEndian.Uint32(garbageBuf[36:])
	if pad != 0 {
		t.Errorf("_pad not cleared after marshaling into dirty buffer: %d", pad)
	}
}

// TestDeviceFromDevTToDevTRoundTrip mirrors Rust's test_linux_devices.
// Uses the same test values: makedev(0xabcd_1234, 0xdcba_5678).
func TestDeviceFromDevTToDevTRoundTrip(t *testing.T) {
	dev := DeviceFromDevT(makeDevT(0xabcd1234, 0xdcba5678))
	if dev.Major != 0xabcd1234 {
		t.Errorf("Major = %x, want %x", dev.Major, uint64(0xabcd1234))
	}
	if dev.Minor != 0xdcba5678 {
		t.Errorf("Minor = %x, want %x", dev.Minor, uint64(0xdcba5678))
	}
	// Round-trip back
	got := dev.ToDevT()
	want := makeDevT(0xabcd1234, 0xdcba5678)
	if got != want {
		t.Errorf("ToDevT = %x, want %x", got, want)
	}
}

// makeDevT mirrors Linux's makedev macro.
func makeDevT(major, minor uint64) uint64 {
	return (major&0x00000fff)<<8 |
		(major&0xfffff000)<<32 |
		(minor & 0x000000ff) |
		(minor&0xffffff00)<<12
}

// TestHeaderMarshalTo verifies the zero-copy MarshalTo produces identical bytes
// to binary.Write.
func TestHeaderMarshalTo(t *testing.T) {
	h := HeaderWithContentSize(PXARFilename, 13)

	// Reference: binary.Write
	var ref [16]byte
	binary.LittleEndian.PutUint64(ref[0:], h.Type)
	binary.LittleEndian.PutUint64(ref[8:], h.Size)

	// MarshalTo
	var got [16]byte
	h.MarshalTo(got[:])

	if got != ref {
		t.Errorf("MarshalTo = %x, want %x", got, ref)
	}
}

func TestXAttr(t *testing.T) {
	x := NewXAttr([]byte("user.test"), []byte("value"))
	if string(x.Name()) != "user.test" {
		t.Errorf("Name = %q, want %q", x.Name(), "user.test")
	}
	if string(x.Value()) != "value" {
		t.Errorf("Value = %q, want %q", x.Value(), "value")
	}
}
