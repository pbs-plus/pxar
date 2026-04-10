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
	ts := StatxTimestampFromDurationSinceEpoch(1430487000*time.Second + 1*time.Millisecond)
	if ts.Secs != 1430487000 {
		t.Errorf("Secs = %d, want 1430487000", ts.Secs)
	}
	if ts.Nanos != 1_000_000 {
		t.Errorf("Nanos = %d, want 1000000", ts.Nanos)
	}
}

func TestStatxTimestampRoundTrip(t *testing.T) {
	d := 1430487000*time.Second + 1_000_000*time.Nanosecond
	ts := StatxTimestampFromDurationSinceEpoch(d)
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
		htype uint64
		want  string
	}{
		{PXARFormatVersion, "FORMAT_VERSION"},
		{PXAREntry, "ENTRY"},
		{PXARFilename, "FILENAME"},
		{PXARSymlink, "SYMLINK"},
		{PXARDevice, "DEVICE"},
		{PXARGoodbye, "GOODBYE"},
		{PXARPayload, "PAYLOAD"},
		{PXARXAttr, "XATTR"},
		{0xdeadbeef, "UNKNOWN"},
	}
	for _, tt := range tests {
		h := Header{Type: tt.htype}
		s := h.String()
		if s[:len(tt.want)] != tt.want {
			t.Errorf("Header{Type:%x}.String() = %q, want prefix %q", tt.htype, s, tt.want)
		}
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
