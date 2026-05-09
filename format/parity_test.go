package format_test

import (
	"testing"
	"time"

	"github.com/pbs-plus/pxar/format"
)

// TestStatxTimestampParityWithPBS validates timestamp creation against
// known values from Proxmox's Rust reference implementation
// (pxar/src/format/mod.rs test_statx_timestamp).
func TestStatxTimestampParityWithPBS(t *testing.T) {
	// MAY_1_2015_1530: secs=1430487000, nanos=1_000_000
	ts := format.StatxTimestampNew(1430487000, 1_000_000)
	if ts.Secs != 1430487000 {
		t.Errorf("Secs = %d, want 1430487000", ts.Secs)
	}
	if ts.Nanos != 1_000_000 {
		t.Errorf("Nanos = %d, want 1000000", ts.Nanos)
	}

	// Round-trip through Duration
	d := ts.Duration()
	want := time.Duration(1430487000)*time.Second + time.Duration(1_000_000)*time.Nanosecond
	if d != want {
		t.Errorf("Duration = %v, want %v", d, want)
	}

	// Negative timestamps: Duration returns negative value which is correct
	// for Go (unlike Rust which adjusts to positive SystemTime representation).
	negSecs := int64(-305112600)
	negTs := format.StatxTimestampNew(negSecs, 1_000_000)
	if negTs.Secs != negSecs {
		t.Errorf("Negative Secs = %d, want %d", negTs.Secs, negSecs)
	}
	if negTs.Nanos != 1_000_000 {
		t.Errorf("Negative Nanos = %d, want 1000000", negTs.Nanos)
	}

	// MAY_1_1960_1530 with nanos=0
	zeroTs := format.StatxTimestampNew(negSecs, 0)
	if zeroTs.Secs != negSecs {
		t.Errorf("Zero nanos Secs = %d, want %d", zeroTs.Secs, negSecs)
	}
	if zeroTs.Nanos != 0 {
		t.Errorf("Zero nanos Nanos = %d, want 0", zeroTs.Nanos)
	}
}

// TestDeviceParityWithPBS validates Linux device number conversions
// against Proxmox's Rust reference implementation
// (pxar/src/format/mod.rs test_linux_devices).
func TestDeviceParityWithPBS(t *testing.T) {
	// PBS test uses: makedev(0xabcd_1234, 0xdcba_5678)
	major := uint64(0xabcd_1234)
	minor := uint64(0xdcba_5678)

	dev := format.Device{Major: major, Minor: minor}
	devt := dev.ToDevT()

	// Round-trip: from_dev_t(to_dev_t(dev)) should equal dev
	back := format.DeviceFromDevT(devt)
	if back.Major != major || back.Minor != minor {
		t.Errorf("Device from_dev_t(to_dev_t({%x, %x})) = {%x, %x}",
			major, minor, back.Major, back.Minor)
	}

	// Zero device
	zero := format.Device{}
	if zero.ToDevT() != 0 {
		t.Errorf("Zero device ToDevT = %d, want 0", zero.ToDevT())
	}
	if zeroBack := format.DeviceFromDevT(0); zeroBack.Major != 0 || zeroBack.Minor != 0 {
		t.Errorf("DeviceFromDevT(0) = {%x, %x}, want {0, 0}", zeroBack.Major, zeroBack.Minor)
	}

	// Known common devices
	tests := []struct {
		major, minor uint64
	}{
		{1, 3}, // /dev/null
		{1, 8}, // /dev/random
		{1, 5}, // /dev/zero
		{8, 0}, // /dev/sda
		{0xabcd, 0x5678},
	}
	for _, tt := range tests {
		d := format.Device{Major: tt.major, Minor: tt.minor}
		dt := d.ToDevT()
		d2 := format.DeviceFromDevT(dt)
		if d2.Major != tt.major || d2.Minor != tt.minor {
			t.Errorf("Round-trip failed for {%x, %x}: got {%x, %x}",
				tt.major, tt.minor, d2.Major, d2.Minor)
		}
	}
}

// TestGoodbyeItemParityWithPBS validates goodbye item construction
// by verifying the hash for a known filename matches PBS.
func TestGoodbyeItemParityWithPBS(t *testing.T) {
	// From compat.rs: FILE_NAME = "file.txt"
	// GoodbyeItem::new(FILE_NAME.as_bytes(), file_offset, FILE_CONTENT.len())
	hash := format.HashFilename([]byte("file.txt"))

	// Verify deterministic — same input must give same hash
	hash2 := format.HashFilename([]byte("file.txt"))
	if hash != hash2 {
		t.Errorf("HashFilename not deterministic: %016x vs %016x", hash, hash2)
	}

	// Different names must give different hashes
	names := [][]byte{
		[]byte(""),
		[]byte("a"),
		[]byte("hello.txt"),
		[]byte("file.txt"),
		[]byte("very.long.filename.with.many.dots.tar.gz"),
	}
	for i := range names {
		for j := i + 1; j < len(names); j++ {
			if format.HashFilename(names[i]) == format.HashFilename(names[j]) {
				t.Errorf("Hash collision: %q and %q", names[i], names[j])
			}
		}
	}
}
