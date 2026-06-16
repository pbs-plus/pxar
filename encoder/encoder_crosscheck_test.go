package encoder

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/format"
)

func hex(b []byte) string {
	const h = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, x := range b {
		out[i*2] = h[x>>4]
		out[i*2+1] = h[x&0xf]
	}
	return string(out)
}

// statMode mirrors the Rust probe's Stat layout: mode, flags=0, uid=1000,
// gid=1000, mtime secs=0x11223344 nanos=0x55667788.
func mkStat(mode uint64) format.Stat {
	return format.Stat{
		Mode:  mode,
		Flags: 0,
		UID:   1000,
		GID:   1000,
		Mtime: format.StatxTimestamp{
			Secs:  0x11223344,
			Nanos: 0x55667788,
		},
	}
}

// TestProbeEncodeV1Crosscheck encodes the same tree as the Rust probe_encode
// example and asserts byte-for-byte equality with the Rust reference output.
func TestProbeEncodeV1Crosscheck(t *testing.T) {
	rootMeta := &pxar.Metadata{Stat: mkStat(format.ModeIFDIR | 0o755)}
	buf := &bytes.Buffer{}
	enc := NewEncoder(buf, nil, rootMeta, nil)

	fileMeta := &pxar.Metadata{Stat: mkStat(format.ModeIFREG | 0o644)}
	off, err := enc.AddFile(fileMeta, "file.txt", []byte("hello world"))
	if err != nil {
		t.Fatal(err)
	}

	symMeta := &pxar.Metadata{Stat: mkStat(format.ModeIFLNK | 0o777)}
	if err := enc.AddSymlink(symMeta, "link", "file.txt"); err != nil {
		t.Fatal(err)
	}

	if err := enc.AddHardlink("hlink", "file.txt", off); err != nil {
		t.Fatal(err)
	}

	fifoMeta := &pxar.Metadata{Stat: mkStat(format.ModeIFIFO | 0o600)}
	if err := enc.AddFIFO(fifoMeta, "pipe"); err != nil {
		t.Fatal(err)
	}

	sockMeta := &pxar.Metadata{Stat: mkStat(format.ModeIFSOCK | 0o644)}
	if err := enc.AddSocket(sockMeta, "sock"); err != nil {
		t.Fatal(err)
	}

	devMeta := &pxar.Metadata{Stat: mkStat(format.ModeIFBLK | 0o660)}
	if err := enc.AddDevice(devMeta, "blockdev", format.Device{Major: 8, Minor: 1}); err != nil {
		t.Fatal(err)
	}

	subMeta := &pxar.Metadata{Stat: mkStat(format.ModeIFDIR | 0o755)}
	if err := enc.CreateDirectory("subdir", subMeta); err != nil {
		t.Fatal(err)
	}
	subfileMeta := &pxar.Metadata{Stat: mkStat(format.ModeIFREG | 0o644)}
	if _, err := enc.AddFile(subfileMeta, "subfile", []byte("sub")); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finish(); err != nil {
		t.Fatal(err)
	}

	emptyMeta := &pxar.Metadata{Stat: mkStat(format.ModeIFDIR | 0o755)}
	if err := enc.CreateDirectory("empty", emptyMeta); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finish(); err != nil {
		t.Fatal(err)
	}

	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	goHex := hex(buf.Bytes())
	fmt.Printf("GO_V1_LEN=%d\n", len(buf.Bytes()))

	const rustLen = 1180
	const rustHex = "efac88e5746495d53800000000000000ed410000000000000000000000000000e8030000e803000044332211000000008877665500000000b317390621117016190000000000000066696c652e74787400efac88e5746495d53800000000000000a4810000000000000000000000000000e8030000e803000044332211000000008877665500000000251a7c0b1b7a14281b0000000000000068656c6c6f20776f726c64b31739062111701615000000000000006c696e6b00efac88e5746495d53800000000000000ffa10000000000000000000000000000e8030000e8030000443322110000000088776655000000005fdcf5dbe771f927190000000000000066696c652e74787400b3173906211170161600000000000000686c696e6b007572bd22849c26512100000000000000d20000000000000066696c652e74787400b31739062111701615000000000000007069706500efac88e5746495d5380000000000000080110000000000000000000000000000e8030000e803000044332211000000008877665500000000b3173906211170161500000000000000736f636b00efac88e5746495d53800000000000000a4c10000000000000000000000000000e8030000e803000044332211000000008877665500000000b3173906211170161900000000000000626c6f636b64657600efac88e5746495d53800000000000000b0610000000000000000000000000000e8030000e803000044332211000000008877665500000000e95c6d5806e9c99f200000000000000008000000000000000100000000000000b317390621117016170000000000000073756264697200efac88e5746495d53800000000000000ed410000000000000000000000000000e8030000e803000044332211000000008877665500000000b317390621117016180000000000000073756266696c6500efac88e5746495d53800000000000000a4810000000000000000000000000000e8030000e803000044332211000000008877665500000000251a7c0b1b7a142813000000000000007375621d73d542a64fec2f40000000000000007dcbad4e41838d0d6300000000000000630000000000000055153e755bed5eef9b000000000000004000000000000000b3173906211170161600000000000000656d70747900efac88e5746495d53800000000000000ed410000000000000000000000000000e8030000e8030000443322110000000088776655000000001d73d542a64fec2f280000000000000055153e755bed5eef380000000000000028000000000000001d73d542a64fec2fe80000000000000089b04ef93b9dfbbc73020000000000004d0000000000000014dac330f66477967c030000000000006c0000000000000098dabbc6b28a43d326020000000000004d00000000000000eb19f9a744bde92aaa0200000000000037000000000000000400b6b3e4d27b9c760000000000000076000000000000006441026e6cfa2bbf6801000000000000f20000000000000080fecb0fd2bbe0efd9010000000000007100000000000000efc00fcf2e0bfe041003000000000000660000000000000055153e755bed5eefb403000000000000e800000000000000"

	if len(buf.Bytes()) != rustLen {
		t.Errorf("length: go=%d rust=%d", len(buf.Bytes()), rustLen)
	}
	if goHex != rustHex {
		// Find first differing byte for diagnostics
		minLen := min(len(rustHex), len(goHex))
		for i := range minLen {
			if goHex[i] != rustHex[i] {
				t.Errorf("first hex diff at byte %d (hex-char %d): go=%q rust=%q",
					i/2, i, goHex[max(0, i-8):minLen], rustHex[max(0, i-8):minLen])
				break
			}
		}
		t.Logf("GO_V1_HEX=%s", goHex)
	}
}

var _ = binary.LittleEndian
