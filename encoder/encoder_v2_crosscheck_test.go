package encoder

import (
	"bytes"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/format"
)

// TestProbeEncodeV2Crosscheck encodes the same V2 split tree as the Rust
// probe_encode_v2 example and asserts byte-for-byte equality of both the
// archive and payload streams.
func TestProbeEncodeV2Crosscheck(t *testing.T) {
	rootMeta := &pxar.Metadata{
		Stat: mkStat(format.ModeIFDIR | 0o755),
		XAttrs: []format.XAttr{
			{Data: []byte("user.foo\x00bar")},
			{Data: []byte("trusted.baz\x00qux")},
		},
	}

	archBuf := &bytes.Buffer{}
	payBuf := &bytes.Buffer{}
	prelude := []byte("PRELUDE_TEST_DATA")
	enc := NewEncoder(archBuf, payBuf, rootMeta, prelude)

	fileMeta := &pxar.Metadata{Stat: mkStat(format.ModeIFREG | 0o644)}
	if _, err := enc.AddFile(fileMeta, "file.txt", []byte("hello world")); err != nil {
		t.Fatal(err)
	}

	fifoMeta := &pxar.Metadata{Stat: mkStat(format.ModeIFIFO | 0o600)}
	if err := enc.AddFIFO(fifoMeta, "pipe"); err != nil {
		t.Fatal(err)
	}

	devMeta := &pxar.Metadata{Stat: mkStat(format.ModeIFBLK | 0o660)}
	if err := enc.AddDevice(devMeta, "blockdev", format.Device{Major: 8, Minor: 1}); err != nil {
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

	goArchHex := hex(archBuf.Bytes())
	goPayHex := hex(payBuf.Bytes())

	const rustArchHex = "0da416df756c0f73180000000000000002000000000000001b777b9f9dd709e321000000000000005052454c5544455f544553545f44415441efac88e5746495d53800000000000000ed410000000000000000000000000000e8030000e80300004433221100000000887766550000000003cd7db52902ab0d1c00000000000000757365722e666f6f0062617203cd7db52902ab0d1f00000000000000747275737465642e62617a00717578b317390621117016190000000000000066696c652e74787400efac88e5746495d53800000000000000a4810000000000000000000000000000e8030000e8030000443322110000000088776655000000007e97bac46b3d9d41200000000000000010000000000000000b00000000000000b31739062111701615000000000000007069706500efac88e5746495d5380000000000000080110000000000000000000000000000e8030000e803000044332211000000008877665500000000b3173906211170161900000000000000626c6f636b64657600efac88e5746495d53800000000000000b0610000000000000000000000000000e8030000e803000044332211000000008877665500000000e95c6d5806e9c99f200000000000000008000000000000000100000000000000b3173906211170161600000000000000656d70747900efac88e5746495d53800000000000000ed410000000000000000000000000000e8030000e8030000443322110000000088776655000000001d73d542a64fec2f280000000000000055153e755bed5eef380000000000000028000000000000001d73d542a64fec2f880000000000000089b04ef93b9dfbbc34010000000000004d000000000000000400b6b3e4d27b9c7600000000000000760000000000000080fecb0fd2bbe0efe700000000000000710000000000000014dac330f6647796a5010000000000007c0000000000000055153e755bed5eef51020000000000008800000000000000"
	const rustPayHex = "d24e4a19c2684c831000000000000000251a7c0b1b7a14281b0000000000000068656c6c6f20776f726c64b5814c988bb7726c1000000000000000"
	const rustArchLen = 729
	const rustPayLen = 59

	if len(archBuf.Bytes()) != rustArchLen {
		t.Errorf("archive length: go=%d rust=%d", len(archBuf.Bytes()), rustArchLen)
	}
	if len(payBuf.Bytes()) != rustPayLen {
		t.Errorf("payload length: go=%d rust=%d", len(payBuf.Bytes()), rustPayLen)
	}
	if goArchHex != rustArchHex {
		minLen := min(len(rustArchHex), len(goArchHex))
		for i := range minLen {
			if goArchHex[i] != rustArchHex[i] {
				t.Errorf("archive first hex diff at byte %d: go=...%s rust=...%s",
					i/2, goArchHex[max(0, i-8):minLen], rustArchHex[max(0, i-8):minLen])
				break
			}
		}
		t.Logf("GO_ARCH_HEX=%s", goArchHex)
	}
	if goPayHex != rustPayHex {
		t.Errorf("payload streams differ:\n  go=%s\nrust=%s", goPayHex, rustPayHex)
	}
}
