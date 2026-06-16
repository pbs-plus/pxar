package encoder

import (
	"bytes"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/format"
)

// TestProbeEncodeStreamSplitCrosscheck encodes two streaming files via
// CreateFile in a split (v2) archive and asserts byte-for-byte equality with
// the Rust reference (probe_encode_stream). This verifies the streaming
// CreateFile goodbye-item size fix in split mode.
func TestProbeEncodeStreamSplitCrosscheck(t *testing.T) {
	rootMeta := &pxar.Metadata{Stat: mkStat(format.ModeIFDIR | 0o755)}
	archBuf := &bytes.Buffer{}
	payBuf := &bytes.Buffer{}
	enc := NewEncoder(archBuf, payBuf, rootMeta, nil)

	fw, err := enc.CreateFile(&pxar.Metadata{Stat: mkStat(format.ModeIFREG | 0o644)}, "stream.bin", 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := fw.WriteAll([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := fw.WriteAll([]byte("world")); err != nil {
		t.Fatal(err)
	}
	if err := fw.Close(); err != nil {
		t.Fatal(err)
	}

	fw2, err := enc.CreateFile(&pxar.Metadata{Stat: mkStat(format.ModeIFREG | 0o644)}, "small.txt", 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := fw2.WriteAll([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := fw2.Close(); err != nil {
		t.Fatal(err)
	}

	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	const rustArchHex = "0da416df756c0f7318000000000000000200000000000000efac88e5746495d53800000000000000ed410000000000000000000000000000e8030000e803000044332211000000008877665500000000b3173906211170161b0000000000000073747265616d2e62696e00efac88e5746495d53800000000000000a4810000000000000000000000000000e8030000e8030000443322110000000088776655000000007e97bac46b3d9d41200000000000000010000000000000000a00000000000000b3173906211170161a00000000000000736d616c6c2e74787400efac88e5746495d53800000000000000a4810000000000000000000000000000e8030000e8030000443322110000000088776655000000007e97bac46b3d9d4120000000000000002a0000000000000003000000000000001d73d542a64fec2f5800000000000000b1f0b5b710ef93dae5000000000000007d00000000000000c32055890daeba027200000000000000750000000000000055153e755bed5eef35010000000000005800000000000000"
	const rustPayHex = "d24e4a19c2684c831000000000000000251a7c0b1b7a14281a0000000000000068656c6c6f776f726c64251a7c0b1b7a14281300000000000000616263b5814c988bb7726c1000000000000000"

	if hex(archBuf.Bytes()) != rustArchHex {
		t.Errorf("archive mismatch:\n  go=%s\nrust=%s", hex(archBuf.Bytes()), rustArchHex)
	}
	if hex(payBuf.Bytes()) != rustPayHex {
		t.Errorf("payload mismatch:\n  go=%s\nrust=%s", hex(payBuf.Bytes()), rustPayHex)
	}
}
