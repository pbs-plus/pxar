package decoder

import (
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"testing"

	"github.com/pbs-plus/pxar/format"
)

// These tests verify that the Go decoder treats a prematurely-ended archive as
// an error, matching the Rust reference (proxmox-pxar). Rust's state machine
// only reaches a clean EOF after the root directory's GOODBYE table; anything
// short of a complete root entry is 'unexpected EOF'.
//
// Captured Rust behavior (probe_trunc.rs):
//   empty                -> "unexpected EOF"
//   version_only         -> "unexpected EOF"  (FORMAT_VERSION with no root dir)
//   header_10b           -> "unexpected EOF"  (truncated header)
//   stat_truncated       -> "unexpected EOF"  (ENTRY claims 40B but stream ends)

func mkRaw(htype uint64, content []byte) []byte {
	full := uint64(format.HeaderSize + len(content))
	v := make([]byte, 0, format.HeaderSize+len(content))
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], htype)
	v = append(v, b[:]...)
	binary.LittleEndian.PutUint64(b[:], full)
	v = append(v, b[:]...)
	v = append(v, content...)
	return v
}

func firstNextErr(t *testing.T, archive []byte) error {
	t.Helper()
	dec := NewDecoder(bytes.NewReader(archive), nil)
	for {
		_, err := dec.Next()
		if err != nil {
			return err
		}
	}
}

func TestEnforceEmptyArchiveIsError(t *testing.T) {
	// Rust: empty stream -> "unexpected EOF"
	err := firstNextErr(t, []byte{})
	if err == nil || err == io.EOF {
		t.Fatalf("expected 'unexpected EOF' for empty archive, got %v", err)
	}
}

func TestEnforceVersionOnlyIsError(t *testing.T) {
	// Rust: FORMAT_VERSION with no following root directory -> "unexpected EOF"
	v2 := make([]byte, 8)
	binary.LittleEndian.PutUint64(v2, 2)
	err := firstNextErr(t, mkRaw(format.Version, v2))
	if err == nil || err == io.EOF {
		t.Fatalf("expected 'unexpected EOF' for version-only archive, got %v", err)
	}
}

func TestEnforceTruncatedStatIsError(t *testing.T) {
	// Rust: ENTRY header claiming 40 bytes but stream ends early -> "unexpected EOF"
	var b []byte
	var x [8]byte
	binary.LittleEndian.PutUint64(x[:], format.PXAREntry)
	b = append(b, x[:]...)
	binary.LittleEndian.PutUint64(x[:], 56) // full = 16 header + 40 content
	b = append(b, x[:]...)
	b = append(b, make([]byte, 20)...) // only 20 of 40 content bytes
	err := firstNextErr(t, b)
	if err == nil || err == io.EOF {
		t.Fatalf("expected 'unexpected EOF' for truncated stat, got %v", err)
	}
}

func TestEnforceFileNoPayloadTerminatorIsError(t *testing.T) {
	// Rust: a regular file entry whose item stream ends without a terminating
	// FILENAME/GOODBYE/PAYLOAD is premature: "unexpected EOF in entry".
	var b bytes.Buffer
	b.Write(mkRaw(format.PXAREntry, statBytes(format.ModeIFDIR|0o755)))
	b.Write(mkRaw(format.PXARFilename, []byte("f\x00")))
	b.Write(mkRaw(format.PXAREntry, statBytes(format.ModeIFREG|0o644)))
	err := firstNextErr(t, b.Bytes())
	if err == nil || err == io.EOF {
		t.Fatalf("expected 'unexpected EOF in entry' for unterminated file, got %v", err)
	}
	if !strings.Contains(err.Error(), "unexpected EOF") {
		t.Errorf("error %q does not match rust 'unexpected EOF in entry'", err.Error())
	}
}

func TestEnforceRootFileNoPayloadIsError(t *testing.T) {
	// Rust: a root regular-file ENTRY with no payload/terminator ->
	// "unexpected EOF in entry".
	err := firstNextErr(t, mkRaw(format.PXAREntry, statBytes(format.ModeIFREG|0o644)))
	if err == nil || err == io.EOF {
		t.Fatalf("expected 'unexpected EOF in entry' for root file, got %v", err)
	}
	if !strings.Contains(err.Error(), "unexpected EOF") {
		t.Errorf("error %q does not match rust 'unexpected EOF in entry'", err.Error())
	}
}

func TestEnforceFIFOEOFIsError(t *testing.T) {
	// Rust sequential decoder (eof_after_entry=false): a FIFO entry with no
	// terminating item still errors "unexpected EOF in entry". Only the
	// accessor's ranged-reader mode accepts EOF on FIFO/Socket.
	err := firstNextErr(t, mkRaw(format.PXAREntry, statBytes(format.ModeIFIFO|0o644)))
	if err == nil || err == io.EOF {
		t.Fatalf("expected 'unexpected EOF in entry' for unterminated FIFO, got %v", err)
	}
	if !strings.Contains(err.Error(), "unexpected EOF") {
		t.Errorf("error %q does not match rust 'unexpected EOF in entry'", err.Error())
	}
}
