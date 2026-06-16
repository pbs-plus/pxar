package decoder

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/pbs-plus/pxar/format"
)

// These tests verify that the Go decoder enforces the same structural ordering
// and unexpected-type constraints as the Rust reference (proxmox-pxar). The
// authoritative Rust behavior was captured by probe_enforce.rs:
//
//   format_version_midstream      -> "Got format version entry at unexpected position"
//   xattr_where_entry_expected    -> "expected pxar entry of type 'Entry', got: XATTR header (...)"
//   unknown_attr_type             -> "unexpected entry type: UNKNOWN header (...)"
//   goodbye_at_begin              -> "expected pxar entry of type 'Entry', got: GOODBYE header (...)"
//   filename_at_begin             -> "expected pxar entry of type 'Entry', got: FILENAME header (...)"
//   prelude_at_begin              -> "Got format version entry at unexpected position"
//   hardlink_as_attribute         -> "encountered unexpected hardlink entry"
//
// The Go decoder MUST reject each of these (error, never panic, never accept).

func ehdr(t uint64, content []byte) []byte {
	return hdr(t, content)
}

func estat(mode uint64) []byte {
	return statBytes(mode)
}

func eu64(v uint64) []byte {
	return u64le(v)
}

// decodeErrOrFail runs the decoder to completion and returns the first error
// (or fails the test if the stream decoded cleanly). A panic is reported as an
// error so we can distinguish "rejected" from "crashed".
func decodeErrOrFail(t *testing.T, archive []byte) error {
	t.Helper()
	err := decodeOneFile(t, archive)
	if err == nil {
		t.Fatalf("expected a decoding error, but archive decoded cleanly")
	}
	if isPanic(err) {
		t.Fatalf("decoder panicked instead of returning a clean error: %v", err)
	}
	return err
}

func TestEnforceFormatVersionMidstream(t *testing.T) {
	// Rust: "Got format version entry at unexpected position"
	var b bytes.Buffer
	b.Write(ehdr(format.PXAREntry, estat(format.ModeIFDIR|0o755)))
	b.Write(ehdr(format.PXARFilename, []byte("x\x00")))
	b.Write(ehdr(format.Version, eu64(2)))
	err := decodeErrOrFail(t, b.Bytes())
	if err.Error() != "Got format version entry at unexpected position" {
		t.Errorf("error %q does not match rust %q", err.Error(), "Got format version entry at unexpected position")
	}
}

func TestEnforcePreludeAtBeginWithoutVersion(t *testing.T) {
	// Rust: a PRELUDE without a preceding FORMAT_VERSION is rejected as
	// "Got format version entry at unexpected position".
	err := decodeErrOrFail(t, ehdr(format.PXARPrelude, []byte("prelude data")))
	if err.Error() != "Got format version entry at unexpected position" {
		t.Errorf("error %q does not match rust %q", err.Error(), "Got format version entry at unexpected position")
	}
}

func TestEnforceUnexpectedItemWhereEntryExpected(t *testing.T) {
	// After FILENAME, Rust expects an ENTRY. An XATTR there is rejected with
	// "expected pxar entry of type 'Entry', got: XATTR header (...)".
	var b bytes.Buffer
	b.Write(ehdr(format.PXAREntry, estat(format.ModeIFDIR|0o755)))
	b.Write(ehdr(format.PXARFilename, []byte("x\x00")))
	b.Write(ehdr(format.PXARXAttr, []byte("user.a\x00v")))
	err := decodeErrOrFail(t, b.Bytes())
	want := "expected pxar entry of type 'Entry', got: XATTR header (dab0229b57dcd03)"
	if err.Error() != want {
		t.Errorf("error %q does not match rust %q", err.Error(), want)
	}
}

func TestEnforceGoodbyeAtBegin(t *testing.T) {
	// Rust: "expected pxar entry of type 'Entry', got: GOODBYE header (...)"
	err := decodeErrOrFail(t, ehdr(format.PXARGoodbye, make([]byte, 24)))
	want := "expected pxar entry of type 'Entry', got: GOODBYE header (2fec4fa642d5731d)"
	if err.Error() != want {
		t.Errorf("error %q does not match rust %q", err.Error(), want)
	}
}

func TestEnforceFilenameAtBegin(t *testing.T) {
	// Rust: "expected pxar entry of type 'Entry', got: FILENAME header (...)"
	err := decodeErrOrFail(t, ehdr(format.PXARFilename, []byte("x\x00")))
	want := "expected pxar entry of type 'Entry', got: FILENAME header (16701121063917b3)"
	if err.Error() != want {
		t.Errorf("error %q does not match rust %q", err.Error(), want)
	}
}

func TestEnforceUnknownAttrType(t *testing.T) {
	// Rust: an unknown item type after an ENTRY is rejected with
	// "unexpected entry type: UNKNOWN header (...)".
	var b bytes.Buffer
	b.Write(ehdr(format.PXAREntry, estat(format.ModeIFREG|0o644)))
	b.Write(ehdr(0xdeadbeefdeadbeef, []byte("junk")))
	err := decodeErrOrFail(t, b.Bytes())
	want := "unexpected entry type: UNKNOWN header (deadbeefdeadbeef)"
	if err.Error() != want {
		t.Errorf("error %q does not match rust %q", err.Error(), want)
	}
}

func TestEnforceHardlinkAsAttribute(t *testing.T) {
	// Rust: a HARDLINK appearing as an attribute (mid-entry) is rejected with
	// "encountered unexpected hardlink entry".
	var b bytes.Buffer
	b.Write(ehdr(format.PXAREntry, estat(format.ModeIFREG|0o644)))
	// hardlink content: 8-byte offset + target name + NUL
	hl := make([]byte, 0, 16)
	hl = append(hl, eu64(0)...)
	hl = append(hl, []byte("target\x00")...)
	b.Write(ehdr(format.PXARHardlink, hl))
	err := decodeErrOrFail(t, b.Bytes())
	if err.Error() != "encountered unexpected hardlink entry" {
		t.Errorf("error %q does not match rust %q", err.Error(), "encountered unexpected hardlink entry")
	}
}

// guard against unused imports if helper signatures change
var _ = io.EOF
var _ = binary.LittleEndian
