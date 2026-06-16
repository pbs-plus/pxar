package decoder

import (
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/format"
)

// These tests assert that the Go decoder faithfully reproduces the Rust
// reference implementation's (proxmox-pxar) behavior when parsing metadata
// items. They were derived by running crafted archives through the Rust
// decoder (tests/probe) and recording the authoritative behavior.
//
// Two categories are exercised:
//   - Malformed metadata items MUST yield a decoding error (Rust aborts with a
//     descriptive message). The Go port must not panic and must not silently
//     accept/corrupt such items.
//   - Well-formed metadata items round-trip correctly (see complete_* cases).
//
// All archives here are built byte-for-byte to match the Rust probe harness so
// the expected behavior is unambiguous.

// --- low-level archive construction helpers (mirror the Rust probe) ---

func hdr(t uint64, content []byte) []byte {
	full := uint64(format.HeaderSize + len(content))
	v := make([]byte, 0, format.HeaderSize+len(content))
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], t)
	v = append(v, b[:]...)
	binary.LittleEndian.PutUint64(b[:], full)
	v = append(v, b[:]...)
	v = append(v, content...)
	return v
}

func statBytes(mode uint64) []byte {
	v := make([]byte, 40)
	binary.LittleEndian.PutUint64(v[0:], mode)
	binary.LittleEndian.PutUint64(v[8:], 0)     // flags
	binary.LittleEndian.PutUint32(v[16:], 1000) // uid
	binary.LittleEndian.PutUint32(v[20:], 1000) // gid
	binary.LittleEndian.PutUint64(v[24:], 100)  // mtime secs
	binary.LittleEndian.PutUint32(v[32:], 0)    // mtime nanos
	binary.LittleEndian.PutUint32(v[36:], 0)    // pad
	return v
}

// buildArchiveWithExtra produces a root dir containing one regular file "f"
// whose metadata section is followed by `extra` bytes. The stream ends right
// after extra (matching the Rust probe's truncated form). Only metadata-item
// parsing is observed, not entry completion.
func buildArchiveWithExtra(extra []byte) []byte {
	var buf bytes.Buffer
	buf.Write(hdr(format.PXAREntry, statBytes(format.ModeIFDIR|0o755)))
	buf.Write(hdr(format.PXARFilename, []byte("f\x00")))
	buf.Write(hdr(format.PXAREntry, statBytes(format.ModeIFREG|0o644)))
	buf.Write(extra)
	return buf.Bytes()
}

// u64le returns a little-endian uint64 byte slice.
func u64le(v uint64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	return b[:]
}

// decodeOneFile decodes the archive and returns the first error encountered, or
// nil if the archive decoded cleanly to EOF. A panic is recovered and reported
// as an error so malformed-input robustness can be asserted.
func decodeOneFile(t *testing.T, archive []byte) error {
	t.Helper()
	dec := NewDecoder(bytes.NewReader(archive), nil)
	var panicErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicErr = &panicSentinel{v: r}
			}
		}()
		for {
			_, err := dec.Next()
			if err == io.EOF {
				return
			}
			if err != nil {
				panicErr = err
				return
			}
		}
	}()
	return panicErr
}

type panicSentinel struct{ v any }

func (p *panicSentinel) Error() string {
	return "PANIC: " + toString(p.v)
}

func toString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case error:
		return x.Error()
	default:
		return ""
	}
}

// isPanic reports whether err is a recovered panic.
func isPanic(err error) bool {
	_, ok := err.(*panicSentinel)
	return ok
}

// --- malformed metadata MUST error, not panic ---

func TestMetadataACLUserBadSize(t *testing.T) {
	// Rust: "bad acl user size: 4 (expected 16)"
	err := decodeOneFile(t, buildArchiveWithExtra(hdr(format.PXARACLUser, make([]byte, 4))))
	if err == nil {
		t.Fatalf("expected error for undersized ACL_USER, got nil")
	}
	if isPanic(err) {
		t.Fatalf("decoder panicked on undersized ACL_USER instead of returning error: %v", err)
	}
}

func TestMetadataACLGroupBadSize(t *testing.T) {
	// Rust: "bad acl group size: 4 (expected 16)"
	err := decodeOneFile(t, buildArchiveWithExtra(hdr(format.PXARACLGroup, make([]byte, 4))))
	if err == nil {
		t.Fatalf("expected error for undersized ACL_GROUP, got nil")
	}
	if isPanic(err) {
		t.Fatalf("decoder panicked on undersized ACL_GROUP instead of returning error: %v", err)
	}
}

func TestMetadataACLDefaultUserBadSize(t *testing.T) {
	err := decodeOneFile(t, buildArchiveWithExtra(hdr(format.PXARACLDefaultUser, make([]byte, 4))))
	if err == nil {
		t.Fatalf("expected error for undersized ACL_DEFAULT_USER, got nil")
	}
	if isPanic(err) {
		t.Fatalf("decoder panicked on undersized ACL_DEFAULT_USER instead of returning error: %v", err)
	}
}

func TestMetadataACLDefaultGroupBadSize(t *testing.T) {
	err := decodeOneFile(t, buildArchiveWithExtra(hdr(format.PXARACLDefaultGroup, make([]byte, 4))))
	if err == nil {
		t.Fatalf("expected error for undersized ACL_DEFAULT_GROUP, got nil")
	}
	if isPanic(err) {
		t.Fatalf("decoder panicked on undersized ACL_DEFAULT_GROUP instead of returning error: %v", err)
	}
}

func TestMetadataACLGroupObjBadSize(t *testing.T) {
	// Rust: "bad acl group object size: 4 (expected 8)"
	err := decodeOneFile(t, buildArchiveWithExtra(hdr(format.PXARACLGroupObj, make([]byte, 4))))
	if err == nil {
		t.Fatalf("expected error for undersized ACL_GROUP_OBJ, got nil")
	}
	if isPanic(err) {
		t.Fatalf("decoder panicked on undersized ACL_GROUP_OBJ instead of returning error: %v", err)
	}
}

func TestMetadataACLDefaultBadSize(t *testing.T) {
	// Rust: "bad acl default size: 8 (expected 32)"
	err := decodeOneFile(t, buildArchiveWithExtra(hdr(format.PXARACLDefault, make([]byte, 8))))
	if err == nil {
		t.Fatalf("expected error for undersized ACL_DEFAULT, got nil")
	}
	if isPanic(err) {
		t.Fatalf("decoder panicked on undersized ACL_DEFAULT instead of returning error: %v", err)
	}
}

func TestMetadataDeviceBadSize(t *testing.T) {
	// Rust: "bad device size: 8 (expected 16)"
	err := decodeOneFile(t, buildArchiveWithExtra(hdr(format.PXARDevice, make([]byte, 8))))
	if err == nil {
		t.Fatalf("expected error for undersized DEVICE, got nil")
	}
	if isPanic(err) {
		t.Fatalf("decoder panicked on undersized DEVICE instead of returning error: %v", err)
	}
}

func TestMetadataQuotaProjIDBadSize(t *testing.T) {
	// Rust: "bad quota project id size: 4 (expected 8)"
	err := decodeOneFile(t, buildArchiveWithExtra(hdr(format.PXARQuotaProjID, make([]byte, 4))))
	if err == nil {
		t.Fatalf("expected error for undersized QUOTA_PROJID, got nil")
	}
	if isPanic(err) {
		t.Fatalf("decoder panicked on undersized QUOTA_PROJID instead of returning error: %v", err)
	}
}

func TestMetadataPayloadRefBadSize(t *testing.T) {
	// Rust reads exactly 16 bytes regardless of content_size; a short
	// content_size desyncs/EOFs. The Go port must not panic on a short
	// PAYLOAD_REF and must surface an error.
	err := decodeOneFile(t, buildArchiveWithExtra(hdr(format.PXARPayloadRef, make([]byte, 8))))
	if err == nil {
		t.Fatalf("expected error for undersized PAYLOAD_REF, got nil")
	}
	if isPanic(err) {
		t.Fatalf("decoder panicked on undersized PAYLOAD_REF instead of returning error: %v", err)
	}
}

func TestMetadataXAttrMissingSeparator(t *testing.T) {
	// Rust: "missing value separator in xattr"
	err := decodeOneFile(t, buildArchiveWithExtra(hdr(format.PXARXAttr, []byte{1, 2, 3, 4})))
	if err == nil {
		t.Fatalf("expected error for XATTR without null separator, got nil")
	}
	if isPanic(err) {
		t.Fatalf("decoder panicked on XATTR without separator instead of returning error: %v", err)
	}
}

// --- duplicate metadata items MUST error, not silently overwrite ---

func dupExtra(htype uint64, content []byte) []byte {
	var b bytes.Buffer
	b.Write(hdr(htype, content))
	b.Write(hdr(htype, content))
	return b.Bytes()
}

func TestMetadataACLGroupObjDuplicate(t *testing.T) {
	// Rust: "multiple acl group object entries detected"
	err := decodeOneFile(t, buildArchiveWithExtra(dupExtra(format.PXARACLGroupObj, u64le(5))))
	if err == nil {
		t.Fatalf("expected error for duplicate ACL_GROUP_OBJ, got nil")
	}
}

func TestMetadataACLDefaultDuplicate(t *testing.T) {
	// Rust: "multiple acl default entries detected"
	err := decodeOneFile(t, buildArchiveWithExtra(dupExtra(format.PXARACLDefault, make([]byte, 32))))
	if err == nil {
		t.Fatalf("expected error for duplicate ACL_DEFAULT, got nil")
	}
}

func TestMetadataFCapsDuplicate(t *testing.T) {
	// Rust: "multiple file capability entries detected"
	err := decodeOneFile(t, buildArchiveWithExtra(dupExtra(format.PXARFCaps, []byte{1, 2, 3})))
	if err == nil {
		t.Fatalf("expected error for duplicate FCAPS, got nil")
	}
}

func TestMetadataQuotaProjIDDuplicate(t *testing.T) {
	// Rust: "multiple quota project id entries detected"
	err := decodeOneFile(t, buildArchiveWithExtra(dupExtra(format.PXARQuotaProjID, u64le(42))))
	if err == nil {
		t.Fatalf("expected error for duplicate QUOTA_PROJID, got nil")
	}
}

// --- sanity: error messages mention the offending item (faithfulness) ---

func TestMetadataErrorMessagesAreDescriptive(t *testing.T) {
	cases := []struct {
		name    string
		archive []byte
		want    string
	}{
		{"acl_user", buildArchiveWithExtra(hdr(format.PXARACLUser, make([]byte, 4))), "acl user"},
		{"acl_group", buildArchiveWithExtra(hdr(format.PXARACLGroup, make([]byte, 4))), "acl group"},
		{"acl_groupobj", buildArchiveWithExtra(hdr(format.PXARACLGroupObj, make([]byte, 4))), "group object"},
		{"acl_default", buildArchiveWithExtra(hdr(format.PXARACLDefault, make([]byte, 8))), "acl default"},
		{"device", buildArchiveWithExtra(hdr(format.PXARDevice, make([]byte, 8))), "device"},
		{"quota", buildArchiveWithExtra(hdr(format.PXARQuotaProjID, make([]byte, 4))), "quota"},
		{"xattr", buildArchiveWithExtra(hdr(format.PXARXAttr, []byte{1, 2, 3})), "xattr"},
		{"groupobj_dup", buildArchiveWithExtra(dupExtra(format.PXARACLGroupObj, u64le(5))), "group object"},
		{"default_dup", buildArchiveWithExtra(dupExtra(format.PXARACLDefault, make([]byte, 32))), "default"},
		{"fcaps_dup", buildArchiveWithExtra(dupExtra(format.PXARFCaps, []byte{1})), "capabilit"},
		{"quota_dup", buildArchiveWithExtra(dupExtra(format.PXARQuotaProjID, u64le(42))), "quota"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := decodeOneFile(t, tc.archive)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(strings.ToLower(err.Error()), tc.want) {
				t.Errorf("error %q does not mention %q", err.Error(), tc.want)
			}
		})
	}
}

// --- well-formed round trip mirrors of the Rust probe2 cases ---

func buildCompleteArchive(extra []byte) []byte {
	var buf bytes.Buffer
	rootStart := buf.Len()
	buf.Write(hdr(format.PXAREntry, statBytes(format.ModeIFDIR|0o755)))
	fStart := buf.Len()
	buf.Write(hdr(format.PXARFilename, []byte("f\x00")))
	buf.Write(hdr(format.PXAREntry, statBytes(format.ModeIFREG|0o644)))
	buf.Write(extra)
	buf.Write(hdr(format.PXARPayload, nil)) // empty payload
	fEnd := buf.Len()
	goodbyeOffset := buf.Len()
	// goodbye item + tail
	var gi bytes.Buffer
	gi.Write(u64le(format.HashFilename([]byte("f"))))
	gi.Write(u64le(uint64(goodbyeOffset - fStart)))
	gi.Write(u64le(uint64(fEnd - fStart)))
	gi.Write(u64le(format.PXARGoodbyeTailMarker))
	gi.Write(u64le(uint64(goodbyeOffset - rootStart)))
	gi.Write(u64le(uint64(gi.Len() + 24 + format.HeaderSize)))
	buf.Write(hdr(format.PXARGoodbye, gi.Bytes()))
	return buf.Bytes()
}

func decodeFileEntry(t *testing.T, archive []byte) *pxar.Entry {
	t.Helper()
	dec := NewDecoder(bytes.NewReader(archive), nil)
	for {
		e, err := dec.Next()
		if err == io.EOF {
			t.Fatal("file entry f not found before EOF")
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if e.Path == "/f" {
			return e
		}
	}
}

func TestCompleteMetadataACLUser(t *testing.T) {
	var c bytes.Buffer
	c.Write(u64le(100))
	c.Write(u64le(7))
	e := decodeFileEntry(t, buildCompleteArchive(hdr(format.PXARACLUser, c.Bytes())))
	if len(e.Metadata.ACL.Users) != 1 {
		t.Errorf("Users len = %d, want 1", len(e.Metadata.ACL.Users))
	}
}

func TestCompleteMetadataACLGroupObj(t *testing.T) {
	e := decodeFileEntry(t, buildCompleteArchive(hdr(format.PXARACLGroupObj, u64le(5))))
	if e.Metadata.ACL.GroupObj == nil {
		t.Errorf("GroupObj = nil, want set")
	}
}

func TestCompleteMetadataACLDefault(t *testing.T) {
	e := decodeFileEntry(t, buildCompleteArchive(hdr(format.PXARACLDefault, make([]byte, 32))))
	if e.Metadata.ACL.Default == nil {
		t.Errorf("Default = nil, want set")
	}
}

func TestCompleteMetadataFCaps(t *testing.T) {
	e := decodeFileEntry(t, buildCompleteArchive(hdr(format.PXARFCaps, []byte{9, 9, 9})))
	if len(e.Metadata.FCaps) == 0 {
		t.Errorf("FCaps empty, want set")
	}
}

func TestCompleteMetadataQuotaProjID(t *testing.T) {
	e := decodeFileEntry(t, buildCompleteArchive(hdr(format.PXARQuotaProjID, u64le(42))))
	if e.Metadata.QuotaProjectID == nil || *e.Metadata.QuotaProjectID != 42 {
		t.Errorf("QuotaProjectID = %v, want 42", e.Metadata.QuotaProjectID)
	}
}

func TestCompleteMetadataXAttr(t *testing.T) {
	var c bytes.Buffer
	c.WriteString("user.a")
	c.WriteByte(0)
	c.WriteString("v")
	e := decodeFileEntry(t, buildCompleteArchive(hdr(format.PXARXAttr, c.Bytes())))
	if len(e.Metadata.XAttrs) != 1 {
		t.Fatalf("XAttrs len = %d, want 1", len(e.Metadata.XAttrs))
	}
	if string(e.Metadata.XAttrs[0].Name()) != "user.a" {
		t.Errorf("xattr name = %q, want %q", e.Metadata.XAttrs[0].Name(), "user.a")
	}
	if string(e.Metadata.XAttrs[0].Value()) != "v" {
		t.Errorf("xattr value = %q, want %q", e.Metadata.XAttrs[0].Value(), "v")
	}
}
