package format

import "testing"

// TestFormatConstantGeneration verifies that our SipHash implementation
// produces the exact same pxar format type constants as Proxmox's Rust
// reference implementation. These constants are computed by hashing
// marker strings with SipHash24 (see pxar/examples/mk-format-hashes.rs).
func TestFormatConstantGeneration(t *testing.T) {
	tests := []struct {
		name   string
		marker string
		want   uint64
	}{
		{"PXAR_FORMAT_VERSION", "__PROXMOX_FORMAT_VERSION__", PXARFormatVersion},
		{"PXAR_ENTRY", "__PROXMOX_FORMAT_ENTRY_V2__", PXAREntry},
		{"PXAR_ENTRY_V1", "__PROXMOX_FORMAT_ENTRY__", PXAREntryV1},
		{"PXAR_PRELUDE", "__PROXMOX_FORMAT_PRELUDE__", PXARPrelude},
		{"PXAR_FILENAME", "__PROXMOX_FORMAT_FILENAME__", PXARFilename},
		{"PXAR_SYMLINK", "__PROXMOX_FORMAT_SYMLINK__", PXARSymlink},
		{"PXAR_DEVICE", "__PROXMOX_FORMAT_DEVICE__", PXARDevice},
		{"PXAR_XATTR", "__PROXMOX_FORMAT_XATTR__", PXARXAttr},
		{"PXAR_ACL_USER", "__PROXMOX_FORMAT_ACL_USER__", PXARACLUser},
		{"PXAR_ACL_GROUP", "__PROXMOX_FORMAT_ACL_GROUP__", PXARACLGroup},
		{"PXAR_ACL_GROUP_OBJ", "__PROXMOX_FORMAT_ACL_GROUP_OBJ__", PXARACLGroupObj},
		{"PXAR_ACL_DEFAULT", "__PROXMOX_FORMAT_ACL_DEFAULT__", PXARACLDefault},
		{"PXAR_ACL_DEFAULT_USER", "__PROXMOX_FORMAT_ACL_DEFAULT_USER__", PXARACLDefaultUser},
		{"PXAR_ACL_DEFAULT_GROUP", "__PROXMOX_FORMAT_ACL_DEFAULT_GROUP__", PXARACLDefaultGroup},
		{"PXAR_FCAPS", "__PROXMOX_FORMAT_FCAPS__", PXARFCaps},
		{"PXAR_QUOTA_PROJID", "__PROXMOX_FORMAT_QUOTA_PROJID__", PXARQuotaProjID},
		{"PXAR_HARDLINK", "__PROXMOX_FORMAT_HARDLINK__", PXARHardlink},
		{"PXAR_PAYLOAD", "__PROXMOX_FORMAT_PXAR_PAYLOAD__", PXARPayload},
		{"PXAR_PAYLOAD_REF", "__PROXMOX_FORMAT_PXAR_PAYLOAD_REF__", PXARPayloadRef},
		{"PXAR_GOODBYE", "__PROXMOX_FORMAT_GOODBYE__", PXARGoodbye},
		{"PXAR_GOODBYE_TAIL_MARKER", "__PROXMOX_FORMAT_PXAR_GOODBYE_TAIL_MARKER__", PXARGoodbyeTailMarker},
		{"PXAR_PAYLOAD_START_MARKER", "__PROXMOX_FORMAT_PXAR_PAYLOAD_START_MARKER__", PXARPayloadStartMarker},
		{"PXAR_PAYLOAD_TAIL_MARKER", "__PROXMOX_FORMAT_PXAR_PAYLOAD_TAIL_MARKER__", PXARPayloadTailMarker},
	}

	for _, tt := range tests {
		got := HashFilename([]byte(tt.marker))
		if got != tt.want {
			t.Errorf("%s: HashFilename(%q) = %016x, want %016x", tt.name, tt.marker, got, tt.want)
		}
	}
}
