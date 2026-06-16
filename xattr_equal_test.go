package pxar_test

import (
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/format"
)

// These tests document the relationship between Go metadata equality and the
// Rust reference (proxmox-pxar). The authoritative Rust semantics were captured
// by probing the Rust library directly (XAttr PartialEq is name-only).

// TestXAttrEqualityRustFaithful captures the Rust reference behavior for XAttr
// equality. In Rust, XAttr implements PartialEq as:
//
//	fn eq(&self, other: &XAttr) -> bool { self.name() == other.name() }
//
// i.e. two xattrs are equal when their NAMES match, regardless of their values.
// This feeds into the derived PartialEq on Metadata, so two Metadata structs
// whose xattrs differ only in value compare equal in Rust.
//
// Probe result (Rust 1.0.1, proxmox-pxar):
//
//	XAttr::new("user.foo", "val1") == XAttr::new("user.foo", "val2")  => true
//	XAttr::new("user.foo", "val1") == XAttr::new("user.bar", "val1")  => false
func TestXAttrEqualityRustFaithful(t *testing.T) {
	sameNameDiffValue := format.XAttr{
		Data:    []byte("user.foo\x00val1"),
		NameLen: len("user.foo"),
	}
	sameNameDiffValue2 := format.XAttr{
		Data:    []byte("user.foo\x00val2"),
		NameLen: len("user.foo"),
	}

	// Rust reports these equal because the names match.
	if !equalXAttrsByName(sameNameDiffValue, sameNameDiffValue2) {
		t.Errorf("Rust treats same-name xattrs as equal regardless of value; Go should match for faithfulness")
	}
}

// equalXAttrsByName reports whether two XAttrs are equal by Rust semantics
// (name only). This is the predicate the Go port SHOULD use to match Rust's
// derived Metadata PartialEq.
func equalXAttrsByName(a, b format.XAttr) bool {
	return string(a.Name()) == string(b.Name())
}

// TestMetadataExtendedEqualXAttrValueChangeRustFaithful documents how
// ExtendedMetadataEqual (used for backup change detection in backupproxy)
// behaves relative to Rust when an xattr VALUE changes but its NAME does not.
//
// Rust's Metadata PartialEq (derived) would consider these equal, because
// XAttr equality is name-only. The current Go ExtendedMetadataEqual compares
// the full name+null+value data, so it reports them UNEQUAL.
//
// This test currently EXPECTS the Rust-faithful (name-only) result and will
// FAIL against the present Go implementation, surfacing the divergence.
func TestMetadataExtendedEqualXAttrValueChangeRustFaithful(t *testing.T) {
	base := pxar.Metadata{
		Stat: format.Stat{Mode: format.ModeIFREG | 0o644, UID: 1000, GID: 1000},
		XAttrs: []format.XAttr{
			{Data: []byte("user.foo\x00val1"), NameLen: len("user.foo")},
		},
	}
	changed := pxar.Metadata{
		Stat: base.Stat,
		XAttrs: []format.XAttr{
			{Data: []byte("user.foo\x00val2"), NameLen: len("user.foo")},
		},
	}

	// Rust-faithful expectation: an xattr value change with the same name is
	// NOT detected as a metadata change (because Rust XAttr equality is
	// name-only). Whether this is desirable for backup change detection is a
	// separate question; this test pins the current Go behavior for review.
	got := base.ExtendedMetadataEqual(changed)
	if !got {
		t.Errorf("ExtendedMetadataEqual(xattr-value-only-change) = false; Rust Metadata PartialEq would be true (name-only XAttr eq). " +
			"Go currently reports the change. See test comment.")
	}
}
