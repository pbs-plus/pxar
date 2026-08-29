package pxar

import (
	"strconv"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
)

func TestParseXattrUnixSecs(t *testing.T) {
	const sec2020 int64 = 1609459200

	tests := []struct {
		name   string
		input  string
		want   int64
		wantOk bool
	}{
		{"unix-agent seconds", strconv.FormatInt(sec2020, 10), sec2020, true},
		{"windows-agent seconds", "1577836800", 1577836800, true},
		{"epoch zero", "0", 0, true},
		{"out-of-scale nanos rejected", strconv.FormatInt(sec2020*int64(time.Second), 10), 0, false},
		{"legacy decimal string", "1136214240", 1136214240, true},
		{"empty value rejected", "", 0, false},
		{"non-numeric rejected", "not-a-time", 0, false},
		{"huge value rejected", "9223372036854775807", 0, false},
		{"negative value rejected", "-100", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ParseXattrUnixSecs([]byte(tt.input))
			if ok != tt.wantOk {
				t.Fatalf("ParseXattrUnixSecs(%q) ok = %v, want %v", tt.input, ok, tt.wantOk)
			}
			if got != tt.want {
				t.Errorf("ParseXattrUnixSecs(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestIsCanonicalXAttr(t *testing.T) {
	for _, name := range []string{
		XAttrOwner, XAttrGroup, XAttrACLs, XAttrFileAttributes,
		XAttrCreationTime, XAttrLastAccessTime, XAttrLastWriteTime,
	} {
		if !IsCanonicalXAttr(name) {
			t.Errorf("IsCanonicalXAttr(%q) = false, want true", name)
		}
	}
	for _, name := range []string{"", "user.custom", "security.selinux", "trusted.foo"} {
		if IsCanonicalXAttr(name) {
			t.Errorf("IsCanonicalXAttr(%q) = true, want false", name)
		}
	}
}

func TestDetectACLFlavor(t *testing.T) {
	posix, err := cbor.Marshal([]struct {
		Tag  string `cbor:"tag"`
		Perm uint64 `cbor:"perm"`
	}{{Tag: "ACL_USER_OBJ", Perm: 7}})
	if err != nil {
		t.Fatalf("marshal posix: %v", err)
	}
	win, err := cbor.Marshal([]struct {
		SID string `cbor:"sid"`
		Rev uint32 `cbor:"rev"`
	}{{SID: "S-1-5-18", Rev: 1}})
	if err != nil {
		t.Fatalf("marshal win: %v", err)
	}
	winZeroTag, _ := cbor.Marshal([]struct {
		SID string `cbor:"sid"`
	}{{}})

	tests := []struct {
		name string
		data []byte
		want ACLFlavor
	}{
		{"posix source payload", posix, ACLFlavorPosix},
		{"windows source payload", win, ACLFlavorWindows},
		{"empty nil", nil, ACLFlavorNone},
		{"empty bytes", []byte{}, ACLFlavorNone},
		{"non-cbor garbage", []byte("not-acl"), ACLFlavorNone},
		{"no discriminator entries", winZeroTag, ACLFlavorNone},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DetectACLFlavor(tt.data); got != tt.want {
				t.Errorf("DetectACLFlavor() = %v, want %v", got, tt.want)
			}
		})
	}
}
