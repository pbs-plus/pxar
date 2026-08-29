package pxar

import (
	"strconv"

	"github.com/fxamacker/cbor/v2"
)

// unixSecsMax bounds a valid Unix-seconds timestamp xattr: roughly year 3000.
const unixSecsMax = 32503680000

// Canonical cross-platform xattr names. pxar archives carry owner, group,
// ACL, and Windows-specific metadata in these user.* xattrs so a backup made
// on one platform restores correctly on another.
const (
	XAttrOwner          = "user.owner"
	XAttrGroup          = "user.group"
	XAttrACLs           = "user.acls"
	XAttrFileAttributes = "user.fileattributes"
	XAttrCreationTime   = "user.creationtime"
	XAttrLastAccessTime = "user.lastaccesstime"
	XAttrLastWriteTime  = "user.lastwritetime"
)

// IsCanonicalXAttr reports whether name is one of the canonical
// cross-platform xattrs above.
func IsCanonicalXAttr(name string) bool {
	switch name {
	case XAttrOwner, XAttrGroup, XAttrACLs, XAttrFileAttributes,
		XAttrCreationTime, XAttrLastAccessTime, XAttrLastWriteTime:
		return true
	}
	return false
}

// ParseXattrUnixSecs decodes a canonical timestamp xattr value: a decimal
// string of non-negative Unix seconds no larger than roughly year 3000.
// Out-of-scale values (nanosecond stamps, MaxInt64 garbage) are rejected so
// callers can fall back to Stat timestamps.
func ParseXattrUnixSecs(data []byte) (secs int64, ok bool) {
	if len(data) == 0 {
		return 0, false
	}
	v, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil || v < 0 || v > unixSecsMax {
		return 0, false
	}
	return v, true
}

// ACLFlavor classifies the encoding of a user.acls payload.
type ACLFlavor int

const (
	ACLFlavorNone ACLFlavor = iota
	ACLFlavorPosix
	ACLFlavorWindows
)

// DetectACLFlavor probes the field discriminator ("sid" vs "tag") in a
// user.acls cbor payload. cbor ignores unknown fields, so a foreign payload
// leaves its discriminator empty and reports ACLFlavorNone. Use it so a
// cross-platform restore never applies a foreign ACL type.
func DetectACLFlavor(data []byte) ACLFlavor {
	if len(data) == 0 {
		return ACLFlavorNone
	}
	var probe []struct {
		SID string `cbor:"sid"`
		Tag string `cbor:"tag"`
	}
	if cbor.Unmarshal(data, &probe) != nil {
		return ACLFlavorNone
	}
	var hasPosix, hasWindows bool
	for _, p := range probe {
		if p.SID != "" {
			hasWindows = true
		}
		if p.Tag != "" {
			hasPosix = true
		}
	}
	switch {
	case hasPosix && !hasWindows:
		return ACLFlavorPosix
	case hasWindows && !hasPosix:
		return ACLFlavorWindows
	default:
		return ACLFlavorNone
	}
}
