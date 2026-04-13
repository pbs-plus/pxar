// Package format defines the pxar binary format types and constants.
//
// All values are stored in little endian ordering.
// The archive contains a list of items. Each item starts with a Header,
// followed by item data.
package format

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

// Hash keys for SipHash24 filename hashing.
// Generated from: echo -n 'PROXMOX ARCHIVE FORMAT' | sha1sum
const (
	HashKey1 uint64 = 0x83ac3f1cfbb450db
	HashKey2 uint64 = 0xaa4f1b6879369fbd
)

// Pxar format type constants.
const (
	PXARFormatVersion      uint64 = 0x730f6c75df16a40d
	PXAREntry              uint64 = 0xd5956474e588acef
	PXAREntryV1            uint64 = 0x11da850a1c1cceff
	PXARPrelude            uint64 = 0xe309d79d9f7b771b
	PXARFilename           uint64 = 0x16701121063917b3
	PXARSymlink            uint64 = 0x27f971e7dbf5dc5f
	PXARDevice             uint64 = 0x9fc9e906586d5ce9
	PXARXAttr              uint64 = 0x0dab0229b57dcd03
	PXARACLUser            uint64 = 0x2ce8540a457d55b8
	PXARACLGroup           uint64 = 0x136e3eceb04c03ab
	PXARACLGroupObj        uint64 = 0x10868031e9582876
	PXARACLDefault         uint64 = 0xbbbb13415a6896f5
	PXARACLDefaultUser     uint64 = 0xc89357b40532cd1f
	PXARACLDefaultGroup    uint64 = 0xf90a8a5816038ffe
	PXARFCaps              uint64 = 0x2da9dd9db5f7fb67
	PXARQuotaProjID        uint64 = 0xe07540e82f7d1cbb
	PXARHardlink           uint64 = 0x51269c8422bd7275
	PXARPayload            uint64 = 0x28147a1b0b7c1a25
	PXARPayloadRef         uint64 = 0x419d3d6bc4ba977e
	PXARGoodbye            uint64 = 0x2fec4fa642d5731d
	PXARGoodbyeTailMarker  uint64 = 0xef5eed5b753e1555
	PXARPayloadStartMarker uint64 = 0x834c68c2194a4ed2
	PXARPayloadTailMarker  uint64 = 0x6c72b78b984c81b5
)

// File mode type constants matching Linux stat.h values.
const (
	ModeIFMT   uint64 = 0o0170000
	ModeIFSOCK uint64 = 0o0140000
	ModeIFLNK  uint64 = 0o0120000
	ModeIFREG  uint64 = 0o0100000
	ModeIFBLK  uint64 = 0o0060000
	ModeIFDIR  uint64 = 0o0040000
	ModeIFCHR  uint64 = 0o0020000
	ModeIFIFO  uint64 = 0o0010000

	ModeISUID uint64 = 0o0004000
	ModeISGID uint64 = 0o0002000
	ModeISVTX uint64 = 0o0001000
)

// Size limits.
const (
	MaxFilenameLen uint64 = 4 * 1024
	MaxPathLen     uint64 = 4 * 1024
	MaxXAttrLen    uint64 = 255 + 64*1024
)

// Header is the binary header preceding every pxar item. 16 bytes total.
type Header struct {
	Type uint64
	Size uint64
}

// HeaderSize is the byte size of Header.
const HeaderSize = 16

// NewHeader creates a Header with the given full size.
func NewHeader(htype, fullSize uint64) Header {
	return Header{Type: htype, Size: fullSize}
}

// HeaderWithContentSize creates a Header where Size = contentSize + HeaderSize.
func HeaderWithContentSize(htype, contentSize uint64) Header {
	return Header{Type: htype, Size: contentSize + HeaderSize}
}

// ContentSize returns the size of the content after the header.
func (h Header) ContentSize() uint64 {
	return h.Size - HeaderSize
}

// MaxContentSize returns the maximum allowed content size for this header type.
func (h Header) MaxContentSize() uint64 {
	switch h.Type {
	case PXARPrelude:
		return ^uint64(0) - HeaderSize
	case PXARFilename:
		return MaxFilenameLen + 1
	case PXARSymlink:
		return MaxPathLen + 1
	case PXARHardlink:
		return MaxPathLen + 1 + 8
	case PXARDevice:
		return 16 // Device struct size
	case PXARXAttr, PXARFCaps:
		return MaxXAttrLen
	case PXARACLUser, PXARACLDefaultUser:
		return 24 // ACLUser struct size
	case PXARACLGroup, PXARACLDefaultGroup:
		return 24 // ACLGroup struct size
	case PXARACLDefault:
		return 32 // ACLDefault struct size
	case PXARACLGroupObj:
		return 8 // ACLGroupObject struct size
	case PXARQuotaProjID:
		return 8 // QuotaProjectID struct size
	case PXAREntry:
		return 40 // Stat struct size
	case PXARPayload, PXARGoodbye:
		return ^uint64(0) - HeaderSize
	case PXARPayloadRef:
		return 16 // PayloadRef struct size
	case PXARPayloadTailMarker:
		return 0
	default:
		return ^uint64(0) - HeaderSize
	}
}

// CheckHeaderSize validates the header's size fields.
func (h Header) CheckHeaderSize() error {
	if h.Size < HeaderSize {
		return fmt.Errorf("invalid header %s: too small (%d)", h.String(), h.Size)
	}
	if h.ContentSize() > h.MaxContentSize() {
		return fmt.Errorf("invalid content size (%d > %d) of entry with %s", h.ContentSize(), h.MaxContentSize(), h.String())
	}
	return nil
}

// String returns a human-readable description of the header.
func (h Header) String() string {
	var name string
	switch h.Type {
	case PXARFormatVersion:
		name = "FORMAT_VERSION"
	case PXARPrelude:
		name = "PRELUDE"
	case PXARFilename:
		name = "FILENAME"
	case PXARSymlink:
		name = "SYMLINK"
	case PXARHardlink:
		name = "HARDLINK"
	case PXARDevice:
		name = "DEVICE"
	case PXARXAttr:
		name = "XATTR"
	case PXARFCaps:
		name = "FCAPS"
	case PXARACLUser:
		name = "ACL_USER"
	case PXARACLDefaultUser:
		name = "ACL_DEFAULT_USER"
	case PXARACLGroup:
		name = "ACL_GROUP"
	case PXARACLDefaultGroup:
		name = "ACL_DEFAULT_GROUP"
	case PXARACLDefault:
		name = "ACL_DEFAULT"
	case PXARACLGroupObj:
		name = "ACL_GROUP_OBJ"
	case PXARQuotaProjID:
		name = "QUOTA_PROJID"
	case PXAREntry:
		name = "ENTRY"
	case PXARPayload:
		name = "PAYLOAD"
	case PXARPayloadRef:
		name = "PAYLOAD_REF"
	case PXARPayloadTailMarker:
		name = "PXAR_PAYLOAD_TAIL_MARKER"
	case PXARGoodbye:
		name = "GOODBYE"
	default:
		name = "UNKNOWN"
	}
	return fmt.Sprintf("%s header (%x)", name, h.Type)
}

// StatxTimestamp represents a high-precision timestamp. 16 bytes.
type StatxTimestamp struct {
	Secs  int64
	Nanos uint32
	_pad  uint32
}

// StatxTimestampZero returns a zero-valued timestamp.
func StatxTimestampZero() StatxTimestamp {
	return StatxTimestamp{}
}

// StatxTimestampNew creates a timestamp from seconds and nanoseconds.
func StatxTimestampNew(secs int64, nanos uint32) StatxTimestamp {
	return StatxTimestamp{Secs: secs, Nanos: nanos}
}

// StatxTimestampFromDurationSinceEpoch creates a timestamp from a positive duration.
func StatxTimestampFromDurationSinceEpoch(d time.Duration) StatxTimestamp {
	return StatxTimestamp{
		Secs:  int64(d / time.Second),
		Nanos: uint32(d % time.Second),
	}
}

// Duration converts the timestamp to a duration since epoch.
func (ts StatxTimestamp) Duration() time.Duration {
	return time.Duration(ts.Secs)*time.Second + time.Duration(ts.Nanos)*time.Nanosecond
}

// Stat contains file metadata similar to Unix stat. 40 bytes.
type Stat struct {
	Mode  uint64
	Flags uint64
	UID   uint32
	GID   uint32
	Mtime StatxTimestamp
}

// FileType returns the file type portion of the mode (Mode & ModeIFMT).
func (s Stat) FileType() uint64 {
	return s.Mode & ModeIFMT
}

// FileMode returns the permission portion of the mode (Mode & ^ModeIFMT).
func (s Stat) FileMode() uint64 {
	return s.Mode & ^ModeIFMT
}

// IsDir reports whether the stat represents a directory.
func (s Stat) IsDir() bool {
	return s.Mode&ModeIFMT == ModeIFDIR
}

// IsSymlink reports whether the stat represents a symbolic link.
func (s Stat) IsSymlink() bool {
	return s.Mode&ModeIFMT == ModeIFLNK
}

// IsRegularFile reports whether the stat represents a regular file.
func (s Stat) IsRegularFile() bool {
	return s.Mode&ModeIFMT == ModeIFREG
}

// IsDevice reports whether the stat represents a device (char or block).
func (s Stat) IsDevice() bool {
	ft := s.Mode & ModeIFMT
	return ft == ModeIFCHR || ft == ModeIFBLK
}

// IsBlockDev reports whether the stat represents a block device.
func (s Stat) IsBlockDev() bool {
	return s.Mode&ModeIFMT == ModeIFBLK
}

// IsCharDev reports whether the stat represents a character device.
func (s Stat) IsCharDev() bool {
	return s.Mode&ModeIFMT == ModeIFCHR
}

// IsFIFO reports whether the stat represents a FIFO.
func (s Stat) IsFIFO() bool {
	return s.Mode&ModeIFMT == ModeIFIFO
}

// IsSocket reports whether the stat represents a socket.
func (s Stat) IsSocket() bool {
	return s.Mode&ModeIFMT == ModeIFSOCK
}

// MetadataEqual reports whether two Stat entries are equivalent for
// metadata change detection. Two entries are considered equal if
// their file type, permissions, uid, gid, and mtime match.
// File size comparison is done separately since Stat doesn't carry size.
func (s Stat) MetadataEqual(other Stat) bool {
	return s.Mode == other.Mode &&
		s.Flags == other.Flags &&
		s.UID == other.UID &&
		s.GID == other.GID &&
		s.Mtime == other.Mtime
}

// StatV1 is the legacy format stat structure. 32 bytes.
type StatV1 struct {
	Mode  uint64
	Flags uint64
	UID   uint32
	GID   uint32
	Mtime uint64
}

// ToStat converts a V1 stat to the current Stat format.
func (v1 StatV1) ToStat() Stat {
	return Stat{
		Mode:  v1.Mode,
		Flags: v1.Flags,
		UID:   v1.UID,
		GID:   v1.GID,
		Mtime: StatxTimestampFromDurationSinceEpoch(time.Duration(v1.Mtime) * time.Nanosecond),
	}
}

// Device represents a device node with major/minor numbers. 16 bytes.
type Device struct {
	Major uint64
	Minor uint64
}

// ToDevT converts to a Linux dev_t value.
func (d Device) ToDevT() uint64 {
	return (d.Major&0x0000_0fff)<<8 |
		(d.Major&0xffff_f000)<<32 |
		(d.Minor & 0x0000_00ff) |
		(d.Minor&0xffff_ff00)<<12
}

// DeviceFromDevT creates a Device from a Linux dev_t value.
func DeviceFromDevT(dev uint64) Device {
	return Device{
		Major: (dev>>8)&0x0000_0fff | (dev>>32)&0xffff_f000,
		Minor: dev&0x0000_00ff | (dev>>12)&0xffff_ff00,
	}
}

// QuotaProjectID represents an ext4/XFS quota project ID. 8 bytes.
type QuotaProjectID struct {
	ProjID uint64
}

// GoodbyeItem is an entry in the goodbye lookup table. 24 bytes.
type GoodbyeItem struct {
	Hash   uint64
	Offset uint64
	Size   uint64
}

// NewGoodbyeItem creates a GoodbyeItem by hashing the filename.
func NewGoodbyeItem(name []byte, offset, size uint64) GoodbyeItem {
	return GoodbyeItem{
		Hash:   HashFilename(name),
		Offset: offset,
		Size:   size,
	}
}

// PayloadRef references file content in a separate payload archive. 16 bytes.
type PayloadRef struct {
	Offset uint64
	Size   uint64
}

// Bytes returns the little-endian bytes of the PayloadRef.
func (pr PayloadRef) Bytes() []byte {
	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[0:8], pr.Offset)
	binary.LittleEndian.PutUint64(buf[8:16], pr.Size)
	return buf[:]
}

// FormatVersion represents the pxar format version.
type FormatVersion int

const (
	FormatVersion1 FormatVersion = 1
	FormatVersion2 FormatVersion = 2
)

// String returns a human-readable format version name.
func (v FormatVersion) String() string {
	switch v {
	case FormatVersion1:
		return "v1"
	case FormatVersion2:
		return "v2"
	default:
		return fmt.Sprintf("unknown(%d)", int(v))
	}
}

// Serialize returns the bytes to write for this format version.
// V1 returns nil (not encoded), V2 returns the version number as LE uint64.
func (v FormatVersion) Serialize() []byte {
	if v == FormatVersion1 {
		return nil
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(v))
	return buf[:]
}

// DeserializeFormatVersion parses a format version number.
func DeserializeFormatVersion(version uint64) (FormatVersion, error) {
	switch version {
	case 1:
		return FormatVersion1, nil
	case 2:
		return FormatVersion2, nil
	default:
		return 0, fmt.Errorf("unknown format version %d", version)
	}
}

// XAttr represents an extended attribute (name + value).
type XAttr struct {
	Data    []byte
	NameLen int
}

// NewXAttr creates an XAttr from a name and value.
func NewXAttr(name, value []byte) XAttr {
	data := make([]byte, 0, len(name)+1+len(value))
	data = append(data, name...)
	data = append(data, 0)
	data = append(data, value...)
	return XAttr{Data: data, NameLen: len(name)}
}

// Name returns the attribute name as a string (without null terminator).
func (x XAttr) Name() []byte {
	return x.Data[:x.NameLen]
}

// Value returns the attribute value.
func (x XAttr) Value() []byte {
	return x.Data[x.NameLen+1:]
}

// FCaps represents file capabilities.
type FCaps struct {
	Data []byte
}

// Prelude is an optional blob at the start of a v2 archive.
type Prelude struct {
	Data []byte
}

// Symlink stores the symlink target path.
type Symlink struct {
	Data []byte
}

// Hardlink stores a hard link target (offset + path).
type Hardlink struct {
	Offset uint64
	Data   []byte
}

// ReadHeader reads a Header from the reader.
func ReadHeader(r io.Reader) (Header, error) {
	var h Header
	err := binary.Read(r, binary.LittleEndian, &h)
	return h, err
}

// WriteHeader writes a Header to the writer.
func WriteHeader(w io.Writer, h Header) error {
	return binary.Write(w, binary.LittleEndian, &h)
}

// ReadStat reads a Stat from the reader.
func ReadStat(r io.Reader) (Stat, error) {
	var s Stat
	err := binary.Read(r, binary.LittleEndian, &s)
	return s, err
}

// WriteStat writes a Stat to the writer.
func WriteStat(w io.Writer, s Stat) error {
	return binary.Write(w, binary.LittleEndian, &s)
}

// ReadDevice reads a Device from the reader.
func ReadDevice(r io.Reader) (Device, error) {
	var d Device
	err := binary.Read(r, binary.LittleEndian, &d)
	return d, err
}

// ReadGoodbyeItem reads a GoodbyeItem from the reader.
func ReadGoodbyeItem(r io.Reader) (GoodbyeItem, error) {
	var g GoodbyeItem
	err := binary.Read(r, binary.LittleEndian, &g)
	return g, err
}

// ReadPayloadRef reads a PayloadRef from the reader.
func ReadPayloadRef(r io.Reader) (PayloadRef, error) {
	var p PayloadRef
	err := binary.Read(r, binary.LittleEndian, &p)
	return p, err
}

// CheckFilename validates that a filename is a legal path component.
func CheckFilename(name []byte) error {
	if len(name) == 0 {
		return fmt.Errorf("empty filename")
	}
	if bytes.Contains(name, slashBytes) {
		return fmt.Errorf("invalid filename: %q", name)
	}
	if name[0] == '.' && (len(name) == 1 || (len(name) == 2 && name[1] == '.')) {
		return fmt.Errorf("invalid filename: %q", name)
	}
	return nil
}

var slashBytes = []byte{'/'}

// MarshalStatBytes serializes a Stat into a 40-byte slice.
func MarshalStatBytes(s Stat) []byte {
	buf := make([]byte, 40)
	binary.LittleEndian.PutUint64(buf[0:], s.Mode)
	binary.LittleEndian.PutUint64(buf[8:], s.Flags)
	binary.LittleEndian.PutUint32(buf[16:], s.UID)
	binary.LittleEndian.PutUint32(buf[20:], s.GID)
	binary.LittleEndian.PutUint64(buf[24:], uint64(s.Mtime.Secs))
	binary.LittleEndian.PutUint32(buf[32:], s.Mtime.Nanos)
	return buf
}

// UnmarshalStatBytes parses a Stat from a 40-byte slice.
func UnmarshalStatBytes(data []byte) Stat {
	return Stat{
		Mode:  binary.LittleEndian.Uint64(data[0:]),
		Flags: binary.LittleEndian.Uint64(data[8:]),
		UID:   binary.LittleEndian.Uint32(data[16:]),
		GID:   binary.LittleEndian.Uint32(data[20:]),
		Mtime: StatxTimestamp{
			Secs:  int64(binary.LittleEndian.Uint64(data[24:])),
			Nanos: binary.LittleEndian.Uint32(data[32:]),
		},
	}
}

// UnmarshalStatV1Bytes parses a StatV1 from a 32-byte slice.
func UnmarshalStatV1Bytes(data []byte) StatV1 {
	return StatV1{
		Mode:  binary.LittleEndian.Uint64(data[0:]),
		Flags: binary.LittleEndian.Uint64(data[8:]),
		UID:   binary.LittleEndian.Uint32(data[16:]),
		GID:   binary.LittleEndian.Uint32(data[20:]),
		Mtime: binary.LittleEndian.Uint64(data[24:]),
	}
}

// MarshalDeviceBytes serializes a Device into a 16-byte slice.
func MarshalDeviceBytes(d Device) []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:], d.Major)
	binary.LittleEndian.PutUint64(buf[8:], d.Minor)
	return buf
}

// MarshalACLUserBytes serializes an ACLUser into a 16-byte slice.
func MarshalACLUserBytes(u ACLUser) []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:], u.UID)
	binary.LittleEndian.PutUint64(buf[8:], uint64(u.Permissions))
	return buf
}

// MarshalACLGroupBytes serializes an ACLGroup into a 16-byte slice.
func MarshalACLGroupBytes(g ACLGroup) []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:], g.GID)
	binary.LittleEndian.PutUint64(buf[8:], uint64(g.Permissions))
	return buf
}

// MarshalACLGroupObjectBytes serializes an ACLGroupObject into an 8-byte slice.
func MarshalACLGroupObjectBytes(o ACLGroupObject) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf[0:], uint64(o.Permissions))
	return buf
}

// UnmarshalACLDefaultBytes parses an ACLDefault from a 32-byte slice.
func UnmarshalACLDefaultBytes(data []byte) *ACLDefault {
	return &ACLDefault{
		UserObjPermissions:  ACLPermissions(binary.LittleEndian.Uint64(data[0:])),
		GroupObjPermissions: ACLPermissions(binary.LittleEndian.Uint64(data[8:])),
		OtherPermissions:    ACLPermissions(binary.LittleEndian.Uint64(data[16:])),
		MaskPermissions:     ACLPermissions(binary.LittleEndian.Uint64(data[24:])),
	}
}

// MarshalACLDefaultBytes serializes an ACLDefault into a 32-byte slice.
func MarshalACLDefaultBytes(d ACLDefault) []byte {
	buf := make([]byte, 32)
	binary.LittleEndian.PutUint64(buf[0:], uint64(d.UserObjPermissions))
	binary.LittleEndian.PutUint64(buf[8:], uint64(d.GroupObjPermissions))
	binary.LittleEndian.PutUint64(buf[16:], uint64(d.OtherPermissions))
	binary.LittleEndian.PutUint64(buf[24:], uint64(d.MaskPermissions))
	return buf
}

// UnmarshalPayloadRefBytes parses a PayloadRef from a 16-byte slice.
func UnmarshalPayloadRefBytes(data []byte) PayloadRef {
	return PayloadRef{
		Offset: binary.LittleEndian.Uint64(data[0:]),
		Size:   binary.LittleEndian.Uint64(data[8:]),
	}
}
