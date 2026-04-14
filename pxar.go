// Package pxar implements the Proxmox Archive format for Go.
//
// pxar is a binary archive format for efficient backup and storage of file system
// metadata and content with support for random access via goodbye tables.
//
// # Entry Model
//
// An archive is a stream of typed entries (Entry). Each entry has a Kind that
// identifies it as a directory, file, symlink, device node, socket, FIFO, or
// goodbye table marker. File entries carry content inline; directory entries
// contain child entries followed by a goodbye table for O(log n) filename
// lookups.
//
// # Metadata
//
// Every filesystem entry carries a Metadata struct with POSIX stat information
// (format.Stat), extended attributes (XAttr), POSIX ACLs, file capabilities
// (FCaps), and quota project IDs. Use MetadataBuilder to construct Metadata
// with a fluent API:
//
//	meta := pxar.FileMetadata(0o644).
//	    Owner(1000, 1000).
//	    Build()
//
// # File Name Validation
//
// Use CheckPathComponent to validate path components before encoding:
//
//	if !pxar.CheckPathComponent(name) {
//	    return fmt.Errorf("invalid filename: %s", name)
//	}
package pxar

import (
	"path/filepath"
	"strings"

	"github.com/pbs-plus/pxar/format"
)

// EntryKind identifies the type of a pxar archive entry.
type EntryKind int

const (
	KindVersion      EntryKind = iota // Format version
	KindPrelude                       // Prelude blob
	KindSymlink                       // Symbolic link
	KindHardlink                      // Hard link
	KindDevice                        // Device node
	KindSocket                        // Unix socket
	KindFifo                          // Named pipe
	KindFile                          // Regular file
	KindDirectory                     // Directory
	KindGoodbyeTable                  // End-of-directory marker
)

// Entry represents an item in a pxar archive.
type Entry struct {
	Path     string
	Metadata Metadata
	Kind     EntryKind

	// File-specific fields
	FileSize      uint64 // valid when Kind == KindFile
	FileOffset    uint64 // byte offset in archive (start of FILENAME header)
	PayloadOffset uint64 // byte offset in payload stream (0 if not split)
	ContentOffset uint64 // absolute byte offset where content begins (after PAYLOAD header for files, start of child entries for dirs)

	// Symlink/Hardlink target
	LinkTarget string

	// Device info
	DeviceInfo format.Device
}

// IsDir reports whether the entry is a directory.
func (e *Entry) IsDir() bool { return e.Kind == KindDirectory }

// IsSymlink reports whether the entry is a symbolic link.
func (e *Entry) IsSymlink() bool { return e.Kind == KindSymlink }

// IsRegularFile reports whether the entry is a regular file.
func (e *Entry) IsRegularFile() bool { return e.Kind == KindFile }

// IsHardlink reports whether the entry is a hard link.
func (e *Entry) IsHardlink() bool { return e.Kind == KindHardlink }

// IsDevice reports whether the entry is a device node.
func (e *Entry) IsDevice() bool { return e.Kind == KindDevice }

// IsFifo reports whether the entry is a named pipe.
func (e *Entry) IsFifo() bool { return e.Kind == KindFifo }

// IsSocket reports whether the entry is a socket.
func (e *Entry) IsSocket() bool { return e.Kind == KindSocket }

// FileName returns just the file name portion of the entry's path.
func (e *Entry) FileName() string {
	return filepath.Base(e.Path)
}

// Metadata holds file metadata found in pxar archives.
type Metadata struct {
	Stat           format.Stat
	XAttrs         []format.XAttr
	ACL            ACL
	FCaps          []byte // file capability data
	QuotaProjectID *uint64
}

// FileType returns the file type portion of the mode.
func (m Metadata) FileType() uint64 { return m.Stat.FileType() }

// FileMode returns the permission bits of the mode.
func (m Metadata) FileMode() uint64 { return m.Stat.FileMode() }

// IsDir reports whether this metadata describes a directory.
func (m Metadata) IsDir() bool { return m.Stat.IsDir() }

// IsSymlink reports whether this metadata describes a symbolic link.
func (m Metadata) IsSymlink() bool { return m.Stat.IsSymlink() }

// IsRegularFile reports whether this metadata describes a regular file.
func (m Metadata) IsRegularFile() bool { return m.Stat.IsRegularFile() }

// IsDevice reports whether this metadata describes a device node.
func (m Metadata) IsDevice() bool { return m.Stat.IsDevice() }

// IsFIFO reports whether this metadata describes a FIFO.
func (m Metadata) IsFIFO() bool { return m.Stat.IsFIFO() }

// IsSocket reports whether this metadata describes a socket.
func (m Metadata) IsSocket() bool { return m.Stat.IsSocket() }

// MetadataEqual reports whether two Metadata entries are equivalent for
// metadata change detection. This compares stat fields, xattrs, ACLs,
// FCaps, and QuotaProjectID. File size is compared separately since
// it's not part of the pxar Stat format.
func (m Metadata) MetadataEqual(other Metadata) bool {
	if !m.Stat.MetadataEqual(other.Stat) {
		return false
	}
	if len(m.XAttrs) != len(other.XAttrs) {
		return false
	}
	for i := range m.XAttrs {
		if !xAttrEqual(m.XAttrs[i], other.XAttrs[i]) {
			return false
		}
	}
	if len(m.FCaps) != len(other.FCaps) {
		return false
	}
	for i := range m.FCaps {
		if m.FCaps[i] != other.FCaps[i] {
			return false
		}
	}
	if !aclEqual(m.ACL, other.ACL) {
		return false
	}
	if m.QuotaProjectID != nil && other.QuotaProjectID != nil {
		if *m.QuotaProjectID != *other.QuotaProjectID {
			return false
		}
	} else if m.QuotaProjectID != nil || other.QuotaProjectID != nil {
		return false
	}
	return true
}

func xAttrEqual(a, b format.XAttr) bool {
	return a.NameLen == b.NameLen && string(a.Data) == string(b.Data)
}

func aclEqual(a, b ACL) bool {
	if len(a.Users) != len(b.Users) {
		return false
	}
	if len(a.Groups) != len(b.Groups) {
		return false
	}
	for i := range a.Users {
		if a.Users[i] != b.Users[i] {
			return false
		}
	}
	for i := range a.Groups {
		if a.Groups[i] != b.Groups[i] {
			return false
		}
	}
	if a.GroupObj != nil && b.GroupObj != nil {
		if *a.GroupObj != *b.GroupObj {
			return false
		}
	} else if a.GroupObj != nil || b.GroupObj != nil {
		return false
	}
	if a.Default != nil && b.Default != nil {
		if *a.Default != *b.Default {
			return false
		}
	} else if a.Default != nil || b.Default != nil {
		return false
	}
	if len(a.DefaultUsers) != len(b.DefaultUsers) {
		return false
	}
	for i := range a.DefaultUsers {
		if a.DefaultUsers[i] != b.DefaultUsers[i] {
			return false
		}
	}
	if len(a.DefaultGroups) != len(b.DefaultGroups) {
		return false
	}
	for i := range a.DefaultGroups {
		if a.DefaultGroups[i] != b.DefaultGroups[i] {
			return false
		}
	}
	return true
}

// ACL holds access control list entries.
type ACL struct {
	Users         []format.ACLUser
	Groups        []format.ACLGroup
	GroupObj      *format.ACLGroupObject
	Default       *format.ACLDefault
	DefaultUsers  []format.ACLUser
	DefaultGroups []format.ACLGroup
}

// IsEmpty reports whether the ACL has no entries.
func (a ACL) IsEmpty() bool {
	return len(a.Users) == 0 &&
		len(a.Groups) == 0 &&
		a.GroupObj == nil &&
		a.Default == nil &&
		len(a.DefaultUsers) == 0 &&
		len(a.DefaultGroups) == 0
}

// MetadataBuilder constructs Metadata using a fluent API.
type MetadataBuilder struct {
	metadata Metadata
}

// NewMetadataBuilder creates a builder with the given type+mode.
func NewMetadataBuilder(mode uint64) *MetadataBuilder {
	return &MetadataBuilder{
		metadata: Metadata{
			Stat: format.Stat{Mode: mode},
		},
	}
}

// FileMetadata creates a builder for a regular file.
func FileMetadata(mode uint64) *MetadataBuilder {
	return NewMetadataBuilder(format.ModeIFREG | (mode & ^format.ModeIFMT))
}

// DirMetadata creates a builder for a directory.
func DirMetadata(mode uint64) *MetadataBuilder {
	return NewMetadataBuilder(format.ModeIFDIR | (mode & ^format.ModeIFMT))
}

// SymlinkMetadata creates a builder for a symlink.
func SymlinkMetadata(mode uint64) *MetadataBuilder {
	return NewMetadataBuilder(format.ModeIFLNK | (mode & ^format.ModeIFMT))
}

// DeviceMetadata creates a builder for a device.
func DeviceMetadata(mode uint64) *MetadataBuilder {
	return NewMetadataBuilder(format.ModeIFCHR | (mode & ^format.ModeIFMT))
}

// FIFOMetadata creates a builder for a FIFO.
func FIFOMetadata(mode uint64) *MetadataBuilder {
	return NewMetadataBuilder(format.ModeIFIFO | (mode & ^format.ModeIFMT))
}

// SocketMetadata creates a builder for a socket.
func SocketMetadata(mode uint64) *MetadataBuilder {
	return NewMetadataBuilder(format.ModeIFSOCK | (mode & ^format.ModeIFMT))
}

// StMode sets the complete mode (type + permissions).
func (b *MetadataBuilder) StMode(mode uint64) *MetadataBuilder {
	b.metadata.Stat.Mode = mode
	return b
}

// FileMode sets just the permission bits.
func (b *MetadataBuilder) FileMode(mode uint64) *MetadataBuilder {
	b.metadata.Stat.Mode = (b.metadata.Stat.Mode & format.ModeIFMT) | (mode & ^format.ModeIFMT)
	return b
}

// UID sets the user ID.
func (b *MetadataBuilder) UID(uid uint32) *MetadataBuilder {
	b.metadata.Stat.UID = uid
	return b
}

// GID sets the group ID.
func (b *MetadataBuilder) GID(gid uint32) *MetadataBuilder {
	b.metadata.Stat.GID = gid
	return b
}

// Owner sets both UID and GID.
func (b *MetadataBuilder) Owner(uid, gid uint32) *MetadataBuilder {
	b.metadata.Stat.UID = uid
	b.metadata.Stat.GID = gid
	return b
}

// Mtime sets the modification time.
func (b *MetadataBuilder) Mtime(ts format.StatxTimestamp) *MetadataBuilder {
	b.metadata.Stat.Mtime = ts
	return b
}

// XAttr adds an extended attribute.
func (b *MetadataBuilder) XAttr(name, value []byte) *MetadataBuilder {
	b.metadata.XAttrs = append(b.metadata.XAttrs, format.NewXAttr(name, value))
	return b
}

// FCaps sets the file capabilities.
func (b *MetadataBuilder) FCaps(data []byte) *MetadataBuilder {
	b.metadata.FCaps = data
	return b
}

// QuotaProjectID sets the quota project ID.
func (b *MetadataBuilder) QuotaProjectID(id uint64) *MetadataBuilder {
	b.metadata.QuotaProjectID = &id
	return b
}

// Build returns the constructed Metadata.
func (b *MetadataBuilder) Build() Metadata {
	return b.metadata
}

// CheckPathComponent validates that a path consists of a single normal component.
func CheckPathComponent(path string) bool {
	path = filepath.Clean(path)
	if path == "." || path == ".." || strings.Contains(path, "/") || strings.Contains(path, "\\") {
		return false
	}
	return true
}
