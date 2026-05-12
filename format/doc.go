// Package format defines the pxar binary format types and constants.
//
// All values are stored in little endian ordering. The archive contains a list
// of items. Each item starts with a 16-byte Header, followed by item data.
//
// # Headers
//
// Every item is prefixed by a Header (Type uint64 + Size uint64). The Type
// field identifies the item's role (ENTRY, FILENAME, PAYLOAD, etc.) and Size
// is the total item size including the header. Header.MarshalTo provides
// zero-copy serialization into a caller-provided buffer.
//
// # Stat
//
// Stat (40 bytes) carries POSIX stat information: mode, flags, uid, gid, and
// a high-precision mtime (StatxTimestamp with Secs/Nanos and 4-byte padding).
// The _pad field at bytes 36-39 is always 0, matching Rust's Endian trait.
//
// StatV1 (32 bytes) uses nanosecond-precision mtime and converts via ToStat().
//
// # Devices
//
// Device encodes major/minor numbers. ToDevT and DeviceFromDevT convert to/from
// Linux dev_t format (matching Rust's makedev/dev_major/dev_minor).
//
// # Goodbye Tables
//
// Directory entries end with a goodbye table — a binary search tree of
// GoodbyeItem entries sorted by filename hash (SipHash24). The BST layout
// enables O(log n) filename lookups without a separate index.
//
// # Payload References
//
// In v2 split archives, PayloadRef maps file content to byte offsets in a
// separate payload stream, decoupling metadata from content for efficient
// catalog access and chunk deduplication.
//
// # SipHash24
//
// HashFilename computes a SipHash-2-4 hash using the pxar fixed key,
// matching the Rust implementation for goodbye table lookups.
package format
