// Package format defines the pxar binary format types and constants.
//
// All values are stored in little endian ordering. The archive contains a list
// of items. Each item starts with a Header, followed by item data.
//
// # Headers
//
// Every item in a pxar archive is prefixed by a 16-byte Header (Type + Size).
// The Type field identifies the item's role (ENTRY, FILENAME, PAYLOAD, etc.)
// and Size is the total item size including the header itself.
//
// # Stat and Metadata
//
// Stat (40 bytes) carries POSIX stat information: mode, flags, uid, gid, and
// a high-precision mtime (StatxTimestamp). The older StatV1 (32 bytes) uses
// nanosecond-precision mtime and is supported for backward compatibility.
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
// catalog access.
package format
