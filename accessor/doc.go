// Package accessor provides random access to pxar archives.
//
// Accessor enables O(log n) filename lookups via goodbye table binary search
// trees (SipHash24 hashed, BST-ordered). It supports both unified (v1) and
// split (v2) formats, lazy metadata decoding (Minimal mode), hardlink
// following, and streaming file content reads.
//
// # Usage
//
//	acc := accessor.NewAccessor(reader)
//	root, _ := acc.ReadRoot()
//	entry, _ := acc.Lookup("/etc/hostname")
//	rc, _ := acc.ReadFileContentReader(entry)
//	defer rc.Close()
//	content, _ := io.ReadAll(rc)
//
// For split archives (v2), provide a payload io.ReadSeeker as the second
// argument to NewAccessor. File content is then read from the payload stream
// when PayloadOffset > 0.
//
// # Hardlink Following
//
// FollowHardlink resolves a hardlink entry to its target file entry by computing
// filenameHeaderOffset - linkOffset from the wire format, then re-reading the full
// entry at that position. This mirrors Rust's Accessor::follow_hardlink:
//
//	link, _ := acc.Lookup("/bin/bunzip2")
//	target, _ := acc.FollowHardlink(link)
//	rc, _ := acc.ReadFileContentReader(target)
//
// # Minimal Mode
//
// ListOption{Minimal: true} skips decoding xattrs, ACLs, fcaps, and other
// extended metadata. Only stat basics (mode, uid, gid, times) are populated.
// This significantly reduces per-entry decode cost for index and browse workloads.
package accessor
