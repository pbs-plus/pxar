// Package accessor provides random access to pxar archives.
//
// Accessor enables O(log n) filename lookups via goodbye table binary search
// trees. It supports both unified (v1) and split (v2) formats, lazy metadata
// decoding (Minimal mode), and batch lookups for efficient prefix sharing.
//
//	# Usage
//
//	acc := accessor.NewAccessor(reader)
//	root, _ := acc.ReadRoot()
//	entry, _ := acc.Lookup("/etc/hostname")
//	err := acc.ListDirectory(int64(root.ContentOffset), accessor.ListOption{}, func(entry *pxar.Entry) error {
//		// process entry
//		return nil
//	})
//
// For split archives (v2), provide a payload io.ReadSeeker as the second
// argument to NewAccessor. File content is then read from the payload stream
// when PayloadOffset > 0.
package accessor
