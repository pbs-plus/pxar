// Package transfer provides utilities for transferring files between pxar archives.
//
// Transfer implements archive-to-archive copy, directory tree walking, catalog
// extraction, and streaming read/write adapters for PBS remote stores, local
// chunk stores, and raw io.Reader/io.Writer streams.
//
// # Readers
//
// FileArchiveReader wraps accessor.Accessor for local .pxar files.
// ChunkedArchiveReader supports lazy on-demand chunk loading from .didx indexes.
// SplitArchiveReader handles v2 split archives (.mpxar + .ppxar).
// PBSArchiveReader connects to Proxmox Backup Server via the reader protocol.
//
// # Writers
//
// StreamArchiveWriter encodes to io.Writer streams (v1 and v2).
// ChunkedArchiveWriter chunks and stores via datastore.ChunkStore.
// SessionArchiveWriter uploads through a backupproxy.BackupSession.
//
// # Walks
//
// WalkTree visits every entry in a directory tree with optional content
// reading. WalkTreeMeta performs metadata-only traversal. WalkFilter
// selects which entry types to visit.
package transfer
