// Package transfer provides utilities for transferring files between pxar archives.
//
// Transfer implements archive-to-archive copy, directory tree walking, catalog
// extraction, lazy chunk loading, dedup-aware writing, and streaming read/write
// adapters for PBS remote stores, local chunk stores, and raw io.Writer streams.
//
// # Readers
//
// All source formats implement ArchiveReader:
//   - FileReader: standalone .pxar files via io.ReadSeeker
//   - ChunkedReader: lazy on-demand chunk loading from .didx indexes
//   - SplitReader: v2 split archives (.mpxar.didx + .ppxar.didx)
//   - PBSReader: PBS remote stores via H2 reader protocol
//   - DecryptingReader: wraps any ArchiveReader for encrypted archives
//
// # Writers
//
// All target formats implement ArchiveWriter:
//   - StreamWriter: encodes to io.Writer (v1 or v2 split)
//   - DedupWriter: same-datastore dedup with chunk reuse
//   - RemoteDedupWriter: PBS remote dedup with chunk injection
//   - SessionWriter: uploads via BackupSession
//
// # Transfer Functions
//
// Copy copies specific paths between archives with optional path mapping.
// CopyTree copies entire directory trees.
//
// # Walk Functions
//
// WalkTree visits every entry with optional content reading.
// WalkTreeWith supports metadata-only mode, type filters, and skip counts.
// WalkTreeMetadata performs metadata-only traversal with a type filter.
//
// # Dedup Utilities
//
// RecordMax provides monotonic offset validation for dedup writers.
// MapFileToPayloadChunks maps file content to payload chunk ranges.
// ReadChunkedFile reads file content from specific chunks.
// ComputeContentDigest computes SHA-256 without full stream reconstruction.
//
// # Lazy Chunk Loading
//
// ReadSeeker implements io.ReadSeeker over chunked data with configurable
// chunk cache. DecryptSource wraps ChunkSource for encrypted chunks.
package transfer
