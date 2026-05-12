// Package datastore provides chunk storage, indexing, and backup catalog
// management for pxar archives.
//
// The package implements the Proxmox Backup Server data model: backup data is
// split into chunks, each chunk is stored as a DataBlob (with optional zstd
// compression, AES-256-GCM encryption, and CRC32 verification), and chunk
// references are tracked in dynamic or fixed index files.
//
// # Chunk Store
//
// ChunkStore manages chunk storage on the local filesystem. Each chunk is
// identified by its SHA-256 digest and stored under a .chunks directory:
//
//	store, _ := datastore.NewChunkStore("/backup/datastore")
//	inserted, size, _ := store.InsertChunk(digest, blobData)
//	blobData, _ := store.LoadChunk(digest)
//
// # Data Blobs
//
// All chunk data is wrapped in a DataBlob envelope containing a magic number
// and CRC32 checksum:
//
//	blob, _ := datastore.EncodeBlob(rawChunk)
//	decoded, _ := datastore.DecodeBlob(blob.Bytes())
//
// Use EncodeCompressedBlob for zstd compression, EncodeEncryptedBlob for
// AES-256-GCM encryption.
//
// # Index Files
//
// Dynamic indexes (.didx) map variable-size chunks (from buzhash chunking)
// to their digests and offsets:
//
//	writer := datastore.NewDynamicIndexWriter(time.Now().Unix())
//	writer.Add(offset, digest)
//	indexData, _ := writer.Finish()
//
//	reader, _ := datastore.ParseDynamicIndex(indexData)
//	info, _ := reader.ChunkInfo(0) // info.Start, info.End, info.Digest
//
// Fixed indexes (.fidx) are used for fixed-size chunks (e.g., raw disk images).
//
// # Restoration
//
// Restorer reconstructs files from chunk indexes using a ChunkSource:
//
//	restorer := datastore.NewRestorer(chunkSource)
//	restorer.RestoreFile(idx, writer)
//	restorer.RestoreRange(idx, offset, length, writer)
//
// # Backup Catalogs
//
// BuildCatalogFast performs parallel catalog extraction from a DIDX.
// BuildDirIndex builds a directory index from goodbye table entries.
// OnDemandCatalog provides lazy catalog loading from chunked data.
//
// BackupType, BackupGroup, BackupDir, and BackupInfo model the PBS backup
// namespace hierarchy (type/id/timestamp). Manifest tracks all files in a
// backup snapshot.
package datastore
