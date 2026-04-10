// Package datastore provides chunk storage, indexing, and backup catalog
// management for pxar archives.
//
// The package implements the Proxmox Backup Server data model: backup data is
// split into chunks, each chunk is stored as a DataBlob (with optional zstd
// compression and CRC32 verification), and chunk references are tracked in
// dynamic or fixed index files.
//
// # Chunk Store
//
// ChunkStore manages chunk storage on the local filesystem. Each chunk is
// identified by its SHA-256 digest and stored under a .chunks directory:
//
//	store, err := datastore.NewChunkStore("/backup/datastore")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Store a chunk
//	digest := sha256.Sum256(data)
//	inserted, size, err := store.InsertChunk(digest, blobData)
//
//	// Load a chunk
//	blobData, err := store.LoadChunk(digest)
//
// # Data Blobs
//
// All chunk data is wrapped in a DataBlob envelope containing a magic number
// and CRC32 checksum:
//
//	blob, err := datastore.EncodeBlob(rawChunk)
//	encoded := blob.Bytes()
//
//	// Decode
//	decoded, err := datastore.DecodeBlob(encoded)
//
// Use EncodeCompressedBlob for zstd compression.
//
// # Index Files
//
// Dynamic indexes (.didx) map variable-size chunks (from buzhash chunking)
// to their digests and offsets:
//
//	writer := datastore.NewDynamicIndexWriter(time.Now().Unix())
//	writer.Add(offset, digest)
//	indexData, err := writer.Finish()
//
//	// Read back
//	reader, err := datastore.ReadDynamicIndex(indexData)
//	count := reader.Count()
//	info, ok := reader.ChunkInfo(0)
//
// Fixed indexes (.fidx) are used for fixed-size chunks (e.g., raw disk images).
//
// # Store Chunker
//
// StoreChunker wires together buzhash chunking, blob encoding, and chunk
// storage into a single pipeline:
//
//	sc := datastore.NewStoreChunker(store, chunkCfg, true) // true = compress
//	results, idxWriter, err := sc.ChunkStream(archiveReader)
//
// # Backup Catalog
//
// BackupType, BackupGroup, BackupDir, and BackupInfo model the PBS backup
// namespace hierarchy (type/id/timestamp). Manifest tracks all files in a
// backup snapshot:
//
//	manifest := &datastore.Manifest{
//	    BackupType: datastore.BackupHost.String(),
//	    BackupID:   "myhost",
//	    BackupTime: time.Now().Unix(),
//	    Files:      []datastore.FileInfo{...},
//	}
//	data, err := manifest.Marshal()
package datastore
