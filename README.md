# pxar — Proxmox Archive Format for Go

A pure Go library implementing the Proxmox Backup Archive (pxar) format for efficient filesystem backup, storage, and restoration. The library provides end-to-end support for encoding, decoding, random access, content-defined chunking, and both local and remote backup storage.

## Overview

The pxar format stores full filesystem trees — files, directories, symlinks, devices, sockets, FIFOs — with POSIX metadata including extended attributes, ACLs, and file capabilities. Archives support random access via goodbye tables (binary search trees over SipHash24 filename hashes).

This library is organized into focused packages:

| Package | Description |
|---------|-------------|
| `pxar` | Core types: Entry, Metadata, MetadataBuilder |
| `format` | Binary format constants, headers, serialization |
| `encoder` | Streaming archive writer |
| `decoder` | Streaming archive reader |
| `accessor` | Random-access archive reader (seek-based) |
| `transfer` | Copy/move files between archives across formats |
| `buzhash` | Content-defined chunking via buzhash rolling hash |
| `datastore` | Chunk storage, blob encoding, index files, backup catalogs |
| `binarytree` | Binary search tree permutation for goodbye tables |
| `fusefs` | Read-only FUSE filesystem over pxar archives |
| `backupproxy` | Pull-mode backup architecture with pluggable transport |

## Installation

```bash
go get github.com/pbs-plus/pxar
```

Requires Go 1.21 or later.

## Quick Start

### Encoding an Archive

```go
package main

import (
    "os"
    pxar "github.com/pbs-plus/pxar"
    "github.com/pbs-plus/pxar/encoder"
)

func main() {
    f, _ := os.Create("backup.pxar")
    defer f.Close()

    rootMeta := pxar.DirMetadata(0o755).Owner(0, 0).Build()
    enc := encoder.NewEncoder(f, nil, &rootMeta, nil)

    // Add a file
    fileMeta := pxar.FileMetadata(0o644).Owner(1000, 1000).Build()
    enc.AddFile(&fileMeta, "hello.txt", []byte("hello world"))

    // Add a directory with a nested file
    subMeta := pxar.DirMetadata(0o755).Owner(1000, 1000).Build()
    enc.CreateDirectory("subdir", &subMeta)
    nestedMeta := pxar.FileMetadata(0o600).Owner(1000, 1000).Build()
    enc.AddFile(&nestedMeta, "secret.txt", []byte("data"))
    enc.Finish() // close subdir

    // Add a symlink
    linkMeta := pxar.SymlinkMetadata(0o777).Build()
    enc.AddSymlink(&linkMeta, "link", "hello.txt")

    enc.Close()
}
```

### Decoding an Archive

```go
package main

import (
    "fmt"
    "os"
    pxar "github.com/pbs-plus/pxar"
    "github.com/pbs-plus/pxar/decoder"
)

func main() {
    f, _ := os.Open("backup.pxar")
    defer f.Close()

    dec := decoder.NewDecoder(f, nil)
    for {
        entry, err := dec.Next()
        if err != nil { // io.EOF when done
            break
        }

        switch entry.Kind {
        case pxar.KindFile:
            fmt.Printf("file: %s (%d bytes)\n", entry.FileName(), entry.FileSize)
        case pxar.KindDirectory:
            fmt.Printf("dir:  %s\n", entry.FileName())
        case pxar.KindSymlink:
            fmt.Printf("link: %s -> %s\n", entry.FileName(), entry.LinkTarget)
        }

        // Read file content inline
        if entry.Kind == pxar.KindFile {
            content, _ := io.ReadAll(dec.Contents())
            _ = content
        }
    }
}
```

### Random Access

The accessor package provides seek-based random access to archives, enabling O(log n) filename lookups without scanning the entire archive:

```go
package main

import (
    "os"
    "github.com/pbs-plus/pxar/accessor"
)

func main() {
    f, _ := os.Open("backup.pxar")
    defer f.Close()

    acc := accessor.NewAccessor(f) // accepts io.ReadSeeker

    // Get root entry
    root, _ := acc.ReadRoot()

    // List directory entries
    entries, _ := acc.ListDirectory(int64(root.ContentOffset))

    // Look up a file by path
    entry, _ := acc.Lookup("subdir/secret.txt")

    // Read file content
    content, _ := acc.ReadFileContent(entry)
}
```

### Transferring Files Between Archives

The transfer package provides unified read/write interfaces for copying files between archives, regardless of format (v1, v2 split, chunked .didx, or PBS remote):

```go
package main

import (
    "bytes"
    pxar "github.com/pbs-plus/pxar"
    "github.com/pbs-plus/pxar/format"
    "github.com/pbs-plus/pxar/transfer"
)

func main() {
    // Open source archive (any format: .pxar, .pxar.didx, .mpxar.didx+.ppxar.didx, PBS)
    src := transfer.NewFileArchiveReader(sourceFile)
    defer src.Close()

    // Create target archive
    var dstBuf bytes.Buffer
    dst := transfer.NewStreamArchiveWriter(&dstBuf)
    rootMeta := pxar.DirMetadata(0o755).Build()
    dst.Begin(&rootMeta, transfer.WriterOptions{Format: format.FormatVersion1})

    // Copy specific files with path mapping
    transfer.Copy(src, dst, []transfer.PathMapping{
        {Src: "/etc/hosts", Dst: "/etc/hosts"},
        {Src: "/var/log/syslog", Dst: "/var/log/syslog"},
    }, transfer.TransferOption{})

    // Copy an entire directory tree (src path → dst path)
    transfer.CopyTree(src, dst, "/etc", "/etc", transfer.TransferOption{})

    // Copy entire source into target
    transfer.CopyTree(src, dst, "/", "/", transfer.TransferOption{})

    dst.Finish()
}
```

#### Building a New PBS Snapshot from Multiple Existing Snapshots

A common use case is creating a new backup snapshot that assembles files from several previous snapshots on the same datastore. This avoids re-uploading chunks that already exist:

```go
package main

import (
    "context"

    pxar "github.com/pbs-plus/pxar"
    "github.com/pbs-plus/pxar/backupproxy"
    "github.com/pbs-plus/pxar/buzhash"
    "github.com/pbs-plus/pxar/format"
    "github.com/pbs-plus/pxar/transfer"
)

func main() {
    ctx := context.Background()

    pbsCfg := backupproxy.PBSConfig{
        BaseURL:       "https://pbs:8007/api2/json",
        Datastore:     "backup",
        AuthToken:     "TOKENID:SECRET",
        SkipTLSVerify: true,
    }

    // Open PBS remote store
    pbsStore := backupproxy.NewPBSRemoteStore(pbsCfg, buzhash.DefaultConfig(), true)

    // Start a new backup session
    session, _ := pbsStore.StartSession(ctx, backupproxy.BackupConfig{
        BackupType: backupproxy.BackupHost,
        BackupID:   "myhost",
    })

    // Open multiple existing snapshots as sources
    snap1, _ := transfer.NewPBSArchiveReader(ctx, transfer.PBSArchiveConfig{
        Config:      pbsCfg,
        BackupType:  "host",
        BackupID:    "myhost",
        BackupTime:  1700000000,
        ArchiveName: "root.pxar.didx",
    })
    defer snap1.Close()

    snap2, _ := transfer.NewPBSArchiveReader(ctx, transfer.PBSArchiveConfig{
        Config:      pbsCfg,
        BackupType:  "host",
        BackupID:    "myhost",
        BackupTime:  1700100000,
        ArchiveName: "root.pxar.didx",
    })
    defer snap2.Close()

    // Write a new v2 split archive into the session
    dst := transfer.NewSplitSessionArchiveWriter(ctx, session, "root.mpxar.didx", "root.ppxar.didx")
    rootMeta := pxar.DirMetadata(0o755).Build()
    dst.Begin(&rootMeta, transfer.WriterOptions{Format: format.FormatVersion2})

    // Copy /etc from snapshot 1 and /var from snapshot 2
    transfer.Copy(snap1, dst, []transfer.PathMapping{
        {Src: "/etc", Dst: "/etc"},
    }, transfer.TransferOption{})
    transfer.Copy(snap2, dst, []transfer.PathMapping{
        {Src: "/var", Dst: "/var"},
    }, transfer.TransferOption{})

    dst.Finish()

    // Finalize the backup session
    manifest, _ := session.Finish(ctx)
    _ = manifest
}
```

For same-datastore transfers, use `DedupSplitArchiveWriter` with a local `ChunkStore` to avoid re-uploading payload chunks that already exist on disk.

For chunked archives, use `ChunkedArchiveReader` or `SplitArchiveReader` (both lazy by default). For encrypted archives, wrap the chunk source with `DecryptingChunkSource`.

#### Same-Datastore Dedup Transfer

When source and target are in the same chunk store (e.g., same PBS datastore), the `DedupSplitArchiveWriter` avoids re-uploading payload chunks that already exist on disk. This is the primary optimization for same-datastore transfers:

```go
// Source is a v2 split archive in the same store
payloadIdx, _ := datastore.ReadDynamicIndex(sourcePayloadIdxData)

writer := transfer.NewDedupSplitArchiveWriter(store, source, config, false, payloadIdx)
rootMeta := pxar.DirMetadata(0o755).Build()
writer.Begin(&rootMeta, transfer.WriterOptions{Format: format.FormatVersion2})

// Write entries — payload chunks with identical content are deduplicated
// by ChunkStore.InsertChunk (no-op for existing digests)
writer.WriteEntry(entry, content)

writer.Finish()

// Check dedup statistics
hits, total := writer.DedupStats()
fmt.Printf("%d/%d payload chunks reused\n", hits, total)
```

The package also provides utilities for working with source payload chunks without full stream reconstruction:

- **`MapFileToPayloadChunks`** — maps a file's payload range to the chunk digests that contain it
- **`ReadFileContentFromChunks`** — reads a file's content by loading only the necessary payload chunks via `RestoreRange`
- **`ComputeContentDigest`** — SHA-256 of a file's content without reconstructing the entire payload stream

#### Lazy Chunk Loading

`ChunkedReadSeeker` implements `io.ReadSeeker` over a chunked archive stream, loading and decoding chunks on demand instead of reconstructing the entire stream into memory. `ChunkedArchiveReader` and `SplitArchiveReader` use this by default — only chunks needed for `Lookup` and `ReadFileContent` calls are loaded. For small archives where full in-memory reconstruction is acceptable, use `NewChunkedArchiveReaderEager` and `NewSplitArchiveReaderEager`.

#### CLI Commands

The `pxar-cli` tool supports archive inspection and transfer:

```bash
# List entries in an archive
pxar-cli ls backup.pxar
pxar-cli ls backup.pxar /subdir

# Extract a file
pxar-cli extract backup.pxar /hello.txt -o hello.txt

# Copy files from one archive to a new archive
pxar-cli cp backup.pxar /hello.txt -o new.pxar

# Copy with destination path remapping
pxar-cli cp backup.pxar /etc/hosts /backup/hosts -o new.pxar

# Merge an entire archive into a new archive
pxar-cli merge backup.pxar -o merged.pxar
```

### Content-Defined Chunking

The buzhash package splits data streams into variable-size chunks based on content, enabling deduplication of unchanged regions:

```go
package main

import (
    "crypto/sha256"
    "github.com/pbs-plus/pxar/buzhash"
)

func main() {
    cfg, _ := buzhash.NewConfig(4096) // ~4 KiB average chunks

    chunker := buzhash.NewChunker(reader, cfg)
    for {
        chunk, err := chunker.Next()
        if err != nil { // io.EOF when done
            break
        }

        digest := sha256.Sum256(chunk)
        // store chunk indexed by digest
    }
}
```

### Chunk Storage and Indexes

The datastore package manages chunk storage, blob encoding/decoding, and index files:

```go
package main

import (
    "crypto/sha256"
    "time"
    "github.com/pbs-plus/pxar/buzhash"
    "github.com/pbs-plus/pxar/datastore"
)

func main() {
    // Create a chunk store
    store, _ := datastore.NewChunkStore("/backup/dataset")

    // Encode a chunk as a blob
    blob, _ := datastore.EncodeBlob(chunkData)
    digest := sha256.Sum256(chunkData)
    store.InsertChunk(digest, blob.Bytes())

    // Build a dynamic index
    idx := datastore.NewDynamicIndexWriter(time.Now().Unix())
    idx.Add(offset, digest)
    indexData, _ := idx.Finish()

    // Read an index back
    reader, _ := datastore.ReadDynamicIndex(indexData)
    for i := 0; i < reader.Count(); i++ {
        info, _ := reader.ChunkInfo(i)
        // info.Start, info.End, info.Digest
    }

    // Manifest for backup snapshots
    manifest := &datastore.Manifest{
        BackupType: datastore.BackupHost.String(),
        BackupID:   "myhost",
        BackupTime: time.Now().Unix(),
        Files:      []datastore.FileInfo{
            {Filename: "root.pxar.didx", Size: 4096, CSum: "abc123"},
        },
    }
    data, _ := manifest.Marshal()
}
```

### Pull-Mode Backup (backupproxy)

The backupproxy package converts Proxmox's push-based backup protocol into a pull configuration. The server (on the PBS machine) orchestrates backups by walking the client's filesystem and uploading to storage. The client only serves raw filesystem data.

#### Detection Modes

The server supports three detection modes controlling how archives are created and whether unchanged files are re-read:

| Mode | Format | Description |
|------|--------|-------------|
| `DetectionLegacy` | v1 single `.pxar` | All file data encoded into one stream. No previous backup needed. |
| `DetectionData` | v2 split `.mpxar` + `.ppxar` | Metadata and payload in separate streams. All file data re-read. |
| `DetectionMetadata` | v2 split `.mpxar` + `.ppxar` | Compares current file metadata (mtime, size, uid, gid, mode, xattrs, ACLs, fcaps) against a previous backup's catalog. Unchanged files reuse payload chunks from the previous snapshot. |

```go
// Legacy mode (single archive)
result, err := srv.RunBackupWithMode(ctx, "/root", backupproxy.BackupConfig{
    BackupType:    datastore.BackupHost,
    BackupID:      "myhost",
    DetectionMode: backupproxy.DetectionLegacy,
})

// Data mode (split archive, all data re-read)
result, err := srv.RunBackupWithMode(ctx, "/root", backupproxy.BackupConfig{
    BackupType:    datastore.BackupHost,
    BackupID:      "myhost",
    DetectionMode: backupproxy.DetectionData,
})

// Metadata mode (incremental, reuses unchanged payload)
result, err := srv.RunBackupWithMode(ctx, "/root", backupproxy.BackupConfig{
    BackupType:    datastore.BackupHost,
    BackupID:      "myhost",
    DetectionMode: backupproxy.DetectionMetadata,
    PreviousBackup: &backupproxy.PreviousBackupRef{
        BackupType: datastore.BackupHost,
        BackupID:   "myhost",
        BackupTime: 1700000000,
        Namespace:  "",
    },
})
```

For local store, set `PreviousBackup.Dir` to the directory containing previous snapshot indexes. For PBS, leave `Dir` empty and the store will download them via `PBSRemoteStore`.

#### Encryption and Signing

The library supports three crypt modes:

| Mode | Description |
|------|-------------|
| `CryptModeNone` | No encryption or signing (default) |
| `CryptModeEncrypt` | AES-256-GCM encryption of chunk data; HMAC-SHA256 manifest signing |
| `CryptModeSignOnly` | No encryption, but HMAC-SHA256 manifest signing for integrity verification |

Encryption uses PBKDF2-HMAC-SHA256 for key derivation and AES-256-GCM (12-byte nonce, empty AAD) for chunk encryption. Manifests are always signed when a `CryptConfig` is provided — they are never encrypted, since PBS must be able to read the manifest. Chunk digests in encrypted mode use `SHA-256(data || id_key)` to prevent cross-key collisions. Key files can be generated with `pxar-cli keygen` and loaded at backup time.

#### Backup Catalogs

All backup modes automatically generate and upload a `catalog.pcat1.didx` file alongside the archive. This catalog enables PBS's web UI and `proxmox-backup-client catalog` commands to browse backup contents without downloading the entire archive. The catalog includes file names, types, sizes, and modification times for every entry in the backup.

#### Extended Attributes and ACLs

The `FileSystemAccessor` interface includes `GetXAttrs`, `GetACL`, and `GetFCaps` methods for collecting extended attributes, POSIX ACLs, and file capabilities. The `osFS` implementation in `cmd/pxar-cli` reads real xattrs and ACLs from the filesystem using `unix.Llistxattr`/`unix.Lgetxattr`. Metadata change detection in `DetectionMetadata` mode compares all extended metadata fields, ensuring xattr/ACL changes trigger re-upload.

#### Basic Usage

```go
package main

import (
    "context"
    "github.com/pbs-plus/pxar/backupproxy"
    "github.com/pbs-plus/pxar/buzhash"
    "github.com/pbs-plus/pxar/datastore"
)

func main() {
    // --- Client side (backed-up machine) ---
    // Implement FileSystemAccessor for local filesystem access,
    // then wrap with LocalClient:
    //
    //   client := backupproxy.NewLocalClient(myFSAccessor)
    //
    // Or implement ClientProvider directly over your transport (gRPC, SSH, etc.)

    // --- Server side (PBS machine) ---
    // Use LocalStore for testing/offline or PBSRemoteStore for PBS:

    chunkCfg, _ := buzhash.NewConfig(4096)

    // Local storage (testing)
    store, _ := backupproxy.NewLocalStore("/tmp/backup", chunkCfg, false)

    // Or PBS remote storage (connects via H2 backup protocol)
    // store := backupproxy.NewPBSRemoteStore(backupproxy.PBSConfig{
    //     BaseURL:       "https://pbs:8007/api2/json",
    //     Datastore:     "my-datastore",
    //     AuthToken:     "TOKENID:SECRET",
    //     SkipTLSVerify: true,
    // }, chunkCfg, true)

    srv := backupproxy.NewServer(client, store)

    result, err := srv.RunBackup(context.Background(), "/", backupproxy.BackupConfig{
        BackupType: datastore.BackupHost,
        BackupID:   "myhost",
        BackupTime: 1700000000,
    })
    if err != nil {
        panic(err)
    }

    fmt.Printf("Backed up %d files, %d dirs, %d bytes in %s\n",
        result.FileCount, result.DirCount, result.TotalBytes, result.Duration)
}
```

#### Pluggable Transport

The `ClientProvider` interface defines what the server calls to access client data. Implement it over any transport:

```go
type MyTransportClient struct { /* gRPC/SSH/HTTP connection */ }

func (c *MyTransportClient) Stat(ctx context.Context, path string) (format.Stat, error) {
    // RPC call to client
}

func (c *MyTransportClient) ReadDir(ctx context.Context, path string) ([]backupproxy.DirEntry, error) {
    // RPC call to client
}

func (c *MyTransportClient) ReadFile(ctx context.Context, path string, offset, length int64) ([]byte, error) {
    // RPC call to client
}

func (c *MyTransportClient) ReadLink(ctx context.Context, path string) (string, error) {
    // RPC call to client
}
```

On the client side, `FileSystemAccessor` provides the same methods without context (local filesystem), and `LocalClient` adapts it to `ClientProvider`.

#### Pluggable Storage Backend

Implement `RemoteStore` and `BackupSession` to support custom storage backends:

```go
type MyStore struct{}

func (s *MyStore) StartSession(ctx context.Context, cfg backupproxy.BackupConfig) (backupproxy.BackupSession, error) {
    // Initialize upload session
}

type MySession struct{}

func (s *MySession) UploadArchive(ctx context.Context, name string, data io.Reader) (*backupproxy.UploadResult, error) {
    // Chunk, store, and index the archive data
}

func (s *MySession) UploadBlob(ctx context.Context, name string, data []byte) error {
    // Store a blob (config, logs)
}

func (s *MySession) Finish(ctx context.Context) (*datastore.Manifest, error) {
    // Finalize the backup and return manifest
}
```

### FUSE Filesystem

Mount a pxar archive as a read-only filesystem:

```go
package main

import (
    "os"
    "github.com/pbs-plus/pxar/fusefs"
)

func main() {
    f, _ := os.Open("backup.pxar")
    fi, _ := f.Stat()

    sess, _ := fusefs.NewSession(f, fi.Size())
    defer sess.Close()

    // Use sess as a fusefs.FileSystem:
    //   sess.Lookup, sess.Getattr, sess.Readdir,
    //   sess.Read, sess.Readlink, sess.ListXAttr, etc.
    //
    // Wrap with a go-fuse bridge to mount with FUSE.
}
```

## Package Reference

### `pxar` — Core Types

- `Entry` — A typed archive entry (file, directory, symlink, etc.) with metadata and optional content
- `Metadata` — POSIX metadata: stat, xattrs, ACLs, fcaps, quota project ID
- `MetadataBuilder` — Fluent builder for Metadata with type-specific constructors (`FileMetadata`, `DirMetadata`, `SymlinkMetadata`, etc.)
- `EntryKind` — Entry type constants (`KindFile`, `KindDirectory`, `KindSymlink`, etc.)

### `format` — Binary Format

- `Header` — Typed size-prefixed header for each pxar item
- `Stat` — 40-byte POSIX stat structure (mode, flags, uid, gid, mtime)
- `Device` — Device major/minor numbers
- `XAttr` — Extended attribute (name + value)
- Mode constants (`ModeIFREG`, `ModeIFDIR`, `ModeIFLNK`, etc.)
- Serialization functions (`MarshalStatBytes`, `ReadHeader`, `WriteHeader`, etc.)
- `HashFilename` — SipHash24 filename hashing for goodbye tables

### `encoder` — Archive Writer

- `NewEncoder(output, payloadOut, metadata, prelude)` — Create a streaming encoder
- `AddFile` / `CreateFile` — Write file entries (inline or streaming)
- `AddSymlink` / `AddHardlink` — Write link entries
- `AddDevice` / `AddFIFO` / `AddSocket` — Write special entries
- `CreateDirectory` / `Finish` — Open/close directory scope
- `Close` — Finalize the archive

### `decoder` — Archive Reader

- `NewDecoder(input, payloadReader)` — Create a streaming decoder
- `Next()` — Advance to the next entry, returns `*pxar.Entry`
- `Contents()` — Get an `io.Reader` for the current file's content

### `accessor` — Random Access

- `NewAccessor(reader)` — Create from an `io.ReadSeeker`
- `ReadRoot()` — Get the root directory entry
- `ListDirectory(offset)` — List entries in a directory
- `Lookup(path)` — O(log n) path lookup via goodbye tables
- `ReadFileContent(entry)` — Read a file's content

### `transfer` — File Transfer Between Archives

- `ArchiveReader` — Unified read interface (ReadRoot, Lookup, ListDirectory, ReadFileContent)
- `ArchiveWriter` — Unified write interface (Begin, WriteEntry, BeginDirectory, EndDirectory, Finish)
- `FileArchiveReader` — Reads from standalone .pxar files
- `ChunkedArchiveReader` — Reads from chunked .pxar.didx archives
- `SplitArchiveReader` — Reads from split .mpxar.didx + .ppxar.didx archives
- `PBSArchiveReader` — Reads from PBS remote stores via H2 reader protocol
- `StreamArchiveWriter` — Writes to v1 or v2 io.Writer streams
- `ChunkedArchiveWriter` — Writes to local ChunkStore producing .didx index
- `SessionArchiveWriter` / `SplitSessionArchiveWriter` — Uploads via BackupSession
- `DecryptingChunkSource` — Decrypts encrypted chunks on the fly
- `Copy` / `CopyTree` / `Merge` — Transfer functions connecting readers to writers
- `WalkTree` — Recursive directory walker with ErrSkipDir support
- `TransferOption` — Configuration for encryption, format, overwrite, progress

### `buzhash` — Content-Defined Chunking

- `NewConfig(avgSize)` — Create chunking configuration
- `NewChunker(reader, config)` — Create a chunker
- `Next()` — Get the next chunk (variable size)
- `Hasher` — Low-level rolling hash

### `datastore` — Chunk Storage and Indexes

- `ChunkStore` — Local filesystem chunk storage keyed by SHA-256 digest
- `DataBlob` — Chunk envelope with magic + CRC32 (uncompressed or zstd)
- `DynamicIndexWriter` / `DynamicIndexReader` — Variable-size chunk index (.didx)
- `FixedIndexWriter` / `FixedIndexReader` — Fixed-size chunk index (.fidx)
- `StoreChunker` — Pipeline: buzhash → SHA-256 → blob encode → store → index
- `Manifest` / `FileInfo` — Backup snapshot manifest (JSON)
- `BackupGroup` / `BackupDir` / `BackupInfo` — Backup namespace hierarchy

### `binarytree` — BST Permutation

- `Copy(n, copyFunc)` — Permute a sorted array into BST order
- `SearchBy(tree, start, skip, compare)` — Binary search on a BST-ordered array

### `fusefs` — FUSE Filesystem

- `Session` — Read-only filesystem session over a pxar archive
- `FileSystem` — Interface compatible with hanwen/go-fuse
- `Attr`, `DirEntry`, `DirEntryIndex` — FUSE-compatible types
- `Node` — Inode tracking with reference counting

### `backupproxy` — Pull-Mode Backup

- `Server` — Backup orchestrator (walk → encode → chunk → upload)
- `ClientProvider` — Interface for accessing client filesystem data
- `FileSystemAccessor` — Client-side local filesystem access
- `LocalClient` — Adapts FileSystemAccessor to ClientProvider
- `RemoteStore` / `BackupSession` — Storage backend interfaces
- `LocalStore` — Local filesystem storage backend
- `PBSRemoteStore` — PBS H2 backup protocol storage backend
- `PBSConfig` — PBS connection configuration (URL, datastore, auth)
- `PBSReader` — PBS backup reader protocol client for restore
- `BackupConfig` — Backup configuration including DetectionMode and PreviousBackup
- `BackupResult` — Backup outcome (file count, dir count, bytes, duration)
- `UploadResult` — Single archive upload result (filename, size, digest)
- `DetectionMode` — Detection mode constants: `DetectionLegacy`, `DetectionData`, `DetectionMetadata`
- `PreviousBackupRef` — Reference to a previous snapshot for metadata mode
- `DirEntry` — Directory entry with Stat and Size for metadata comparison
- `PreviousSnapshotSource` — Interface for reading previous backup catalogs and chunks

## Architecture

```
Backup Data Flow:

  Client (backed-up machine)           Server (PBS machine)
  =========================          =====================

  FileSystemAccessor                  Server{client, store}
        │                                    │
        │◄──── ClientProvider ───────────────│
        │  Stat, ReadDir,                    │
        │  ReadFile, ReadLink                │
        │────►                               │
                                     ┌────────▼─────────┐
                                     │ RunBackup()       │
                                     │                   │
                                     │ bytes.Buffer      │
                                     │  Writer → Encoder │
                                     │  Reader → Upload  │
                                    │                   │
                                    │ walkDir():        │
                                    │  dir:  Create → recurse → Finish
                                    │  file: ReadFile → AddFile
                                    │  link: AddSymlink │
                                    │                   │
                                    │ enc.Close()       │
                                    │ session.Finish()  │
                                    └────────┬─────────┘
                                             │
                                      RemoteStore
                                      ├── LocalStore (testing)
                                      └── PBSRemoteStore (PBS H2 Protocol)
```

## Disclaimer

**This library is not yet battle-tested.** It is under active development and should not be used in production environments. The API may change without notice, and there may be bugs or edge cases that have not been discovered. Use at your own risk.

## License

MIT License - see [LICENSE](LICENSE) file for details.
