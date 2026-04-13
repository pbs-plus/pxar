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
- `BackupConfig` / `BackupResult` / `UploadResult` — Configuration and result types

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
                                    │ io.Pipe()         │
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

## License

MIT License - see [LICENSE](LICENSE) file for details.
