# pxar — Proxmox Backup Archive Format for Go

A pure Go library implementing the Proxmox Backup Archive (pxar) format for efficient filesystem backup, storage, and restoration. Faithfully ported from the Rust reference implementation (`proxmox-pxar` + `proxmox-backup`) with identical wire format, SipHash24 hashing, BST goodbye tables, and content-defined chunking.

## Overview

The pxar format stores full filesystem trees — files, directories, symlinks, hardlinks, devices, sockets, FIFOs — with POSIX metadata including extended attributes, ACLs, and file capabilities. Archives support random access via goodbye tables (binary search trees over SipHash24 filename hashes).

This library is organized into focused packages:

| Package       | Description                                                |
| ------------- | ---------------------------------------------------------- |
| `pxar`        | Core types: Entry, Metadata, MetadataBuilder, ACL          |
| `format`      | Binary format constants, headers, serialization, SipHash24 |
| `encoder`     | Streaming archive writer (v1 unified and v2 split)         |
| `decoder`     | Streaming archive reader                                   |
| `accessor`    | Random-access archive reader with FollowHardlink           |
| `transfer`    | Copy/move files between archives across formats            |
| `buzhash`     | Content-defined chunking via buzhash rolling hash          |
| `datastore`   | Chunk storage, blob encoding, indexes, backup catalogs     |
| `binarytree`  | Binary search tree permutation for goodbye tables          |
| `fusefs`      | Read-only FUSE filesystem over pxar archives               |
| `vfs`         | Offset-based remote filesystem with RPC transport          |
| `backupproxy` | Pull-mode backup architecture with pluggable transport     |

## Installation

```bash
go get github.com/pbs-plus/pxar
```

Requires Go 1.26 or later.

## Quick Start

### Encoding an Archive

```go
package main

import (
    "os"
    pxar "github.com/pbs-plus/pxar"
    "github.com/pbs-plus/pxar/encoder"
    "github.com/pbs-plus/pxar/format"
)

func main() {
    f, _ := os.Create("backup.pxar")
    defer f.Close()

    ts := format.NewStatxTimestampFromDuration(1430487000 * time.Second)
    rootMeta := pxar.DirMetadata(0o755).Owner(0, 0).Mtime(ts).Build()
    enc := encoder.NewEncoder(f, nil, &rootMeta, nil)

    // Add a file (returns LinkOffset for hardlink targets)
    fileMeta := pxar.FileMetadata(0o644).Owner(1000, 1000).Mtime(ts).Build()
    offset, _ := enc.AddFile(&fileMeta, "hello.txt", []byte("hello world"))

    // Add a hardlink pointing to the file above
    enc.AddHardlink("link.txt", "hello.txt", offset)

    // Add a directory with a nested file
    subMeta := pxar.DirMetadata(0o755).Owner(1000, 1000).Mtime(ts).Build()
    enc.CreateDirectory("subdir", &subMeta)
    nestedMeta := pxar.FileMetadata(0o600).Owner(1000, 1000).Mtime(ts).Build()
    enc.AddFile(&nestedMeta, "secret.txt", []byte("data"))
    enc.Finish() // close subdir

    // Add a symlink
    linkMeta := pxar.SymlinkMetadata(0o777).Build()
    enc.AddSymlink(&linkMeta, "link", "hello.txt")

    // Add a device node
    devMeta := pxar.DeviceMetadata(0o666).Build()
    enc.AddDevice(&devMeta, "null", format.Device{Major: 1, Minor: 3})

    // Add special files
    fifoMeta := pxar.FIFOMetadata(0o666).Build()
    enc.AddFIFO(&fifoMeta, "myfifo")
    sockMeta := pxar.SocketMetadata(0o600).Build()
    enc.AddSocket(&sockMeta, "mysock")

    enc.Close()
}
```

#### Split Archives (v2)

For v2 split archives, metadata and payload are written to separate streams. This enables payload deduplication and efficient catalog access:

```go
var metaBuf, payloadBuf bytes.Buffer
enc := encoder.NewEncoder(&metaBuf, &payloadBuf, &rootMeta, nil)

// Regular files write content to the payload stream
enc.AddFile(&fileMeta, "data.bin", fileContent)

// PayloadRef references existing payload data without re-reading it
enc.AddPayloadRef(&fileMeta, "unchanged.dat", fileSize, payloadOffset)

// Track payload position for external chunk injection
pos := enc.PayloadPosition()
enc.Advance(virtualSize)

enc.Close()
```

#### Streaming Large Files

For files too large to buffer in memory, use `CreateFile` to obtain a `FileWriter`:

```go
fw, _ := enc.CreateFile(&fileMeta, "large.bin", fileSize)
io.Copy(fw, largeReader)
fw.Close()
```

### Decoding an Archive

```go
package main

import (
    "fmt"
    "io"
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
        if err == io.EOF {
            break
        }
        if err != nil {
            log.Fatal(err)
        }

        switch entry.Kind {
        case pxar.KindFile:
            fmt.Printf("file: %s (%d bytes)\n", entry.FileName(), entry.FileSize)
        case pxar.KindDirectory:
            fmt.Printf("dir:  %s\n", entry.FileName())
        case pxar.KindSymlink:
            fmt.Printf("symlink: %s -> %s\n", entry.FileName(), entry.LinkTarget)
        case pxar.KindHardlink:
            fmt.Printf("hardlink: %s -> %s\n", entry.FileName(), entry.LinkTarget)
        case pxar.KindDevice:
            fmt.Printf("device: %s (%d:%d)\n", entry.FileName(),
                entry.DeviceInfo.Major, entry.DeviceInfo.Minor)
        case pxar.KindFIFO:
            fmt.Printf("fifo: %s\n", entry.FileName())
        case pxar.KindSocket:
            fmt.Printf("socket: %s\n", entry.FileName())
        }

        // Stream file content
        if entry.Kind == pxar.KindFile && entry.FileSize > 0 {
            content, _ := io.ReadAll(dec.Contents())
            _ = content
        }
    }
}
```

### Random Access

The accessor package provides seek-based random access to archives, enabling O(log n) filename lookups via SipHash24 goodbye tables:

```go
package main

import (
    "fmt"
    "io"
    "os"

    pxar "github.com/pbs-plus/pxar"
    "github.com/pbs-plus/pxar/accessor"
)

func main() {
    f, _ := os.Open("backup.pxar")
    defer f.Close()

    acc := accessor.NewAccessor(f) // accepts io.ReadSeeker

    // Get root entry
    root, _ := acc.ReadRoot()

    // Stream directory entries with zero-allocation callback
    acc.ListDirectory(int64(root.ContentOffset), accessor.ListOption{}, func(entry *pxar.Entry) error {
        fmt.Println(entry.FileName())
        return nil
    })

    // Look up a file by path (O(log n) via goodbye table BST)
    entry, _ := acc.Lookup("subdir/secret.txt")

    // Stream file content (returns io.ReadCloser)
    rc, _ := acc.ReadFileContentReader(entry)
    defer rc.Close()
    content, _ := io.ReadAll(rc)

    // Follow a hardlink to its target entry
    linkEntry, _ := acc.Lookup("/link.txt")
    target, _ := acc.FollowHardlink(linkEntry)
    rc2, _ := acc.ReadFileContentReader(target)
    defer rc2.Close()

    // Minimal mode — skips xattrs/ACLs/fcaps, faster for index workloads
    acc.ListDirectory(int64(root.ContentOffset), accessor.ListOption{Minimal: true}, func(entry *pxar.Entry) error {
        return nil
    })

    // Read individual entries at known offsets
    entry, _ = acc.ReadEntryAt(offset)       // full metadata
    entry, _ = acc.ReadEntryAtMinimal(offset) // stat only
}
```

#### FollowHardlink

`FollowHardlink` resolves a hardlink entry to its target file entry by computing `filenameHeaderOffset - linkOffset` from the wire format, then re-reading the full entry at that position. This mirrors Rust's `Accessor::follow_hardlink`:

```go
link, _ := acc.Lookup("/bin/bunzip2")
target, _ := acc.FollowHardlink(link)
rc, _ := acc.ReadFileContentReader(target)
// target now has FileSize, ContentOffset, and full metadata from the original file
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
    // Open source archive (any format)
    src := transfer.NewFileReader(sourceFile)
    defer src.Close()

    // Create target archive
    var dstBuf bytes.Buffer
    dst := transfer.NewStreamWriter(&dstBuf)
    rootMeta := pxar.DirMetadata(0o755).Build()
    dst.Begin(&rootMeta, transfer.Options{Format: format.FormatVersion1})

    // Copy specific files with path mapping
    transfer.Copy(src, dst, []transfer.PathMapping{
        {Src: "/etc/hosts", Dst: "/etc/hosts"},
        {Src: "/var/log/syslog", Dst: "/var/log/syslog"},
    }, transfer.CopyOption{})

    // Copy an entire directory tree
    transfer.CopyTree(src, dst, "/etc", "/etc", transfer.CopyOption{})

    dst.Finish()
}
```

#### ArchiveReader Interface

All source formats implement `ArchiveReader`:

```go
type ArchiveReader interface {
    ReadRoot() (*pxar.Entry, error)
    Lookup(path string) (*pxar.Entry, error)
    ListDirectory(dirOffset int64, opts accessor.ListOption, fn func(*pxar.Entry) error) error
    ReadFileContentReader(entry *pxar.Entry) (io.ReadCloser, error)
    ReadCatalog(fn func(transfer.CatalogEntry) error) error
    Close() error
}
```

Implementations:

- **`FileReader`** — standalone .pxar files via `io.ReadSeeker`
- **`ChunkedReader`** — lazy on-demand chunk loading from .didx indexes
- **`SplitReader`** — v2 split archives (.mpxar.didx + .ppxar.didx)
- **`PBSReader`** — PBS remote stores via H2 reader protocol
- **`DecryptingReader`** — wraps any `ArchiveReader` to decrypt encrypted chunks

#### ArchiveWriter Interface

All target formats implement `ArchiveWriter`:

```go
type ArchiveWriter interface {
    Begin(rootMeta *pxar.Metadata, opts Options) error
    WriteEntry(entry *pxar.Entry, content []byte) error
    WriteEntryRef(entry *pxar.Entry, payloadOffset uint64) error
    WriteEntryReader(entry *pxar.Entry, r io.Reader, size uint64) error
    BeginDirectory(name string, meta *pxar.Metadata) error
    EndDirectory() error
    Finish() error
    Close() error
}
```

Implementations:

- **`StreamWriter`** — writes to `io.Writer` (v1 or v2)
- **`RemoteDedupWriter`** — PBS remote dedup with chunk injection
- **`SessionWriter`** — uploads via `BackupSession`

**`DedupWriter`** is not a full `ArchiveWriter` — it lacks `WriteEntryRef`
since it handles dedup internally by reusing source payload chunks. Use it directly
via its own `WriteEntry`/`WriteEntryReader` methods.

#### Same-Datastore Dedup Transfer

When source and target are in the same chunk store, `DedupWriter` reuses payload chunks without re-uploading:

```go
writer := transfer.NewDedupWriter(store, source, config, false, payloadIdx)
writer.Begin(&rootMeta, transfer.Options{Format: format.FormatVersion2})
writer.WriteEntry(entry, content)
writer.Finish()

hits, total := writer.DedupStats()
fmt.Printf("%d/%d payload chunks reused\n", hits, total)
```

For PBS remote stores, `RemoteDedupWriter` injects original chunks via `UploadPayloadInterleaved`, uploading only new data:

```go
writer, _ := transfer.NewRemoteDedupWriter(ctx, session, metaName, payloadName)
writer.Begin(&rootMeta, transfer.Options{Format: format.FormatVersion2})
writer.WriteEntryRef(entry, payloadOffset) // monotonic offset validated
writer.Finish()
```

The `WriteEntryRef` method enforces strictly monotonic payload offsets via `RecordMax`, preventing corrupt previous archives from injecting backwards `PXAR_PAYLOAD_REF` offsets.

#### Lazy Chunk Loading

`ReadSeeker` implements `io.ReadSeeker` over a chunked archive stream, loading and decoding chunks on demand:

```go
cr, _ := transfer.NewReadSeeker(idx, source, 4) // 4-chunk cache
_, _ = cr.Seek(offset, io.SeekStart)
content, _ := io.ReadAll(cr)
cr.Close()
```

`ChunkedReader` and `SplitReader` use this by default. For eager loading, use `NewChunkedReaderEager` and `NewSplitReaderEager`.

#### Payload Chunk Utilities

The transfer package provides utilities for working with source payload chunks without full stream reconstruction:

- **`MapFileToPayloadChunks`** — maps a file's payload range to the chunk digests that contain it
- **`ReadChunkedFile`** — reads a file's content by loading only necessary chunks
- **`ComputeContentDigest`** — SHA-256 of a file's content without reconstructing the entire stream
- **`RecordMax`** — monotonic offset guard for dedup writers

#### Walking Archives

```go
// Walk all entries with content reading
transfer.WalkTree(reader, "/", func(entry *pxar.Entry, content []byte) error {
    fmt.Println(entry.Path)
    return nil
})

// Walk with options (metadata only, filters, skip count)
transfer.WalkTreeWith(reader, "/", transfer.WalkOption{
    MetaOnly: true,
    Filter:   transfer.WalkFiles,
}, func(entry *pxar.Entry, content []byte) error {
    return nil
})

// Walk metadata only with type filter
transfer.WalkTreeMetadata(reader, "/", transfer.WalkFiles, func(entry *pxar.Entry) error {
    fmt.Printf("%s: %v\n", entry.Path, entry.Kind)
    return nil
})
```

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
        if err == io.EOF {
            break
        }
        if err != nil {
            log.Fatal(err)
        }
        digest := sha256.Sum256(chunk)
        // store chunk indexed by digest
    }
}
```

Use `buzhash.DefaultConfig()` for the standard 4 MiB chunk size. The chunker uses a 256-entry buzhash table and 64-byte sliding window, matching the Rust implementation bit-for-bit.

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

    // Encode a chunk as a blob (magic + CRC32 envelope)
    blob, _ := datastore.EncodeBlob(chunkData)
    digest := sha256.Sum256(chunkData)
    inserted, size, _ := store.InsertChunk(digest, blob.Bytes())

    // Build a dynamic index
    idx := datastore.NewDynamicIndexWriter(time.Now().Unix())
    idx.Add(offset, digest)
    indexData, _ := idx.Finish()

    // Read an index back
    reader, _ := datastore.ParseDynamicIndex(indexData)
    for i := 0; i < reader.Count(); i++ {
        info, _ := reader.ChunkInfo(i)
        // info.Start, info.End, info.Digest
    }

    // Restore a file from its chunk index
    restorer := datastore.NewRestorer(chunkSource)
    restorer.RestoreFile(idx, writer)
    // Or restore a range (offset + length)
    restorer.RestoreRange(idx, offset, length, writer)
}
```

#### Backup Catalogs

The datastore package provides fast catalog building from chunked archives:

- **`BuildCatalogFast`** — parallel catalog extraction from a DIDX with configurable workers
- **`BuildDirIndex`** — builds a `DirIndex` from a directory's goodbye table entries
- **`OnDemandCatalog`** — lazy catalog that loads directory metadata on demand from chunked data
- **`CatalogChild`** — lightweight entry with name, type, size, and mtime

Catalogs are uploaded as `catalog.pcat1.didx` alongside the archive, enabling PBS's web UI to browse backup contents without downloading the full archive.

#### Manifests

```go
manifest := &datastore.Manifest{
    BackupType: datastore.BackupHost.String(),
    BackupID:   "myhost",
    BackupTime: time.Now().Unix(),
    Files: []datastore.BackupFileInfo{
        {Filename: "root.pxar.didx", Size: 4096, CSum: "abc123"},
    },
}
data, _ := manifest.Marshal()
```

### Pull-Mode Backup (backupproxy)

The backupproxy package converts Proxmox's push-based backup protocol into a pull configuration. The server (on the PBS machine) orchestrates backups by walking the client's filesystem and uploading to storage. The client only serves raw filesystem data.

#### Detection Modes

The server supports three detection modes controlling how archives are created and whether unchanged files are re-read:

| Mode                | Format                       | Description                                                                                                                                                                             |
| ------------------- | ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DetectionLegacy`   | v1 single `.pxar`            | All file data encoded into one stream. No previous backup needed.                                                                                                                       |
| `DetectionData`     | v2 split `.mpxar` + `.ppxar` | Metadata and payload in separate streams. All file data re-read.                                                                                                                        |
| `DetectionMetadata` | v2 split `.mpxar` + `.ppxar` | Compares current file metadata (mtime, size, uid, gid, mode, xattrs, ACLs, fcaps) against a previous backup's catalog. Unchanged files reuse payload chunks from the previous snapshot. |

```go
// Legacy mode (single archive)
result, err := srv.RunBackupWithMode(ctx, "/root", backupproxy.BackupConfig{
    BackupType:    datastore.BackupHost,
    BackupID:      "myhost",
    DetectionMode: backupproxy.DetectionLegacy,
})

// Metadata mode (incremental, reuses unchanged payload)
result, err := srv.RunMetadataBackup(ctx, "/root", backupproxy.BackupConfig{
    BackupType:    datastore.BackupHost,
    BackupID:      "myhost",
    DetectionMode: backupproxy.DetectionMetadata,
    PreviousBackup: &backupproxy.PreviousBackupRef{
        BackupType: datastore.BackupHost,
        BackupID:   "myhost",
        BackupTime: 1700000000,
    },
})
```

#### Encryption and Signing

The library supports three crypt modes:

| Mode                | Description                                                                |
| ------------------- | -------------------------------------------------------------------------- |
| `CryptModeNone`     | No encryption or signing (default)                                         |
| `CryptModeEncrypt`  | AES-256-GCM encryption of chunk data; HMAC-SHA256 manifest signing         |
| `CryptModeSign`     | No encryption, but HMAC-SHA256 manifest signing for integrity verification |

Encryption uses PBKDF2-HMAC-SHA256 for key derivation and AES-256-GCM (12-byte nonce, empty AAD) for chunk encryption. Manifests are always signed when a `CryptConfig` is provided — they are never encrypted, since PBS must be able to read the manifest. Chunk digests in encrypted mode use `SHA-256(data || id_key)` to prevent cross-key collisions.

#### Pluggable Transport

The `ClientProvider` interface defines what the server calls to access client data:

```go
type ClientProvider interface {
    Stat(ctx context.Context, path string) (format.Stat, error)
    ReadDir(ctx context.Context, path string) ([]DirEntry, error)
    OpenFile(ctx context.Context, path string) (io.ReadCloser, uint64, error)
    ReadLink(ctx context.Context, path string) (string, error)
    GetXAttrs(ctx context.Context, path string) ([]format.XAttr, error)
    GetACL(ctx context.Context, path string) (pxar.ACL, error)
    GetFCaps(ctx context.Context, path string) ([]byte, error)
}
```

On the client side, `FSAccessor` provides the same methods without context (local filesystem), and `LocalClient` adapts it to `ClientProvider`.

#### Pluggable Storage Backend

Implement `RemoteStore` and `BackupSession` to support custom storage backends:

```go
type RemoteStore interface {
    StartSession(ctx context.Context, config BackupConfig) (BackupSession, error)
    ReadPreviousArchive(ctx context.Context, ...) ([]byte, error)
    NewPreviousSnapshotSource(ctx context.Context, ...) (PreviousSnapshotSource, error)
}

type BackupSession interface {
    UploadArchive(ctx context.Context, name string, data io.Reader) (*UploadResult, error)
    UploadSplitArchive(ctx context.Context, ...) (*SplitArchiveResult, error)
    UploadBlob(ctx context.Context, name string, data []byte) error
    UploadPayloadInterleaved(ctx context.Context, name string, newData io.Reader, injections <-chan InjectChunks) (*UploadResult, error)
    Finish(ctx context.Context) (*datastore.Manifest, error)
}
```

Built-in implementations:

- **`LocalStore`** — local filesystem storage (testing, offline)
- **`PBSStore`** — PBS H2 backup protocol with HTTP/2 multiplexing

#### PBS Reader Protocol

For restoring backups, `PBSReader` provides access to the Proxmox Backup Server reader protocol via HTTP/2:

```go
reader := backupproxy.NewPBSReader(cfg, "host", "mybackup", backupTime)
reader.Connect(ctx)
defer reader.Close()

// Download an index file
didxData, _ := reader.DownloadFile("root.pxar.didx")

// Download a chunk by digest
chunkData, _ := reader.DownloadChunk(digest)

// Restore entire file or range
idx, _ := datastore.ParseDynamicIndex(didxData)
var buf bytes.Buffer
reader.RestoreFile(idx, &buf)
reader.RestoreFileRange(idx, 1024, 1024, &buf)
```

`PBSReader.AsChunkSource()` returns a `datastore.ChunkSource` compatible with `Restorer`, `ReadSeeker`, and `SplitReader`.

#### Basic Usage

```go
// Server side (PBS machine)
chunkCfg, _ := buzhash.NewConfig(4096)
store, _ := backupproxy.NewLocalStore("/tmp/backup", chunkCfg, false)
srv := backupproxy.NewServer(client, store)

result, err := srv.RunBackupWithMode(ctx, "/", backupproxy.BackupConfig{
    BackupType:    datastore.BackupHost,
    BackupID:      "myhost",
    DetectionMode: backupproxy.DetectionData,
})
if err != nil {
    panic(err)
}

fmt.Printf("Backed up %d files, %d dirs, %d bytes in %s\n",
    result.FileCount, result.DirCount, result.TotalBytes, result.Duration)
```

### FUSE Filesystem

Mount a pxar archive as a read-only filesystem (compatible with hanwen/go-fuse, no dependency):

```go
f, _ := os.Open("backup.pxar")
fi, _ := f.Stat()

sess, _ := fusefs.NewSession(f, fi.Size())
defer sess.Close()

// Filesystem operations
inode, attr, _ := sess.Lookup(fusefs.RootInode, "example.txt")
buf := make([]byte, attr.Size)
n, _ := sess.Read(inode, buf, 0)
entries, _ := sess.Readdir(fusefs.RootInode, 0)
target, _ := sess.Readlink(symlinkInode)
```

## Package Reference

### `pxar` — Core Types

- **`Entry`** — Typed archive entry with metadata, content offsets, and hardlink support
  - `Kind` — Entry type (`KindFile`, `KindDirectory`, `KindSymlink`, `KindHardlink`, `KindDevice`, `KindFIFO`, `KindSocket`)
  - `FileOffset` — Position of the entry's FILENAME header in the archive
  - `FileSize` — Content size for regular files
  - `ContentOffset` — Position of PAYLOAD/PAYLOAD_REF data
  - `PayloadOffset` — Offset into the v2 payload stream
  - `LinkTarget` — Symlink/hardlink target path
  - `LinkOffset` — Relative offset from hardlink's FILENAME to target's FILENAME (wire format)
  - `DeviceInfo` — Device major/minor numbers
  - `Metadata` — Full POSIX metadata
  - Predicates: `IsDir()`, `IsSymlink()`, `IsRegularFile()`, `IsHardlink()`, `IsDevice()`, `IsFIFO()`, `IsSocket()`
  - `FileName()`, `PathBytes()`, `FileNameBytes()`

- **`Metadata`** — POSIX metadata: Stat, XAttrs, ACLs, FCaps, QuotaProjectID
  - `ExtendedMetadataEqual(other)` — compares all extended metadata fields
  - Predicates: `IsDir()`, `IsSymlink()`, `IsRegularFile()`, `IsDevice()`, `IsFIFO()`, `IsSocket()`
  - `FileType()`, `FileMode()`

- **`ACL`** — POSIX ACL (users, groups, default, default users, default groups)
  - `IsEmpty()` — true when no ACL entries present

- **`MetadataBuilder`** — Fluent builder with type-specific constructors
  - `FileMetadata(mode)`, `DirMetadata(mode)`, `SymlinkMetadata(mode)`, `DeviceMetadata(mode)`, `FIFOMetadata(mode)`, `SocketMetadata(mode)`
  - Chainable: `.UID(u)`, `.GID(g)`, `.Owner(u,g)`, `.Mtime(ts)`, `.XAttr(name,val)`, `.FCaps(data)`, `.QuotaProjectID(id)`
  - `.Build()` returns `Metadata`

- `SplitPath(path)` — Split a rooted path into components
- `CheckPathComponent(path)` — Validate a path component
- Sentinel errors: `ErrNotFound`, `ErrInvalidFilename`, `ErrInvalidHeader`, `ErrNotDirectory`, `ErrNotRegularFile`

### `format` — Binary Format

- **`Header`** — 16-byte typed size-prefixed header (little endian)
  - `NewHeader(htype, fullSize)`, `HeaderWithContentSize(htype, contentSize)`
  - `ContentSize()`, `MaxContentSize()`, `CheckHeaderSize()`
  - `MarshalTo(dst []byte)` — zero-copy serialization into caller buffer
  - `String()` — human-readable type name

- **`Stat`** — 40-byte POSIX stat (mode, flags, uid, gid, mtime as StatxTimestamp)
  - Includes `_pad` field at bytes 36-39 (always 0, matches Rust `Endian` trait)
  - `FileType()`, `FileMode()`, `StatEqual(other)`
  - Predicates: `IsDir()`, `IsSymlink()`, `IsRegularFile()`, `IsDevice()`, `IsBlockDev()`, `IsCharDev()`, `IsFIFO()`, `IsSocket()`

- **`StatV1`** — 32-byte legacy stat (nanosecond mtime), converts via `ToStat()`

- **`StatxTimestamp`** — `{Secs int64, Nanos uint32}` with 4-byte padding
  - `NewStatxTimestamp(secs, nanos)`, `NewStatxTimestampFromDuration(d)`
  - `Duration()` — convert back to `time.Duration` (supports pre-epoch)

- **`Device`** — `{Major uint64, Minor uint64}`
  - `ToDevT()` — encode as `dev_t` (matches Rust `makedev`)
  - `DeviceFromDevT(dev)` — decode from `dev_t`

- **`PayloadRef`** — 16-byte reference to payload stream offset + file size
  - `UnmarshalPayloadRefBytes(data)` — deserialize
- **`GoodbyeItem`** — goodbye table entry (hash + offset) for BST directory lookup
- **`QuotaProjectID`** — project quota identifier
- **`XAttr`** — Extended attribute (name + value), created with `NewXAttr(name, value)`
- **`ACLUser`**, **`ACLGroup`**, **`ACLGroupObject`**, **`ACLDefault`** — POSIX ACL entry types
  - `MarshalACLUserBytes`, `MarshalACLGroupBytes`, `MarshalACLGroupObjectBytes`, `MarshalACLDefaultBytes`
  - `UnmarshalACLDefault(data)` — deserialize ACL default entries
- **`ACLPermissions`** — bitmask of ACL permission flags
- **`FormatVersion`** — Archive format version with `Serialize()` and `DeserializeFormatVersion()`

- `HeaderSize` = 16 — constant for header size
- Mode constants: `ModeIFREG`, `ModeIFDIR`, `ModeIFLNK`, `ModeIFBLK`, `ModeIFCHR`, `ModeIFIFO`, `ModeIFSOCK`, etc.
- Type constants: `PXAREntry`, `PXARFilename`, `PXARPayload`, `PXARPayloadRef`, `PXARGoodbye`, `PXARHardlink`, `PXARSymlink`, `PXARDevice`, `PXARACLUser`, `PXARACLGroup`, `PXARACLDefault`, `PXARFCaps`, `PXARXAttr`, `format.Version`, `PXARPrelude`, `PXARPayloadTailMarker`
- Serialization: `MarshalStatBytesInto(buf, stat)`, `AppendStatBytesInto(dst, stat)`, `UnmarshalStatBytes(data)`, `UnmarshalStatV1Bytes(data)`, `MarshalDeviceBytes(device)`
- `CheckFilename(name)` — validate filename bytes
- `HashFilename(name)` — SipHash24 filename hashing for goodbye tables (matches Rust key)

### `encoder` — Archive Writer

- `NewEncoder(output, payloadOut, metadata, prelude)` — Create encoder; `payloadOut` non-nil enables v2 split
- `AddFile(metadata, name, content)` → `(LinkOffset, error)` — write file with inline content
- `CreateFile(metadata, name, size)` → `(*FileWriter, error)` — streaming file writer
- `AddPayloadRef(metadata, name, fileSize, payloadOffset)` → `(LinkOffset, error)` — reference existing payload
- `AddSymlink(metadata, name, target)` — write symlink
- `AddHardlink(name, target, targetOffset)` — write hardlink (uses relative offset)
- `AddDevice(metadata, name, device)` — write device node
- `AddFIFO(metadata, name)` — write FIFO
- `AddSocket(metadata, name)` — write socket
- `CreateDirectory(name, metadata)` — open directory scope
- `Finish()` — close current directory, return to parent
- `Close()` — finalize archive (write goodbye table, close root)
- `PayloadPosition()` — current payload stream write position
- `Advance(size)` — advance payload position for virtual content

**`LinkOffset`** — opaque file position token returned by `AddFile`/`AddPayloadRef`, passed to `AddHardlink`

**`FileWriter`** — `io.Writer` for streaming file content
  - `Write(data)`, `WriteAll(data)`, `Close()` — finalize entry
  - `FileOffset()` → `LinkOffset` — position token for hardlink targets

### `decoder` — Archive Reader

- `NewDecoder(input, payloadReader)` — Create decoder; `payloadReader` for v2 split
- `Next()` → `(*pxar.Entry, error)` — advance to next entry (`io.EOF` when done)
- `Contents()` → `io.Reader` — stream current file's content (valid until next `Next()`)

### `accessor` — Random Access

- `NewAccessor(reader, ...payloadReader)` — create from `io.ReadSeeker`
- `ReadRoot()` — get root directory entry
- `ListDirectory(offset, opts, fn)` — zero-allocation callback-based directory streaming
- `Lookup(path)` — O(log n) path lookup via goodbye table BST
- `ReadFileContentReader(entry)` → `(io.ReadCloser, error)` — streaming content read
- `FollowHardlink(entry)` → `(*pxar.Entry, error)` — resolve hardlink to target file entry
- `ReadEntryAt(offset)` — read full entry at known offset
- `ReadEntryAtMinimal(offset)` — read entry with stat only (skips xattrs/ACLs/fcaps)
- `ListOption{Minimal: true}` — skip extended metadata during listing

### `transfer` — File Transfer Between Archives

- **`ArchiveReader`** — unified read interface (ReadRoot, Lookup, ListDirectory, ReadFileContentReader, ReadCatalog, Close)
- **`ArchiveWriter`** — unified write interface (Begin, WriteEntry, WriteEntryRef, WriteEntryReader, BeginDirectory, EndDirectory, Finish, Close)
- **`FileReader`** — reads from standalone .pxar files
  - `NewFileReader(reader)`, `NewSplitFileReader(metaReader, payloadReader)`
- **`ChunkedReader`** — lazy on-demand chunk loading from .didx
  - `NewChunkedReader(idxData, source)`, `NewChunkedReaderEager(idxData, source)`
- **`SplitReader`** — reads from .mpxar.didx + .ppxar.didx
  - `NewSplitReader(metaIdxData, payloadIdxData, source)` — lazy
  - `NewSplitReaderEager(metaIdxData, payloadIdxData, source)` — eager
  - `NewSplitReaderMetaOnly(metaIdxData, source)` — metadata only, no payload
- **`PBSReader`** — reads from PBS remote via H2 reader protocol
  - `NewPBSReader(ctx, cfg)` — `PBSReaderConfig` holds backup ref + PBS config
- **`DecryptingReader`** — wraps any ArchiveReader, delegates with optional decryption layer
- **`StreamWriter`** — writes to io.Writer
  - `NewStreamWriter(output)` — v1
  - `NewSplitStreamWriter(output, payloadOut)` — v2 split
- **`DedupWriter`** — same-datastore dedup with chunk reuse
  - `NewDedupWriter(store, source, config, compress, sourcePayloadIdx)`
  - `DedupStats()` → `(hits, total int)`, `ReferenceSourcePayloadChunks()`
  - `MetaIndexData()`, `PayloadIndexData()` — index results
  - Not a full `ArchiveWriter` — lacks `WriteEntryRef`
- **`RemoteDedupWriter`** — PBS remote dedup with chunk injection
  - `NewRemoteDedupWriter(ctx, session, metaName, payloadName)`
  - `Encoder()`, `AdvancePayloadPosition(n)` — direct encoder access
- **`SessionWriter`** — uploads via BackupSession
  - `NewSessionWriter(ctx, session, metaName, payloadName)`
  - `Encoder()` — direct encoder access
- **`ReadSeeker`** — io.ReadSeeker over chunked data with configurable cache
  - `NewReadSeeker(idx, source, maxCache)`, `ReadAt(p, offset)`, `Seek(offset, whence)`, `Close()`
- **`DecryptSource`** — wraps ChunkSource for encrypted chunks
  - `NewDecryptSource(inner, cc)`, `GetChunk(digest)`
- **`TreeWalker`** — iterator-based archive walking
  - `NewTreeWalker(reader, opts)`, `Next()`, `Entry()`, `Err()`
- `Copy(src, dst, mappings, opts)` — copy specific paths between archives
- `CopyTree(src, dst, srcPath, dstPath, opts)` — copy entire directory tree
- `WalkTree(reader, path, fn)` — walk all entries with content reading
- `WalkTreeWith(reader, path, opts, fn)` — walk with options (MetaOnly, Filter, SkipCount)
- `WalkTreeMetadata(reader, path, filter, fn)` — metadata-only walk with type filter
- `RecordMax(last, offset)` — monotonic offset guard for dedup writers
- `MapFileToPayloadChunks(idx, offset, size)` → `[]ChunkRange` — map file to payload chunk ranges
- `ReadChunkedFile(source, idx, offset, size)` → `([]byte, error)` — read from specific chunks
- `ComputeContentDigest(source, idx, offset, size)` → `([32]byte, error)` — SHA-256 without full reconstruction

**Types**: `PathMapping{Src, Dst}`, `CopyOption{}`, `Options{Format}`, `WalkOption{MetaOnly, Filter, SkipCount}`, `WalkFilter` bitmask (`WalkFiles`, `WalkDirs`, etc.), `WalkFunc`, `MetadataWalkFunc`, `CatalogEntry{Path, ParentPath, Kind, FileSize}`, `ChunkRange{StartChunk, EndChunk, StartOffset, EndOffset}`

### `buzhash` — Content-Defined Chunking

- `NewConfig(avgSize)` — create config (must be power of two)
- `DefaultConfig()` — standard 4 MiB chunk configuration
- `NewChunker(reader, config)` — create chunker
- `Next()` → `([]byte, error)` — get next chunk
- `Hasher` — low-level rolling hash with 64-byte sliding window

### `datastore` — Chunk Storage and Indexes

- **`ChunkStore`** — local filesystem chunk storage keyed by SHA-256 digest
  - `InsertChunk(digest, data)` → `(inserted bool, size int, err error)`
  - `LoadChunk(digest)`, `TouchChunk(digest)`, `ChunkPath(digest)`

- **`DataBlob`** — chunk envelope with magic + CRC32
  - `EncodeBlob(data)`, `EncodeBlobTo(dst, data)` — encode with CRC32
  - `EncodeCompressedBlob(data)`, `EncodeCompressedBlobTo(dst, data)` — zstd + CRC32
  - `EncodeEncryptedBlob(data, cryptConfig, compress)`, `EncodeEncryptedBlobTo(dst, data, cc, compress)` — AES-256-GCM + optional zstd
  - `DecodeBlob(raw)`, `DecodeBlobInto(dst, raw)` — decode (handles compressed)
  - `DecodeEncryptedBlob(raw, cryptConfig)` — decrypt + decode
  - `Bytes()`, `IsCompressed()`, `IsEncrypted()`, `Magic()`, `Csum()`, `ComputeCsum()`

- **`DynamicIndexWriter`** / **`DynamicIndexReader`** — variable-size chunk index (.didx)
  - `NewDynamicIndexWriter(ctime)`, `Add(offset, digest)`, `Finish()`
  - `ParseDynamicIndex(data)` → `*DynamicIndexReader`
  - `Count()`, `IndexBytes()`, `CTime()`, `ChunkInfo(i)`, `ChunkFromOffset(offset)`, `IndexDigest(pos)`
  - `Entry(i)` → `DynamicEntry`, `ComputeDigest(data)`

- **`FixedIndexWriter`** / **`FixedIndexReader`** — fixed-size chunk index (.fidx)
  - `NewFixedIndexWriter(ctime, size, chunkSize)`, `Add(offset, digest)`, `Finish()`
  - `ParseFixedIndex(data)` → `*FixedIndexReader`
  - `Count()`, `IndexBytes()`, `CTime()`, `ChunkInfo(i)`, `ChunkFromOffset(offset)`, `IndexDigest(pos)`

- **`StoreChunker`** — chunking with store integration
  - `NewStoreChunker(store, config, compress)`
  - `ChunkStream(r, fn)` → `([]ChunkResult, *DynamicIndexWriter, error)` — chunk a stream, store each chunk
  - `ChunkResult` holds `Digest`, `Size`, `Offset`

- **`Restorer`** — reconstruct files from chunks
  - `NewRestorer(chunkSource)`, `RestoreFile(idx, writer)`, `RestoreRange(idx, offset, length, writer)`
  - `ChunkStoreSource` wraps `ChunkStore` as a `ChunkSource`

- **`ChunkSource`** — interface: `GetChunk(digest [32]byte) ([]byte, error)`
  - Implemented by `ChunkStoreSource`, `PBSReader.AsChunkSource()`, `DecryptSource`

- **`CryptConfig`** — encryption key configuration (PBKDF2 + AES-256-GCM)
  - `NewCryptConfig(encKey [32]byte)` → `(*CryptConfig, error)`
  - `Encrypt(plaintext)`, `Decrypt(ciphertext)`, `AuthTag(data)`
  - `Fingerprint()` → `[32]byte`, `FormatFingerprint(fp)` → `string`
  - `KeyConfig`, `KeyDerivationConfig`, `UnprotectedInfo` — key file structures
  - `CreateRandomKey()`, `GenerateKeyFile(password)`, `LoadKeyFile(data, password)`, `LoadKeyFileNoPassword(data)`
  - `SignManifest(manifest, cc)`, `VerifyManifestSignature(manifest, cc)`
  - `CryptMode` constants: `CryptModeNone`, `CryptModeEncrypt`, `CryptModeSign`
  - `IsEncryptedMagic(magic)`, `IsCompressedMagic(magic)`, `BlobHeaderSizeFor(magic)`

- **Backup Catalogs**
  - `BuildCatalogFast(metaIdx, source, opts)` → `(*Catalog, error)` — parallel extraction
  - `BuildDirIndex(metaIdx, source, opts)` → `(*BuildResult, error)` — directory index
  - `OnDemandCatalog` — lazy catalog with `HasDir`, `DirPaths`, `NumDirs`, `ListDir`
  - `CatalogChild` — lightweight entry (name, type, size, mtime)
  - `CatalogWriter` / `CatalogReader` — pcat1 binary catalog serialization
  - `ReadCatalogTree(data)` → `(*CatalogTreeEntry, error)` — read entire catalog tree

- **`Manifest`** / **`FileInfo`** — backup snapshot manifest (JSON)
  - `Marshal()`, `UnmarshalManifest(data)`, `VerifyFile(filename, csum, size)`

- **`BackupType`** (`BackupHost`, `BackupVM`), **`BackupGroup`**, **`BackupDir`**, **`BackupInfo`** — namespace hierarchy
  - `ParseBackupType(s)`, `BackupType.String()`

### `binarytree` — BST Permutation

- `Copy(n, copyFunc)` — permute sorted array into BST order
- `SearchBy(tree, start, skip, compare)` — binary search on BST-ordered array

### `fusefs` — FUSE Filesystem

- **`FileSystem`** — interface implemented by Session
- **`Session`** — read-only filesystem session over a pxar archive
  - `NewSession(reader, size)` → `(*Session, error)`, `Close()`
  - `Lookup(parentInode, name)` → `(inode, Attr, error)`
  - `Getattr(inode)` → `(Attr, error)`, `Open(inode, flags)`, `Release(inode)`
  - `Readdir(inode, offset)` → `([]DirEntryIndex, error)`
  - `Read(inode, buf, offset)` → `(int, error)`, `Readlink(inode)` → `(string, error)`
  - `ListXAttr(inode)` → `([]string, error)`, `GetXAttr(inode, attr)` → `([]byte, error)`
  - `Forget(inode, count)`, `Access(inode, mask)`, `Statfs()` → `(syscall.Statfs_t, error)`
- **`Node`** — cached inode with parent, content range, entry range info
- **`Attr`** — file attributes (Inode, Mode, UID, GID, Size, Atime/Mtime/Ctime, Blocks, Nlink, Rdev)
- **`DirEntryIndex`** — directory entry (Inode + Name)
- **`EntryRangeInfo`** — entry byte range in archive (Start + End offset)
- **`ContentRange`** — file content byte range (Offset + Size)
- `RootInode` constant, `IsDirInode(inode)` helper
- `StatToAttr(inode, stat, fileSize)` — convert format.Stat to Attr

### `backupproxy` — Pull-Mode Backup

- **`Server`** — backup orchestrator (walk → encode → chunk → upload)
  - `NewServer(client, store)`, `RunBackup(ctx, root, config)`, `RunBackupWithMode(ctx, root, config)`, `RunSplitBackup(ctx, root, config)`, `RunMetadataBackup(ctx, root, config)`
- **`ClientProvider`** — interface for accessing client filesystem data
  - `Stat`, `ReadDir`, `OpenFile`, `ReadLink`, `GetXAttrs`, `GetACL`, `GetFCaps`
- **`FileOpener`** — optional interface for streaming file reads (`OpenFile`)
- **`FSAccessor`** — client-side local filesystem access (no context)
- **`LocalClient`** — adapts FSAccessor to ClientProvider
- **`NoExtendedAttrs`** — FSAccessor stub that returns empty xattrs/ACL/fcaps
- **`RemoteStore`** — storage backend interface (session + snapshot reader)
  - `RemoteStoreBase` (StartSession), `SnapshotReader` (ReadPreviousArchive, NewPreviousSnapshotSource)
- **`BackupSession`** — upload session interface
  - `UploadArchive`, `UploadSplitArchive`, `UploadBlob`, `UploadPayloadInterleaved`, `Finish`
- **`KnownChunkRef`** — reference to a chunk already in the datastore (Digest + Size)
- **`UploadResult`** — upload outcome (Filename, Size, Digest)
- **`SplitArchiveResult`** — split upload outcome (Meta + Payload UploadResult)
- **`LocalStore`** — local filesystem storage backend
- **`PBSStore`** — PBS H2 backup protocol backend
- **`PBSReader`** — PBS reader protocol client for restore
  - `Connect(ctx)`, `DownloadFile(name)`, `DownloadChunk(digest)`, `AsChunkSource()`, `RestoreFile(idx, w)`, `RestoreFileRange(idx, offset, length, w)`
- **`PBSConfig`** — PBS connection configuration (BaseURL, Datastore, AuthToken, Namespace, SkipTLSVerify)
- **`BackupConfig`** — backup configuration (BackupType, BackupID, BackupTime, DetectionMode, CryptConfig, ChunkConfig, Compress, PreviousBackup)
- **`BackupResult`** — backup outcome (Manifest, TotalBytes, FileCount, DirCount, Duration, CatalogUploaded)
- **`DetectionMode`** — `DetectionLegacy`, `DetectionData`, `DetectionMetadata`
- **`PreviousBackupRef`** — reference to previous snapshot for metadata mode
- **`PreviousSnapshotSource`** — interface for reading previous backup data
  - `NewPreviousSnapshotSourceFromDir(dir)` — local filesystem implementation
- **`DirEntry`** — directory entry with Stat, Size, XAttrs, ACL, FCaps
- **`SnapshotCatalog`** / **`SnapshotEntry`** — catalog of previous snapshot entries
  - `BuildCatalog(metaIdx, source)` — build catalog from DIDX
  - `EntryMatches(current, metadata, prev)` — compare metadata for change detection

## Architecture

```
Backup Data Flow:

  Client (backed-up machine)           Server (PBS machine)
  =========================          =====================

  FSAccessor                  Server{client, store}
        │                                    │
        │◄──── ClientProvider ───────────────│
        │  Stat, ReadDir,                    │
        │  OpenFile, ReadLink                │
        │────►                               │
                                     ┌────────▼─────────┐
                                     │ RunBackup()       │
                                     │                   │
                                     │ Encoder           │
                                     │  .AddFile()       │
                                     │  .AddSymlink()    │
                                     │  .AddHardlink()   │
                                     │  .AddDevice()     │
                                     │  .Close()         │
                                     │                   │
                                     │ walkDir():        │
                                     │  dir:  Create → recurse → Finish
                                     │  file: OpenFile → AddFile
                                     │  link: ReadLink → AddSymlink
                                     │                   │
                                     │ session.Finish()  │
                                     └────────┬─────────┘
                                              │
                                       RemoteStore
                                       ├── LocalStore (testing)
                                       └── PBSStore (PBS H2 Protocol)
```

## Verification

This library has been validated against Proxmox's Rust reference implementation:

- **Wire format**: All struct sizes, hash keys, mode constants, and device conversions verified (`format/format.go` ↔ `proxmox-pxar/src/format/mod.rs`)
- **SipHash-2-4**: All 23 pxar format type constants produce identical hashes
- **BST permutation**: Binary tree array layout matches PBS for sizes 1–1000
- **Goodbye tables**: BST layout, hash sorting, and tail marker verified against Rust encoder
- **Chunker**: BUZHASH_TABLE (256 entries), config parameters, and chunk boundary logic are bit-identical
- **Encoder**: File encoding (v1/v2), hardlinks (relative offset), symlinks, devices, payload refs, prelude validation — all match Rust
- **Accessor**: Random-access lookup, hardlink following (`FollowHardlink` mirrors `follow_hardlink`), minimal decoding mode
- **Flow control**: Connection-level and stream-level WINDOW_UPDATE frames in H2 client (half-window threshold)
- **Dedup**: `RecordMax` with `Option<u64>`-equivalent semantics, dedup collision identity tests
- **ACL wire format**: User/Group object sizes match Rust (`size_of::<acl::User>()` = 16 bytes)
- **Stat pad field**: `_pad = 0` at bytes 36-39 matches Rust `Endian` trait

Parity tests run in CI on every push and pull request via GitHub Actions.

## Disclaimer

**This library is not yet battle-tested.** It is under active development and should not be used in production environments. The API may change without notice, and there may be bugs or edge cases that have not been discovered. Use at your own risk.

## License

MIT License - see [LICENSE](LICENSE) file for details.
