// Package backupproxy provides a pull-mode backup architecture where a server
// (running on the PBS machine) orchestrates backups by walking a remote client's
// filesystem, encoding pxar archives, chunking with buzhash, and uploading to
// a backup store. The client only serves raw filesystem data.
//
// The transport between server and client is pluggable — this package defines
// interfaces and message types. Users provide their own transport implementation
// (gRPC, HTTP, SSH, etc.).
//
// # Detection Modes
//
// Three modes control how archives are created and whether unchanged files are
// re-read:
//   - DetectionLegacy: v1 single .pxar, all data encoded in one stream
//   - DetectionData: v2 split .mpxar + .ppxar, all data re-read
//   - DetectionMetadata: v2 split, compares metadata against previous backup,
//     reuses unchanged payload chunks
//
// # Encryption
//
// Three crypt modes are supported:
//   - CryptModeNone: no encryption or signing (default)
//   - CryptModeEncrypt: AES-256-GCM encryption + HMAC-SHA256 manifest signing
//   - CryptModeSignOnly: no encryption, HMAC-SHA256 manifest signing
//
// Encryption uses PBKDF2-HMAC-SHA256 for key derivation and AES-256-GCM for
// chunk encryption. Manifests are signed but never encrypted (PBS must read them).
//
// # Extended Metadata
//
// Extended attributes, POSIX ACLs, and file capabilities are collected via
// the FileSystemAccessor/ClientProvider interfaces and encoded into archives.
// Metadata change detection compares all fields (stat, xattrs, ACLs, fcaps)
// to trigger re-upload when they change.
//
// # Backup Catalogs
//
// All backup modes automatically generate and upload a catalog.pcat1.didx file,
// enabling PBS's web UI to browse backup contents without downloading the
// full archive.
//
// # PBS Reader Protocol
//
// PBSReader provides access to the Proxmox Backup Server reader protocol
// (proxmox-backup-reader-protocol-v1) via HTTP/2. This enables downloading
// index files and individual chunks, and restoring files:
//
//	reader := backupproxy.NewPBSReader(cfg, "host", "mybackup", backupTime)
//	reader.Connect(ctx)
//	defer reader.Close()
//
//	didxData, _ := reader.DownloadFile("root.pxar.didx")
//	idx, _ := datastore.ParseDynamicIndex(didxData)
//
//	var buf bytes.Buffer
//	reader.RestoreFile(idx, &buf)
//	// Or partial range:
//	reader.RestoreFileRange(idx, 1024, 1024, &buf)
//
// Use AsChunkSource() to integrate with Restorer, ChunkedReadSeeker, etc.
package backupproxy

import (
	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
)

// DirEntry represents a single entry from a directory listing on the client.
type DirEntry struct {
	QuotaProjectID *uint64
	Name           string
	ACL            pxar.ACL
	XAttrs         []format.XAttr
	FCaps          []byte
	Stat           format.Stat
	Size           uint64
}

// DetectionMode controls how file changes are detected between backup runs.
type DetectionMode int

const (
	// DetectionLegacy creates a single self-contained pxar v1 archive.
	// All file data and metadata are read and encoded in one stream.
	DetectionLegacy DetectionMode = iota

	// DetectionData creates split pxar v2 archives (.mpxar + .ppxar).
	// All file data is still read fully, but metadata and payload are
	// stored in separate streams for more efficient catalog access.
	DetectionData

	// DetectionMetadata creates split pxar v2 archives (.mpxar + .ppxar)
	// but only re-reads file content for files whose metadata (size, mtime,
	// uid, gid, mode, xattrs) has changed since the previous backup.
	// Unchanged files reuse payload chunks from the previous backup.
	DetectionMetadata
)

func (d DetectionMode) String() string {
	switch d {
	case DetectionLegacy:
		return "legacy"
	case DetectionData:
		return "data"
	case DetectionMetadata:
		return "metadata"
	default:
		return "unknown"
	}
}

// BackupConfig holds parameters for a single backup operation.
type BackupConfig struct {
	PreviousBackup *PreviousBackupRef
	CryptConfig    *datastore.CryptConfig
	BackupID       string
	Namespace      string
	CryptMode      datastore.CryptMode
	ChunkConfig    buzhash.Config
	BackupType     datastore.BackupType
	BackupTime     int64
	DetectionMode  DetectionMode
	Compress       bool
}

// PreviousBackupRef identifies a previous backup snapshot for metadata comparison.
type PreviousBackupRef struct {
	BackupID   string
	Namespace  string
	Dir        string
	BackupType datastore.BackupType
	BackupTime int64
}

// UploadResult describes the outcome of an archive upload.
type UploadResult struct {
	Filename string   // e.g., "root.pxar.didx"
	Size     uint64   // total index size
	Digest   [32]byte // SHA-256 of the index
}
