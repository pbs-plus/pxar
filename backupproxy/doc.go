// Package backupproxy provides a pull-mode backup architecture where a server
// (running on the PBS machine) orchestrates backups by walking a remote client's
// filesystem, encoding pxar archives, chunking with buzhash, and uploading to
// a backup store. The client only serves raw filesystem data.
//
// The transport between server and client is pluggable — this package defines
// interfaces and message types. Users provide their own transport implementation
// (gRPC, HTTP, SSH, etc.).
//
// # Features
//
// All backup modes automatically generate and upload a catalog.pcat1.didx file,
// enabling PBS's web UI to browse backup contents without downloading the
// full archive.
//
// The library supports three crypt modes:
//   - CryptModeNone: no encryption or signing (default)
//   - CryptModeEncrypt: AES-256-GCM encryption of chunk data; HMAC-SHA256 manifest signing
//   - CryptModeSign: no encryption, but HMAC-SHA256 manifest signing for integrity verification
//
// Encryption uses PBKDF2-HMAC-SHA256 for key derivation and AES-256-GCM for chunk
// encryption. Manifests are signed but never encrypted (PBS must read them).
//
// Extended attributes, POSIX ACLs, and file capabilities are collected from the
// filesystem via the FileSystemAccessor interface and encoded into archives.
// Metadata change detection compares all extended metadata fields (stat, xattrs,
// ACLs, fcaps) to trigger re-upload when they change.
//
// # PBS Reader Protocol
//
// For restoring backups, the PBSReader type provides access to the Proxmox Backup
// Server reader protocol (proxmox-backup-reader-protocol-v1) via HTTP/2. This
// enables efficient downloading of:
//   - Index files (.didx, .fidx, .blob) via GET /download
//   - Individual chunks by digest via GET /chunk
//
// The reader integrates with the datastore.Restorer to reconstruct files from
// their chunks, supporting both full file restoration and partial/range reads.
//
// Example:
//
//	reader := backupproxy.NewPBSReader(cfg, "host", "mybackup", backupTime)
//	if err := reader.Connect(ctx); err != nil {
//	    return err
//	}
//	defer reader.Close()
//
//	// Download index
//	didxData, _ := reader.DownloadFile("root.pxar.didx")
//	idx, _ := datastore.ParseDynamicIndex(didxData)
//
//	// Restore entire file
//	var buf bytes.Buffer
//	reader.RestoreFile(idx, &buf)
//
//	// Or restore just a range (e.g., bytes 1024-2048)
//	reader.RestoreFileRange(idx, 1024, 1024, &buf)
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
