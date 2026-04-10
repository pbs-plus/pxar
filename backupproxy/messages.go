// Package backupproxy provides a pull-mode backup architecture where a server
// (running on the PBS machine) orchestrates backups by walking a remote client's
// filesystem, encoding pxar archives, chunking with buzhash, and uploading to
// a backup store. The client only serves raw filesystem data.
//
// The transport between server and client is pluggable — this package defines
// interfaces and message types. Users provide their own transport implementation
// (gRPC, HTTP, SSH, etc.).
package backupproxy

import (
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
)

// DirEntry represents a single entry from a directory listing on the client.
type DirEntry struct {
	Name string
	Stat format.Stat
}

// BackupConfig holds parameters for a single backup operation.
type BackupConfig struct {
	Store       string              // datastore name
	BackupType  datastore.BackupType // vm, ct, or host
	BackupID    string              // backup identifier
	BackupTime  int64               // Unix timestamp for this snapshot
	Compress    bool                // compress chunks with zstd
	ChunkConfig buzhash.Config      // buzhash chunking parameters
}

// UploadResult describes the outcome of an archive upload.
type UploadResult struct {
	Filename string   // e.g., "root.pxar.didx"
	Size     uint64   // total index size
	Digest   [32]byte // SHA-256 of the index
}
