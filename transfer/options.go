package transfer

import (
	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
)

// TransferOption configures a file transfer operation.
type TransferOption struct {
	// SourceCryptConfig provides decryption configuration for the source archive.
	// Required when the source archive is encrypted.
	SourceCryptConfig *datastore.CryptConfig

	// TargetCryptConfig provides encryption configuration for the target archive.
	// Required when the target archive should be encrypted.
	TargetCryptConfig *datastore.CryptConfig

	// TargetFormat specifies the output format (v1 or v2).
	TargetFormat format.FormatVersion

	// Overwrite allows overwriting existing entries in the target archive.
	Overwrite bool

	// ProgressCallback is called during transfer to report progress.
	ProgressCallback func(path string, bytes uint64)
}

// WalkFunc is called for each entry encountered during WalkTree.
// entry is the archive entry. content is the file data (nil for non-files).
// Return nil to continue, or an error to stop.
type WalkFunc func(entry *pxar.Entry, content []byte) error