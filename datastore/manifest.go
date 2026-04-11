package datastore

import (
	"encoding/json"
	"fmt"
)

// FileInfo describes a file in a backup manifest.
type FileInfo struct {
	Filename string `json:"filename"`
	CryptMode string `json:"crypt-mode,omitempty"`
	Size     uint64 `json:"size"`
	CSum     string `json:"csum"`
}

// Manifest represents a backup manifest (index.json).
type Manifest struct {
	BackupType string    `json:"backup-type"`
	BackupID   string    `json:"backup-id"`
	BackupTime int64     `json:"backup-time"`
	Files      []FileInfo `json:"files"`
	Signature  string    `json:"signature,omitempty"`
}

// Marshal serializes the manifest to JSON.
func (m *Manifest) Marshal() ([]byte, error) {
	return json.MarshalIndent(m, "", "  ")
}

// UnmarshalManifest parses a manifest from JSON.
func UnmarshalManifest(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("unmarshal manifest: %w", err)
	}
	return &m, nil
}

// VerifyFile checks that a file's checksum and size match the manifest.
func (m *Manifest) VerifyFile(filename, csum string, size uint64) error {
	for _, f := range m.Files {
		if f.Filename == filename {
			if f.CSum != csum {
				return fmt.Errorf("checksum mismatch for %s: got %s, want %s", filename, csum, f.CSum)
			}
			if f.Size != size {
				return fmt.Errorf("size mismatch for %s: got %d, want %d", filename, size, f.Size)
			}
			return nil
		}
	}
	return fmt.Errorf("file %s not found in manifest", filename)
}

// AddFile adds a file entry to the manifest.
func (m *Manifest) AddFile(filename string, size uint64, csum string) {
	m.Files = append(m.Files, FileInfo{
		Filename: filename,
		Size:     size,
		CSum:     csum,
	})
}
