package datastore

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// BackupType identifies the kind of backup.
type BackupType int

const (
	BackupVM   BackupType = iota
	BackupCT
	BackupHost
)

func (bt BackupType) String() string {
	switch bt {
	case BackupVM:
		return "vm"
	case BackupCT:
		return "ct"
	case BackupHost:
		return "host"
	default:
		return "unknown"
	}
}

// ParseBackupType parses a backup type string.
func ParseBackupType(s string) (BackupType, error) {
	switch s {
	case "vm":
		return BackupVM, nil
	case "ct":
		return BackupCT, nil
	case "host":
		return BackupHost, nil
	default:
		return 0, fmt.Errorf("unknown backup type: %s", s)
	}
}

// BackupGroup represents a collection of backup snapshots (e.g., vm/100).
type BackupGroup struct {
	Type BackupType
	ID   string
	Base string // base directory (datastore root)
}

// Path returns the relative path for this group (e.g., "vm/100").
func (g BackupGroup) Path() string {
	return filepath.Join(g.Type.String(), g.ID)
}

// FullPath returns the absolute path under the base directory.
func (g BackupGroup) FullPath() string {
	return filepath.Join(g.Base, g.Path())
}

// ListSnapshots returns all backup snapshots in this group.
func (g BackupGroup) ListSnapshots() ([]BackupDir, error) {
	entries, err := os.ReadDir(g.FullPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var snapshots []BackupDir
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		ts, err := time.Parse(time.RFC3339, e.Name())
		if err != nil {
			continue // skip non-timestamp dirs
		}
		snapshots = append(snapshots, BackupDir{
			Group:     g,
			Timestamp: ts,
		})
	}
	return snapshots, nil
}

// Destroy removes the backup group directory.
func (g BackupGroup) Destroy() error {
	return os.RemoveAll(g.FullPath())
}

// BackupDir represents a single backup snapshot.
type BackupDir struct {
	Group     BackupGroup
	Timestamp time.Time
}

// Path returns the relative path (e.g., "vm/100/2023-11-14T22:13:20Z").
func (d BackupDir) Path() string {
	return filepath.Join(d.Group.Path(), d.Timestamp.UTC().Format(time.RFC3339))
}

// FullPath returns the absolute path under the base directory.
func (d BackupDir) FullPath() string {
	return filepath.Join(d.Group.Base, d.Path())
}

// Info returns detailed information about this backup snapshot.
func (d BackupDir) Info() (*BackupInfo, error) {
	dir := d.FullPath()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var files []string
	protected := false
	for _, e := range entries {
		name := e.Name()
		if name == ".protected" {
			protected = true
			continue
		}
		if name == "." || name == ".." {
			continue
		}
		files = append(files, name)
	}

	return &BackupInfo{
		Dir:       d,
		Files:     files,
		Protected: protected,
	}, nil
}

// Create creates the snapshot directory on disk.
func (d BackupDir) Create() error {
	return os.MkdirAll(d.FullPath(), 0o755)
}

// BackupInfo holds metadata about a backup snapshot.
type BackupInfo struct {
	Dir       BackupDir
	Files     []string
	Protected bool
}

// Protect marks the backup as protected by creating a .protected file.
func (info *BackupInfo) Protect() error {
	path := filepath.Join(info.Dir.FullPath(), ".protected")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		return err
	}
	info.Protected = true
	return nil
}

// Unprotect removes the protection marker.
func (info *BackupInfo) Unprotect() error {
	path := filepath.Join(info.Dir.FullPath(), ".protected")
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	info.Protected = false
	return nil
}

// ListBackupGroups returns all backup groups in the datastore base directory.
func ListBackupGroups(base string) ([]BackupGroup, error) {
	var groups []BackupGroup
	entries, err := os.ReadDir(base)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		bt, err := ParseBackupType(e.Name())
		if err != nil {
			continue
		}
		typeDir := filepath.Join(base, e.Name())
		ids, err := os.ReadDir(typeDir)
		if err != nil {
			continue
		}
		for _, id := range ids {
			if id.IsDir() {
				groups = append(groups, BackupGroup{
					Type: bt,
					ID:   id.Name(),
					Base: base,
				})
			}
		}
	}
	return groups, nil
}
