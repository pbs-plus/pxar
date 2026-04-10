package datastore

import (
	"testing"
)

func TestManifestRoundTrip(t *testing.T) {
	m := &Manifest{
		BackupType: "vm",
		BackupID:   "100",
		BackupTime: 1700000000,
		Files: []FileInfo{
			{
				Filename: "drive.qemu.fidx",
				Size:     10485760,
				CSum:     "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
			},
			{
				Filename: "config.blob",
				Size:     256,
				CSum:     "f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6f1e2",
			},
		},
	}

	data, err := m.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	got, err := UnmarshalManifest(data)
	if err != nil {
		t.Fatal(err)
	}

	if got.BackupType != m.BackupType {
		t.Errorf("type = %q, want %q", got.BackupType, m.BackupType)
	}
	if got.BackupID != m.BackupID {
		t.Errorf("id = %q, want %q", got.BackupID, m.BackupID)
	}
	if got.BackupTime != m.BackupTime {
		t.Errorf("time = %d, want %d", got.BackupTime, m.BackupTime)
	}
	if len(got.Files) != len(m.Files) {
		t.Fatalf("files = %d, want %d", len(got.Files), len(m.Files))
	}
	for i, f := range got.Files {
		if f.Filename != m.Files[i].Filename {
			t.Errorf("file %d: name = %q, want %q", i, f.Filename, m.Files[i].Filename)
		}
		if f.Size != m.Files[i].Size {
			t.Errorf("file %d: size = %d, want %d", i, f.Size, m.Files[i].Size)
		}
		if f.CSum != m.Files[i].CSum {
			t.Errorf("file %d: csum mismatch", i)
		}
	}
}

func TestManifestVerifyFile(t *testing.T) {
	m := &Manifest{
		BackupType: "host",
		BackupID:   "myhost",
		BackupTime: 1700000000,
		Files: []FileInfo{
			{
				Filename: "root.pxar.didx",
				Size:     5000,
				CSum:     "aabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd",
			},
		},
	}

	if err := m.VerifyFile("root.pxar.didx", "aabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd", 5000); err != nil {
		t.Errorf("valid file should verify: %v", err)
	}

	if err := m.VerifyFile("root.pxar.didx", "wrong", 5000); err == nil {
		t.Error("wrong checksum should fail verification")
	}

	if err := m.VerifyFile("root.pxar.didx", "aabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd", 9999); err == nil {
		t.Error("wrong size should fail verification")
	}

	if err := m.VerifyFile("nonexistent.didx", "x", 0); err == nil {
		t.Error("missing file should fail verification")
	}
}

func TestManifestEmptyFiles(t *testing.T) {
	m := &Manifest{
		BackupType: "ct",
		BackupID:   "200",
		BackupTime: 1700000000,
	}

	data, err := m.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	got, err := UnmarshalManifest(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Files) != 0 {
		t.Errorf("files = %d, want 0", len(got.Files))
	}
}

func TestManifestUnmarshalInvalid(t *testing.T) {
	_, err := UnmarshalManifest([]byte("not json"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}
