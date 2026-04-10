package decoder

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
	pxar "github.com/pbs-plus/pxar"
)

func TestDecodeSimpleFile(t *testing.T) {
	archive := encodeSimpleArchive(t, "test.txt", []byte("hello world"))
	dec := NewDecoder(bytes.NewReader(archive), nil)
	entries := collectEntries(t, dec)

	if len(entries) < 2 {
		t.Fatalf("expected at least 2 entries, got %d", len(entries))
	}

	if !entries[0].IsDir() {
		t.Errorf("first entry kind = %v, want directory", entries[0].Kind)
	}

	var fileEntry *pxar.Entry
	for i := range entries {
		if entries[i].FileName() == "test.txt" {
			fileEntry = &entries[i]
			break
		}
	}
	if fileEntry == nil {
		t.Fatal("file test.txt not found")
	}
	if !fileEntry.IsRegularFile() {
		t.Errorf("test.txt kind = %v, want file", fileEntry.Kind)
	}
	if fileEntry.FileSize != 11 {
		t.Errorf("test.txt size = %d, want 11", fileEntry.FileSize)
	}
}

func TestDecodeMultipleFiles(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMetadata(0o755), nil)
	meta := fileMetadata(0o644, 1000, 1000)
	enc.AddFile(meta, "file1.txt", []byte("content1"))
	enc.AddFile(meta, "file2.txt", []byte("content2"))
	enc.Close()

	dec := NewDecoder(bytes.NewReader(buf.Bytes()), nil)
	entries := collectEntries(t, dec)

	names := make(map[string]bool)
	for _, e := range entries {
		names[e.FileName()] = true
	}
	if !names["file1.txt"] {
		t.Error("file1.txt not found")
	}
	if !names["file2.txt"] {
		t.Error("file2.txt not found")
	}
}

func TestDecodeNestedDirectories(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMetadata(0o755), nil)
	enc.CreateDirectory("subdir", dirMetadata(0o755))
	enc.AddFile(fileMetadata(0o644, 1000, 1000), "nested.txt", []byte("nested"))
	enc.Finish()
	enc.Close()

	dec := NewDecoder(bytes.NewReader(buf.Bytes()), nil)
	entries := collectEntries(t, dec)

	paths := make(map[string]bool)
	for _, e := range entries {
		paths[e.Path] = true
	}
	if !paths["/subdir"] {
		t.Errorf("subdir not found in paths: %v", paths)
	}
}

func TestDecodeSymlink(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMetadata(0o755), nil)
	enc.AddSymlink(symlinkMetadata(0o777, 1000, 1000), "link", "/usr/bin/python3")
	enc.Close()

	dec := NewDecoder(bytes.NewReader(buf.Bytes()), nil)
	entries := collectEntries(t, dec)

	var linkEntry *pxar.Entry
	for i := range entries {
		if entries[i].FileName() == "link" {
			linkEntry = &entries[i]
			break
		}
	}
	if linkEntry == nil {
		t.Fatal("link not found")
	}
	if !linkEntry.IsSymlink() {
		t.Errorf("link kind = %v, want symlink", linkEntry.Kind)
	}
	if linkEntry.LinkTarget != "/usr/bin/python3" {
		t.Errorf("link target = %q, want %q", linkEntry.LinkTarget, "/usr/bin/python3")
	}
}

func TestDecodeHardlink(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMetadata(0o755), nil)
	offset, _ := enc.AddFile(fileMetadata(0o644, 1000, 1000), "original.txt", []byte("content"))
	enc.AddHardlink("link.txt", "original.txt", offset)
	enc.Close()

	dec := NewDecoder(bytes.NewReader(buf.Bytes()), nil)
	entries := collectEntries(t, dec)

	var linkEntry *pxar.Entry
	for i := range entries {
		if entries[i].FileName() == "link.txt" {
			linkEntry = &entries[i]
			break
		}
	}
	if linkEntry == nil {
		t.Fatal("link.txt not found")
	}
	if !linkEntry.IsHardlink() {
		t.Errorf("link.txt kind = %v, want hardlink", linkEntry.Kind)
	}
}

func TestDecodeDevice(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMetadata(0o755), nil)
	enc.AddDevice(deviceMetadata(format.ModeIFCHR|0o644, 0, 0), "null", format.Device{Major: 1, Minor: 3})
	enc.Close()

	dec := NewDecoder(bytes.NewReader(buf.Bytes()), nil)
	entries := collectEntries(t, dec)

	for _, e := range entries {
		if e.FileName() == "null" {
			if !e.IsDevice() {
				t.Errorf("null kind = %v, want device", e.Kind)
			}
			if e.DeviceInfo.Major != 1 || e.DeviceInfo.Minor != 3 {
				t.Errorf("device = %v, want Major=1, Minor=3", e.DeviceInfo)
			}
		}
	}
}

func TestDecodeFIFO(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMetadata(0o755), nil)
	enc.AddFIFO(fifoMetadata(0o644, 1000, 1000), "pipe")
	enc.Close()

	dec := NewDecoder(bytes.NewReader(buf.Bytes()), nil)
	entries := collectEntries(t, dec)

	found := false
	for _, e := range entries {
		if e.FileName() == "pipe" && e.IsFifo() {
			found = true
		}
	}
	if !found {
		t.Error("FIFO entry not found or wrong kind")
	}
}

func TestDecodeSocket(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMetadata(0o755), nil)
	enc.AddSocket(socketMetadata(0o644, 1000, 1000), "sock")
	enc.Close()

	dec := NewDecoder(bytes.NewReader(buf.Bytes()), nil)
	entries := collectEntries(t, dec)

	found := false
	for _, e := range entries {
		if e.FileName() == "sock" && e.IsSocket() {
			found = true
		}
	}
	if !found {
		t.Error("socket entry not found or wrong kind")
	}
}

func TestDecodeFileContents(t *testing.T) {
	content := []byte("This is file content for testing.")
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMetadata(0o755), nil)
	enc.AddFile(fileMetadata(0o644, 1000, 1000), "data.bin", content)
	enc.Close()

	dec := NewDecoder(bytes.NewReader(buf.Bytes()), nil)

	for {
		entry, err := dec.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if entry.IsRegularFile() && entry.FileName() == "data.bin" {
			reader := dec.Contents()
			if reader == nil {
				t.Fatal("Contents() returned nil for file entry")
			}
			got, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("ReadAll contents: %v", err)
			}
			if string(got) != string(content) {
				t.Errorf("content = %q, want %q", got, content)
			}
		}
	}
}

func TestDecodeMetadata(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMetadata(0o755), nil)
	enc.AddFile(fileMetadata(0o644, 1000, 1000), "test.txt", []byte("hello"))
	enc.Close()

	dec := NewDecoder(bytes.NewReader(buf.Bytes()), nil)

	for {
		entry, err := dec.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if entry.FileName() == "test.txt" {
			if entry.Metadata.Stat.UID != 1000 {
				t.Errorf("UID = %d, want 1000", entry.Metadata.Stat.UID)
			}
			if entry.Metadata.Stat.GID != 1000 {
				t.Errorf("GID = %d, want 1000", entry.Metadata.Stat.GID)
			}
			if entry.Metadata.Stat.FileMode() != 0o644 {
				t.Errorf("FileMode = %o, want 0o644", entry.Metadata.Stat.FileMode())
			}
		}
	}
}

func TestDecodeXAttrs(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMetadata(0o755), nil)
	meta := fileMetadata(0o644, 1000, 1000)
	meta.XAttrs = []format.XAttr{
		format.NewXAttr([]byte("user.test"), []byte("value")),
	}
	enc.AddFile(meta, "xattr.txt", []byte("content"))
	enc.Close()

	dec := NewDecoder(bytes.NewReader(buf.Bytes()), nil)

	for {
		entry, err := dec.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if entry.FileName() == "xattr.txt" {
			if len(entry.Metadata.XAttrs) != 1 {
				t.Fatalf("expected 1 xattr, got %d", len(entry.Metadata.XAttrs))
			}
			if string(entry.Metadata.XAttrs[0].Name()) != "user.test" {
				t.Errorf("xattr name = %q, want %q", entry.Metadata.XAttrs[0].Name(), "user.test")
			}
			if string(entry.Metadata.XAttrs[0].Value()) != "value" {
				t.Errorf("xattr value = %q, want %q", entry.Metadata.XAttrs[0].Value(), "value")
			}
		}
	}
}

func TestRoundTripEncodeDecode(t *testing.T) {
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMetadata(0o755), nil)

	enc.AddFile(fileMetadata(0o644, 1000, 1000), "regular.txt", []byte("file content"))
	enc.AddSymlink(symlinkMetadata(0o777, 0, 0), "symlink", "/target")
	enc.AddDevice(deviceMetadata(format.ModeIFCHR|0o644, 0, 0), "chardev", format.Device{Major: 1, Minor: 3})
	enc.AddFIFO(fifoMetadata(0o644, 1000, 1000), "fifo")
	enc.AddSocket(socketMetadata(0o644, 1000, 1000), "socket")
	enc.CreateDirectory("subdir", dirMetadata(0o755))
	enc.AddFile(fileMetadata(0o600, 0, 0), "secret.txt", []byte("secret"))
	enc.Finish()
	enc.Close()

	dec := NewDecoder(bytes.NewReader(buf.Bytes()), nil)

	kinds := make(map[pxar.EntryKind]int)
	for {
		entry, err := dec.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		kinds[entry.Kind]++

		if entry.Kind == pxar.KindFile {
			if entry.FileSize == 0 {
				t.Errorf("file %q has zero size", entry.Path)
			}
		}
	}

	if kinds[pxar.KindDirectory] < 2 {
		t.Errorf("expected at least 2 directories, got %d", kinds[pxar.KindDirectory])
	}
	if kinds[pxar.KindFile] != 2 {
		t.Errorf("expected 2 files, got %d", kinds[pxar.KindFile])
	}
	if kinds[pxar.KindSymlink] != 1 {
		t.Errorf("expected 1 symlink, got %d", kinds[pxar.KindSymlink])
	}
	if kinds[pxar.KindDevice] != 1 {
		t.Errorf("expected 1 device, got %d", kinds[pxar.KindDevice])
	}
	if kinds[pxar.KindFifo] != 1 {
		t.Errorf("expected 1 fifo, got %d", kinds[pxar.KindFifo])
	}
	if kinds[pxar.KindSocket] != 1 {
		t.Errorf("expected 1 socket, got %d", kinds[pxar.KindSocket])
	}
}

func TestDecodeV2Archive(t *testing.T) {
	var archiveBuf bytes.Buffer
	var payloadBuf bytes.Buffer
	enc := encoder.NewEncoder(&archiveBuf, &payloadBuf, dirMetadata(0o755), nil)
	enc.AddFile(fileMetadata(0o644, 1000, 1000), "file.txt", []byte("content"))
	enc.Close()

	dec := NewDecoder(bytes.NewReader(archiveBuf.Bytes()), bytes.NewReader(payloadBuf.Bytes()))
	entries := collectEntries(t, dec)

	if len(entries) < 2 {
		t.Fatalf("expected at least 2 entries, got %d", len(entries))
	}

	// First entry should be version
	if entries[0].Kind != pxar.KindVersion {
		t.Errorf("first entry kind = %v, want version", entries[0].Kind)
	}
}

func TestDecodePrelude(t *testing.T) {
	var archiveBuf bytes.Buffer
	var payloadBuf bytes.Buffer
	prelude := []byte("test prelude")
	enc := encoder.NewEncoder(&archiveBuf, &payloadBuf, dirMetadata(0o755), prelude)
	enc.AddFile(fileMetadata(0o644, 1000, 1000), "file.txt", []byte("content"))
	enc.Close()

	dec := NewDecoder(bytes.NewReader(archiveBuf.Bytes()), bytes.NewReader(payloadBuf.Bytes()))
	entries := collectEntries(t, dec)

	// Should have: version, prelude, root dir, file
	if len(entries) < 3 {
		t.Fatalf("expected at least 3 entries, got %d", len(entries))
	}

	if entries[0].Kind != pxar.KindVersion {
		t.Errorf("first entry = %v, want version", entries[0].Kind)
	}
	if entries[1].Kind != pxar.KindPrelude {
		t.Errorf("second entry = %v, want prelude", entries[1].Kind)
	}
}

// Verify binary compatibility: manually construct a minimal V1 archive and decode it
func TestDecodeManualV1Archive(t *testing.T) {
	var buf bytes.Buffer
	w := binary.Write

	// Root directory ENTRY
	stat := make([]byte, 40)
	binary.LittleEndian.PutUint64(stat[0:], format.ModeIFDIR|0o755)
	binary.LittleEndian.PutUint64(stat[8:], 0) // flags
	binary.LittleEndian.PutUint32(stat[16:], 0) // uid
	binary.LittleEndian.PutUint32(stat[20:], 0) // gid
	binary.LittleEndian.PutUint64(stat[24:], uint64(1430487000)) // mtime secs
	binary.LittleEndian.PutUint32(stat[32:], 0) // mtime nanos

	w(&buf, binary.LittleEndian, &format.Header{Type: format.PXAREntry, Size: uint64(16 + len(stat))})
	buf.Write(stat)

	// FILENAME "test.txt"
	filename := append([]byte("test.txt"), 0)
	w(&buf, binary.LittleEndian, &format.Header{Type: format.PXARFilename, Size: uint64(16 + len(filename))})
	buf.Write(filename)

	// File ENTRY
	fileStat := make([]byte, 40)
	binary.LittleEndian.PutUint64(fileStat[0:], format.ModeIFREG|0o644)
	binary.LittleEndian.PutUint64(fileStat[8:], 0)
	binary.LittleEndian.PutUint32(fileStat[16:], 1000)
	binary.LittleEndian.PutUint32(fileStat[20:], 1000)
	binary.LittleEndian.PutUint64(fileStat[24:], uint64(1430487000))
	binary.LittleEndian.PutUint32(fileStat[32:], 0)

	w(&buf, binary.LittleEndian, &format.Header{Type: format.PXAREntry, Size: uint64(16 + len(fileStat))})
	buf.Write(fileStat)

	// PAYLOAD
	content := []byte("hello")
	w(&buf, binary.LittleEndian, &format.Header{Type: format.PXARPayload, Size: uint64(16 + len(content))})
	buf.Write(content)

	// GOODBYE (empty: just tail marker)
	tailItem := make([]byte, 24)
	binary.LittleEndian.PutUint64(tailItem[0:], format.PXARGoodbyeTailMarker)
	binary.LittleEndian.PutUint64(tailItem[8:], 0) // offset to root entry
	binary.LittleEndian.PutUint64(tailItem[16:], uint64(16+24)) // goodbye size
	w(&buf, binary.LittleEndian, &format.Header{Type: format.PXARGoodbye, Size: uint64(16 + len(tailItem))})
	buf.Write(tailItem)

	// Root GOODBYE (same)
	w(&buf, binary.LittleEndian, &format.Header{Type: format.PXARGoodbye, Size: uint64(16 + len(tailItem))})
	buf.Write(tailItem)

	dec := NewDecoder(bytes.NewReader(buf.Bytes()), nil)
	entries := collectEntries(t, dec)

	if len(entries) < 2 {
		t.Fatalf("expected at least 2 entries, got %d", len(entries))
	}

	found := false
	for _, e := range entries {
		if e.FileName() == "test.txt" && e.IsRegularFile() {
			found = true
		}
	}
	if !found {
		t.Error("test.txt not found in decoded entries")
	}
}

// Helpers

func encodeSimpleArchive(t *testing.T, name string, content []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMetadata(0o755), nil)
	_, err := enc.AddFile(fileMetadata(0o644, 1000, 1000), name, content)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	enc.Close()
	return buf.Bytes()
}

func collectEntries(t *testing.T, dec *Decoder) []pxar.Entry {
	t.Helper()
	var entries []pxar.Entry
	for {
		entry, err := dec.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		entries = append(entries, *entry)
	}
	return entries
}

func dirMetadata(mode uint64) *pxar.Metadata {
	ts := format.StatxTimestampFromDurationSinceEpoch(1430487000 * time.Second)
	return &pxar.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFDIR | mode,
			Mtime: ts,
		},
	}
}

func fileMetadata(mode uint64, uid, gid uint32) *pxar.Metadata {
	ts := format.StatxTimestampFromDurationSinceEpoch(1430487000 * time.Second)
	return &pxar.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFREG | mode,
			UID:   uid,
			GID:   gid,
			Mtime: ts,
		},
	}
}

func symlinkMetadata(mode uint64, uid, gid uint32) *pxar.Metadata {
	ts := format.StatxTimestampFromDurationSinceEpoch(1430487000 * time.Second)
	return &pxar.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFLNK | mode,
			UID:   uid,
			GID:   gid,
			Mtime: ts,
		},
	}
}

func deviceMetadata(mode uint64, uid, gid uint32) *pxar.Metadata {
	ts := format.StatxTimestampFromDurationSinceEpoch(1430487000 * time.Second)
	return &pxar.Metadata{
		Stat: format.Stat{
			Mode:  mode,
			UID:   uid,
			GID:   gid,
			Mtime: ts,
		},
	}
}

func fifoMetadata(mode uint64, uid, gid uint32) *pxar.Metadata {
	ts := format.StatxTimestampFromDurationSinceEpoch(1430487000 * time.Second)
	return &pxar.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFIFO | mode,
			UID:   uid,
			GID:   gid,
			Mtime: ts,
		},
	}
}

func socketMetadata(mode uint64, uid, gid uint32) *pxar.Metadata {
	ts := format.StatxTimestampFromDurationSinceEpoch(1430487000 * time.Second)
	return &pxar.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFSOCK | mode,
			UID:   uid,
			GID:   gid,
			Mtime: ts,
		},
	}
}
