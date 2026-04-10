package encoder

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/sonroyaalmerol/pxar/format"
	pxar "github.com/sonroyaalmerol/pxar"
)

func TestEncodeSimpleFile(t *testing.T) {
	var buf bytes.Buffer
	metadata := dirMetadata(0o755)
	enc := NewEncoder(&buf, nil, metadata, nil)
	if enc == nil {
		t.Fatal("NewEncoder returned nil")
	}

	fileMeta := fileMetadata(0o644, 1000, 1000)
	_, err := enc.AddFile(fileMeta, "test.txt", []byte("hello world"))
	if err != nil {
		t.Fatalf("AddFile: %v", err)
	}

	err = enc.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}

	data := buf.Bytes()
	if len(data) < format.HeaderSize {
		t.Fatalf("archive too short: %d bytes", len(data))
	}

	htype := binary.LittleEndian.Uint64(data[0:8])
	if htype != format.PXAREntry {
		t.Errorf("first header type = %x, want ENTRY %x", htype, format.PXAREntry)
	}
}

func TestEncodeDirectoryWithFiles(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, nil, dirMetadata(0o755), nil)

	fileMeta := fileMetadata(0o644, 1000, 1000)
	_, err := enc.AddFile(fileMeta, "file1.txt", []byte("content1"))
	if err != nil {
		t.Fatalf("AddFile file1: %v", err)
	}
	_, err = enc.AddFile(fileMeta, "file2.txt", []byte("content2"))
	if err != nil {
		t.Fatalf("AddFile file2: %v", err)
	}

	err = enc.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}

	data := buf.Bytes()
	found := false
	for i := 0; i+format.HeaderSize <= len(data); {
		htype := binary.LittleEndian.Uint64(data[i : i+8])
		if htype == format.PXARGoodbye {
			found = true
			break
		}
		hsize := binary.LittleEndian.Uint64(data[i+8 : i+16])
		if hsize < format.HeaderSize {
			t.Fatalf("corrupt archive: header size %d at offset %d", hsize, i)
		}
		i += int(hsize)
	}
	if !found {
		t.Error("archive does not contain a GOODBYE entry")
	}
}

func TestEncodeNestedDirectories(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, nil, dirMetadata(0o755), nil)

	err := enc.CreateDirectory("subdir", dirMetadata(0o755))
	if err != nil {
		t.Fatalf("CreateDirectory: %v", err)
	}

	fileMeta := fileMetadata(0o644, 1000, 1000)
	_, err = enc.AddFile(fileMeta, "nested.txt", []byte("nested content"))
	if err != nil {
		t.Fatalf("AddFile: %v", err)
	}

	err = enc.Finish()
	if err != nil {
		t.Fatalf("Finish subdir: %v", err)
	}

	err = enc.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}

	data := buf.Bytes()
	if len(data) == 0 {
		t.Fatal("archive is empty")
	}

	// Verify there are 2 GOODBYE entries (root + subdir)
	goodbyeCount := 0
	for i := 0; i+format.HeaderSize <= len(data); {
		htype := binary.LittleEndian.Uint64(data[i : i+8])
		hsize := binary.LittleEndian.Uint64(data[i+8 : i+16])
		if hsize < format.HeaderSize {
			break
		}
		if htype == format.PXARGoodbye {
			goodbyeCount++
		}
		i += int(hsize)
	}
	if goodbyeCount != 2 {
		t.Errorf("expected 2 GOODBYE entries, got %d", goodbyeCount)
	}
}

func TestEncodeSymlink(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, nil, dirMetadata(0o755), nil)

	linkMeta := symlinkMetadata(0o777, 1000, 1000)
	err := enc.AddSymlink(linkMeta, "link", "/usr/bin/python3")
	if err != nil {
		t.Fatalf("AddSymlink: %v", err)
	}

	err = enc.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestEncodeHardlink(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, nil, dirMetadata(0o755), nil)

	fileMeta := fileMetadata(0o644, 1000, 1000)
	offset, err := enc.AddFile(fileMeta, "original.txt", []byte("content"))
	if err != nil {
		t.Fatalf("AddFile: %v", err)
	}

	err = enc.AddHardlink("link.txt", "original.txt", offset)
	if err != nil {
		t.Fatalf("AddHardlink: %v", err)
	}

	err = enc.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestEncodeDevice(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, nil, dirMetadata(0o755), nil)

	devMeta := deviceMetadata(format.ModeIFCHR|0o644, 0, 0)
	err := enc.AddDevice(devMeta, "null", format.Device{Major: 1, Minor: 3})
	if err != nil {
		t.Fatalf("AddDevice: %v", err)
	}

	err = enc.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestEncodeFIFO(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, nil, dirMetadata(0o755), nil)

	fifoMeta := fifoMetadata(0o644, 1000, 1000)
	err := enc.AddFIFO(fifoMeta, "pipe")
	if err != nil {
		t.Fatalf("AddFIFO: %v", err)
	}

	err = enc.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestEncodeSocket(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, nil, dirMetadata(0o755), nil)

	sockMeta := socketMetadata(0o644, 1000, 1000)
	err := enc.AddSocket(sockMeta, "sock")
	if err != nil {
		t.Fatalf("AddSocket: %v", err)
	}

	err = enc.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestEncodeXAttrs(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, nil, dirMetadata(0o755), nil)

	fileMeta := fileMetadata(0o644, 1000, 1000)
	fileMeta.XAttrs = []format.XAttr{
		format.NewXAttr([]byte("user.test"), []byte("value")),
	}
	_, err := enc.AddFile(fileMeta, "xattr.txt", []byte("content"))
	if err != nil {
		t.Fatalf("AddFile with xattr: %v", err)
	}

	err = enc.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Verify XATTR header exists
	data := buf.Bytes()
	found := false
	for i := 0; i+format.HeaderSize <= len(data); {
		htype := binary.LittleEndian.Uint64(data[i : i+8])
		hsize := binary.LittleEndian.Uint64(data[i+8 : i+16])
		if hsize < format.HeaderSize {
			break
		}
		if htype == format.PXARXAttr {
			found = true
			break
		}
		i += int(hsize)
	}
	if !found {
		t.Error("archive does not contain XATTR entry")
	}
}

func TestEncodeSplitArchive(t *testing.T) {
	var archiveBuf bytes.Buffer
	var payloadBuf bytes.Buffer

	enc := NewEncoder(&archiveBuf, &payloadBuf, dirMetadata(0o755), nil)

	fileMeta := fileMetadata(0o644, 1000, 1000)
	content := []byte("payload content")
	_, err := enc.AddFile(fileMeta, "file.txt", content)
	if err != nil {
		t.Fatalf("AddFile: %v", err)
	}

	err = enc.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}

	payloadData := payloadBuf.Bytes()
	if len(payloadData) < format.HeaderSize {
		t.Fatalf("payload too short: %d bytes", len(payloadData))
	}
	htype := binary.LittleEndian.Uint64(payloadData[0:8])
	if htype != format.PXARPayloadStartMarker {
		t.Errorf("payload start marker = %x, want %x", htype, format.PXARPayloadStartMarker)
	}

	// Archive should contain FORMAT_VERSION for v2
	archiveData := archiveBuf.Bytes()
	htype = binary.LittleEndian.Uint64(archiveData[0:8])
	if htype != format.PXARFormatVersion {
		t.Errorf("archive first header = %x, want FORMAT_VERSION %x", htype, format.PXARFormatVersion)
	}
}

func TestEncodeWithPrelude(t *testing.T) {
	var archiveBuf bytes.Buffer
	var payloadBuf bytes.Buffer
	prelude := []byte("test prelude data")
	enc := NewEncoder(&archiveBuf, &payloadBuf, dirMetadata(0o755), prelude)

	fileMeta := fileMetadata(0o644, 1000, 1000)
	_, err := enc.AddFile(fileMeta, "test.txt", []byte("hello"))
	if err != nil {
		t.Fatalf("AddFile: %v", err)
	}

	err = enc.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}

	data := archiveBuf.Bytes()
	htype := binary.LittleEndian.Uint64(data[0:8])
	if htype != format.PXARFormatVersion {
		t.Errorf("first header = %x, want FORMAT_VERSION %x", htype, format.PXARFormatVersion)
	}

	// Second item should be PRELUDE
	hsize := binary.LittleEndian.Uint64(data[8:16])
	offset := int(hsize)
	htype = binary.LittleEndian.Uint64(data[offset : offset+8])
	if htype != format.PXARPrelude {
		t.Errorf("second header = %x, want PRELUDE %x", htype, format.PXARPrelude)
	}
}

func TestCreateFileStreaming(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, nil, dirMetadata(0o755), nil)

	fileMeta := fileMetadata(0o644, 1000, 1000)
	fw, err := enc.CreateFile(fileMeta, "stream.txt", 12)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}

	n, err := fw.Write([]byte("hello "))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != 6 {
		t.Errorf("Write returned %d, want 6", n)
	}

	err = fw.WriteAll([]byte("world!"))
	if err != nil {
		t.Fatalf("WriteAll: %v", err)
	}

	err = fw.Close()
	if err != nil {
		t.Fatalf("FileWriter.Close: %v", err)
	}

	err = enc.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}

	data := buf.Bytes()
	if len(data) == 0 {
		t.Fatal("archive is empty")
	}
}

// Helpers

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
